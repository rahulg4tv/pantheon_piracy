# 01 — `dht_peer_count.py` (Code Reference)

> **Role:** The core DHT peer counter. For each tracked info_hash it runs an iterative
> Kademlia `get_peers` walk, collects peer IPs, geolocates them, and writes distinct
> IPs to `hashes_v2.db.peers`. Runs as **8 always-on systemd workers** (4 active + 4
> dormant) plus a cron `--new-only` tier. The largest file in the project (~2,600 lines).
>
> Function-level walkthrough for review. The DHT layer is the *weak* signal (undercounts
> 3–39×) but adds unique coverage (trackerless swarms + long tail). See `00_OVERVIEW.md`.

---

## Where it runs
- **systemd (always-on):** `dht-peer-count{,-w1,-w2,-w3}` (active tier) + `dht-dormant{,-w1,-w2,-w3}` (dormant tier).
- **cron (`--new-only`):** 01:05 / 10:05 / 18:05 UTC — baselines just-discovered hashes.
- **Key args:** `--workers`, `--concurrency`, `--timeout`, `--loop`/`--loop-delay`, `--skip-dead-days`, `--active-only N` / `--dormant-only N` / `--new-only`, `--active-min-peers`, `--slice N/M`, `--worker-id`.
- **Tier examples:** active = `--concurrency 30 --loop-delay 60 --active-only 1 --active-min-peers 3 --slice N/7`; dormant = `--concurrency 50 --loop-delay 120 --dormant-only 3 --slice N/4`.

## Data flow
```
hashes_v2.db.hashes  →  load_hashes()  →  per-hash get_peers_by_country()  →  geolocate
        ↑ (work list, tiered/sliced)                                          ↓
        └──────────  peers table  ←  _upsert_peers_threadsafe()  ←  ip_country
   also: /data/peer_counts/<date>_w<N>.csv (per-pass CSV)   ·   announce_peer queue
```

---

## Key in-memory structures (the "three tables" of a crawler)
- **Node pool** — the routing table. IPv4 kept in a **NumPy structured array** `_POOL_ARR` (`nid`, `ip_int`, `port`, `verified`, `responses`, `timeouts`) + `_POOL_IDX` (nid→row) for O(1) ops; IPv6 in a dict `_NODE_POOL_V6`. Persisted to `node_pool*.json` per tier. This is what makes XOR-closest selection fast at 200K nodes.
- **Hash→node cache** (`_HASH_NODE_CACHE`) — the best responder nodes for each hash from last pass, reused as warm leads (persisted to `hash_node_cache_*.json`).
- **Transaction table** — `DHTTransport._waiters` (tid → asyncio.Queue): every outbound `get_peers` registers a tid; replies are matched back to it (unexpected tid = ignored/bad).
- **`_KNOWN_HASHES`** — set of all tracked info_hashes for O(1) `announce_peer` matching.

---

## Functions (grouped)

### Encoding / utility helpers
`_ip4_to_int`/`_int_to_ip4` (120/127) — IPv4 ↔ int for the NumPy pool. `_bencode`/`bdecode` (581/617) — minimal bencode for KRPC messages. `_compact_to_peers[_v6]` (771/787), `_compact_to_nodes_full[_v6]` (808/820) — parse BitTorrent compact peer/node blobs. `_xor_dist` (702) — Kademlia XOR distance (the "closeness" metric).

### GeoIP
`_get_geo_reader` (628) — lazy singleton `geoip2` reader on `GeoLite2-Country.mmdb`. `ip_to_country(ip)` (641) — IP → ISO-2 country (the geolocation step), lru-cached.

### Token / auth (for `announce_peer`)
`_make_token`/`_verify_token` (193/199) — HMAC token tied to a peer IP, so we only record `announce_peer` hits we actually solicited.

### Node-pool management
`_pool_add` (273) — merge discovered nodes into `_POOL_ARR` (dedup, mark verified, cap size). `_pool_swap_remove` (228) / `_evict_bad_nodes` (244) — O(1) removal of nodes that only ever timed out. `load/save_node_pool[_v6]`, `load/save_hash_node_cache` — JSON persistence. **Reviewer note:** the numpy pool is read **without copying** during XOR-select (see `get_peers_by_country`), relying on asyncio's single-threaded yield points — a deliberate memory optimization (avoids GBs of copies at high concurrency).

### BEP-33 (swarm-size estimate)
`_estimate_bloom` (707), `_compute_bep33` (728) — decode the BFsd/BFpe bloom filters nodes return into seeder/leecher estimates. `_merge_socket_results` (736) — union peers + OR-merge bloom bytes across the N parallel sockets for one hash.

### `class DHTTransport` (842) — the UDP engine
asyncio `DatagramProtocol`. Holds `own_node_id`, the `_waiters` transaction table, and the `announce_queue`.
- `send_get_peers(addr, info_hash, node_id)` — build+send a `get_peers` query (with `scrape=1` for BEP-33, `want=[n4,n6]` for BEP-32), register a tid+Queue, return them.
- `datagram_received` — route replies to the waiting Queue by tid; handle inbound queries.
- **`announce_peer` handler (~922):** when a peer announces a hash in `_KNOWN_HASHES` with a valid token, push `(hash, peer_ip)` onto `announce_queue` — **passive peer capture** between active passes (enabled by the Sybil/neighbor trick below). ACKs every query per BEP-5.

### `get_peers_by_country(info_hash_hex, dht, timeout, rounds)` (976) — THE per-hash job
The iterative Kademlia walk (documented in detail in the learning guide / FLOW doc). In short:
1. Build targets: cached leaf nodes + XOR-closest pool nodes (vectorized NumPy `argpartition` on top-8 bytes) + bootstrap.
2. For `rounds` (default 8), query up to 64 nodes in parallel via inner `query_one`. **Sybil trick:** forge sender id = `target_id[:15] + 5 random` so the target stores us as a neighbour → future announces flow to us.
3. Parse replies: `values`/`values6` → peer IPs; `nodes`/`nodes6` → closer nodes (sorted by XOR for next round); `BFsd`/`BFpe` → bloom estimates; credit/penalize node health.
4. Cache 5 XOR-closest responders; geolocate all IPs in a bounded thread pool; return `(by_country, ip_country, bloom_bytes)`.

### DB write path
`init_peers_table` (1355) — schema + indexes. `upsert_peers(db, hash, ip_country, today)` (1375) — `INSERT … ON CONFLICT(hash,ip) DO UPDATE last_seen`; writes a `_queried_` sentinel row when zero peers (marks "scanned today"). `_get_thread_db` (1408) — thread-local connection, **`timeout=60` + `busy_timeout=60000` + `synchronous=NORMAL`** (§36). `_upsert_peers_threadsafe` (1424) — **the resilient writer:** retries on "database is locked" with backoff, and on persistent lock **logs+drops one idempotent upsert rather than crashing the worker** (§36).

### Work-list selection
`load_hashes(...)` (1463) — builds the per-pass work list from `hashes` with the tier filters: `--skip-dead-days` (exclude hashes with only a `_queried_` sentinel in last N days), `--active-only` (≥ `active_min_peers` real peers in last N days), `--dormant-only`, `--new-only`, and `--slice N/M` keyspace partitioning. **Reviewer note:** opens its connection with `timeout=60`+`busy_timeout` (§41 — the last unguarded reader, hardened).

### CSV output (async, non-blocking)
`_csv_writer_loop`/`csv_queue_put`/`csv_queue_flush` (1670–1722) — a background thread drains a queue to the per-pass CSV so DHT I/O never blocks on disk. `get_worker_csv_path` (1729) — per-worker file (`<date>_w<N>.csv`).

### `run(hashes, concurrency, timeout, …)` (1748) — one pass
Sets up N `DHTTransport` sockets, a semaphore(`concurrency`), the announce-drain task, then `process_one` per hash:
- **`process_one(row)` (~1858):** adaptive timeout by `seeders` (popular → up to 20s; dead → 6s); run `get_peers_by_country` across all sockets in parallel; `_merge_socket_results`; write IPs via the thread-safe upsert; emit per-country CSV rows (+ a BEP-33-only row if no IPs but bloom data exists).

### `main()` (2290) + the pass loop (~2399–2566)
Parses args, derives `--slice`/`--worker-id`, opens the main DB (`timeout=30`, `busy_timeout=15000`, WAL), selects the tier-specific node pool, then **loops**:
- `load_hashes` → if empty **sleep & continue** (the §34 guard — never `exit 0`, which would trip `Restart=always`→start-limit-hit); else `run(...)` the pass.
- **Between passes:** `PRAGMA wal_checkpoint(TRUNCATE)` (§36 — caps WAL growth; wrapped so a busy checkpoint can't break the loop), then sleep `loop_delay`.
`_run_worker`/`_merge_worker_pools`/`_merge_worker_csvs` (2221/2170/2206) — multi-worker spawn + pool/CSV merge for `--workers N` mode.

---

## Gotchas / invariants (for reviewers)
- **Never `exit 0` on an empty pass** in `--loop` mode → `Restart=always` + `StartLimitBurst` = permanent `start-limit-hit` (§34). The sleep-and-continue guard must stay.
- **Writer must never crash on a lock** — `_upsert_peers_threadsafe` drops one idempotent row instead (re-found next pass) (§36). All four reader/writer connections now carry `busy_timeout` (§36/§41).
- **WAL can still balloon** under 8 always-on readers despite the between-pass checkpoint → handled by `wal_maintenance.sh` (§48).
- **`_queried_` sentinel** (`ip='_queried_'`) means "scanned, no peers" — must be excluded everywhere downstream (it is, in `export_nbcu.py`).
- **NumPy pool read-without-copy** is safe only because asyncio is single-threaded; don't introduce real threads that mutate the pool mid-select.
- **Tiers are sliced** (`N/M` + worker-id) so the 8 workers don't duplicate work; active uses `/7`, dormant `/4`.

## Change history
`SESSION_CHANGES.md` §34 (empty-pass loop guard), §36 (lock-retry writer + WAL checkpoint + synchronous=NORMAL), §41 (reader busy_timeout), §24 (peer_count is a weak sample — ranking uses BEP-33).
