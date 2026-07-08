# 07 — `tracker_harvest_service.py` (+ `tracker_harvest.py`) — Code Reference

> **Role:** The **heavy lifter** — continuously announces to BitTorrent trackers to
> harvest the live swarm's peer IPs, writing distinct IPs to its **own** DB
> (`harvest_peers.db`). This is the *strong* signal that reaches real-world
> magnitude (the DHT undercounts 3–39×). Runs as the `tracker-harvest` systemd service.
>
> Two files: `tracker_harvest.py` = the protocol library (one infohash → peers);
> `tracker_harvest_service.py` = the long-running loop (worklist, geo, DB, retention).

---

## Where it runs
- **systemd (always-on):** `tracker-harvest.service`, env `MAX_HASHES=15000 ROUNDS=4 CONC=32 CYCLE_SLEEP=15`.
- **Manual lib test:** `python3 tracker_harvest.py <infohash_hex> …` (prints distinct IPs/peers per hash).
- **Config (env):** `MAX_HASHES` (hashes/cycle) · `ROUNDS` (announce rounds/tracker/hash) · `CONC` (concurrent hashes) · `CYCLE_SLEEP` · `RETENTION_DAYS` (4) · `ONESHOT=1` (one cycle, for tests).

## Data flow
```
hashes_v2.db.hashes  →(read-only worklist, seeders-first)→  harvest_infohash() per hash
                                                                   ↓ peers (ip:port)
                          GeoLite2 country_of(ip)  →  _upsert  →  harvest_peers.db.peers
```
**Why a separate DB (the key design decision):** the 8 always-on DHT workers hold
continuous read locks on `hashes_v2.db`, so its WAL can never fully checkpoint. If
the harvester wrote there too, its heavy volume would inflate that WAL unboundedly
(~1 GB/min measured). Its **own single-writer DB** can be TRUNCATE-checkpointed to
zero on a timer. `export_nbcu.py` ATTACHes both and unions distinct IPs.

---

## `tracker_harvest.py` — the protocol library

### `harvest_infohash(ih_hex, rounds=4) -> set[(ip,port)]`  (the per-hash job)
Fans out to **every tracker in parallel** (ThreadPool) and unions all returned peers:
`_udp_announce` for each UDP tracker + `_http_announce` for each HTTP tracker.

### `_udp_announce(host, port, info_hash, rounds)` — BEP-15
1. **Connect handshake:** send magic `_PROTOCOL_ID`, receive a `connection_id`.
2. **Announce:** packed request carrying the **info_hash**, a random `peer_id`, `left=random nonzero` (look like a leecher), `numwant=_NUMWANT` (ask for max), and a **fresh random `key` each round** so the tracker returns a *different swarm slice* each time. Parse compact peers from the reply.
3. Repeat `rounds`, stopping early when a round yields no new IPs (slice exhausted).

### `_http_announce(url, info_hash, rounds)`
Same idea over HTTP: GET the tracker URL with `info_hash` + `compact=1`; `_extract_bencode_peers` pulls the compact `5:peers<len>:<bytes>` blob without a full bencode parse; `_parse_compact_peers` → `{(ip,port)}`.

### Helpers
`_rand_peer_id` (random client id) · `_parse_compact_peers` (6-byte IPv4:port records) · `_public_only(peers)` — drops private/multicast IPs (keeps only globally-routable). The `if __name__` block makes it a standalone CLI for spot-checking a hash.

---

## `tracker_harvest_service.py` — the loop

### Module config (44–57)
`READ_DB` (shared DHT DB — **read-only**, worklist + seeders only) · `HARVEST_DB`/`DB_PATH` (our writes) · `MAX_HASHES`/`ROUNDS`/`CONC`/`CYCLE_SLEEP`/`RETENTION_DAYS`/`ONESHOT`.

### GeoIP (64–90)
`_find_geodb` globs `/data/geoip/*.mmdb`; sets up `country_of(ip)` via `geoip2` (or `maxminddb` fallback) → ISO-2 or `"XX"`.

### DB (94–121)
`_get_db` (95) — **thread-local** connection, `timeout=60` + `busy_timeout=60000` + `synchronous=NORMAL` (one conn per worker thread; never share sqlite across threads). `_init_harvest_db` (105) — creates `peers` with the **exact same schema/PK as the DHT collector's** (so the union in `export_nbcu.py` is identical) + a `last_seen` index.

### `_upsert(hash, ip_country, today)`  (124)
`executemany` `INSERT … ON CONFLICT(hash,ip) DO UPDATE last_seen` — same upsert semantics as `dht_peer_count.upsert_peers`; writes a `_queried_` sentinel when a hash returned zero peers. One transaction per hash.

### `load_hashes(limit)`  (144) — the worklist
Read-only against the shared DB, **ordered seeders-first** (`COALESCE(h.seeders,0) DESC, p.last_seen DESC`). **Reviewer note (§34):** popularity-first is deliberate — the metric we reproduce is dominated by each title's biggest live swarms, so the harvest budget must hit high-`seeders` hashes regardless of DHT recency. The old recency-first ordering starved popular-but-DHT-quiet titles (e.g. Spider-Noir got 24/63 high-seeder hashes harvested).

### `harvest_hash(ih, today)`  (176)
`th._public_only(th.harvest_infohash(ih, rounds=ROUNDS))` → geolocate each IP → `_upsert`. Wrapped so a single hash's failure can't kill the cycle.

### `run_cycle()`  (185)
`load_hashes(MAX_HASHES)` → `ThreadPoolExecutor(CONC)` running `harvest_hash` over all of them; progress every 2000; returns (hashes, ip-writes).

### Retention & WAL (204–251)
`_prune_old` (204) — `DELETE FROM peers WHERE last_seen < today-RETENTION_DAYS` (§32). Safe: the export only reads a recent date, and a churned IP re-inserts the moment it's re-seen. `_checkpoint_loop` (230) — daemon thread every 90s: periodically `_prune_old` (~hourly) then `PRAGMA wal_checkpoint(TRUNCATE)` — which **actually drains to zero** here because this DB has a single writer (the whole point of the separate-DB design).

### `main()`  (254)
Validate geodb → `_init_harvest_db` → prune once → start the checkpoint thread → **loop:** `run_cycle()` then sleep `CYCLE_SLEEP` (or exit if `ONESHOT`).

---

## Gotchas / invariants (for reviewers)
- **Never write peers to `hashes_v2.db` from here** — it would inflate the shared WAL that the DHT workers prevent from checkpointing. Writes go to `harvest_peers.db` only.
- **Same `peers` schema/PK as the DHT collector** — required for `export_nbcu.py`'s `UNION` to dedupe correctly.
- **Seeders-first worklist** is intentional (§34); don't revert to recency-first.
- **Fresh `key` per announce round** is what makes repeated harvesting return *new* swarm slices (peer churn ≈ 23%/3min) — the mechanism that accumulates the full daily union.
- Retention prune + single-writer TRUNCATE checkpoint keep this DB bounded (~380 MB/day of new pairs otherwise).

## Change history
`SESSION_CHANGES.md` §26–§28 (built + deployed), §30 (separate DB), §32 (retention prune), §34 (seeders-first worklist).
