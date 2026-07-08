# `dht_peer_count.py` — function guide (with sample data) + navigation map

Goal of this doc: make the live DHT collector (2,647 lines) easy to follow by showing **what data each
function takes in and returns**, with concrete examples — so you can understand the data flowing
through without reading the body. Pairs with the file at `dht_peer_count.py`. Line numbers as of
2026-06-09; **the source file is unchanged**.

> Mental model: load a warm node pool → for each tracked infohash, walk the DHT (Kademlia XOR hops)
> toward the nodes *closest to that hash*, `get_peers`, collect peer IPs + BEP-33 bloom filters,
> GeoIP-bucket the IPs, and write one `(hash, country, count, bep33)` row per country to CSV + the
> `peers` table. Multiprocessing: N workers each run their own asyncio loop + node pool, merged at the end.

---

## The pipeline in one picture
```
hashes_v2.db  ── load_hashes() ──▶  [{hash, ip_id, title, category, seeders}, …]  (live first)
                                              │  (for each hash, via process_one)
                                              ▼
                  get_peers_by_country(hash, socket)   ← mainline BitTorrent DHT (Kademlia walk)
                  run once PER SOCKET (num_sockets≥1)
                                              │
                  ({"US":12,"IN":5}, {ip→cc}, bfsd:256B, bfpe:256B)   ← one tuple per socket
                                              │
                  _merge_socket_results(...)  ← union IPs, OR-merge blooms, estimate once
                                              ▼
                  ({"US":12,"IN":5}, {ip→cc}, {"seeders":31,"leechers":464})
                       │                  │              │
                       │                  │              └── BEP-33 swarm estimate (scrape)
                       │                  └── peer→country map → upsert_peers() into SQLite
                       ▼
                  process_one → CSV rows ──▶ csv_queue_put ──▶ YYYY-MM-DD.csv
```
Parallel to the active walk, `DHTTransport.datagram_received` passively catches `announce_peer`
messages for watched hashes → `_drain_announces` geolocates + writes them too (free peers between passes).

Mental shorthand: **`load_hashes` = the worklist**, **`get_peers_by_country` = the measurement (per socket)**,
**`_merge_socket_results` = combine sockets**, **`process_one`/`run` = glue + persistence**, **bencode/compact
helpers = wire plumbing**, **the numpy node pool = the warm routing table that makes each walk fast.**

---

## Function reference (input → sample output)

Returns are shown as `call → sample value`. Where a function mutates global state instead of returning,
that is called out explicitly.

### IP ↔ integer helpers (L122, L129)
```python
_ip4_to_int("82.221.103.244")   # → 1390606324   (big-endian uint32; 0 on bad/IPv6 input)
_int_to_ip4(1390606324)         # → "82.221.103.244"
```

### `_make_token(ip)` / `_verify_token(ip, token)` (L195, L201) — announce_peer auth (BEP-5)
```python
_make_token("5.6.7.8")              # → b'\x9a\x1c\xf3\x40'   (4-byte HMAC, tied to ip + current hour)
_verify_token("5.6.7.8", b'\x9a\x1c\xf3\x40')   # → True  (accepts current OR previous hour)
_verify_token("5.6.7.8", b'\x00\x00\x00\x00')   # → False
```

### `_load_known_hashes()` (L213) — load watchlist for O(1) announce lookups
Reads every `hash` from `hashes_v2.db` into the module-global `_KNOWN_HASHES` (a `set[bytes]`).
```python
_load_known_hashes()   # → 2_134_889   (count loaded; side effect: _KNOWN_HASHES populated)
# _KNOWN_HASHES now = { b'\xab\x12…20 bytes…', … }
```

### Node-pool mutators (L230, L246, L275) — return nothing useful; they edit globals
```python
_pool_add([(b'<20B nid>', "1.2.3.4", 6881)], verified_ids={b'<20B nid>'})  # → None (inserts/refreshes row)
_evict_bad_nodes()    # → 1273   (count of dead nodes removed; ≥5 timeouts AND 0 responses)
_pool_swap_remove(42) # → None   (O(1) numpy removal: swap slot 42 with last, shrink _POOL_N)
```
These maintain `_POOL_ARR` (numpy struct array, fields `nid/ip_int/port/verified/last_seen/responses/timeouts`).

### `load_node_pool()` / `save_node_pool()` (L385, L449) — disk persistence (return None)
`load_node_pool()` reads `node_pool.json` into `_POOL_ARR`; verified nodes older than 6h are demoted to
unverified. `save_node_pool()` *queues* an async write and returns immediately. On-disk row format:
```python
["ab12…40-hex-nid", "82.221.103.244", 6881, 1, 1718450000]
#  node_id_hex        ip                port  verified  last_seen_unix
```

### `load_hash_node_cache()` / `save_hash_node_cache()` (L541, L557) — per-hash leaf-node cache
The cache lets next pass start at the closest leaf nodes instead of re-walking all 8 hops.
```python
# _HASH_NODE_CACHE after load:
{ "ab12cd…(40 hex)": [["fe9a…40-hex-nid", "67.215.246.10", 6881], …up to 5…], … }
```

### `_bencode(obj)` (L591) — encode Python → bencode bytes (KRPC wire format)
```python
_bencode({b"t": b"\x00\x01", b"y": b"q"})   # → b'd1:t2:\x00\x011:y1:qe'  (d…e dict, keys sorted)
_bencode(45000)        # → b'i45000e'
_bencode([b"a", 1])    # → b'l1:ai1ee'
```

### `bdecode(bytes)` (L627) — decode bencode bytes → Python
```python
bdecode(b'd1:rd2:id20:....e1:y1:re')
# → {b'r': {b'id': b'....'}, b'y': b'r'}     (keys/strings stay BYTES — that's why code uses r.get(b"values"))
```

### `ip_to_country(ip)` (L651) — GeoIP lookup (lru_cached, 200K entries)
```python
ip_to_country("82.221.103.244")   # → "IS"
ip_to_country("5.6.7.8")          # → "DE"
ip_to_country("10.0.0.1")         # → "XX"   (private/unknown — never raises)
```

### `_xor_dist(node_id, ih_int)` (L712) — Kademlia distance
```python
_xor_dist(b'<20-byte node_id>', 0xab12…160-bit-int)   # → 8973…  (a big int; smaller = closer)
```

### `_estimate_bloom(bf)` (L717) — BEP-33 bloom filter (256 B) → swarm-size estimate
Input: a 256-byte (2048-bit) filter; set bits ≈ how many distinct IPs announced.
```python
_estimate_bloom(bytearray(256))                  # → 0      (all-zero filter — no announces)
_estimate_bloom(<filter with ~620 bits set>)     # → 464    (estimated peers)
_estimate_bloom(<completely full filter>)        # → 50000  (capped — overflowed)
```

### `_compute_bep33(bfsd, bfpe)` (L738) — raw bloom bytes → seeder/leecher dict
```python
_compute_bep33(b'<256B seeders>', b'<256B leechers>')
# → {"seeders": 31, "leechers": 464}
```

### `_merge_socket_results(results)` (L746) — combine N per-socket walks  ← see deep-dive
Input: a list of `(by_country, ip_country, bfsd_bytes, bfpe_bytes)` tuples (one per socket).
```python
_merge_socket_results([
    ({"US":7,"IN":3}, {"5.6.7.8":"US", …}, b'<256B>', b'<256B>'),   # socket 0
    ({"US":6,"GB":4}, {"5.6.7.8":"US", …}, b'<256B>', b'<256B>'),   # socket 1
])
# → ( {"US":12,"IN":3,"GB":4},                 # by_country: recounted from deduped IP union
#     {"5.6.7.8":"US", "212.11.30.47":"GB", …},# ip_country: union (last-write-wins, same IP→same cc)
#     {"seeders":31, "leechers":464} )         # bep33: blooms OR-merged FIRST, estimated ONCE
```

### Compact-format parsers (L781–L845) — wire bytes → Python collections
```python
_compact_to_peers(b'\x05\x06\x07\x08\x1a\xe1')      # → {"5.6.7.8"}        (6B chunks: 4 IP + 2 port)
_compact_to_peers_v6(b'<18-byte chunk>')            # → {"2001:db8::1"}    (18B: 16 IP + 2 port)
_compact_to_nodes_full(b'<26-byte chunk>')          # → [(b'<20B nid>', "67.215.246.10", 6881)]
_compact_to_nodes_full_v6(b'<38-byte chunk>')       # → [(b'<20B nid>', "2001:db8::1", 6881)]
```
Note `_compact_to_peers` returns only **IP strings** (port dropped after the >0 sanity check); the
node parsers keep the **node_id** because the walk XORs it against the infohash to measure closeness.

### `class DHTTransport` (L852) — one UDP socket / one DHT identity
- `send_get_peers(addr, info_hash, node_id)` (L956) → `(tid, queue)` — fires a `get_peers` query
  (`scrape=1` for BEP-33, `want=[n4,n6]` for BEP-32) and returns the 4-byte transaction id + an
  `asyncio.Queue` the reply lands in.
  ```python
  tid, q = dht.send_get_peers(("67.215.246.10",6881), b'<20B ih>', b'<20B sender>')
  # tid → b'\x3f\xa1\x0c\x9e ;  q → asyncio.Queue (datagram_received put_nowait's the decoded reply)
  ```
- `datagram_received(data, addr)` (L883) → `None` — the router: routes `y=r` responses to the waiting
  queue by `tid`; answers inbound `ping`/`find_node`/`get_peers`/`announce_peer` so we stay in routing
  tables and passively capture announces for watched hashes.

### `get_peers_by_country(info_hash_hex, dht, timeout=10, rounds=8)` (L986) — core per-hash walk  ← deep-dive below
Runs ONE Kademlia walk on ONE socket. Returns a 4-tuple (raw bloom bytes returned, NOT yet estimated,
so callers can OR-merge across sockets):
```python
await get_peers_by_country("ab12cd…40hex", dht)
# → ( {"US": 12, "IN": 5, "GB": 3},          # by_country  {country: peer_count}
#     {"5.6.7.8": "US", "212.11.30.47": "GB", …},  # ip_country {peer_ip: country}
#     b'<256 raw bloom bytes BFsd>',          # seeders filter (un-estimated)
#     b'<256 raw bloom bytes BFpe>' )         # leechers filter (un-estimated)
# zero-peer hash → ({}, {}, b'<256B>', b'<256B>')   (skips GeoIP entirely)
```

### `_drain_announces(queue, today, announce_buffer)` (L1333) — passive-peer background task
Long-running per-pass task. For each `(hash_hex, peer_ip)` off the queue: geolocate → upsert to SQLite →
buffer in `announce_buffer` (`{hash_hex: {country: set[ip]}}`). Returns the total count when cancelled.
```python
await announce_task   # (after cancel) → 1827   ; announce_buffer e.g. {"ab12…": {"US": {"5.6.7.8", …}}}
```

### `init_peers_table(db)` / `upsert_peers(db, hash, ip_country, today)` (L1375, L1395) — return None
`peers` schema: `(hash, ip, country, first_seen, last_seen)`, PK `(hash, ip)`. `upsert_peers` inserts new
IPs or bumps `last_seen`; **empty `ip_country` writes a sentinel** `ip='_queried_', country='XX'` so the
hash counts as "done today" for resume even with zero peers.
```python
upsert_peers(db, "ab12…", {"5.6.7.8": "US"}, "2026-06-13")   # → None (row upserted)
upsert_peers(db, "ab12…", {}, "2026-06-13")                  # → None (sentinel _queried_ row)
```

### `_upsert_peers_threadsafe(hash, ip_country, today)` (L1444) — DB write from executor threads
Routes through the single-writer IPC (`dht_single_writer`) when enabled, else a thread-local connection
with 6-try locked-retry backoff. Returns None; drops one upsert (self-heals next pass) only after 6 locks.

### `load_hashes(category, limit, skip_dead_days, active_only_days, …)` (L1492) — build the worklist
Returns the per-pass list of hash dicts, **ordered live-first** (most recent real peer DESC, then source
seeders DESC). The various flags filter to active / dormant / new / non-dead subsets.
```python
load_hashes(category="Series", limit=3)
# → [
#   {"hash": "ab12cd…40hex", "ip_id": "tt1520211", "title": "The Walking Dead", "category": "Series", "seeders": 812},
#   {"hash": "cd34ef…",      "ip_id": "tt0944947", "title": "Game of Thrones",  "category": "Series", "seeders": 540},
#   {"hash": "ef56ab…",      "ip_id": "tt0903747", "title": "Breaking Bad",     "category": "Series", "seeders": 318},
# ]
```

### `count_hashes(db, category, limit)` (L1640)
```python
count_hashes(db, category="Series")   # → 418302   (fast COUNT(*), no dead-day filter)
```

### CSV helpers (L1668–L1763)
```python
get_csv_path()                                   # → Path(".../data/peer_counts/2026-06-13.csv")
get_worker_csv_path(Path(".../2026-06-13.csv"), 1)  # → Path(".../2026-06-13_w1.csv")
get_worker_csv_path(Path(".../2026-06-13.csv"), None) # → unchanged (single-process mode)
csv_queue_put(rows, csv_path, write_header)      # → None (non-blocking enqueue; writer thread persists)
csv_queue_flush()                                # → None (blocks until queue drained)
```
A written CSV row (FIELDNAMES order) looks like:
```python
{"date":"2026-06-13","run_time":"15:23","hash":"ab12cd…","ip_id":"tt1520211",
 "title":"The Walking Dead","category":"Series","seeders":812,
 "country":"US","peer_count":12,"bep33_seeders":31,"bep33_leechers":464}
```
`bep33_*` are written on the **first-country row only** (per-hash, not per-country) to avoid double-counting.

### `run(...)` (L1770) — one full pass over a hash list
Sets up N UDP sockets + the announce listener, runs every hash through `process_one` in saturated
chunks, flushes CSV/DB, evicts bad nodes, prints stats.
```python
await run(hashes=[…], concurrency=150, timeout=10, …)   # → 4127   (count of hashes that had ≥1 peer)
```

### `process_one(row)` (L1877, nested in `run`) — query one hash across all sockets → CSV rows
Runs `get_peers_by_country` on every socket, merges, writes to DB, returns the per-country CSV rows.
```python
await process_one({"hash":"ab12…","ip_id":"tt1520211","title":"The Walking Dead",
                   "category":"Series","seeders":812})
# → [ {…,"country":"GB","peer_count":3,"bep33_seeders":31,"bep33_leechers":464},   # first row carries bep33
#     {…,"country":"IN","peer_count":5,"bep33_seeders":0, "bep33_leechers":0},
#     {…,"country":"US","peer_count":12,"bep33_seeders":0,"bep33_leechers":0} ]
# no real peers but BEP-33 had data → [ {…,"country":"XX","peer_count":0,"bep33_seeders":31,"bep33_leechers":464} ]
# truly nothing → []
```

### Entrypoint helpers (L2089–L2164)
```python
_build_already_done(csv_path, db, "2026-06-13")   # → {"ab12…", "cd34…", …}  (hashes done today: CSV + sentinels)
_print_pass_summary(1, 4127, 612.0, loop=True)    # → None (prints "Pass 1 done in 10m12s — 4,127 hashes had peers")
_print_final_summary(csv_path)                    # → None (prints top countries/categories/titles)
```

### Multiprocessing (L2173–L2300) — return None except `_run_worker`
```python
_save_worker_pool(0)            # → None  (writes node_pool_w0.json atomically)
_merge_worker_pools(3)          # → None  (unions worker pools → node_pool.json)
_merge_worker_csvs(3, csv, True)# → None  (concats .YYYY-MM-DD_wN.csv temps → final daily CSV)
_run_worker(0, chunk, 150, …)   # → (0, 4127)   (worker_id, with_peers_count)
```

### `main()` (L2312) — entrypoint
Parses args, picks tier-specific pool/cache paths, warms the pool, then loops passes (single- or
multi-process) until midnight / SIGTERM / one-shot completion. Returns None; side effect is the daily CSV
+ updated `peers` table + persisted node pool.

---

## Deep-dive: how `get_peers_by_country` actually works (step by step)

It's a **converging Kademlia `get_peers` walk** on a single socket: start at bootstrap + the XOR-closest
pool nodes, keep asking the nodes *closest to the infohash* for `rounds` rounds (default 8), collecting
peers + BEP-33 filters + new routing nodes along the way.

**State it keeps (per call):**
| var | holds | sample |
|---|---|---|
| `ih_int` | infohash as a 160-bit int (for XOR) | `0xab12…` |
| `all_peer_ips` | sampled swarm peer IPs (v4 + v6) | `{"5.6.7.8", "212.11.30.47", …}` |
| `bfsd_merged`,`bfpe_merged` | OR-merged BEP-33 filters (256 B each) | `bytearray(256)` accumulating set bits |
| `targets` | `(node_id, ip, port)` to ask next round | seeded from bootstrap + pool |
| `queried` | `(ip, port)` already asked (dedup) | `{("67.215.246.10",6881), …}` |
| `all_verified_ids` | responders across all rounds | used to refresh the hash→node cache |

**1. Seed targets.** Bootstrap nodes (no known node_id → `None`) **plus** `_xor_select()` — the 300
pool nodes closest to this hash by XOR. `_xor_select` prepends cached leaf nodes from
`_HASH_NODE_CACHE[hash]` (so a previously-walked hash starts at its leaves), prefers verified, skips bad
nodes, and runs the numpy `argpartition` in an executor so it never blocks the loop:
```python
targets = [(None,"67.215.246.10",6881), …bootstrap…] + [(b'<20B nid>',"82.221.103.244",6881), …300…]
```

**2. Each round, query up to 64 unqueried targets concurrently** (`query_one` via `asyncio.gather`).
The outgoing query forges a **Sybil sender id** (target's first 15 bytes + 5 random) so the target stores
us as a neighbour. `send_get_peers` sets `scrape=1` (BEP-33) and `want=[n4,n6]` (BEP-32). Each
`query_one` also credits `responses`/`timeouts` on the pool node (Step 3 quality tracking).

**3. Parse each reply's `r` dict** and update state:
```python
all_peer_ips |= _compact_to_peers(r.get(b"values", []))      # IPv4 swarm peers
all_peer_ips |= _compact_to_peers_v6(r.get(b"values6", []))  # IPv6 swarm peers (BEP-32)
new_nodes     = _compact_to_nodes_full(r.get(b"nodes", b"")) # closer routing nodes for next round
# r[b"id"] (20B) → mark responder verified
# r[b"BFsd"]/r[b"BFpe"] (256B each) → OR byte-by-byte into bfsd_merged / bfpe_merged
```
Two reply shapes: a **far node** returns `nodes` (→ next round's targets, XOR-sorted closest-first); a
**close node holding the swarm** returns `values` (→ peers) plus `BFsd`/`BFpe` blooms.

**4. Converge.** Discovered nodes are XOR-sorted and become next round's `targets`, and are added to the
pool (`_pool_add`) with the round's verified ids — so each round lands nearer the infohash. Optional
env-gated early-stop bails once a round adds no new peers.

**5. After the walk:** if any peers were found, cache the 5 XOR-closest verified responders into
`_HASH_NODE_CACHE[hash]` for next pass, then geolocate `all_peer_ips` in the bounded GeoIP thread pool.

**6. Return** the per-socket tuple (note: **raw bloom bytes**, not yet estimated):
```python
return dict(by_country), ip_country, bytes(bfsd_merged), bytes(bfpe_merged)
# → ({"US":12,"IN":5,"GB":3}, {"5.6.7.8":"US", …}, b'<256B>', b'<256B>')
```

### Why merge happens later (`_merge_socket_results`)
`process_one` runs this walk once per socket, then merges. Merging blooms with a plain bit-OR **before**
calling `_estimate_bloom` is correct per BEP-33 (bloom union = OR); estimating each socket separately and
summing would over-count. IPs are deduped across sockets, then `by_country` is **recounted** from the
union so a peer seen on two sockets counts once.

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Change how a hash is queried / the XOR walk | `get_peers_by_country` L986, `query_one` L1146, `_xor_select` L1047 |
| Change peer-IP → country bucketing | `ip_to_country` L651, `_geolocate_all` L1320 |
| Change how sockets are combined | `_merge_socket_results` L746 |
| Change what gets written to DB | `upsert_peers` L1395, `_upsert_peers_threadsafe` L1444 |
| Change the CSV columns / output | `FIELDNAMES` L1663, CSV sections L1659–L1763 |
| Change the worklist / which hashes run | `load_hashes` L1492 |
| Change node-pool persistence / cache | L182–L568 (pool + cache), `_pool_add` L275 |
| Change worker count / merge behaviour | L2169 (multiprocessing), `main` L2312 |
| Seeder/leecher (BEP-33) estimates | `_compute_bep33` L738, `_estimate_bloom` L717 |
| Passive announce_peer capture | `DHTTransport.datagram_received` L883, `_drain_announces` L1333 |
