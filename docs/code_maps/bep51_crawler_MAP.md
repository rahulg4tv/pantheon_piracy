# `bep51_crawler.py` — Navigation Map + function guide (with sample data)

Read-only structural index of the BEP-51 infohash-discovery crawler (1,346 lines, 32
top-level functions, 1 nested, 1 class with 7 methods, 10 sections). Maps the file at
`bep51_crawler.py`. This doc now also shows **what each function takes in and returns**,
with concrete examples, so the file is followable without scrolling. Line numbers as of
2026-06-08.

> Mental model: walk the BitTorrent DHT and ask nodes for `sample_infohashes`
> (BEP-51) to DISCOVER brand-new torrent infohashes (vs `dht_peer_count.py` which
> counts peers for hashes we already track). Rotate the target node_id across all
> 256 first-byte prefixes for full-keyspace coverage, dedup against `hashes_v2.db`,
> optionally resolve each new hash to a real media title (torrent caches → BEP-09
> direct peer metadata) and insert into the DB; always write a daily CSV.

---

## The pipeline in one picture
```
bootstrap routers + node_pool.json
        │
        ▼  run_crawler(duration, …)                 ← asyncio DHT walk
        │   ├─ _next_target()  → rotate first byte 0x00..0xff (full keyspace)
        │   ├─ sample_one(addr, target)             ← BEP-51 sample_infohashes query
        │   └─ process_one(addr)                    ← parse reply
        │        ├─ samples (N×20-byte infohashes) → dedup vs known_hashes → new_hashes
        │        └─ nodes (routing) → feed back into node_queue (converging crawl)
        ▼
new_hashes = [{"hash":"0a1b…40hex", "num":312, "seen_at":"…Z", "first_seen":"2026-06-15"}, …]
        │
        ├──▶ write_csv()                            → data/discovered/2026-06-15.csv
        │
        └──▶ (optional --filter-media)
             resolve_media_batch()                  ← 2-pass enrichment
               Pass 1: _resolve_via_torrent_cache() ← itorrents.org / torrage.info  (~1-5%)
               Pass 2: _resolve_via_bep09()         ← DHT get_peers → TCP metadata (~30-50%)
        │
        ▼  insert_discovered_hashes()               ← match catalog → real ip_id, write rows
data/hashes_v2.db  (table: hashes)
```
Mental model: **`run_crawler` = discovery**, **`resolve_media_batch` = enrichment**,
**`insert_discovered_hashes` = persistence**. The bencode / compact-node helpers are
plumbing for the DHT wire protocol; `BEP51Transport` is the UDP socket layer.

---

## Section-by-section (structural index)

### L58 — Config / constants
Paths (`DB_PATH`, `NODE_POOL_PATH`, `DISCOVERED_DIR`), bootstrap hosts, torrent-cache
URLs, HTTP headers, batch caps, category-classification regex rules, adult-content filter.

### L74 — Media resolution: torrent-cache path
- `L142 _is_adult` · `L147 _guess_category` · `L159 _bdecode_name_from_torrent` · `L174 _resolve_via_torrent_cache`

### L200 — BEP-09 / BEP-10: metadata directly from DHT peers
- `L220 _send_bt_msg` · `L225 _recv_bt_msg` · `L258 _get_peers_dht` · `L356 _fetch_bep09_metadata` · `L458 _resolve_via_bep09` · `L499 resolve_hash_media` · `L521 _run_resolve_pass` · `L569 resolve_media_batch`

### L621 — Bencode (minimal, no external deps)
- `L625 _bencode` · `L638 _bdecode_inner` · `L661 bdecode`

### L665 — Compact node/peer parsing
- `L669 _compact_to_nodes`

### L680 — Bootstrap / node pool
- `L684 _resolve_bootstrap` · `L695 load_node_pool`

### L711 — DHT transport (BEP-51 sample_infohashes)
- `L715 class BEP51Transport` (`__init__`, `connection_made`, `_send`, `datagram_received`, `error_received`, `send_sample_infohashes`, `cancel`)

### L797 — Database helpers
- `L801 load_known_hashes` · `L824 _normalise_title` · `L842 _match_catalog_title` · `L881 insert_discovered_hashes`

### L950 — Crawl coroutine
- `L954 sample_one` · `L987 run_crawler` (nested `L1052 _next_target`, `L1066 process_one`)

### L1177 — CSV output
- `L1184 write_csv` (`L1181 FIELDNAMES`)

### L1193 — Main / entrypoint
- `L1197 main`

---

## Function reference (input → sample output)

The single most useful thing here: **what each function RETURNS**, with a concrete value.
Sample bytes are illustrative (real shapes/lengths; bytes stay `bytes`, not `str`).

### `_is_adult(title)` — adult-content gate (L142)
Returns `bool` — True rejects the torrent before any media classification.
```python
_is_adult("Brazzers.2024.XXX.1080p")   # → True
_is_adult("The Boys S04E08 1080p")     # → False
```

### `_guess_category(title)` — classify a torrent name (L147)
Returns one of `"Movies" | "Series" | "Anime" | "Adult" | "Unknown"` (str). Only the three
in `MEDIA_CATEGORIES` survive media-filtering; `"Adult"`/`"Unknown"` get dropped.
```python
_guess_category("Dune Part Two (2024) 2160p BluRay x265")  # → "Movies"
_guess_category("The Bear S03E01 1080p WEB-DL")            # → "Series"
_guess_category("[SubsPlease] One Piece - 1100 (1080p)")   # → "Anime"
_guess_category("random_disk_image.iso")                   # → "Unknown"
```

### `_bdecode_name_from_torrent(data)` — pull `name` out of raw .torrent bytes (L159)
Input: raw bencoded .torrent bytes. Returns the `name` field as `str`, or `None` if absent.
```python
_bdecode_name_from_torrent(b"d...4:name20:Dune.Part.Two.2024...e")
# → "Dune.Part.Two.2024"
_bdecode_name_from_torrent(b"not a torrent")   # → None
```

### `_resolve_via_torrent_cache(hash_hex)` — download .torrent from caches (L174)
Input: 40-hex infohash. Tries itorrents.org then torrage.info. Returns a media dict, or
`None` (not cached, or cached but not media). Hit rate ~1-5%.
```python
_resolve_via_torrent_cache("0a1b2c…40hex")
# → {"name": "Dune.Part.Two.2024.2160p", "category": "Movies", "seeders": 0}
_resolve_via_torrent_cache("deadbeef…")        # → None
```

### `_send_bt_msg(sock, payload)` / `_recv_bt_msg(sock, deadline)` — BT wire I/O (L220, L225)
`_send_bt_msg` returns `None` (side-effect: writes a 4-byte length prefix + payload).
`_recv_bt_msg` returns one message's `bytes`, or `None` on timeout/disconnect/oversize.
```python
_send_bt_msg(sock, b"\x14\x00...")             # → None  (sent on the wire)
_recv_bt_msg(sock, time.time()+8)              # → b"\x14\x00d...e"   (one ext message)
_recv_bt_msg(sock, time.time()+8)              # → None   (peer went quiet)
```

### `_get_peers_dht(info_hash, timeout=5.0)` — two-round DHT get_peers walk (L258)
Input: 20-byte raw infohash. Returns up to 10 `(ip, port)` peer tuples that hold the
torrent (empty list if none found). Synchronous — safe inside a ThreadPoolExecutor.
```python
_get_peers_dht(bytes.fromhex("0a1b2c…40hex"))
# → [("82.45.11.9", 51413), ("190.2.144.7", 6881), ("5.6.7.8", 49152)]
_get_peers_dht(bytes.fromhex("deadbeef…"))     # → []
```

### `_fetch_bep09_metadata(ip, port, info_hash, timeout=8.0)` — download info-dict from a peer (L356)
Input: a peer `(ip, port)` + 20-byte infohash. TCP handshake → BEP-10 ext handshake →
BEP-09 `ut_metadata` pieces → reassemble → **SHA1-verify**. Returns the raw bencoded
info-dict `bytes` (guaranteed `sha1(bytes) == info_hash`), or `None` on any failure.
```python
_fetch_bep09_metadata("82.45.11.9", 51413, ih)
# → b"d6:lengthi1543503872e4:name18:Dune.Part.Two.2024...e"   (verified)
_fetch_bep09_metadata("5.6.7.8", 49152, ih)    # → None  (no metadata / bad SHA1)
```

### `_resolve_via_bep09(hash_hex)` — find peers → fetch metadata → classify (L458)
Input: 40-hex infohash. get_peers walk → try up to 5 peers → extract `name` → keep only
media. Returns a media dict or `None`. Hit rate ~30-50% for actively-shared hashes.
```python
_resolve_via_bep09("0a1b2c…40hex")
# → {"name": "Dune.Part.Two.2024.2160p.BluRay", "category": "Movies", "seeders": 0}
_resolve_via_bep09("deadbeef…")                # → None
```

### `resolve_hash_media(hash_hex, bep09=False)` — single-hash resolver (L499)
Cache first; if `bep09=True` and cache misses, fall back to BEP-09. Returns
`{"name", "category", "seeders"}` or `None`.
```python
resolve_hash_media("0a1b2c…40hex")             # → None (cache miss, bep09 off)
resolve_hash_media("0a1b2c…40hex", bep09=True)
# → {"name": "The.Bear.S03E01.1080p", "category": "Series", "seeders": 0}
```

### `_run_resolve_pass(candidates, bep09, max_workers, label)` — one threaded pass (L521)
Input: list of hash dicts. Runs `resolve_hash_media` across a thread pool. Returns
`(media_results, resolved_hashes_set)` — the enriched dicts and the set of hashes that resolved.
```python
_run_resolve_pass([{"hash":"0a..","num":312}, …], bep09=False, max_workers=40, label="cache")
# → ([{"hash":"0a..","num":312,"name":"Dune…","category":"Movies","seeders":0}, …],
#    {"0a1b2c…", "7f9e1d…"})
```

### `resolve_media_batch(new_hashes, max_workers=40, cap=50000, enable_bep09=False)` — 2-pass batch resolver (L569)
Input: all discovered hash dicts. Sorts by `num` DESC, caps the batch, runs cache pass
(+ optional BEP-09 pass). Returns the list of enriched media dicts (subset that resolved).
```python
resolve_media_batch(new_hashes, enable_bep09=True)
# → [{"hash":"0a..","num":312,"seen_at":"…Z","first_seen":"2026-06-15",
#     "name":"Dune.Part.Two.2024.2160p","category":"Movies","seeders":0}, …]
```

### `_bencode(obj)` — encode Python → bencode bytes (KRPC wire format) (L625)
Dicts sorted by key. Returns `bytes`.
```python
_bencode(b"abc")                 # → b'3:abc'
_bencode(312)                    # → b'i312e'
_bencode([b"a", 1])              # → b'l1:ai1ee'
_bencode({b"y": b"q", b"t": b"\x00\x01"})
# → b'd1:t2:\x00\x011:y1:qe'     (keys sorted: t before y)
```

### `bdecode(bytes)` / `_bdecode_inner(bytes, pos)` — decode bencode (L661, L638)
`bdecode` returns the decoded object; `_bdecode_inner` returns `(value, next_pos)`.
Strings/keys come back as **bytes**, not str — that's why the code uses `r.get(b"values")`.
```python
bdecode(b"d1:y1:r1:rd2:idi5eee")
# → {b'y': b'r', b'r': {b'id': 5}}
_bdecode_inner(b"i312e", 0)      # → (312, 5)
```

### `_compact_to_nodes(data)` — compact node list → tuples (L669)
Input: a `nodes` blob, N × 26 bytes (20-byte node-id + 4-byte IP + 2-byte port). Returns
`[(node_id_bytes, ip_str, port_int)]`; entries with port 0 are skipped.
```python
_compact_to_nodes(b"<20-byte id>\x52\x2d\x0b\x09\xc8\xd5" + b"<26 more bytes>")
# → [(b'<20-byte id>', "82.45.11.9", 51413), (b'…', "190.2.144.7", 6881)]
_compact_to_nodes(b"")           # → []
```

### `_resolve_bootstrap()` / `load_node_pool()` — seed nodes (L684, L695)
`_resolve_bootstrap` DNS-resolves the 4 hardcoded routers → `[(ip, port)]`.
`load_node_pool` reads `data/node_pool.json` (warm start from the main scanner) →
`[(ip, port)]`; empty list if the file is absent/unreadable.
```python
_resolve_bootstrap()
# → [("67.215.246.10", 6881), ("82.221.103.244", 6881), …]
load_node_pool()
# → [("82.45.11.9", 51413), ("190.2.144.7", 6881), …]   (or [] if no file)
```

### `BEP51Transport.send_sample_infohashes(addr, target)` — send a BEP-51 query (L772)
Input: a node `(ip, port)` + 20-byte `target` node_id (selects keyspace slice). Sends the
query, registers a tid→queue waiter. Returns `(tid_bytes, asyncio.Queue)`; the reply later
lands in that queue via `datagram_received`.
```python
tid, q = transport.send_sample_infohashes(("82.45.11.9", 51413), target=b"\x0a"+b"…19…")
# → (b'\x9c\x12\x4a\x01', <asyncio.Queue>)
# later: msg = await q.get()  →  {b'y':b'r', b'r':{b'samples':b'…', b'num':312, b'nodes':b'…'}}
```

### `load_known_hashes()` — skip-set for the crawl (L801)
Returns a `set[str]` of every 40-hex hash already in `hashes_v2.db`; empty set if the DB
is missing. Discovery dedups against this so we only emit truly-new hashes.
```python
load_known_hashes()   # → {"0a1b2c…", "7f9e1d…", …}   (e.g. 4.2M entries)
```

### `_normalise_title(raw)` — clean a torrent name for matching (L824)
Strips episode/quality/group/year markers → clean lowercase title (str).
```python
_normalise_title("Bones.S12E03.The.New.Tricks.1080p.WEB-DL")  # → "bones"
_normalise_title("The Boys S04E08 1080p")                      # → "the boys"
_normalise_title("Dune.Part.Two.2024.2160p.BluRay-RARBG")      # → "dune part two"
```

### `_match_catalog_title(conn, raw_title)` — map title → catalog ip_id (L842)
Normalise → exact LOWER() match, then prefix match against the `titles` table. Returns
`(ip_id, canonical_title)` or `None`.
```python
_match_catalog_title(conn, "The.Boys.S04E08.1080p")   # → ("tt1190634", "The Boys")
_match_catalog_title(conn, "totally unknown thing")    # → None
```

### `insert_discovered_hashes(new_hashes, today, filter_media=False)` — write rows (L881)
Inserts into `hashes_v2.db.hashes` (INSERT OR IGNORE). With `filter_media=False`: placeholder
rows (title `"BEP-51 Discovery"`, `bep51-<hash[:12]>` ip_id). With `filter_media=True`: only
resolved media, catalog-matched to real ip_ids where possible. Returns count inserted (int).
```python
insert_discovered_hashes(media_hashes, "2026-06-15", filter_media=True)   # → 1843
# sample row written:
# (hash="0a1b2c…", ip_id="tt15239678", title="Dune: Part Two",
#  category="Movies", source="bep51", first_seen="2026-06-15",
#  last_seen="2026-06-15", seeders=0)
```

### `sample_one(addr, target, transport, timeout, rate_limited)` — one query, parsed (L954)
Awaits the reply. Returns the response `b"r"` dict, or `None` on timeout/rate-limit.
Side-effect: records the node's `interval` rate limit.
```python
await sample_one(("82.45.11.9", 51413), target, proto, 3.0, rate_limited)
# → {b'samples': b'<N×20 bytes>', b'num': 312, b'interval': 21,
#    b'nodes': b'<26-byte chunks>', b'id': b'<20>'}
# → None   (timed out, or skipped because rate_limited[addr] > now)
```

### `run_crawler(duration_secs, concurrency, num_sockets, query_timeout, known_hashes, min_num)` — the crawl loop (L987)
The core. Seeds nodes, rotates 256-prefix targets, samples, dedups, feeds routing nodes
back into the queue. Returns `(new_hashes, stats)`.
```python
await run_crawler(1800, 200, 2, 3.0, known, min_num=10)
# → ( [{"hash":"0a1b2c…40hex", "num":312, "seen_at":"2026-06-15T15:23:29Z",
#       "first_seen":"2026-06-15"}, … 47,000 more …],
#     {"queries":182431, "responses":58210, "nodes_added":410552,
#      "samples_total":640180, "new_found":47210} )
```

### `_next_target()` (nested in run_crawler) — rotating keyspace target (L1052)
Returns a 20-byte node_id whose **first byte** cycles 0x00→0xff (then restarts) so the
crawl samples every corner of the DHT, not just near our own id.
```python
_next_target()   # → b'\x00' + <19 random bytes>
_next_target()   # → b'\x01' + <19 random bytes>   (… 0x02, 0x03, … 0xff, wrap)
```

### `process_one(addr, proto)` (nested in run_crawler) — handle one node's reply (L1066)
Returns `None` (mutates shared state). Splits the reply: `samples` → dedup → `new_hashes`
(respecting `min_num`); `nodes` → `node_queue` for further crawling.

### `write_csv(new_hashes, path)` — append today's CSV (L1184)
Returns `None`. Writes header on first run; columns = `FIELDNAMES`.
```python
write_csv(new_hashes, Path("data/discovered/2026-06-15.csv"))   # → None
# file rows:
# hash,num,seen_at,first_seen
# 0a1b2c…40hex,312,2026-06-15T15:23:29Z,2026-06-15
```

### `main()` — entrypoint: parse args → crawl → CSV → optional resolve+insert (L1197)
Returns `None`; prints a run summary. Glue for the whole pipeline.

---

## Deep-dive: the BEP-51 `sample_infohashes` crawl, step by step

`run_crawler` is a **converging keyspace sweep**. Unlike `get_peers` (which converges on
ONE infohash), here we rotate the `target` across the whole keyspace and harvest whatever
each node happens to be storing.

**State it keeps:**
| var | holds | sample |
|---|---|---|
| `node_queue` | nodes still to query (FIFO `deque`) | `deque([("82.45.11.9",51413), …])` |
| `seen_nodes` | nodes already queried (don't re-ask) | `{("82.45.11.9",51413), …}` |
| `discovered` | infohashes seen THIS session (intra-run dedup) | `{"0a1b2c…", …}` |
| `new_hashes` | hashes NOT in `known_hashes` → the output | `[{"hash":"0a…","num":312,…}, …]` |
| `rate_limited` | `addr → next_allowed_monotonic` (BEP-51 `interval`) | `{("82.45.11.9",51413): 10234.7}` |

**1. Seed the queue** with the saved warm node pool + bootstrap routers (capped at 5000,
shuffled):
```python
seed = list(set(load_node_pool() + _resolve_bootstrap()))
node_queue.extend(seed[:5000])
```

**2. Pick a rotating target.** Each `process_one` calls `_next_target()` so the query's
`target` node_id cycles its first byte 0x00..0xff — full-keyspace coverage over time:
```python
target = _next_target()      # → b'\x07' + 19 random bytes
```

**3. Send the BEP-51 query** (`send_sample_infohashes`). The outgoing message:
```python
{b"t": tid, b"y": b"q", b"q": b"sample_infohashes",
 b"a": {b"id": own_node_id, b"target": target}}
# _bencode → b'd1:ad2:id20:<id>6:target20:<target>e1:q17:sample_infohashes1:t4:<tid>1:y1:qe'
```

**4. Read the reply** (`sample_one` awaits the tid-queue). A BEP-51 response:
```python
{b"y": b"r", b"r": {
    b"id":       b"<20-byte node id>",
    b"interval": 21,                       # rate limit: don't re-ask for 21 s
    b"num":      312,                       # total hashes THIS node stores
    b"samples":  b"<N × 20 raw infohashes>",  # up to 20 random samples
    b"nodes":    b"<26-byte routing chunks>"  # closer nodes to keep crawling
}}
```

**5. Split the reply** (`process_one`):
```python
# samples → 20-byte slices → hex → dedup → keep if new (and num >= min_num)
for i in range(len(raw_samples)//20):
    ih_hex = raw_samples[i*20:i*20+20].hex()    # "0a1b2c…40hex"
    if ih_hex not in discovered and ih_hex not in known_hashes:
        new_hashes.append({"hash": ih_hex, "num": 312, "seen_at": "…Z",
                           "first_seen": "2026-06-15"})
# nodes → feed routing nodes back into node_queue (converging crawl)
for node_id, ip, port in _compact_to_nodes(raw_nodes):
    node_queue.append((ip, port))
```

**6. Loop** until `duration_secs` elapses, keeping ~`concurrency*2` tasks in flight across
`num_sockets` UDP sockets, re-seeding from bootstrap if the queue ever fully drains.

**7. Return** the new hashes + counters:
```python
return new_hashes, {"queries":…, "responses":…, "nodes_added":…,
                    "samples_total":…, "new_found":…}
```
Then `main` writes the CSV and (optionally) runs `resolve_media_batch` →
`insert_discovered_hashes`.

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Change how nodes are sampled / keyspace coverage | `run_crawler` L987, `_next_target` L1052, `send_sample_infohashes` L772 |
| Change per-query / rate-limit behaviour | `sample_one` L954 |
| Change how samples are parsed / which hashes are kept (min_num) | `process_one` L1066 |
| Change media classification rules | `CATEGORY_RULES`/`_ADULT_PATTERNS` L100–139, `_guess_category` L147, `_is_adult` L142 |
| Change torrent-cache resolution | `_resolve_via_torrent_cache` L174, `TORRENT_CACHES` L82 |
| Change BEP-09 direct-peer metadata fetch | `_get_peers_dht` L258, `_fetch_bep09_metadata` L356, `_resolve_via_bep09` L458 |
| Change the two-pass resolve pipeline / batch caps | `resolve_media_batch` L569, `_run_resolve_pass` L521 |
| Change what gets written to the DB / catalog matching | `insert_discovered_hashes` L881, `_match_catalog_title` L842, `_normalise_title` L824 |
| Change CSV output format | `write_csv` L1184, `FIELDNAMES` L1181 |
| Change node-pool warm start / bootstrap | `load_node_pool` L695, `_resolve_bootstrap` L684 |
| Add/change CLI flags or the run summary | `main` L1197 |
| Change bencode / compact-node parsing | `_bencode` L625, `bdecode` L661, `_compact_to_nodes` L669 |

---

## Quick test recipe
```bash
# short crawl, CSV only, keep everything:
python bep51_crawler.py --duration 2

# media-filtered insert with BEP-09 second pass, busy nodes only:
python bep51_crawler.py --duration 5 --min-num 10 --filter-media --bep09
# expect: new=…/min during crawl, then cache pass (~1-5%) + BEP-09 pass (~30-50%),
#         then "DB inserted: N media hashes into hashes_v2.db"
```
