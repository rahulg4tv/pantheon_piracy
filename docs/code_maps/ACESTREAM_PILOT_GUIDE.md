# acestream_pilot.py — function guide (with sample data) + how to make it better

Goal of this doc: make the file easy to follow by showing **what data each function takes in and
returns**, with concrete examples — and then concrete ways to simplify `probe_dht`, which has grown
dense. Pairs with the file at `acestream_pilot.py`.

---

## The pipeline in one picture
```
DEFAULT_QUERIES (["nhl","sky sports",…])
        │
        ▼  fetch_channels()                         ← AceStream search API
{ "ab12…40hex": {name:"NHL", country:"us", availability:1, …}, … }
        │   (for each infohash)
        ▼  probe_dht(infohash)                      ← mainline BitTorrent DHT
( {("5.6.7.8",6881), …}  ,  {seeders:0, leechers:464} )
        │            │                  │
        │            │                  └── BEP-33 swarm estimate  = THE DEMAND NUMBER
        │            └── raw sampled peers          = used only for the geo split
        ▼  country_of(geo, ip)                      ← GeoLite2 mmdb
{"GB":41,"VN":50,"US":11, …}
        │
        ▼  main() writes rows → acestream_pilot.db (table acestream_demand)
```
Mental model: **`fetch_channels` = the catalog**, **`probe_dht` = the measurement**, **`country_of` = the
geo split**, **`main` = glue + persistence**. The bencode/parse helpers are plumbing for `probe_dht`.

---

## Function reference (input → sample output)

### `_benc(obj)` — encode Python → bencode bytes (KRPC wire format)
```python
_benc({b"t": b"\x00\x01", b"y": b"q"})
# → b'd1:t2:\x00\x011:y1:qe'      (d…e = dict, 2:.. = 2-byte string, sorted keys)
_benc(45000)        # → b'i45000e'
_benc([b"a", 1])    # → b'l1:ai1ee'
```

### `_bdec(bytes)` — decode bencode bytes → Python
```python
_bdec(b'd1: td2:id20:....e1:y1:re')
# → {b't': {b'id': b'....'}, b'y': b'r'}      (keys/strings stay BYTES, not str)
```
Note: everything comes back as `bytes` keys/values — that's why the code uses `r.get(b"values")`,
`b"BFsd"`, etc.

### `_parse_nodes(blob)` — compact node info → list of (node-id, ip, port)
Input: a `nodes` blob, N × 26 bytes (20-byte node-id + 4-byte IP + 2-byte port).
```python
_parse_nodes(b'\x9a..20..\xde' + b'\x43\xd7\xf6\x0a' + b'\x1a\xe1' + …)
# → [(b'\x9a…20-byte-id', '67.215.246.10', 6881), (b'…', '82.221.103.244', 6881), …]
```
The **node-id** matters: `probe_dht` XORs it with the infohash to measure "closeness" (Kademlia).

### `_parse_values(vals)` — compact peer list → set of (ip, port)
Input: the `values` list from a DHT response — each item 6 bytes (4 IP + 2 port). These are **actual
swarm peers** (people watching the channel).
```python
_parse_values([b'\x05\x06\x07\x08\x1a\xe1', b'\xd4\x0b\x1e\x2f\x1a\xe1'])
# → {('5.6.7.8', 6881), ('212.11.30.47', 6881)}
```

### `_estimate_bloom(bf)` — BEP-33 bloom filter (256 bytes) → swarm-size estimate
Input: a 256-byte (2048-bit) bloom filter where set bits ≈ how many distinct IPs announced.
```python
_estimate_bloom(bytearray(256))                 # all-zero filter → 0   (no announces)
_estimate_bloom(<filter with ~620 bits set>)    # → 464   (estimated leechers)
_estimate_bloom(<completely full filter>)        # → 50000 (capped — overflowed)
```
This is the headline metric. `BFsd` → seeders, `BFpe` → leechers.

### `fetch_channels(queries, page_size=50)` — search API → channel catalog
Input: `["nhl", "sky sports"]`. Output: dict keyed by 40-hex infohash.
```python
fetch_channels(["nhl"])
# → {
#   "ab12cd…(40 hex)": {
#       "name": "NHL", "categories": "sport", "country": "us",
#       "language": "en", "availability": 1, "bitrate": 3500
#   },
#   …
# }
```
Dedup rule: if the same infohash shows up under multiple queries, keep the sighting with the highest
`availability`.

### `probe_dht(ih_hex, budget=14)` — DHT walk → (peers, BEP-33)  ← see deep-dive below
```python
probe_dht("ab12cd…40hex")
# → ( {('5.6.7.8',6881), ('212.11.30.47',6881), …41 raw sampled peers…},
#     {"seeders": 0, "leechers": 464} )
```

### `make_geo(mmdb_path)` / `country_of(reader, ip)` — GeoIP
```python
geo = make_geo("/data/geoip/GeoLite2-Country.mmdb")   # → geoip2 Reader, or None if no mmdb
country_of(geo, "5.6.7.8")     # → "US"
country_of(None,  "5.6.7.8")   # → "??"   (graceful when mmdb absent)
```

### `main()` — glue: enumerate → probe → geo → write rows
One output row per (run_ts, infohash, peer_country). Sample row tuple:
```python
("2026-06-13T15:23:29Z", "ab12cd…", "Sky Sports Main Event [UK]", "sport",
 "gb", 1.0, "GB", 41, 0, 464)
#  run_ts                infohash    name                         cats  ch_cc avail peer_cc n seed leech
```

---

## Deep-dive: how `probe_dht` actually works (step by step)

It's a **converging Kademlia `get_peers` walk**: start far away, keep asking the nodes *closest to the
infohash* until time runs out, collecting peers + BEP-33 filters along the way.

**State it keeps:**
| var | holds | sample |
|---|---|---|
| `frontier` | nodes still to ask → their XOR distance to the infohash | `{('67.215.246.10',6881): 1<<161, …}` |
| `queried` | nodes already asked (don't re-ask) | `{('67.215.246.10',6881), …}` |
| `peers` | sampled swarm peers | `{('5.6.7.8',6881), …}` |
| `bfsd`,`bfpe` | OR-merged BEP-33 filters (256 B each) | `bytearray(256)` accumulating set bits |

**1. Seed the frontier** with the 4 bootstrap routers at "max distance" (`1<<161`) so they're queried
first:
```python
frontier = {('67.215.246.10',6881): 1<<161, ('82.221.103.244',6881): 1<<161, …}
```

**2. Each round, ask the 16 CLOSEST unqueried nodes.** The outgoing message (a `get_peers` query with
`scrape=1` to request BEP-33):
```python
q = {b"t": b"\x00\x01", b"y": b"q", b"q": b"get_peers",
     b"a": {b"id": MY_ID, b"info_hash": ih, b"scrape": 1}}
# _benc(q) → b'd1:ad2:id20:<MY_ID>9:info_hash20:<ih>6:scrapei1ee1:q9:get_peers1:t2:\x00\x011:y1:qe'
```

**3. Read replies for ~1.4 s.** Two response shapes come back:

*a) A far node that doesn't have the swarm — returns closer NODES:*
```python
{b'y': b'r', b'r': {b'id': b'<20>', b'token': b'<20>',
                    b'nodes': b'<26-byte chunks>'}}
# → _parse_nodes adds them to frontier with distance = node_id XOR infohash
```
*b) A close node that HAS the swarm — returns VALUES (peers) + BEP-33 filters:*
```python
{b'y': b'r', b'r': {b'id': b'<20>', b'token': b'<20>',
                    b'values': [b'\x05\x06\x07\x08\x1a\xe1', …],   # → peers
                    b'BFsd': b'<256 bytes>',                       # → OR into bfsd (seeders)
                    b'BFpe': b'<256 bytes>'}}                      # → OR into bfpe (leechers)
```
`values` get unioned into `peers`; `BFsd`/`BFpe` get **OR-merged** bit-by-bit into `bfsd`/`bfpe`
(bloom union per BEP-33).

**4. Converge:** newly-learned nodes are added to `frontier` keyed by `node_id XOR infohash`, so next
round's "16 closest" are nearer the infohash → more likely to hold the swarm. Loop until `budget`
seconds elapse.

**5. Return:** the sampled `peers` (for the geo split) and the BEP-33 estimate (the demand number):
```python
return peers, {"seeders": _estimate_bloom(bfsd), "leechers": _estimate_bloom(bfpe)}
```

---

## How to make `probe_dht` better

It currently does five jobs in one ~60-line function (socket, seed, send-batch, recv-parse, bloom-merge),
with bare loops and magic numbers. Suggested cleanups, in priority order:

### 1. Name the magic numbers (quick win)
```python
BATCH        = 16      # closest nodes asked per round
RECV_WINDOW  = 1.4     # seconds to collect replies after a send-batch
SOCK_TIMEOUT = 1.2
FILTER_LEN   = 256     # BEP-33 bloom filter bytes
FAR          = 1 << 161  # "unknown distance" so bootstraps sort first
```

### 2. Replace the byte-at-a-time bloom OR with a one-liner (clearer + faster)
```python
# instead of:  for i in range(256): bfsd[i] |= sd[i]
def _or_into(acc: bytearray, new: bytes) -> None:
    for i, b in enumerate(new):
        acc[i] |= b
# …or vectorized: bfsd = bytes(a | b for a, b in zip(bfsd, sd))
```

### 3. Extract response-handling into one helper (the biggest readability win)
Pull the "decode one datagram, update state" block out of the nested loop:
```python
def _ingest(data, ih_int, peers, bfsd, bfpe, frontier, queried):
    """Decode one KRPC reply; merge peers/filters; push new nodes onto the frontier."""
    try:
        r = _bdec(data).get(b"r")
    except Exception:
        return
    if not isinstance(r, dict):
        return
    if isinstance(r.get(b"values"), list):
        peers |= _parse_values(r[b"values"])
    for key, acc in ((b"BFsd", bfsd), (b"BFpe", bfpe)):
        f = r.get(key)
        if isinstance(f, bytes) and len(f) == FILTER_LEN:
            _or_into(acc, f)
    for nid, ip, port in _parse_nodes(r.get(b"nodes", b"")):
        if (ip, port) not in queried and (ip, port) not in frontier:
            frontier[(ip, port)] = int.from_bytes(nid, "big") ^ ih_int
```
Then `probe_dht`'s main loop becomes a readable skeleton: send batch → recv window calling `_ingest`.

### 4. (Bigger) Make it a small `DHTProbe` class
State (`peers`, `bfsd`, `bfpe`, `frontier`, `queried`) lives on `self`; methods `seed()`, `send_round()`,
`recv(window)`, `result()`. Turns the 60-line function into ~4 short methods and makes it unit-testable
(feed a canned datagram into `_ingest`, assert the parsed peers/filters — using the sample data above).

### 5. Bound the frontier (robustness)
`frontier`/`queried` grow every round; on a long budget they can balloon. Cap with e.g. keep only the
closest ~512 frontier entries each round, and stop early if no new nodes/peers arrived for 2 rounds
(a "dry" check, like the title pipeline).

### 6. Add type hints + a one-line return-shape docstring
`def probe_dht(ih_hex: str, budget: float = 14) -> tuple[set[tuple[str,int]], dict[str,int]]:` — makes
the "(peers, {seeders,leechers})" contract obvious without reading the body.

### The real refactor (production)
The cleanest "better" is to **delete most of `probe_dht`** and call
`dht_peer_count.get_peers_by_country` instead — it already has the shared warm node pool, concurrency,
multi-socket, and BEP-33, and counts more peers. The self-contained prober exists only so the pilot runs
anywhere; once on the box, the production engine is both simpler (less code here) and higher-recall. See
`ACESTREAM_PILOT_FINDINGS.md` → "swap pilot prober" follow-up.

---

## Quick test recipe (verify behavior with the samples above)
```bash
# one channel, short budget, no DB write noise:
python3 acestream_pilot.py --queries "nhl" --limit 1 --budget 10 --db /tmp/t.db
# expect: leechers≈swarm size (0 if no live game), sample=raw peers, geo split if mmdb present
```
