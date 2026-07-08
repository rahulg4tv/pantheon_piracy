# Harvest subsystem — Navigation Map

Read-only structural index of the four tracker/PEX harvest files —
`tracker_harvest.py` (233 L, 7 fns), `tracker_harvest_service.py` (291 L, 13 fns
incl. 2 `country_of` defs in an import-fallback try/except),
`pex_harvest.py` (155 L, 10 fns), `harvest_velocity.py` (120 L, 4 fns).
**The source files are unchanged.** Line numbers as of 2026-06-09.

> Mental model: `dht_peer_count.py` only ever sees a structural SAMPLE of each
> swarm (peers announced to the ~8 DHT nodes XOR-closest to the infohash); its
> all-time 16-day distinct-IP union ≈ NBCU's *single-day* count. The harvest
> subsystem closes that gap: `tracker_harvest.py` is the stateless engine
> (BEP-15 UDP + HTTP announce → distinct peer IPs); `tracker_harvest_service.py`
> loops it over the hot-hash worklist all day, GeoIP-buckets, and writes a
> SEPARATE `harvest_peers.db` — because the 7 DHT workers hold continuous read
> locks on the shared `hashes_v2.db` that block WAL-checkpoint frame reclamation,
> so any write volume there inflates the shared WAL unboundedly (~1GB/min).
> `pex_harvest.py` (BEP-11 ut_pex) and `harvest_velocity.py` (fast re-harvest of
> surging new releases) are two more isolated peer sources; `export_nbcu.py`
> ATTACHes all of them and unions distinct IPs up to NBCU magnitude.

---

## The two-file pipeline in one picture
```
hashes_v2.db (shared, read-only)
        │
        ▼  load_hashes(25000)                         ← worklist: seeders DESC, fresh-first, DHT recency
["e0a1…(40 hex)", "7b3c…", …25000 infohashes…]
        │   (for each infohash, CONC=16 in parallel)
        ▼  harvest_infohash(ih, rounds=4)             ← tracker_harvest.py engine
{("82.45.6.7",6881), ("1.2.3.4",51413), …~1,800 (ip,port)…}   ← UDP+HTTP announce, all trackers
        │
        ▼  _public_only(peers)                        ← drop private/multicast/bogon IPs
{("82.45.6.7",6881), …~1,750…}
        │
        ▼  country_of(ip)  (per distinct IP)          ← GeoLite2-Country.mmdb
{"82.45.6.7":"GB", "1.2.3.4":"US", …}
        │
        ▼  _upsert(hash, {ip:country}, "2026-06-15")  ← harvest_peers.db (OUR db, single writer)
rows: (hash, ip, country, first_seen, last_seen)
```
Mental model: **`tracker_harvest.py` = the measurement** (talk to trackers, get
raw peers), **`tracker_harvest_service.py` = the loop + geo + persistence**
(worklist → engine → GeoIP → our DB), running all day so swarm churn accumulates
the daily distinct-IP union. The `_udp_announce` / `_http_announce` helpers are the
wire-protocol plumbing; everything else is glue.

---

## tracker_harvest.py
The engine: given an infohash, announce (BEP-15 UDP + HTTP/compact) to a fixed
pool of public trackers over several rounds and return distinct peer IP:ports.
Stateless / importable (`harvest_infohash`), no DB.

- `L66 _rand_peer_id` — fresh Azureus-style random peer_id per round (tracker hands a different swarm slice each time)
- `L72 _parse_compact_peers` — decode compact 6-byte (IPv4+port) peer blob → set of (ip, port)
- `L81 _udp_announce` — BEP-15 UDP connect-handshake + announce loop for one tracker, paging `rounds` times, breaks when a round adds no new IPs
- `L142 _http_announce` — HTTP/compact announce loop for one tracker (urllib), same paging/early-exit
- `L173 _extract_bencode_peers` — pull the compact `5:peers<len>:<bytes>` field out of an HTTP tracker body without a full bencode parser
- `L192 harvest_infohash` — **public entry**: fan out to every UDP+HTTP tracker in a ThreadPool, union all peer IP:ports
- `L209 _public_only` — filter a peer set to globally-routable, non-multicast IPs

Module constants: `UDP_TRACKERS` L40, `HTTP_TRACKERS` L54, `_PROTOCOL_ID` L60,
`_ANNOUNCE_PORT` L61, `_UDP_TIMEOUT` L62, `_NUMWANT` L63. CLI entrypoint L221.

## tracker_harvest_service.py
The continuous systemd service: loop the engine over the popularity-ordered hot
worklist, GeoIP-bucket, and write the separate `harvest_peers.db` peers table
(single writer → its WAL self-checkpoints).

- `L60 _utc_date` — today's UTC date string (matches the date the merge reads)
- `L64 _find_geodb` — locate the GeoLite2 `.mmdb` on disk (glob of known paths)
- `L77 country_of` — IP → ISO country via `geoip2` reader (primary path; `'XX'` on miss)
- `L85 country_of` — IP → ISO country via `maxminddb` (ImportError fallback path)
- `L95 _get_db` — lazy per-thread WAL sqlite connection to `HARVEST_DB` (never shared across threads)
- `L105 _init_harvest_db` — create the `peers` table + `last_seen` index in our own DB (same schema/PK as the DHT collector's)
- `L124 _upsert` — upsert one hash's `{ip: country}` (or a `_queried_` sentinel row), ON CONFLICT update last_seen, one txn
- `L144 load_hashes` — **worklist**: read-only against shared DB, order by seeders DESC, fresh-hash (≤2d) tiebreak, then DHT peer recency
- `L188 harvest_hash` — harvest one hash (engine → `_public_only` → GeoIP) and upsert it
- `L197 run_cycle` — one full cycle: load worklist, ThreadPool over `harvest_hash`, progress logging
- `L216 _prune_old` — delete `(hash, ip)` rows older than `RETENTION_DAYS` (churned-out, dead weight)
- `L242 _checkpoint_loop` — daemon thread: periodic `wal_checkpoint(TRUNCATE)` + hourly prune on `HARVEST_DB`
- `L266 main` — **entrypoint**: init DB, start checkpoint thread, loop `run_cycle` with `CYCLE_SLEEP` (or one-shot)

DB targets: `READ_DB` (shared, read-only) L44, `HARVEST_DB`/`DB_PATH` (our writes) L45–46.

---

## Function reference (input → sample output)
Concrete shapes for every notable function in the two tracker-harvest files. Types
and sample values are taken from the real code (peer sets are
`set[tuple[str, int]]`, GeoIP returns ISO-2 strings or `"XX"`, DB writes return
the int IP-count). Use these to know exactly what each call hands back without
reading the body.

### tracker_harvest.py — the stateless engine

#### `_rand_peer_id()` → 20-byte BitTorrent peer_id
Fresh per call (per round) so the tracker hands a *different* random slice of the
swarm each time → more unique IPs.
```python
_rand_peer_id()
# → b'-HT0001-\x3f\x91\xaa\x02\x7c\xd5\x10\x88\xe4\x6b\x19\xff'
#   (8-byte Azureus-style prefix "-HT0001-" + 12 random bytes = 20 bytes total)
```

#### `_parse_compact_peers(blob)` → set of (ip, port)
Input: the compact peer field from a tracker response — N × 6 bytes (4-byte IPv4 +
2-byte big-endian port). No DNS, pure unpack.
```python
_parse_compact_peers(b'\x52\x2d\x06\x07\x1a\xe1' + b'\x01\x02\x03\x04\xc8\xd5')
# → {('82.45.6.7', 6881), ('1.2.3.4', 51413)}
_parse_compact_peers(b'')        # → set()      (empty / no peers)
```

#### `_udp_announce(host, port, info_hash, rounds)` → set of (ip, port)  ← see deep-dive below
One tracker, BEP-15 UDP: connect-handshake then announce, repeated up to `rounds`
times (each round a fresh peer_id/key → new swarm slice), early-exit when a round
adds no new IPs. Returns just this tracker's peers.
```python
_udp_announce("tracker.opentrackr.org", 1337, bytes.fromhex("e0a1…40hex"), 4)
# → {('82.45.6.7', 6881), ('203.0.113.9', 51413), …~400 (ip,port)…}
_udp_announce("dead.tracker.invalid", 1337, ih, 4)   # → set()   (DNS/socket fail → empty, never raises)
```

#### `_http_announce(url, info_hash, rounds)` → set of (ip, port)
Same idea over HTTP/compact via urllib (GET with `compact=1&event=started`). Loops
`rounds` times, breaks on error or when no new peers arrive.
```python
_http_announce("https://tracker.gbitt.info:443/announce", ih, 4)
# → {('98.7.6.5', 6881), …~120 (ip,port)…}
_http_announce(url, ih, 4)        # → set()   (HTTP error / non-compact body → empty)
```

#### `_extract_bencode_peers(body)` → raw compact peers bytes
Input: a full HTTP tracker response body (bencoded). Finds the `5:peers<len>:<bytes>`
field and slices out just the `<bytes>` — no full bencode parser. Output feeds
straight into `_parse_compact_peers`.
```python
_extract_bencode_peers(b'd8:intervali1800e5:peers12:\x52\x2d\x06\x07\x1a\xe1\x01\x02\x03\x04\xc8\xd5e')
# → b'\x52\x2d\x06\x07\x1a\xe1\x01\x02\x03\x04\xc8\xd5'   (12 bytes = 2 peers)
_extract_bencode_peers(b'd14:failure reason…e')          # → b''   (no peers field)
```

#### `harvest_infohash(ih_hex, rounds=4)` → set of (ip, port)  ← public entry point
Fan out to every UDP + HTTP tracker in a ThreadPool, union all their peer sets.
Failures per-tracker are swallowed, so the union is best-effort. Includes private/
bogon IPs at this stage (caller filters).
```python
harvest_infohash("e0a1b2c3…40hex", rounds=4)
# → {('82.45.6.7', 6881), ('1.2.3.4', 51413), ('10.0.0.5', 6881), …~1,800 (ip,port)…}
harvest_infohash("0000…dead infohash", rounds=4)   # → set()   (no tracker has the swarm)
```

#### `_public_only(peers)` → set of (ip, port), filtered
Drop anything not globally routable (private RFC1918, loopback, multicast, bogon).
Same tuple shape in, smaller set out.
```python
_public_only({('82.45.6.7',6881), ('10.0.0.5',6881), ('127.0.0.1',6881)})
# → {('82.45.6.7', 6881)}        (private + loopback dropped)
```

### tracker_harvest_service.py — the loop, geo, persistence

#### `_utc_date()` → today's UTC date string
Date-only (no time) so it matches the `last_seen = <date>` rows the merge reads.
```python
_utc_date()        # → "2026-06-15"
```

#### `_find_geodb()` → path to a GeoLite2 .mmdb, or None
Globs known locations; first hit wins. `None` → `main()` aborts (FATAL).
```python
_find_geodb()      # → "/data/geoip/GeoLite2-Country.mmdb"
_find_geodb()      # → None   (no mmdb anywhere on disk)
```

#### `country_of(ip)` → ISO-2 country code (or "XX")
Two definitions selected at import time: `geoip2` reader (primary) or `maxminddb`
(ImportError fallback). Both have the SAME contract — ISO-2 string, `"XX"` on any
miss/error. Never raises.
```python
country_of("82.45.6.7")     # → "GB"
country_of("1.2.3.4")       # → "US"
country_of("203.0.113.9")   # → "XX"   (not in DB / private / lookup error)
```

#### `_get_db()` → sqlite3.Connection (per-thread)
Lazily opens (once per thread) a WAL connection to `HARVEST_DB` with a 60s busy
timeout; cached on `threading.local`. Never shared across threads.
```python
_get_db()          # → <sqlite3.Connection to /data/db/harvest_peers.db>
```

#### `_init_harvest_db()` → None (side effect: creates table + index)
Creates `peers(hash, ip, country, first_seen, last_seen)` PK `(hash, ip)` and the
`last_seen` index in our own DB — same schema/PK as the DHT collector's so
`export_nbcu.py` can union them identically. Returns nothing.
```python
_init_harvest_db()     # → None   (peers table + idx_peers_lastseen now exist)
```

#### `_upsert(hash_val, ip_country, today)` → int (count of IPs written)
Insert/update one hash's peers in one transaction (ON CONFLICT → refresh
`country` + `last_seen`). If `ip_country` is empty, writes a single `_queried_`
sentinel row instead (records "we looked, found nothing"). Returns `len(ip_country)`.
```python
_upsert("e0a1…", {"82.45.6.7": "GB", "1.2.3.4": "US"}, "2026-06-15")
# → 2          (two (hash, ip) rows upserted)
_upsert("e0a1…", {}, "2026-06-15")
# → 0          (no peers; one '_queried_' sentinel row written instead)
```

#### `load_hashes(limit)` → list of infohash hex strings  ← the worklist
Read-only query against the shared DHT DB. Ordered seeders DESC (popularity),
then fresh-hash (`first_seen` within 2 days) DESC, then DHT peer recency DESC.
Returns up to `limit` hashes, most-worth-harvesting first.
```python
load_hashes(25000)
# → ["e0a1b2c3…(40 hex)", "7b3c9f10…", "a1d4…", …up to 25000 hashes…]
load_hashes(3)
# → ["e0a1b2c3…", "7b3c9f10…", "a1d4e5f6…"]
```

#### `harvest_hash(ih, today)` → int (IPs written for this hash)
The per-hash unit of work: run the engine → `_public_only` → GeoIP-bucket each
distinct IP → `_upsert`. Engine failures are caught (treated as empty swarm).
Returns the upsert count.
```python
harvest_hash("e0a1b2c3…", "2026-06-15")
# → 1750       (1,750 distinct public IPs harvested + geo-tagged + written)
harvest_hash("dead…", "2026-06-15")
# → 0          (no peers; '_queried_' sentinel written)
```

#### `run_cycle()` → (hashes_processed, total_ip_writes)
One full pass: load the worklist, ThreadPool `harvest_hash` over it (CONC=16),
log progress every 2000 hashes. Returns the cycle totals.
```python
run_cycle()
# → (25000, 4120335)      (25k hashes harvested, ~4.1M ip-writes this cycle)
```

#### `_prune_old()` → int (rows deleted)
Delete every `(hash, ip)` whose `last_seen` is older than `RETENTION_DAYS` —
churned-out IPs that are dead weight. Best-effort; returns `0` on any error.
```python
_prune_old()       # → 812043   (rows older than the 4-day cutoff deleted)
_prune_old()       # → 0        (nothing stale, or a transient DB error)
```

#### `_checkpoint_loop(interval=90)` → never returns (daemon thread)
Infinite loop: every `interval` s run `wal_checkpoint(TRUNCATE)` on `HARVEST_DB`
(reclaims WAL frames — possible because we're the only writer), and roughly hourly
call `_prune_old`. Has no return value; runs for the life of the process.

#### `main()` → never returns normally (process entrypoint)
Abort if no GeoDB; else init DB, prune once, start the checkpoint thread, then
loop `run_cycle` forever sleeping `CYCLE_SLEEP` between cycles (or one cycle and
exit when `ONESHOT=1`). Each cycle prints a summary line:
```
[cycle 7 2026-06-15] harvested 25,000 hashes, 4,120,335 ip-writes in 98s
```

---

## Deep-dive: how `_udp_announce` actually works (step by step)
This is the most intricate function — the BEP-15 UDP tracker protocol that turns
one (tracker, infohash) into a set of peer IP:ports. Everything else in the engine
is fan-out (`harvest_infohash`) or parsing around it.

**What it returns:** `set[tuple[str, int]]` — the distinct peers this one tracker
reported across all rounds. Always a set, never raises (every socket error path
`break`s out and returns what's accumulated so far, possibly empty).

**1. Resolve the tracker, open a UDP socket.** DNS failure → return `set()`
immediately (this tracker is simply skipped by the caller):
```python
addr = (socket.gethostbyname("tracker.opentrackr.org"), 1337)   # → ("198.51.100.7", 1337)
sock = socket.socket(AF_INET, SOCK_DGRAM); sock.settimeout(2.5)
```

**2. Per round — the connect handshake.** Send the BEP-15 magic
`_PROTOCOL_ID = 0x41727101980` with action=0 and a random transaction id; the
tracker replies with a 64-bit `connection_id`:
```python
req  = struct.pack(">QII", 0x41727101980, 0, txn)        # → 16 bytes out
resp = sock.recv(16)                                      # ← b'\x00\x00\x00\x00<txn>\x9a\xf3…' (action=0, conn_id)
conn_id = struct.unpack(">Q", resp[8:16])[0]             # → e.g. 0x9af3c2…  (valid ~1 min)
```
Bad length / action≠0 / mismatched txn → `break` (give up on this tracker).

**3. Send the announce.** A 98-byte packet: `conn_id`, action=1, fresh txn, the
20-byte `info_hash`, a fresh `_rand_peer_id()`, a nonzero random `left` (so we look
like a leecher), `numwant=200`, port 6881. Note downloaded/uploaded=0 and
event=0 — we read the swarm without ever transferring data:
```python
req = struct.pack(">QII20s20sQQQIIIiH", conn_id, 1, txn, info_hash,
                  _rand_peer_id(), 0, random.getrandbits(40), 0, 0, 0,
                  random.getrandbits(32), 200, 6881)      # → 98 bytes out
resp = sock.recv(4096)                                    # ← announce reply
```

**4. Parse the reply → peers.** Reply header is `action=1, txn`, then
`interval, leechers, seeders` (skipped), then the compact peer list from byte 20
onward. Those bytes go through `_parse_compact_peers` and get unioned in:
```python
action, rtxn = struct.unpack(">II", resp[:8])     # expect action==1, rtxn==txn
peers |= _parse_compact_peers(resp[20:])          # the 6-byte-per-peer tail
```

**5. Page across rounds, early-exit when dry.** After each round, if `peers` did
not grow, the tracker is handing back the same slice → `break`. Otherwise loop
again with a *new* peer_id/key so the tracker rotates a different random slice in:
```python
if len(peers) == before:    # this round added nothing new
    break
```

**6. Return** the accumulated set (closed socket in `finally`):
```python
return peers     # e.g. {('82.45.6.7',6881), ('203.0.113.9',51413), …~400…}
```
`harvest_infohash` does this for all 12 UDP + 3 HTTP trackers in parallel and
unions every set → the full ~1,800-peer raw result before `_public_only` filtering.

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Add/remove a tracker | `UDP_TRACKERS` tracker_harvest.py L40, `HTTP_TRACKERS` L54 |
| Change announce rounds / paging | `_udp_announce` L81 / `_http_announce` L142 (loop); default `rounds` arg in `harvest_infohash` L192; `ROUNDS` env in services |
| Change `numwant` / announce port | `_NUMWANT` L63, `_ANNOUNCE_PORT` L61 (tracker_harvest.py) |
| Filter peer IPs (public-only) | `_public_only` tracker_harvest.py L209 |
| Change per-cycle hash cap | `MAX_HASHES` L47 → `load_hashes` L144 / `run_cycle` L199 (tracker_harvest_service.py) |
| Change service concurrency | `CONC` L49 → `run_cycle` L202 (service); `VEL_CONC`→`CONC` L28 harvest_velocity.py; `PEX_CONC`/`PEX_HASH_CONC` L30–31 pex_harvest.py |
| Change worklist priority / ordering | `load_hashes` tracker_harvest_service.py L144; `hot_set` harvest_velocity.py L46; `worklist` pex_harvest.py L116 |
| Change GeoIP bucketing | `country_of` service L77/L85 (+ `_find_geodb` L64); `country` pex_harvest.py L34 |
| Change which DB is written | `HARVEST_DB`/`DB_PATH` service L45–46; `PEX_DB`/`DB_PATH` pex_harvest.py L23; `VEL_DB` harvest_velocity.py L24 |
| Change upsert / table schema | `_upsert` L124 / `_init_harvest_db` L105 (service); `init_db` L90 pex; `DDL`/`UPSERT` L35/L38 + `run` L90 velocity |
| Change retention / WAL checkpoint | `_prune_old` L216 / `_checkpoint_loop` L242 (service); `run` L96–99 (velocity) |
| Change PEX handshake / timeout / duration | `_pex_peer` L60 (connect timeout 8s, `PSTR`/`EXTB` L26–27); `DUR`/`SEED_CAP` L28–29 pex_harvest.py |
| Change harvest-velocity hot-set criteria | `HOT_K`/`HOT_NEW_DAYS`/`HOT_MIN_SEED` L25–27 → `hot_set` L46 (harvest_velocity.py) |
| Change loop / refresh cadence | `CYCLE_SLEEP` service L50; `VEL_LOOP_SLEEP`/`VEL_REFRESH_SEC` L30–31 velocity; `--loop-delay` pex L145 |

## Config (env vars)
**tracker_harvest.py:** `ROUNDS` (4) — announce rounds in CLI mode.

**tracker_harvest_service.py:** `MAX_HASHES` (25000) — hashes harvested per cycle;
`ROUNDS` (4) — announce rounds per tracker per hash; `CONC` (16) — concurrent
hashes; `CYCLE_SLEEP` (30) — seconds between cycles; `ONESHOT` (=1 → single
cycle and exit); `RETENTION_DAYS` (4) — prune (hash,ip) older than this.

**pex_harvest.py:** `PEX_DB` (`/data/db/pex_peers.db`); `PEX_DUR` (60) — per-hash
PEX collection seconds; `PEX_SEED_CAP` (60) — seed peers per hash; `PEX_CONC`
(40) — concurrent seed connections; `PEX_HASH_CONC` (3) — hashes in parallel.
(CLI flags: `--once`, `--loop`, `--limit` 60, `--loop-delay` 600.)

**harvest_velocity.py:** `VEL_DB` (`/data/db/harvest_velocity_peers.db`); `HOT_K`
(400) — hot-set size; `HOT_NEW_DAYS` (3) — freshness window; `HOT_MIN_SEED`
(150) — min seeders; `VEL_CONC` (24) — concurrency; `VEL_ROUNDS` (4) — announce
rounds; `VEL_LOOP_SLEEP` (8) — seconds between passes; `VEL_REFRESH_SEC` (300) —
hot-set refresh interval; `VEL_RETENTION_DAYS` (4); `VEL_CKPT_EVERY` (5) — passes
between TRUNCATE+prune; `ONESHOT_N` (30) — hashes for `--oneshot`.
