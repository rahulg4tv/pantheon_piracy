# 03 — `bep51_crawler.py` (Code Reference)

> **Role:** Widest-coverage hash discovery. Uses **BEP-51 `sample_infohashes`** to pull
> raw info_hashes straight off the DHT (millions/day), resolves each to a real title
> (torrent-cache → optional BEP-09 direct fetch), keeps only media, and inserts into
> `hashes_v2.db`. This is how we find content the indexers never list.

---

## Where it runs
- **cron:** `02:30 UTC` → `bep51_crawler.py --duration 30 --sockets 2 --insert --filter-media --min-num 10 --bep09`.
- **Key args:** `--duration` (crawl seconds) · `--sockets` (UDP sockets) · `--min-num` (min DHT "num" to keep) · `--insert` · `--filter-media` (only insert resolved media) · `--bep09` (enable the high-hit-rate direct-fetch pass) · `--resolve-workers`.

## Data flow
```
DHT  →(sample_infohashes)→  raw hashes  →  resolve_media_batch  →  _match_catalog_title
                                              (cache → BEP-09)            ↓
                                          insert_discovered_hashes → hashes_v2.db.hashes
```

---

## Discovery — BEP-51 crawl

### `class BEP51Transport` (715)
asyncio UDP protocol; `send_sample_infohashes(addr, target)` issues the BEP-51 query (asks a node for a sample of infohashes it knows near `target`), with a tid→Queue transaction table like the DHT counter.

### `sample_one(addr, target, transport, timeout, rate_limited)` (954)
One `sample_infohashes` query. **Respects per-node rate limits:** a node's reply carries an `interval`; we skip that node for `interval` seconds (`rate_limited` dict) — BEP-51 etiquette, avoids bans.

### `run_crawler(duration, concurrency, num_sockets, …)` (987)
The crawl loop:
- Creates N UDP sockets (each a distinct node_id → probes different DHT slices).
- Seeds a node queue from bootstrap + the saved node pool (`load_node_pool`, capped 5000, shuffled).
- **Keyspace rotation:** `_next_target()` cycles the target's first byte through all 256 values so we sample every corner of the DHT, not just near our id.
- `process_one` per node (semaphore-bounded): `sample_one` → collect returned hashes; new ones (not in `known_hashes`, `num ≥ min_num`) go to `new_hashes`; discovered nodes extend the queue. Runs until `duration` elapses; returns `(new_hashes, stats)`.

---

## Resolution — hash → real title

### `resolve_hash_media(hash_hex, bep09=False)` (499)
Two strategies in order: (1) **torrent cache** (`_resolve_via_torrent_cache`, itorrents.org→torrage.info — fast, ~0.1% hit), (2) **BEP-09 direct fetch** (`_resolve_via_bep09`, opt-in — slower, ~30–50% hit). Returns `{name, category, seeders}` or `None`.

### `resolve_media_batch(new_hashes, …, enable_bep09)` (569)
Two-pass pipeline over the batch (sorted by DHT `num` desc — busiest first):
- **Pass 1** torrent cache, 40 workers, all candidates (capped).
- **Pass 2** BEP-09 (if enabled), 15 workers, over the top un-resolved (`_BEP09_BATCH_CAP`).
`_run_resolve_pass` (521) drives each pass with a thread pool + progress/hit-rate logging.

### BEP-09 internals
`_get_peers_dht` (258) — find peers for the hash via DHT `get_peers`. `_fetch_bep09_metadata(ip, port, …)` (356) — open a BT connection and pull the metadata (`ut_metadata`) extension → the torrent's `info` dict → name. `_send_bt_msg`/`_recv_bt_msg` (220/225) — length-prefixed BT wire framing. `_bdecode_name_from_torrent` (159) — extract `name` from a bencoded torrent.

### Categorization
`_guess_category(title)` (147) / `_is_adult(title)` (142) — keyword classify Movie/Series/Anime and drop adult/junk.

---

## Catalog match + insert

### `_match_catalog_title(conn, raw_title)` (842) + `_normalise_title` (824)
Match a resolved torrent name against the `titles` catalog → `(ip_id, canonical_title)` or `None`. The map-to-Pantheon step at discovery time.

### `insert_discovered_hashes(new_hashes, today, …)` (881)
Insert into `hashes`. With `--filter-media`, **only inserts hashes resolved to real Movies/Series/Anime** (placeholder `BEP-51 Discovery`/`Unknown` rows are NOT inserted — this is what keeps junk out and means no separate enrichment pass is needed). `load_known_hashes` (801) — preloads existing hashes so the crawl only surfaces genuinely new ones.

### `main()` (1197)
Parse args → `run_crawler` for `--duration` → `resolve_media_batch` (with `--bep09`) → `insert_discovered_hashes` (gated by `--filter-media`) → optional `write_csv` → summary.

---

## Gotchas / invariants (for reviewers)
- **Respect BEP-51 `interval` rate limits** (`sample_one`) — ignoring them gets nodes to stop responding.
- **`--filter-media` is the junk gate** — without it the DB fills with unresolved `BEP-51 Discovery`/`Unknown` placeholders (the legacy mode `enrich.py` used to clean; now retired). The 02:30 cron uses `--filter-media`.
- **BEP-09 is opt-in** (`--bep09`) — much higher hit rate but slower/heavier (direct TCP to peers); the cron enables it within the 30-min budget.
- Keyspace-prefix rotation matters for breadth — without it the crawl only samples near our own node id.
- `--min-num` filters low-popularity noise at the source.

## Change history
Stable discovery component; the §44 release-group-strip note applies to the *matcher in `trending`*, but `_match_catalog_title` here is the bep51-side equivalent. `--filter-media` + BEP-09 are the current cron mode.
