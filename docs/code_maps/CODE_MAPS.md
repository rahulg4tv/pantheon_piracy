# Code Maps — navigation index

Read-only structural maps for the large files in this repo. Each map lists every
function with its line number and a one-line purpose, plus a **"function reference
(input → sample output)"** section showing *what each function returns with a concrete
sample value*, and a "where to look for common tasks" table. **The source files are
never modified** — these exist so a big file can be navigated, and its data shapes
understood, without reading the code. Regenerate a map after large edits (line numbers drift).

| File | Lines | Map | Role in the pipeline |
|---|---:|---|---|
| `dht_peer_count.py` | 2,647 | [map](dht_peer_count_MAP.md) | **Channel-1 / count.** For each known infohash, walks the DHT (Kademlia) to the closest nodes, `get_peers`, GeoIP-buckets peer IPs → CSV + `peers` table. |
| `bep51_crawler.py` | 1,346 | [map](bep51_crawler_MAP.md) | **Channel-1 / discover.** BEP-51 DHT sampling to find NEW infohashes to track (feeds the worklist the counter consumes). |
| harvest subsystem (4 files) | 799 | [map](harvest_MAP.md) | **Channel-1 / scale.** `tracker_harvest.py` (announce engine) + `tracker_harvest_service.py` (continuous loop, separate DB) + `pex_harvest.py` (BEP-11 PEX) + `harvest_velocity.py` (worklist priority). Brings DHT's sample up to NBCU magnitude. |
| `trending_hash_collector.py` | 1,835 | [map](trending_hash_collector_MAP.md) | **Catalog onboarding.** Cron-driven: pulls trending titles (TMDB/AniList), searches torrent sources, matches names to the catalog (`_name_matches_title`), inserts new hashes. |
| `collect.py` | 846 | [map](collect_MAP.md) | Torrent/hash collection module (see map for active-vs-legacy status). |
| `merge_and_upload.py` | 623 | [map](merge_and_upload_MAP.md) | **Daily merge.** Unions DHT + harvest distinct IPs per (title, country), resolves `ip_id`, aggregates the demand feed, uploads to S3. |
| `export_nbcu.py` | 498 | [map](export_nbcu_MAP.md) | **Deliverable.** Emits the NBCU-equivalent per-title, per-country distinct-IP output. |
| `pantheon_web.py` | 703 | [map](pantheon_web_MAP.md) | **Dashboard.** Flask/gunicorn on :8090, API routes (incl. `/api/acestream`) + inline vanilla-JS/SVG frontend (Overview/Titles/Countries/Trends/Streaming/Live Sport). |
| `acestream_pilot.py` | 430 | [guide](ACESTREAM_PILOT_GUIDE.md) | **Channel-1 / live IPTV.** Resolves AceStream live channels → infohashes (search API) → mainline-DHT BEP-33 swarm demand → `acestream_pilot.db`. Guide includes the per-function sample I/O + a `probe_dht` step-by-step. |

## Pipeline at a glance
```
 bep51_crawler.py ─ discovers infohashes ─┐
                                          ├─► dht_peer_count.py ─┐
 trending_hash_collector.py ─ onboards ───┘   (DHT peer counts)  │
   catalog titles → hashes_v2.db                                 ├─► merge_and_upload.py ─► export_nbcu.py ─► S3
 tracker_harvest_service.py ─ tracker-announce peers ────────────┘    (union, ip_id, aggregate)   (NBCU format)
                                                                              │
                                                                  pantheon_web.py (dashboard reads the feed + DBs)
```

## How these were generated
Per file: read fully, extract every `def`/`class`/route with exact line numbers,
write a one-line purpose (from the docstring, or inferred from the code where
absent). All line numbers were cross-checked against the source (149 claims,
100% match as of 2026-06-09).
