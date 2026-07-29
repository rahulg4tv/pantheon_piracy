# docs/ — what to read, in what order

Start with the [root README](../README.md) for the four-stage flow. Then:

## Read these first, in this order

| | Doc | Why |
|---|---|---|
| 1 | [`WORKED_EXAMPLE.md`](WORKED_EXAMPLE.md) | **The fastest way in.** One film followed end to end with tiny numbers you can verify by hand — matching, the distinct-IP union, geo, the output row. |
| 2 | [`DATA_MODEL.md`](DATA_MODEL.md) | The same ideas stated precisely: infohash, `ip_id`, the distinct-IP union, the day window. Almost every design decision follows from these four, and most past bugs came from getting one slightly wrong. |
| 3 | [`09_END_TO_END_FLOW.md`](09_END_TO_END_FLOW.md) | The same flow as the root README, but with every data store and the cron/systemd wiring. |
| 4 | [`00_OVERVIEW.md`](00_OVERVIEW.md) | Architecture overview and the schedule. |

## Per-component reference

Each numbered doc covers one script — what it does, how it is invoked, and the
non-obvious decisions inside it.

| Doc | Component | Stage |
|---|---|---|
| [`02_trending_hash_collector.md`](02_trending_hash_collector.md) | Catalog matching + targeted search | 1 — pick hashes |
| [`03_bep51_crawler.md`](03_bep51_crawler.md) | BEP-51 DHT infohash sampling | 1 |
| [`04_collect.md`](04_collect.md) | Indexer/API collection + enrichment | 1 |
| [`01_dht_peer_count.md`](01_dht_peer_count.md) | DHT `get_peers` swarm sampling | 2 — count peers |
| [`07_tracker_harvest_service.md`](07_tracker_harvest_service.md) | Tracker announce harvesting | 2 |
| [`06_merge_and_upload.md`](06_merge_and_upload.md) | Roll-up, union, S3 upload | 3 — export |
| [`08_export_nbcu.md`](08_export_nbcu.md) | Daily per-country distinct-IP feed | 3 |
| [`05_prune_dead_hashes.md`](05_prune_dead_hashes.md) | Retention / dead-hash pruning | ops |

## Operations

| Doc | Use it when |
|---|---|
| [`11_NEW_BOX_SETUP.md`](11_NEW_BOX_SETUP.md) | Building a host from scratch — services, cron, directory layout |
| [`10_ops_runbook.md`](10_ops_runbook.md) | Something is broken: WAL growth, stalled collection, failed export |

## Design & research

| Doc | Subject |
|---|---|
| [`MATCHING_QUALITY_DESIGN.md`](MATCHING_QUALITY_DESIGN.md) | How torrent names are matched to catalog titles, and the failure classes |
| [`STREAMING_COVERAGE_RUNBOOK.md`](STREAMING_COVERAGE_RUNBOOK.md) | The web-streaming channel |
| [`ACESTREAM_PILOT_FINDINGS.md`](ACESTREAM_PILOT_FINDINGS.md) | AceStream live-sports pilot — findings and go/no-go |
| [`Peer_Discovery_DHT_Tracker_PEX.pdf`](Peer_Discovery_DHT_Tracker_PEX.pdf) | Background on the three peer-discovery mechanisms |
| [`12_LEARNING_GUIDE.pdf`](12_LEARNING_GUIDE.pdf) | Guided introduction to the protocols |
| [`13_SYSTEM_COMPARISON.pdf`](13_SYSTEM_COMPARISON.pdf) | Comparison against alternative measurement approaches |
| [`Pantheon_Piracy_Overview.pptx`](Pantheon_Piracy_Overview.pptx) | Slide overview for a non-engineering audience |

`code_maps/` holds per-file structural maps (functions, call graph) for quick
orientation in the larger scripts.

## If you only have ten minutes

Read the [root README](../README.md) flow diagram, then
[`WORKED_EXAMPLE.md`](WORKED_EXAMPLE.md) — one film, tiny numbers, the whole
pipeline. That is enough to read any number the system produces and know what it
means, including what it deliberately does not mean.
