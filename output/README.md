# output/ — secondary export paths

The primary feeds live in the repo root: [`../export_nbcu.py`](../export_nbcu.py)
(daily per-country CSV) and [`../merge_and_upload.py`](../merge_and_upload.py)
(rolled-up feed to S3). This folder holds the two paths that exist for
*efficiency* rather than for the product itself.

## `compact_peer_counts.py` — CSV to partitioned Parquet

The collectors append raw per-worker CSVs continuously; a day is millions of
rows across several files. Scanning those for any historical question is slow and
repetitive, so this compacts each finished day into date-partitioned Parquet.

Analytical queries then read the Parquet tree instead of the CSVs — the same
numbers, a fraction of the I/O.

**Do not delete the source CSVs on the same day they are compacted.** The daily
merge reads them, and it runs after compaction; removing them breaks the feed.

## `db_export_eu.py` — the EU collection node

Peer geography is measured from where the *peers* are, but which peers you can
reach depends on where you crawl from. A second node in another region improves
coverage of European swarms.

This exports the active hash set so that node knows what to collect. It ships
**hashes outward**, not peer IPs inward — worth knowing when reasoning about
where a given number was observed.
