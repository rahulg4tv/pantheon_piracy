# output/ — secondary export & compaction

The primary feeds live in root (`merge_and_upload.py` → S3, `export_nbcu.py` → the daily per-country CSV feed).

- **compact_peer_counts.py** — daily CSV → partitioned Parquet compaction (cron)
- **db_export_eu.py** — export active hashes/peers to the EU bootstrap DB (systemd `db-export-eu`)
