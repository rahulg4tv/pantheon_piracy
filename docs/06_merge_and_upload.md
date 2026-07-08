# 06 — `merge_and_upload.py` (Code Reference)

> **Role:** The **LEGACY dashboard** feed builder. Merges per-pass DHT peer-count CSVs,
> canonicalizes titles, aggregates to `ip_id`, computes day-over-day velocity + spike
> alerts, and uploads a CSV to S3 for the existing dashboard. **DHT-only (weaker signal)** —
> the per-country demand product is `export_nbcu.py` (08), not this. Kept only to keep the
> dashboard alive. Pandas-based.

---

## Where it runs
- **systemd:** `merge-and-upload.timer` at `05/11/17/23:00 UTC` (4×/day).
- **Args:** `--date` (default: derived; see `get_date`).

## Data flow
```
/data/peer_counts/<date>_w*.csv  (DHT per-pass)  ┐
announce-only peers (DHT announce_peer hits)     ├─ merge → canonicalize → aggregate(ip_id)
titles catalog + hash metadata                   ┘        → velocity + spike alerts
                                                           → CSV → S3 (dashboard)
```

---

## Functions (pipeline order)

### Inputs
`get_date(args)` (58) — resolve the run date. `download_eu_csvs(date)` (69) — **legacy EU pull** (degrades gracefully if absent — EU node is retired, so this is now a no-op in practice). `load_peer_counts(date)` (94) — read the day's DHT per-pass CSVs into a DataFrame. `load_announce_only_peers(date, csv_hashes)` (143) — fold in peers seen only via `announce_peer` (not in the pass CSVs). `dedup_passes(df)` (207) — collapse multiple passes to distinct per hash/country.

### Canonicalization (the §2/§3 fix area)
`load_hash_metadata` (230) — hash → title/category/ip_id. `normalize_title` (246) / `load_titles_catalog` (253) — catalog for title canonicalization. `build_canonical_map(titles_df, …)` (264) — build alias→canonical-title + canonical-id maps (fixes the ip_id fragmentation where one title had many ids). `canonicalize(df, alias_map, canon_title)` (337) — apply it. **Reviewer note:** this is the legacy equivalent of the catalog mapping `export_nbcu.py` does; the §2/§3/§16/§17 work fixed fragmentation here so dashboard rankings matched.

### Aggregate + signals
`merge_metadata(peer_df, meta_df)` (355) — attach title/category. `aggregate_to_ip_id(df)` (363) — sum to one row per ip_id (+ country breakdown). `add_peer_velocity(df, date)` (404) — day-over-day delta vs yesterday's output. `send_spike_alerts(df, date)` (452) — SNS alert on big movers.

### Output
`upload_to_s3(local_path, date)` (535) — push the CSV to S3. `main()` (544) — runs the chain end to end.

---

## Gotchas / invariants (for reviewers)
- **Legacy / DHT-only** — weaker than the harvester-backed `export_nbcu.py`. Don't confuse this with the demand feed; this exists for the dashboard only.
- **EU path is vestigial** — `download_eu_csvs` no-ops now that the EU node is retired (`00_OVERVIEW`); the graceful-absence handling means it just logs "continuing without EU data".
- **Ranking uses BEP-33 leechers, not raw DHT peer_count** (§24) — peer_count is a weak sample; the canonicalization (§2/§3) is what made dashboard order match NBCU historically.
- Pandas in-memory — fine at current volumes; it's a once-per-run batch, not a hot path.

## Change history
`SESSION_CHANGES.md` §2/§3 (ip_id fragmentation + ranking), §16/§17 (catalog ip_id + remap), §24 (ranking signal), §31 (CSV not parquet; 5 AM parquet cron retired).
