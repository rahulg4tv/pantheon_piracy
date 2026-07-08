# `merge_and_upload.py` — function guide (with sample data) + navigation map

Goal of this doc: make the 6-hourly merge/upload job easy to follow by showing **what data each
function takes in and what it RETURNS**, with concrete examples (input → sample output). Pairs with the
file at `merge_and_upload.py` (623 lines, 16 top-level functions). Line numbers as of 2026-06-08.

> Mental model: pull the day's peer-IP signal from two sources — the active DHT scan's per-worker CSVs
> (+ EU CSVs from S3) and the passive announce-only peers in the SQLite `peers` table — concat them,
> collapse duplicate catalog entries so one show is one `ip_id`, take MAX per pass then SUM per
> `(ip_id, country)`, attach DB metadata + day-over-day velocity, write an atomic CSV, fire SNS spike
> alerts, and upload to `s3://YOUR_S3_BUCKET/merged/YYYY/MM/DD/`.

---

## The pipeline in one picture
```
--date / clock (05 UTC→yesterday, else today)
        │
        ▼  get_date(args)                              → "2026-06-14"
        │
        ▼  download_eu_csvs(date)                       ← S3 peer-counts-eu/  (side effect: writes _w8/_w9.csv locally)
        │
        ▼  load_peer_counts(date)                       ← /data/peer_counts/{date}_w*.csv (active DHT scan)
DataFrame[date, run_time, hash, ip_id, title, category, seeders, country, peer_count, bep33_*]
        │
        ▼  load_announce_only_peers(date, csv_hashes)   ← SQLite peers⋈hashes (passive announce-only)
        │   (concat onto peer_df)
        ▼  build_canonical_map(titles_df, peer_df) → (alias_map, canon_title)   ← SQLite titles catalog
        ▼  canonicalize(peer_df, alias_map, canon_title)   (one show = one ip_id + one title)
        ▼  dedup_passes(peer_df)                        (MAX peer_count per hash×country×day)
        ▼  merge_metadata(peer_df, load_hash_metadata())   (+source, first_seen, last_seen)
        ▼  aggregate_to_ip_id(merged)                   (SUM peer_count per ip_id×country)
        ▼  add_peer_velocity(final, date)               (+peer_count_delta vs yesterday's CSV)
        ▼  send_spike_alerts(final, date)               → SNS publish (skipped on --dry-run)
        ▼  main() atomic CSV write → upload_to_s3()     → s3://…/merged/YYYY/MM/DD/{date}.csv
```
Mental model: **`load_*` = ingest**, **`build_canonical_map`/`canonicalize` = de-fragment shows**,
**`dedup_passes`/`aggregate_to_ip_id` = collapse to the demand number**, **`add_peer_velocity`/
`send_spike_alerts` = trend + notify**, **`main` = glue + persistence**. `normalize_title` is plumbing
for the canonical map.

---

## Function reference (input → sample output)

### `get_date(args)` — pick the date to process → `str` "YYYY-MM-DD"
One-line: `--date` wins; otherwise yesterday for the pre-06 UTC run, today for the 11/17/23 runs.
```python
get_date(Namespace(date="2026-06-10"))   # → "2026-06-10"   (explicit flag wins)
get_date(Namespace(date=None))           # → "2026-06-14"   if now is 03:00 UTC → yesterday
get_date(Namespace(date=None))           # → "2026-06-15"   if now is 17:00 UTC → today
```

### `download_eu_csvs(date)` — fetch EU node CSVs from S3 → `None` (side effect only)
One-line: downloads `peer-counts-eu/{date}_w8.csv`, `_w9.csv` into `PEER_COUNTS_DIR` so the later glob
picks them up. Returns nothing; never raises (logs a warning on failure or when none exist).
```python
download_eu_csvs("2026-06-14")
# → None   (side effect: /data/peer_counts/2026-06-14_w8.csv, _w9.csv now exist on disk)
# stdout: "[merge]   Downloaded EU: 2026-06-14_w8.csv (812 KB)"
# if bucket prefix empty → "[merge] No EU CSVs found in S3 yet — skipping", returns None
```

### `load_peer_counts(date)` — load & concat active-scan CSVs → `pd.DataFrame`
One-line: globs per-worker files `{date}_w*.csv` (falls back to single `{date}.csv`), concats them, and
backfills missing `bep33_*` columns with 0. Raises `FileNotFoundError` only if nothing matches.
```python
load_peer_counts("2026-06-14")
# → pandas DataFrame, one row per (hash, country, pass), e.g.:
#    date        run_time  hash       ip_id            title            category  seeders  country  peer_count  bep33_seeders  bep33_leechers
#    2026-06-14  17:02     a1b2…(40h) series-tt1234567 Rick and Morty   series    312      US       540         300            5100
#    2026-06-14  17:02     c3d4…       series-tt1234567 Rick and Morty   series    280      GB       210         300            5100
#    …
# (shape ≈ 1.2M rows × 11 cols on a normal day)
```

### `load_announce_only_peers(date, csv_hashes)` — passive peers from SQLite → `pd.DataFrame`
One-line: counts `DISTINCT ip` per hash×country in the `peers` table for `date` (joined to `hashes` for
ip_id/title/category/seeders), then drops any hash already in `csv_hashes`. Returns CSV-schema rows
(`run_time="announce"`, `bep33_*=0`); returns an empty DataFrame when none qualify.
```python
load_announce_only_peers("2026-06-14", csv_hashes={"a1b2…", "c3d4…"})
# → DataFrame with the same columns as load_peer_counts (minus the dropped hashes):
#    date        run_time  hash    ip_id            title       category  seeders  country  peer_count  bep33_seeders  bep33_leechers
#    2026-06-14  announce  e5f6…   series-tt9988776 The Pitt    series    0        VN       37          0              0
#    …
# if nothing today → empty DataFrame (df.empty is True)
```

### `dedup_passes(df)` — collapse multiple daily passes → `pd.DataFrame`
One-line: a hash can be scanned several times a day; takes `MAX(peer_count)` (and MAX of seeders/bep33)
per `(date, hash, ip_id, title, category, country)` so IPs aren't double-counted across passes.
```python
dedup_passes(df_with_3_passes)
# input:  3 rows for (a1b2…, US) with peer_count 410, 540, 505
# → 1 row for (a1b2…, US) with peer_count = 540  (the MAX)
#    date        hash    ip_id            title           category  country  peer_count  seeders  bep33_seeders  bep33_leechers
#    2026-06-14  a1b2…   series-tt1234567 Rick and Morty  series    US       540         312      300            5100
# row count typically drops ~3× (one row per pass → one row per hash×country)
```

### `load_hash_metadata()` — hash → provenance from SQLite → `pd.DataFrame`
One-line: reads `hash, source, first_seen, last_seen` from the `hashes` table (no filter — full table).
```python
load_hash_metadata()
# → DataFrame:
#    hash    source   first_seen   last_seen
#    a1b2…   jackett  2026-01-03   2026-06-14
#    c3d4…   dht      2026-05-30   2026-06-14
#    …   (≈ all known hashes, e.g. 480k rows × 4 cols)
```

### `normalize_title(s)` — fuzzy-match key for a title → `str`
One-line: lowercases, collapses internal whitespace, strips; non-strings become `""`.
```python
normalize_title("  INVINCIBLE   Season 2 ")   # → "invincible season 2"
normalize_title("Rick and Morty")             # → "rick and morty"
normalize_title(None)                         # → ""        (non-str guard)
normalize_title(float("nan"))                 # → ""
```

### `load_titles_catalog()` — the catalog (one row per ip_id) → `pd.DataFrame`
One-line: reads `ip_id, title, category, imdb_id, hashes_found` from the `titles` table — the basis for
canonical-title selection.
```python
load_titles_catalog()
# → DataFrame:
#    ip_id            title           category  imdb_id     hashes_found
#    series-tt1234567 Rick and Morty  series    tt1234567   88
#    series-Q15659308 rick and morty  series    None        12
#    movie-tt0133093  The Matrix      movie     tt0133093   40
#    …
```

### `build_canonical_map(titles_df, extra_df=None)` — de-duplicate shows → `tuple[dict, dict]`  ← see deep-dive
One-line: groups catalog (+ optional peer rows for orphan ip_ids) by `(normalized title, category)`,
picks one canonical ip_id per group (has-imdb → most hashes → lexically first), and returns
`(alias_map, canon_title)`.
```python
build_canonical_map(titles_df, extra_df=peer_df)
# → (
#     # alias_map: every duplicate id (incl. the canonical itself) → the canonical id
#     {"series-tt1234567": "series-tt1234567",
#      "series-Q15659308": "series-tt1234567",      # casing-split / Q-dup folded in
#      "series-Q131431817": "series-tt31938062"},   # orphan from peer stream folded in
#     # canon_title: EVERY ip_id → its chosen display title (not just dups)
#     {"series-tt1234567": "Rick and Morty",
#      "series-Q15659308": "Rick and Morty",
#      "movie-tt0133093":  "The Matrix"}
#   )
# stdout: "[merge] Canonical map: 1,204 duplicate title groups → 2,933 ip_ids remapped"
```
Note `canon_title` has an entry for **every** ip_id seen (defaults to the row's own title), while
`alias_map` only contains ids that belonged to a duplicate group.

### `canonicalize(df, alias_map, canon_title)` — apply the maps to peer rows → `pd.DataFrame`
One-line: remaps each row's `ip_id` via `alias_map` and overwrites `title` with the canonical one, so the
later groupby never splits one show across rows.
```python
canonicalize(peer_df, alias_map, canon_title)
# input row:   ip_id="series-Q15659308", title="rick and morty"
# → output row: ip_id="series-tt1234567", title="Rick and Morty"   (peer_count etc. unchanged)
# stdout: "[merge] Canonicalized ip_ids: 41,002 → 38,069 (2,933 merged)"
```

### `merge_metadata(peer_df, meta_df)` — attach hash provenance → `pd.DataFrame`
One-line: left-joins peer rows to hash metadata on `hash`, adding `source, first_seen, last_seen`
(NaN where a hash has no metadata row).
```python
merge_metadata(peer_df, meta_df)
# → peer_df + 3 columns:
#    hash    ip_id            title           country  peer_count  source   first_seen   last_seen
#    a1b2…   series-tt1234567 Rick and Morty  US       540         jackett  2026-01-03   2026-06-14
```

### `aggregate_to_ip_id(df)` — collapse hashes into the demand number → `pd.DataFrame`
One-line: groups hash-level rows to `(date, ip_id, title, category, country)`, SUMming `peer_count`,
counting distinct hashes, MAXing seeders, SUMming bep33, min/max-ing seen dates, then reorders columns.
```python
aggregate_to_ip_id(merged)
# input:  many hash rows for series-tt1234567 / US (peer_count 540, 210, 95, …)
# → one row per ip_id×country, columns in fixed output order:
#    date        ip_id            title           category  source   country  hash_count  peer_count  seeders  bep33_seeders  bep33_leechers  first_seen  last_seen
#    2026-06-14  series-tt1234567 Rick and Morty  series    jackett  US       7           1845        312      2100           34700           2026-01-03  2026-06-14
# stdout: "[merge] Aggregated: 96,210 rows (38,069 unique ip_ids)"
```

### `add_peer_velocity(df, date)` — day-over-day delta → `pd.DataFrame`
One-line: loads yesterday's merged CSV (or legacy parquet) and adds
`peer_count_delta = today − yesterday` per `(ip_id, country)`; 0 everywhere if no prior file exists.
```python
add_peer_velocity(final, "2026-06-14")
# → final + one column:
#    ip_id            country  peer_count  peer_count_delta
#    series-tt1234567 US       1845        +312        (yesterday was 1533 → growing)
#    movie-tt0133093  GB       80          -15         (shrinking)
#    series-tt9988776 VN       37          0           (first day / no prior row)
# no yesterday file → all peer_count_delta = 0, warns and returns
# stdout: "[merge] Peer velocity: 12,034 rising  9,981 falling  74,195 stable"
```

### `send_spike_alerts(df, date)` — SNS notify on surging titles → `None` (side effect only)
One-line: keeps rows with `peer_count ≥ SPIKE_MIN_PEERS` (100) and `delta > 0`, sums per ip_id across
countries, keeps those with `pct_change ≥ SPIKE_PCT_THRESHOLD` (0.50), and publishes one SNS message with
the top-5 countries per title. Returns `None`; logs and returns early when nothing qualifies.
```python
send_spike_alerts(final, "2026-06-14")
# → None   (side effect: one boto3 sns.publish)
# Subject: "[Piracy Spike] 3 title(s) on 2026-06-14"
# Message (excerpt):
#   🚨 Piracy Spike Alert — 2026-06-14
#   3 title(s) surged >50% since last run
#     The Pitt (series)
#       Peers: 1,200 → 2,950  (+1,750 / +146%)
#       Top: US:1,400  GB:610  VN:430  CA:300  AU:210
# no qualifying rows → returns None, stdout: "[merge] Spike alerts: no qualifying rows"
```

### `upload_to_s3(local_path, date)` — push the merged CSV to S3 → `None` (side effect only)
One-line: uploads `local_path` to `merged/YYYY/MM/DD/<filename>` in `BUCKET`.
```python
upload_to_s3(Path("/data/merged/2026-06-14.csv"), "2026-06-14")
# → None   (side effect: object at s3://YOUR_S3_BUCKET/merged/2026/06/14/2026-06-14.csv)
```

### `main()` — orchestrate the full pipeline → `None`
One-line: glue — get_date → download EU → load CSV → announce supplement → canonicalize → dedup → join
meta → aggregate → velocity → spike alerts → atomic CSV write → S3 upload (honors `--dry-run`).
```python
main()   # → None
# Production output: /data/merged/2026-06-14.csv (written atomically via .tmp.csv → rename)
#                    + S3 upload + SNS alerts
# --dry-run output:  /tmp/merged_dryrun_2026-06-14.csv only (no SNS, no S3, no prod overwrite)
# stdout final line: "[merge] Done."
```

---

## Deep-dive: how `build_canonical_map` actually works (step by step)

This is the trickiest function — it decides **which ip_id wins** so one show isn't counted as several. It
fixes two fragmentation classes: (A) the *same* ip_id appearing with inconsistent title casing, and (B)
the *same show under two different ip_ids* (e.g. the IMDB `series-tt…` id plus a legacy `series-Q…` id
from before the Q→tt DB remap).

**Inputs:**
| arg | holds | sample |
|---|---|---|
| `titles_df` | the authoritative catalog | `[ip_id, title, category, imdb_id, hashes_found]` rows |
| `extra_df` (optional) | the day's peer rows, to catch *orphan* ids that exist only in the stream | the live `peer_df` |

**1. Fold in orphan ip_ids from the peer stream.** Take `(ip_id, title, category)` from `extra_df` for
ids **not** already in `titles_df`, mark them `imdb_id="" , hashes_found=0` (so they can never win
canonical), and append. This lets a stale `series-Q131431817` still emitted by a long-running DHT worker
alias onto the imdb-authoritative `series-tt31938062` ("The Pitt") instead of splitting the count.

**2. Compute the grouping key and a winner-ranking.**
```python
df["norm"]     = df["title"].map(normalize_title)          # "INVINCIBLE" → "invincible"
df = df[df["norm"] != ""]                                  # drop blank titles
df["has_imdb"] = df["imdb_id"].notna() & imdb_id.strip()!=""   # bool
df["hashes_found"] = pd.to_numeric(df["hashes_found"]).fillna(0)
```

**3. Seed `canon_title` with every id's own title** (so even non-duplicate ids get a default):
```python
canon_title = dict(zip(df["ip_id"], df["title"]))   # {"movie-tt0133093": "The Matrix", …}
alias_map   = {}
```

**4. For each `(norm, category)` group with >1 distinct ip_id, pick the canonical and remap the group.**
Sort by `has_imdb` desc → `hashes_found` desc → `ip_id` asc (stable tie-break); the first row wins:
```python
grp = grp.sort_values(["has_imdb","hashes_found","ip_id"], ascending=[False,False,True])
canon_ip, canon_name = grp.iloc[0]["ip_id"], grp.iloc[0]["title"]
for ip in grp["ip_id"]:
    alias_map[ip]   = canon_ip      # incl. canon_ip → itself
    canon_title[ip] = canon_name    # all variants now show the same title
```
Single-id groups are skipped (no alias needed), but they keep their default `canon_title` from step 3.

**5. Return** the two maps. `canonicalize` then applies them to the peer rows:
```python
return alias_map, canon_title
# alias_map:   {"series-Q15659308": "series-tt1234567", "series-tt1234567": "series-tt1234567", …}
# canon_title: {<every ip_id>: <its display title>}
```
Why not just join on `imdb_id`? The duplicate `Q` entries usually have **no** `imdb_id`, so grouping is
done on normalized title + category instead — and kept *within* category so two unrelated same-named
titles (a movie vs a series) never merge.

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Change which date is processed / run-hour logic | `get_date` L58 |
| Change CSV source loading / worker-file glob / fallback | `load_peer_counts` L94 |
| Pull in / change EU-node CSVs from S3 | `download_eu_csvs` L69 |
| Change announce-only (passive peers) supplement / its SQL | `load_announce_only_peers` L143 |
| Change pass dedup (MAX-per-pass) | `dedup_passes` L207 |
| Change ip_id resolution / duplicate-show collapsing | `build_canonical_map` L264, `canonicalize` L337, `normalize_title` L246 |
| Change the per-(ip_id,country) aggregation / output schema | `aggregate_to_ip_id` L363 |
| Change hash/title metadata loaded from the DB | `load_hash_metadata` L230, `load_titles_catalog` L253 |
| Change day-over-day delta (peer velocity) | `add_peer_velocity` L404 |
| Change spike-alert thresholds / SNS message | `send_spike_alerts` L452, config L52–L55 |
| Change S3 upload path / bucket | `upload_to_s3` L535, config L44–L50 |
| Change overall step order / dry-run / atomic write | `main` L544 |

---

## Quick test recipe
```bash
# full pipeline for one date, no S3/SNS, output to /tmp only:
python3 merge_and_upload.py --date 2026-06-14 --dry-run
# expect: /tmp/merged_dryrun_2026-06-14.csv, "[merge] DRY-RUN: skipping …" lines, "[merge] Done."
```
