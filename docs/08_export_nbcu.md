# 08 — `export_nbcu.py` (Code Reference)

> **Role:** Builds our **primary daily deliverable** — the per-country distinct-peer-IP demand feed.
> Reads the day's peers (DHT + harvester), maps each title to its Pantheon `ip_id`,
> geolocates/aggregates, flags datacenter/VPN, and writes one CSV. **Read-only on all
> data sources; the only thing it writes is the output CSV.**
>
> Function-by-function walkthrough for code review. Pairs with `00_OVERVIEW.md`
> (pipeline context) and `10_ops_runbook.md` (ops).

---

## Where it runs
- **systemd:** `export-nbcu.timer` → `run_export_nbcu_daily.sh` at **23:55 UTC** (which calls this, then `velocity_rank.py`, then uploads both to S3).
- **Manual:** `python3 export_nbcu.py --date 2026-05-30 --out /data/daily/2026-05-30.csv`
- **Args:** `--date` (default: today UTC) · `--out` (default `/data/daily/<date>.csv`).

## Data flow (in → out)
```
hashes_v2.db  (peers + hashes + titles)            ┐
harvest_peers.db (peers)                           ├─ export_nbcu → /data/daily/<date>.csv
/data/catalog/{movies,series,anime}_info.parquet   │  (ip_id mapping)
/data/geoip/asn/GeoLite2-ASN.mmdb                  ┘  (datacenter/VPN flag)
```
**Why two peer DBs:** the harvester writes to its own DB so its heavy write volume
never bloats the DHT collector's WAL. We `ATTACH` it read-only and `UNION` both.

---

## Output schema (10 cols)
`TITLE, IP_ID, IMDB_ID, ANIME_ID, DATE, CATEGORY, COUNTRY_4, IP_COUNT, DC_IP_COUNT, UNMAPPED`

| Column | Meaning | Reviewer note |
|---|---|---|
| `IP_ID` | **Pantheon catalog id** — mapped, never minted; blank if not in catalog | invariant: never fabricated (§43) |
| `IMDB_ID` | real `tt…` for movie/series; blank for anime/unmatched | |
| `ANIME_ID` | MyAnimeList id (the `anime-<mal>` suffix); blank otherwise | reliable even if `titles.mal_id` NULL |
| `CATEGORY` | `Video: TV` / `Video: Movie` / `Video: Anime` | anime split by ip_id, not category col |
| `COUNTRY_4` | one of 14 named markets or `Other` | |
| `IP_COUNT` | **distinct peer IPs** for title×country×day (the metric) | floored ≥ `FLOOR` (10) |
| `DC_IP_COUNT` | how many of those IPs are datacenter/VPN | residential = `IP_COUNT − DC_IP_COUNT` |
| `UNMAPPED` | `1` when no Pantheon ip_id (IP_ID blank) | surfaces catalog gaps; rows kept, not dropped |

---

## Module constants (lines 55–84)
- `DB` / `HARVEST_DB` — the two SQLite sources (main opened `?mode=ro`).
- `NAMED` — ISO-2 → bucket label for the 14 named countries. Anything else → `"Other"`.
- `CAT` — `hashes.category` → feed label (`Series`/`TV`→`Video: TV`, `Movies`/`Movie`→`Video: Movie`, `Anime`→`Video: Anime`).
- `FLOOR = 10` — noise floor; (title, country) rows below this are dropped.
- `CATALOG_DIR` — where the Pantheon parquets live.
- `ASN_DB`, `DC_ASNS`, `DC_KW` — datacenter/VPN detection: a curated set of cloud/VPN endpoint ASNs **plus** hosting/VPN org-name keywords. **Residential-ISP words are deliberately excluded** so consumer ISPs are never mis-flagged. (Same logic as `export_asn_ab.py`.)

---

## Functions

### `_make_is_dc() -> (is_dc: str→bool)`  (line 87)
Factory returning a cached `is_dc(ip)` predicate backed by GeoLite2-ASN.
- Opens the ASN mmdb once; if missing/unreadable, returns a function that's **always `False`** (so `DC_IP_COUNT=0` everywhere and the feed stays valid — graceful degradation).
- `is_dc(ip)`: per-IP `cache` dict so each distinct IP is looked up **once** across the whole export (millions of IP occurrences, far fewer distinct IPs). Flags datacenter if the ASN ∈ `DC_ASNS` **or** the org string contains any `DC_KW` substring.
- **Reviewer note:** every lookup is wrapped `try/except → False` — a bad/unknown IP never crashes the export.

### `_load_catalog() -> (imdb2ipid, valid_ipids)`  (121)
Loads the **authoritative ip_id mappings** from the catalog parquets.
- `imdb2ipid`: `imdb_id → ip_id` from `movies_info` + `series_info` (`setdefault`, first wins on dupes).
- `valid_ipids`: full set of catalog ip_ids (movies + series + **anime**, which is ip_id-only / MAL-keyed).
- Missing parquets → empties → every title falls through to `UNMAPPED` (never a fabricated id).

### `_resolve_ip_id(raw_ip_id, imdb_id, imdb2ipid, valid) -> (ip_id, unmapped)`  (151)
The **map-don't-mint** core. Order:
1. `imdb_id` → catalog ip_id (authoritative for movies/series).
2. else if `raw_ip_id` is itself a valid catalog id (anime MAL ids, legit `series-Q…`) → keep it.
3. else → `("", 1)` — blank IP_ID + `UNMAPPED=1`.
- **Reviewer note:** grouping still happens on the *raw* hashes ip_id (see `export`), so two raw ids mapping to the same catalog id won't merge — rare/acceptable; keeps counts identical to pre-mapping.

### `_category(category, ip_id) -> str`  (166)
Feed category. **Anime is detected by ip_id prefix** (`anime-`/`mal-`) *first* — robust even if `hashes.category` was mis-tagged — then falls back to the `CAT` map. (This split anime out of TV in §39.)

### `_ids(imdb_id, ip_id) -> (IMDB_ID, ANIME_ID)`  (179)
Derives the two public id columns:
- anime ip_id → `("", "<mal>")`.
- has imdb → `(imdb, "")`.
- `film-tt…` ip_id → extract the embedded `tt…`.
- else → `("", "")` (unmatched; `IP_ID` still identifies the row).

### `export(date, out)`  (200) — main flow
1. **Query** the day's rows. If `harvest_peers.db` exists, `ATTACH` it and `UNION ALL` both `peers` tables; `JOIN hashes` (ip_id/title/category) + `LEFT JOIN titles` (imdb_id). Filter `last_seen = date AND ip != '_queried_'` (`_queried_` = the DHT "scanned, no peers" sentinel).
2. `_load_catalog()` + `_make_is_dc()` once.
3. **Aggregate** (`agg`): `raw_ip_id → bucket → set(ip)`. The **set** is what dedupes an IP seen by *both* DHT and harvester. `titlemeta` caches per-title resolved fields.
4. **Build rows:** per (title, bucket), `n = len(ips)`; skip if `n < FLOOR`; `dc = count of is_dc(ip)`; emit the 10-col row; accumulate `by_title`.
5. **Write** CSV sorted by `IP_COUNT` desc.
6. **Summary to stdout** (→ `export_nbcu.log`): row/title counts, total IP, datacenter %, unmapped counts, top-15 titles.

---

## Gotchas / invariants (for reviewers)
- **`last_seen` migration:** `peers.last_seen` is "most-recent-day-seen" and moves forward on re-sighting, so querying a *past* date later **undercounts** it. This export is correct only because it runs **on the day**. Never regenerate a historical feed from `peers` — transform the delivered CSV instead (§40).
- **Counts unchanged by the id/DC work:** grouping is by raw ip_id and `FLOOR` is unchanged, so adding IP_ID/ANIME_ID/UNMAPPED/DC_IP_COUNT did **not** move any IP_COUNT (verified §43/§50).
- **Datacenter IPs are kept, not dropped** — real seedbox/VPN demand; dropping widens the NBCU gap (§40). `DC_IP_COUNT` lets a consumer subtract for residential-only.
- **Read-only:** no writes to either DB; safe to run anytime.

## Change history
`SESSION_CHANGES.md` §29 (built), §39 (Video: Anime), §43 (catalog-mapped IP_ID + UNMAPPED), §50 (DC_IP_COUNT).
