# `export_nbcu.py` — function guide (with sample data) + navigation map

Goal of this doc: make the file easy to follow by showing **what each function takes in and what it
RETURNS**, with concrete sample values — plus a step-by-step of the core export query. Pairs with the
file at `export_nbcu.py` (498 lines, 9 top-level functions, 1 nested). Line numbers as of 2026-06-08.

> Mental model: read the UNION of peer IPs from up to 4 source DBs (DHT + tracker harvest + velocity +
> PEX, all ATTACHed read-only) for one `last_seen=DATE`, join each peer's hash → title/category/ip_id,
> assign each IP exactly one country, then emit one CSV row per `(title, country)` whose distinct-IP
> `IP_COUNT` clears the floor (sub-floor tail + ungeolocated → per-title "Other"). This is OUR demand
> feed — the `nbcu` name is historical (the benchmark we replace, not copy).

---

## The pipeline in one picture
```
--date 2026-05-30
        │
        ▼  ATTACH harvest/velocity/pex DBs, build UNION ALL over peers     (export, L340–349)
4 source SELECTs → one peers stream: (hash, country, ip, last_seen, src)
        │
        ▼  JOIN hashes (title/cat/ip_id) + LEFT JOIN titles (imdb_id)      (export, L350–357)
        │     WHERE last_seen=DATE AND ip != '_queried_'
rows: (ip_id, title, category, imdb_id, country, ip, src)
        │
        ▼  _load_catalog()  → imdb_id→ip_id, valid ip_ids, ip_id→title     (authoritative ip_id)
        ▼  _resolve_ip_id / _ids / _category                              (per-row identity)
        ▼  one country per (ip_id, ip)  → agg / agg_dht / agg_harv / agg_pex (L379–400)
        │
        ▼  _build_canon_v2()  → merge legacy-Q fragments into canonical twin (L406–415)
        ▼  emit loop: floor + "Other" rollup, per-source counts, is_dc      (L420–463)
        │
        ▼  sort by IP_COUNT desc → write CSV → print summary                (L465–489)
2026-05-30.csv  (one row per title × country)
```
Mental model: **`export` = the whole pipeline**; the `_*` helpers are pure identity/labelling plumbing it
calls per-row; the source-DB UNION + Python dedup is the recall trick (DHT alone undercounts ~3–39×).

---

## Function reference (input → sample output)

### `_country_label(iso)` — ISO-2 code → full country name (L80)
One-liner: normalize a 2-letter country code to a display label (NAMED legacy buckets first, then
overrides, then runtime `pycountry`, else the raw code); memoized in `_LABEL_CACHE`.
```python
_country_label("US")    # → "United States"   (from NAMED)
_country_label("VN")    # → "Vietnam"          (from _LABEL_OVERRIDES)
_country_label("PT")    # → "Portugal"         (via pycountry, cached)
_country_label("ZZ")    # → "ZZ"               (unknown → raw passthrough)
_country_label("??")    # → "??"               (ungeolocated sentinel, returned as-is)
```

### `_make_is_dc()` → `is_dc(ip) -> bool` — datacenter/VPN tester (L125)
One-liner: opens GeoLite2-ASN once and returns a memoized predicate; flags an IP as DC/VPN if its ASN
is in `DC_ASNS` or its org name matches `DC_KW`. If the ASN db is missing, returns an always-False
function (so `DC_IP_COUNT=0` everywhere and the feed stays valid).
```python
is_dc = _make_is_dc()        # → <function>  (closure over the open mmdb + cache)
is_dc("104.18.0.1")          # → True    (AS13335 Cloudflare-ish / hosting → datacenter)
is_dc("86.21.4.7")           # → False   (residential BT consumer ISP)
# when ASN_DB absent:
is_dc("104.18.0.1")          # → False   (graceful no-op)
```

### `_load_catalog()` → `(imdb2ipid, valid, ip2title)` — authoritative ip_id source (L159)
One-liner: read movies/series/anime parquet from `CATALOG_DIR` into three maps; returns empties if the
parquet files are missing (every title then falls through to UNMAPPED rather than a fabricated id).
```python
imdb2ipid, valid, ip2title = _load_catalog()
# imdb2ipid → {"tt0903747": "series-tt0903747", "tt1375666": "film-tt1375666", …}  imdb_id → ip_id
# valid     → {"series-tt0903747", "film-tt1375666", "anime-21", …}                set of real ip_ids
# ip2title  → {"series-tt0903747": "Breaking Bad", "anime-21": "One Piece", …}     ip_id → official name
# parquet missing → ({}, set(), {})
```

### `_resolve_ip_id(raw_ip_id, imdb_id, imdb2ipid, valid)` → `(ip_id, unmapped_flag)` (L195)
One-liner: map a title to its Pantheon ip_id — imdb_id first, else the raw id if it's already valid, else
blank + UNMAPPED=1. Never mints an id.
```python
_resolve_ip_id("series-Q98", "tt0903747", imdb2ipid, valid)  # → ("series-tt0903747", 0)  imdb hit
_resolve_ip_id("anime-21",   "",          imdb2ipid, valid)  # → ("anime-21", 0)           raw is valid
_resolve_ip_id("series-Qxyz","",          imdb2ipid, valid)  # → ("", 1)                    not in catalog
```

### `_category(category, ip_id)` → feed CATEGORY label (L210)
One-liner: pick the feed category; anime detected via the `anime-`/`mal-` ip_id prefix first (robust to
mis-tagged `category` columns), else the `CAT` lookup, default `"Video: TV"`.
```python
_category("Series", "series-tt0903747")  # → "Video: TV"
_category("Movies", "film-tt1375666")    # → "Video: Movie"
_category("Series", "anime-21")          # → "Video: Anime"   (ip_id prefix wins over category)
_category(None,     "series-Qxyz")       # → "Video: TV"      (default)
```

### `_ids(imdb_id, ip_id)` → `(IMDB_ID, ANIME_ID)` — public id columns (L223)
One-liner: derive the two public id columns; exactly one (or neither) is populated.
```python
_ids("tt0903747", "series-tt0903747")  # → ("tt0903747", "")     movie/series → IMDB_ID
_ids("",          "anime-21")          # → ("", "21")            anime → ANIME_ID (MAL id)
_ids("",          "film-tt1375666")    # → ("tt1375666", "")     tt embedded in ip_id
_ids("",          "series-Qxyz")       # → ("", "")              unmatched → both blank
```

### `_build_canon(c)` → legacy→canonical merge map (L244, OLDER path — not called by `export`)
One-liner: from the full `hashes` catalog, map a leftover legacy `film-Q`/`series-Q` ip_id to the SAME
title's single canonical id; conservative (only when exactly one canonical + one-or-more legacy).
```python
_build_canon(conn)
# → {"series-Q98836216": "anime-40748", "film-Q12345": "film-tt0111161", …}   legacy id → canonical id
```
Superseded by `_build_canon_v2`; kept for the old full-scan path.

### `_build_canon_v2(titlemeta)` → merge map (L271, the path `export` USES)
One-liner: build the legacy→canonical merge map keyed on the AUTHORITATIVE id (imdb_id, else MAL
anime_id) over only today's `titlemeta`; ids resolving to the same imdb/MAL collapse to one target, and
same-title UNMAPPED fragments fold into the lone resolved id of that title. Faster than `_build_canon`
(no full `hashes` scan). Contains nested `_target(rids)` (L297) which deterministically picks the merge
target (prefer a resolved/mapped id, then by id string).
```python
# titlemeta: raw_ip_id -> (title, cat_label, imdb, anime, resolved_ip, unmapped)
_build_canon_v2({
  "anime-40748":      ("Jujutsu Kaisen", "Video: Anime", "", "40748", "anime-40748", 0),
  "series-Q98836216": ("Jujutsu Kaisen", "Video: Anime", "", "",      "",            1),
})
# → {"series-Q98836216": "anime-40748"}      source raw id → target raw id (single chosen winner)
# no merges needed → {}
```

### `export(date, out)` → `None` (writes CSV + prints summary) (L324)
One-liner: **the whole pipeline** — ATTACH source DBs, UNION+join peers for `date`, dedup distinct IP
per (title, country), de-fragment, emit CSV, print summary. Returns nothing; its output is the file at
`out` plus a stdout report.
```python
export("2026-05-30", "/data/daily/2026-05-30.csv")
# (no return value)
# writes CSV rows like (one dict per title × country):
# {"TITLE":"Breaking Bad","IP_ID":"series-tt0903747","IMDB_ID":"tt0903747","ANIME_ID":"",
#  "DATE":"2026-05-30","CATEGORY":"Video: TV","COUNTRY_4":"United States","IP_COUNT":4821,
#  "DC_IP_COUNT":612,"UNMAPPED":0,"IP_COUNT_DHT":1203,"IP_COUNT_HARVEST":4655,"IP_COUNT_PEX":210}
# prints:
#   wrote 38,402 rows  (9,114 titles, sum IP_COUNT=2,317,885) -> /data/daily/2026-05-30.csv
#     datacenter/VPN IPs: 281,043 (12.1% of IP_COUNT)  residential=2,036,842
#     unmapped (no Pantheon ip_id): 1,204 titles / 3,991 rows
#     Top 15 titles by summed IP_COUNT: …
```
**CSV column order** (`DictWriter` fieldnames, L467–470):
`TITLE, IP_ID, IMDB_ID, ANIME_ID, DATE, CATEGORY, COUNTRY_4, IP_COUNT, DC_IP_COUNT, UNMAPPED, IP_COUNT_DHT, IP_COUNT_HARVEST, IP_COUNT_PEX`
(note the dict insertion order differs slightly from the written column order — the `DictWriter`
fieldnames list is authoritative.)

---

## Deep-dive: how the core export query works (step by step)

The heart of `export` is one ATTACH-and-UNION SQL statement followed by Python dedup. It exists because
**no single peer source is complete**: DHT undercounts 3–39×; tracker harvest fills most of the gap; the
velocity lane catches day-0 churn; PEX adds peer-exchange sightings. Unioning then deduping in Python
(not SQL `DISTINCT`) lets one IP seen by several sources count **once** while still attributing it to a
source for the per-lane columns.

**State it keeps (all keyed on the RAW `hashes.ip_id`, the title key):**
| var | holds | sample |
|---|---|---|
| `ipctry` | the ONE country chosen per `(ip_id, ip)` | `{("anime-21","5.6.7.8"): "GB"}` |
| `ipsrc` | which lanes saw each `(ip_id, ip)` | `{("anime-21","5.6.7.8"): {"dht","harv"}}` |
| `agg` | distinct IP set per `ip_id` → country | `{"anime-21": {"GB": {"5.6.7.8", …}}}` |
| `agg_dht/agg_harv/agg_pex` | same, per source lane | `{"anime-21": {"GB": {"5.6.7.8"}}}` |
| `titlemeta` | `ip_id` → `(title, cat, imdb, anime, resolved, unmapped)` | see `_build_canon_v2` sample |

**1. Build the UNION** (L340–349). Always include the main DB's `peers` (`'dht'`); ATTACH each optional
DB read-only if present and append its SELECT. Note both VELOCITY and PEX-vs-harvest labelling — the
**velocity lane is tagged `'harv'`** (it folds into the harvest column), only PEX gets `'pex'`:
```python
union = ["SELECT hash, country, ip, last_seen, 'dht'  AS src FROM peers"]
#  + "… 'harv' AS src FROM hv.peers"   (harvest_peers.db)
#  + "… 'harv' AS src FROM vv.peers"   (velocity — SAME 'harv' tag)
#  + "… 'pex'  AS src FROM pe.peers"   (pex_peers.db)
```

**2. Join + filter** (L350–357). Each peer row → `hashes` (title/category/ip_id), LEFT JOIN `titles`
(imdb_id), filtered to the requested day and excluding the `_queried_` sentinel:
```sql
SELECT h.ip_id, h.title, h.category, t.imdb_id, p.country, p.ip, p.src
FROM ( <union> ) p
JOIN hashes h ON h.hash = p.hash
LEFT JOIN titles t ON t.ip_id = h.ip_id
WHERE p.last_seen = ?           -- the --date day; this is the day's distinct-IP union
  AND p.ip != '_queried_'
-- one row example: ("anime-21","One Piece","Anime","", "GB","5.6.7.8","harv")
```

**3. One country per IP** (L379–391). For each row, set `titlemeta[ip_id]` (last-wins), record the lane
in `ipsrc`, and pick ONE country per `(ip_id, ip)` — first valid (non-`"??"`) country wins. This dedup
matters because the same IP can geolocate differently across lanes; without it a title's distinct-IP
total inflates (observed 3.7× on Jujutsu Kaisen).

**4. Fan out into the aggregates** (L392–400). Add each `(ip_id, ip)` to `agg` under its chosen country,
and into `agg_dht`/`agg_harv`/`agg_pex` per the lanes recorded in `ipsrc`.

**5. De-fragment** (L406–415). Apply `_build_canon_v2(titlemeta)`: for each `src→tgt`, pop `src`'s
per-country IP sets out of every aggregate and union them onto `tgt` (so distinct IPs dedupe across the
merged ids), then carry the canonical id's `titlemeta`.

**6. Emit** (L420–463). Per `ip_id`: relabel TITLE to the catalog official-cased name; emit one row per
country that clears `FLOOR` (10), rolling the sub-floor tail + `"??"` into a per-title `"Other"` row (only
if it itself clears the floor). For each emitted row compute `IP_COUNT=len(ips)`, `DC_IP_COUNT` via
`is_dc`, and the per-lane counts from the matching aggregate.

**7. Write + summarize** (L465–489). Sort rows by `IP_COUNT` desc, write the CSV, print totals
(rows/titles/sum IP_COUNT, DC %, unmapped counts, top-15 titles).

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Change output columns / CSV format | emit-loop dict `L447–461` + `DictWriter` fieldnames `L467–470` |
| Change country bucketing / "Other" rollup | emit loop `L430–437`, IP→country assignment `L389–400` |
| Change country labels (ISO → name) | `_country_label` L80, `NAMED` L65, `_LABEL_OVERRIDES` L78 |
| Change the noise floor | `FLOOR` L105 (used L431, L436) |
| Change which titles / peers are included | UNION build `L340–349`, join query `L350–357` (filter `last_seen=? AND ip != '_queried_'`) |
| Change the date / date range | `export(date,...)` query bind `L357`, `__main__` `--date` `L494` |
| Add / remove a source DB (DHT/harvest/velocity/pex) | path consts `L60–63`, ATTACH+UNION `L340–349`, per-src agg `L394–400`, per-src counts `L444–446` |
| Change ip_id resolution / catalog mapping | `_resolve_ip_id` L195, `_load_catalog` L159 |
| Change category assignment | `_category` L210, `CAT` L102 |
| Change datacenter/VPN flagging | `_make_is_dc` L125, `DC_ASNS` L112, `DC_KW` L115 |
| Change title de-duplication / merging | `_build_canon_v2` L271, applied L406–415 |
| Change sort order of rows | `out_rows.sort` L465 |

---

## Quick test recipe
```bash
# one day, custom output path:
/home/ec2-user/venv/bin/python3 export_nbcu.py --date 2026-05-30 --out /tmp/t.csv
# expect: "wrote N rows (M titles, sum IP_COUNT=…) -> /tmp/t.csv" + DC% + unmapped + top-15.
# A missing optional DB (harvest/velocity/pex) is silently skipped; the dht lane always runs.
```
