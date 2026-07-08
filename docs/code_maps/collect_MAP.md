# `collect.py` — Navigation Map + function guide (with sample data)

Read-only structural index of the daily hash-collection pipeline (846 lines, 18
top-level functions, 0 nested, 9 sections). Generated to make the file analysable
without scrolling — **the source file is unchanged**. Line numbers as of 2026-06-08.

Goal of this doc: make the file easy to follow by showing **what data each function
takes in and returns**, with concrete examples. Pairs with the file at `collect.py`.

> Mental model: `main()` fans out to ~8 source fetchers (Jackett, 1337x via
> FlareSolverr, EZTV, AnimeTosho, Nyaa, TorrentGalaxy, BitMagnet DHT), each
> returning a normalized list of `{hash, raw_name, category, seeders, ...}` dicts.
> All items are `upsert`-ed (dedup by 40-hex infohash) into `data/hashes.db`,
> then missing titles/IDs are backfilled via TMDB (Movies/Series) and Jikan-MAL
> (Anime). This is the active ingest stage that *feeds* the hashes the DHT
> peer-count collector later walks.

---

## The pipeline in one picture
```
main()  ──fan-out──►  fetch_jackett()   fetch_1337x()   fetch_eztv()
                      fetch_animetosho() fetch_nyaa()    fetch_torrentgalaxy()
                      fetch_bitmagnet()
        │
        │  each returns a list of normalized "item" dicts (the COMMON SHAPE):
        ▼
{ "hash":"<40-hex>", "title":"", "raw_name":"The.Bear.S03...", "category":"Series",
  "imdb_id":"tt...", "tmdb_id":"", "seeders":1234, "trackers":["eztvx.to"] }
        │
        ▼  upsert(db, items)                  ← dedup by 40-hex infohash
(inserted, updated)  →  rows merged into data/hashes.db (table `hashes`)
        │
        ▼  enrich_missing(db)                 ← TMDB: backfill imdb_id/tmdb_id/title (Movies+Series)
        ▼  enrich_anime(db)                   ← Jikan/MAL: backfill mal_id/title (Anime)
        │
        ▼  main() prints summary + top-15-by-seeders table
```
Mental model: **every fetcher emits the same item-dict shape**, **`upsert` = the
merge/dedup**, **`enrich_*` = the ID backfill**, **`main` = glue + persistence +
report**. `_clean_name`/`_extract_year` are plumbing for the enrichers;
`_parse_jackett`/`_flare_get`/`_tgx_hash_from_link` are plumbing for their fetchers.

**The common "item" dict** (what every fetcher yields and `upsert` consumes):
```python
{
  "hash":     "1a2b3c…(40 lowercase hex)",   # btih infohash, the primary key
  "title":    "",            # usually "" — filled later by TMDB/MAL enrichment
  "raw_name": "The.Bear.S03.1080p.WEB-DL.x265-NTb",  # original torrent name
  "category": "Series",      # one of: "Movies" | "Series" | "Anime"
  "imdb_id":  "tt0903747",   # "" if unknown at fetch time
  "tmdb_id":  "",            # "" until TMDB enrichment
  "mal_id":   "",            # (some fetchers omit this key entirely)
  "seeders":  1234,          # 0 if the source doesn't report it (e.g. 1337x scrape)
  "trackers": ["eztvx.to"],  # source tag(s); unioned on update
}
```

---

## Function reference (input → sample output)

### `init_db(db)` — create/upgrade the `hashes` table + indexes
Input: an open `sqlite3.Connection`. Side-effecting; returns `None`. Creates the
`hashes` table (PK = `hash`), ALTERs in `raw_name`/`mal_id` if upgrading an old DB,
builds 4 indexes (`seeders`, `category`, `last_seen`, `imdb_id`), commits.
```python
init_db(sqlite3.connect("data/hashes.db"))   # → None  (table + indexes now exist)
```

### `upsert(db, items)` — insert new hashes / merge into existing → `(inserted, updated)`
Input: connection + list of item dicts. Skips rows whose hash isn't exactly 40 chars.
New hash → INSERT; seen hash → UPDATE that keeps **max(seeders)**, **unions trackers**,
and `COALESCE`-fills any blank title/imdb/tmdb/mal. Returns a count tuple.
```python
upsert(db, [
    {"hash":"1a2b…40hex", "raw_name":"The Bear S03", "category":"Series",
     "seeders":900, "trackers":["eztvx.to"], "imdb_id":"tt0903747"},
    {"hash":"1a2b…40hex", "raw_name":"The Bear S03", "category":"Series",
     "seeders":1200, "trackers":["1337x"]},     # same hash, higher seeders
])
# → (1, 1)      # one row inserted, then that same row updated:
#               # seeders→1200 (max), trackers→"1337x,eztvx.to" (union), last_seen→today
```

### `_extract_year(raw)` — first 4-digit year in a torrent name → `str`
```python
_extract_year("Dune.Part.Two.2024.2160p.WEB-DL")   # → "2024"
_extract_year("The Bear S03 1080p")                 # → ""   (no 19xx/20xx found)
```

### `_clean_name(raw)` — strip resolution/codec/release tags → searchable title `str`
Removes file extension, S01E01 markers, quality/codec markers, year, and
`[group]`/`(group)` tags; converts dots→spaces when the name has no spaces.
```python
_clean_name("The.Bear.S03E01.1080p.WEB-DL.x265-NTb")   # → "The Bear"
_clean_name("Dune.Part.Two.2024.2160p.HDR")            # → "Dune Part Two"
_clean_name("[SubsPlease] Frieren - 28 (1080p)")       # → "Frieren - 28"
```

### `tmdb_enrich(raw_name, category)` — TMDB search → `{title, imdb_id, tmdb_id}`
Cleans the name, queries TMDB `search/movie` or `search/tv` (year-preferred when
multiple hits), then a second call to `external_ids` for the IMDB id. Cached per
`"clean|category"`. Returns all-empty strings if `TMDB_KEY` unset, name too short,
non-Latin, or no hit.
```python
tmdb_enrich("Dune.Part.Two.2024.2160p", "Movies")
# → {"title": "Dune: Part Two", "imdb_id": "tt15239678", "tmdb_id": "693134"}

tmdb_enrich("Зелёный.слоник.1999", "Movies")    # non-Latin → skipped
# → {"title": "", "imdb_id": "", "tmdb_id": ""}
```

### `mal_enrich(raw_name)` — Jikan/MAL anime search → `{title, mal_id}`
Cleans the name, queries Jikan `/v4/anime` (type=tv, limit 3), takes the first hit
(prefers `title_english`). Rate-limited (`sleep(0.5)`), cached per clean name.
```python
mal_enrich("[SubsPlease] Sousou no Frieren - 28 (1080p)")
# → {"title": "Frieren: Beyond Journey's End", "mal_id": "52991"}

mal_enrich("???")                # too short / no Latin
# → {"title": "", "mal_id": ""}
```

### `enrich_anime(db)` — backfill `mal_id`/`title` for Anime rows missing `mal_id`
Input: connection. SELECTs Anime rows with blank `mal_id` and non-blank `raw_name`,
calls `mal_enrich` on each, UPDATEs in place. Returns `None`; prints progress.
```python
enrich_anime(db)
# stdout:  "  Enriching 37 anime hashes via MAL ...
#           Enriched 31 anime hashes with MAL data."
# → None   (31 rows now have mal_id/title)
```

### `enrich_missing(db)` — backfill `imdb_id`/`tmdb_id`/`title` for Movies/Series missing `imdb_id`
Input: connection. SELECTs Movies/Series rows with blank `imdb_id` and non-blank
`raw_name`, calls `tmdb_enrich`, UPDATEs in place. Returns `None`; prints progress.
```python
enrich_missing(db)
# stdout:  "  Enriching 412 hashes via TMDB ...
#           Enriched 388 hashes with TMDB data."
# → None
```

### `_parse_jackett(raw, tracker)` — one Jackett result list → list of item dicts
Input: raw Jackett `Results` list + a tracker label. Filters to Movie (2xxx) / TV
(5xxx) / Anime cats, drops non-40-hex hashes, derives `imdb_id` (prefixes `tt` if the
API gave a bare number).
```python
_parse_jackett([
  {"InfoHash":"AB12…40HEX", "Title":"Dune Part Two 2024 2160p",
   "Category":[2040], "Imdb":"15239678", "Seeders":"812", "Tracker":"TPB"}
], tracker="thepiratebay")
# → [{"hash":"ab12…40hex", "title":"Dune Part Two 2024 2160p",
#     "raw_name":"Dune Part Two 2024 2160p", "category":"Movies",
#     "imdb_id":"tt15239678", "tmdb_id":"", "seeders":812,
#     "trackers":["thepiratebay"]}]
```

### `fetch_jackett()` — query every local Jackett indexer × all cats → list of items
Input: none (reads env + `~/Library/.../Jackett/Indexers/*.json`). Returns `[]` if
`JACKETT_KEY` unset. Otherwise GETs each indexer's `/results` across Movie+TV+Anime
cats and concatenates `_parse_jackett` output.
```python
fetch_jackett()
# stdout: "  Querying 6 Jackett indexers ...
#           thepiratebay                          312 items
#           yts                                    98 items  ..."
# → [ {item dict}, {item dict}, …  ~600 items across all indexers ]
```

### `fetch_bitmagnet(min_seeders=1)` — local DHT GraphQL top-seeded → list of items
Input: min seeders. POSTs a GraphQL query for the 500 top-seeded torrents, keeps only
`contentType` movie/tv above the threshold. `title` left blank (TMDB fills it later).
```python
fetch_bitmagnet(min_seeders=10)
# → [{"hash":"9f8e…40hex", "title":"", "raw_name":"Inside.Out.2.2024.1080p…",
#     "category":"Movies", "imdb_id":"", "tmdb_id":"", "seeders":4521,
#     "trackers":["bitmagnet-dht"]}, …]
# (returns [] and prints "BitMagnet unavailable: …" if the local crawler is down)
```

### `fetch_eztv(pages=3)` — eztvx.to JSON API → list of Series items
Input: page count. Pages the API, dedups by hash, normalizes; derives `imdb_id`
(prefixes `tt`, keeps only valid `tt…`). Every item is `category:"Series"`.
```python
fetch_eztv(pages=3)
# → [{"hash":"c3d4…40hex", "title":"", "raw_name":"The Bear S03E01 1080p WEB H264-NHTFS",
#     "category":"Series", "imdb_id":"tt0903747", "tmdb_id":"", "seeders":1187,
#     "trackers":["eztvx.to"]}, …]
```

### `_flare_get(url, timeout_ms=40000)` — POST a URL through FlareSolverr → solved HTML `str`
Raises `Exception` if FlareSolverr's solution status isn't 200.
```python
_flare_get("https://1337x.to/cat/Movies/1/")
# → "<!DOCTYPE html><html>…full Cloudflare-solved page source…</html>"
# raises Exception("FlareSolverr HTTP 403") on a non-200 solution
```

### `fetch_1337x(pages=1, max_per_cat=20)` — FlareSolverr scrape → list of items
Input: pages + per-category cap. For each of Movies/Series/Anime, scrapes the list
page, follows each `/torrent/…` detail page, extracts the magnet btih + `<h1>` title.
**`seeders` is always 0** (not scraped from detail pages).
```python
fetch_1337x(pages=1, max_per_cat=20)
# stdout: "  Movies: 18 items / Series: 17 items / Anime: 15 items"
# → [{"hash":"7a6b…40hex", "title":"", "raw_name":"Deadpool & Wolverine 2024 1080p",
#     "category":"Movies", "imdb_id":"", "tmdb_id":"", "mal_id":"", "seeders":0,
#     "trackers":["1337x"]}, …]
```

### `_tgx_hash_from_link(download_url)` — follow Jackett redirect → btih hash `str`
GETs the Jackett download proxy URL without following redirects, pulls `btih:<40hex>`
out of the `Location` header. Returns `""` on any failure / non-redirect.
```python
_tgx_hash_from_link("http://localhost:9117/dl/torrentgalaxyclone/?…")
# → "e5f6…40hex"        (lowercased)
# → ""                  if no redirect / no btih in Location
```

### `fetch_torrentgalaxy()` — Jackett Torznab (torrentgalaxyclone) → list of items
Input: none. Returns `[]` if `JACKETT_KEY` unset. Per category (Movies/Series/Anime)
GETs the Torznab XML, parses `<item>`s, reads the `seeders` torznab attr, and resolves
each hash via `_tgx_hash_from_link`.
```python
fetch_torrentgalaxy()
# stdout: "  Movies: 44 items / Series: 41 items / Anime: 22 items"
# → [{"hash":"b1c2…40hex", "title":"", "raw_name":"Dune Part Two 2024 2160p UHD",
#     "category":"Movies", "imdb_id":"", "tmdb_id":"", "mal_id":"", "seeders":603,
#     "trackers":["torrentgalaxy"]}, …]
```

### `fetch_animetosho()` — AnimeTosho RSS → list of Anime items
Input: none. Parses the RSS, pulls the btih hash from each `<description>` — hex
directly, else base32-decoded to hex. `seeders` always 0. All `category:"Anime"`.
```python
fetch_animetosho()
# → [{"hash":"d2e3…40hex", "title":"", "raw_name":"[Erai-raws] Frieren - 28 [1080p]",
#     "category":"Anime", "imdb_id":"", "tmdb_id":"", "mal_id":"", "seeders":0,
#     "trackers":["animetosho"]}, …]
# (returns [] and prints "AnimeTosho fetch error: …" on a fetch/parse failure)
```

### `fetch_nyaa()` — Nyaa.si RSS feeds → list of Anime items
Input: none. Reads both Nyaa feeds, uses the `nyaa:` XML namespace to read
`nyaa:infoHash` and `nyaa:seeders`. Dedups across feeds. All `category:"Anime"`.
```python
fetch_nyaa()
# → [{"hash":"f4a5…40hex", "title":"", "raw_name":"[SubsPlease] Frieren - 28 (1080p) …",
#     "category":"Anime", "imdb_id":"", "tmdb_id":"", "seeders":214,
#     "trackers":["nyaa.si"]}, …]
```

### `main()` — entrypoint: parse args → run fetchers → upsert → enrich → report
Input: CLI args (`--skip-jackett/-flare/-1337x/-bitmagnet/-nyaa/-animetosho/-eztv/-tgx`,
`--skip-enrich`, `--min-seeders N`). Opens the DB, runs each enabled fetcher, optional
seeder filter, `upsert`, then TMDB + MAL enrichment, then prints a summary and the
top-15-by-seeders table. Returns `None`.
```python
# python collect.py --min-seeders 10
# stdout (tail):
#   New inserted  : 1,204
#   Updated       : 3,889
#   Total in DB   : 218,455  (+1,204)
#   Movies  : 91,233 hashes  (78,114 with IMDB ID)
#   Series  : 84,901 hashes  (71,002 with IMDB ID)
#
#   Top 15 Movies + Series by seeders:
#     Seeders   Cat       IMDB          Title
#     ---------------------------------------------------------------------------
#       4,521  Movies    tt22022452    Inside Out 2
#       3,997  Series    tt0903747     The Bear
# → None
```

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Change the DB schema / indexes | `init_db` L53 |
| Change dedup / merge-on-update logic | `upsert` L83 |
| Change title-cleaning regexes | `_clean_name` L141, `_extract_year` L135 |
| Change TMDB lookup / IMDB matching | `tmdb_enrich` L165, `enrich_missing` L292 |
| Change MAL (anime) lookup | `mal_enrich` L228, `enrich_anime` L262 |
| Add / tweak a Jackett source | `fetch_jackett` L359, `_parse_jackett` L330 |
| Change BitMagnet DHT query / filters | `fetch_bitmagnet` L395 |
| Add / tweak an RSS or scrape source | `fetch_eztv` L455, `fetch_1337x` L511, `fetch_animetosho` L634, `fetch_nyaa` L690 |
| Change TorrentGalaxy hash resolution | `_tgx_hash_from_link` L566, `fetch_torrentgalaxy` L577 |
| Change FlareSolverr request behaviour | `_flare_get` L502 |
| Add a CLI flag / change run order / summary | `main` L726 |
| Change category buckets (Movie/TV/Anime) | constants L44–46 |
