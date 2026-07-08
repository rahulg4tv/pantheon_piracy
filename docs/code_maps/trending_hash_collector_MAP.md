# `trending_hash_collector.py` — function guide (with sample data) + navigation map

Goal of this doc: make the file easy to follow by showing **what each notable function takes in and
returns**, with concrete examples — so anyone understands the data shapes without reading the code.
Pairs with the file at `trending_hash_collector.py` (1,941 lines). Line numbers as of 2026-06-15.

> Mental model: pull trending titles from **TPB top-100 + EZTV + YTS + Nyaa/SubsPlease RSS** (already
> have hashes), plus **TMDB (movies/TV) + AniList (anime)** (titles only → must search for hashes).
> For each torrent, resolve it to a single Pantheon `ip_id` (IMDB-direct → fuzzy `Catalog.fuzzy_match`,
> gated by `_name_matches_title` + ambiguity/year guards), then upsert new hashes into `hashes_v2.db`.
> Runs on cron at 01:00/06:00/06:30/10:00/18:00 UTC.

---

## The pipeline in one picture
```
TPB/EZTV/YTS/Nyaa fetchers          TMDB / AniList fetchers
  (hash already known)               (title only → search sources for hashes)
        │                                     │
        ▼  list[dict] torrents                ▼  list[dict] pre-matched (ip_id already set)
[{hash, raw_name, seeders,           [{hash, raw_name, seeders, source,
  source, category, imdb_id?}, …]      category, ip_id, matched_title, imdb_id?}, …]
        │                                     │
        ▼  main(): dedupe by hash             │
        ▼  per torrent, resolve ip_id:        │
   imdb_id? → Catalog.find_by_imdb (O(1), exact)
   else     → parse_torrent_name → is_collection? is_ambiguous_title?
              → Catalog.fuzzy_match (word-index → SequenceMatcher, year+sequel guards)
              → year cross-check vs titles-table ground truth
        │                                     │
        └──────────────┬──────────────────────┘
                       ▼  upsert_hashes() → INSERT new / bump seeders on existing
                       │    (alias_best_match may redirect to a more-specific ip_id)
                       ▼  ensure_title_row() + post_ingest_audit()
                  hashes_v2.db  (tables: hashes, titles)
```
Mental model: **fetchers = the raw catalog of torrents**, **`Catalog` + matchers = resolve to ip_id**,
**`upsert_hashes` = persistence**, **`post_ingest_audit` = false-positive tripwire**.

Every fetcher returns the **same row dict shape** so the rest of the pipeline is source-agnostic:
```python
{
  "hash":     "a1b2c3…(40 lowercase hex)",   # the infohash (the demand key)
  "raw_name": "The.Bear.S03E01.1080p.WEB.H264-NTb",
  "seeders":  812,
  "source":   "eztv",                        # SOURCE_* tag
  "category": "Series",                       # "Movies" | "Series" | "Anime"
  # optionally: "imdb_id": "tt1234567"  (YTS/TMDB only — enables direct match)
}
```

---

## Function reference (input → sample output)

### `_fetch(url, retries=3, delay=2.0)` — HTTP GET with retry/backoff → raw bytes (L110)
Retries `retries` times with `delay`-second sleeps; raises the last error if all fail.
```python
_fetch("https://apibay.org/precompiled/data_top100_207.json")
# → b'[{"id":"123","name":"...","info_hash":"AB12...","seeders":"812",...}, …]'   (bytes)
```

### `is_collection(raw_name)` — multi-title pack detector → bool (L147)
True for box sets / complete series / film ranges that can't map to one ip_id.
```python
is_collection("The Lord of the Rings Trilogy 1080p")     # → True
is_collection("Harry Potter 1 to 8 Films BluRay")         # → True
is_collection("Friends Complete Series S01-S10")          # → True
is_collection("Dune Part Two (2024) 2160p")               # → False
is_collection("The Criterion Collection: Seven Samurai")  # → False  (Criterion is single-film)
```

### `is_ambiguous_title(title)` — needs-a-year-anchor detector → bool (L177)
True when a title is too short/common/acronym to fuzzy-match safely without a year.
```python
is_ambiguous_title("It")            # → True   (Class A: all stop-words)
is_ambiguous_title("Another")       # → True   (Class B: known ambiguous solo)
is_ambiguous_title("CIA")           # → True   (Class C: acronym)
is_ambiguous_title("M.I.A.")        # → True   (Class C)
is_ambiguous_title("Go")            # → True   (Class C: ≤3 chars)
is_ambiguous_title("Demon Slayer")  # → False  (distinctive)
is_ambiguous_title("Attack on Titan")  # → False
```

### `_title_words(title)` — significant words of a title → set[str] (L227)
Drops stop-words and tech tokens (codecs, resolutions, S01E01…).
```python
_title_words("The Bear S03E01 1080p x265")   # → {"bear"}
_title_words("Attack on Titan Final Season") # → {"attack", "titan", "final", "season"}
```

### `_strip_release_group(raw_name)` — drop trailing scene group + extension → str (L233)
Only removes the final `-GROUP` token (stops `ELiTE` matching the show "Elite"). Mid-name hyphens
(e.g. `Spider-Noir`) survive.
```python
_strip_release_group("Widows.Bay.S01E04.1080p.x265-ELiTE")  # → "Widows.Bay.S01E04.1080p.x265"
_strip_release_group("Dune.2021.1080p.WEB-DL.mkv")          # → "Dune.2021.1080p.WEB-DL"
_strip_release_group("Spider-Noir.S01E01-NTb")              # → "Spider-Noir.S01E01"
```

### `_name_matches_title(raw_name, title, year="", category="")` — title-overlap guard → bool (L253)
Guards title-only searches (apibay/TPB, Jackett). Length-scaled word-overlap rule. **The most
safety-critical filter** — see step-by-step deep-dive below.
```python
_name_matches_title("The Bear S03E01 1080p WEB H264-NTb", "The Bear")           # → True
_name_matches_title("One Piece - 1164 [1080p]", "One Piece 4D")                 # → False (needs all words for short titles)
_name_matches_title("Michael Clayton (2007) 1080p", "Michael", "2025", "Movies")  # → False (n<=2 movie: parsed title must equal title words)
_name_matches_title("Dune Part Two 2024 2160p", "Dune: Part Two", "2024", "Movies")  # → True
_name_matches_title("Masters of the Universe 1987", "Masters of the Universe", "2026", "Movies")  # → False (year differs >1)
```

### `_normalize(text)` — lowercase / strip accents / drop punctuation → str (L458)
```python
_normalize("Pokémon: The First Movie!")   # → "pokemon the first movie"
_normalize("Amélie (2001)")                # → "amelie 2001"
_normalize(None)                            # → ""
```

### `parse_torrent_name(raw)` — PTN parse → (clean_title, year_or_None) (L470)
Uses the `PTN` library to strip codecs/quality/groups/season tags.
```python
parse_torrent_name("Dune.Part.Two.2024.2160p.WEB-DL.x265-GROUP")  # → ("Dune Part Two", "2024")
parse_torrent_name("The.Bear.S03E01.1080p.WEB.H264-NTb")          # → ("The Bear", None)
parse_torrent_name("[SubsPlease] Frieren - 12 (1080p)")           # → ("Frieren", None)
```

### `class Catalog` — the title→ip_id index (L485)
`__init__` loads all three parquets (≈41K movies, ≈17K series, ≈28K anime), concatenates them, and
builds in-memory lookups. Notable members it populates:
```python
cat = Catalog()
cat.imdb_map["tt1160419"]   # → ("film-tt1160419", "Dune: Part Two", "Movies")
cat._exact["dune part two"] # → {"Movies": 12345}   (normalized title → {category: record index})
cat._imdb_idx["tt1160419"]  # → 12345               (imdb_id → record index)
cat._word_idx["dune"]       # → [12345, 67890, …]   (inverted word index → record indices)
cat.by_ipid["film-tt1160419"]  # → {"imdb_id": "tt1160419", "mal_id": None}
cat.by_ipid["anime-21"]        # → {"imdb_id": None, "mal_id": "21"}
# Each _records entry: (ip_id, title, category, title_norm, year, imdb_id)
```

### `Catalog.find_by_imdb(imdb_id)` — O(1) IMDB lookup → (ip_id, title, category) | None (L570)
```python
cat.find_by_imdb("tt1160419")   # → ("film-tt1160419", "Dune: Part Two", "Movies")
cat.find_by_imdb("tt0000000")   # → None
```

### `Catalog.find_by_mal(mal_id)` — MAL id → anime ip_id | None (L573)
Never mints — returns None if Pantheon doesn't carry that MAL id.
```python
cat.find_by_mal(21)       # → "anime-21"   (One Piece, if in catalog)
cat.find_by_mal(999999)   # → None         (not tracked)
```

### `Catalog.fuzzy_match(title, category, year=None, threshold=0.82, imdb_id=None)` → (ip_id, title, category) | None (L579)
The fuzzy title resolver. IMDB-direct → exact → word-index candidates → SequenceMatcher, with hard
year filter + sequel-number guard + bidirectional word-coverage guard. See deep-dive below.
```python
cat.fuzzy_match("Dune Part Two", "Movies", year="2024")
# → ("film-tt1160419", "Dune: Part Two", "Movies")

cat.fuzzy_match("Iron Man 2", "Movies")
# → ("film-tt1228705", "Iron Man 2", "Movies")     (sequel guard keeps it off "Iron Man 3")

cat.fuzzy_match("Qwertyuiop Nonexistent Show", "Series")
# → None
```
Nested helpers it uses:
- `_seq_number("iron man 2")` → `2`; `_seq_number("part iii")` → `3`; `_seq_number("the matrix")` → `None` (L655)
- `_scores(norm_q, norm_t, q_words, t_words)` → `(seq_ratio, containment)` e.g. `(0.93, 1.0)` (L677)
- `_best_in(pool)` → `(best_score, best_record)` e.g. `(0.93, ("film-tt1160419", "Dune: Part Two", "Movies"))` (L686)

### Source fetchers — all return `list[dict]` rows (shape above)

| Function | Line | Input | Sample return |
|---|---|---|---|
| `fetch_tpb_top100(cat_id, category)` | L768 | `(207, "Movies")` | `[{"hash":"ab12…","raw_name":"Deadpool & Wolverine 2024 1080p","seeders":4210,"source":"tpb_top100","category":"Movies"}, …]` |
| `fetch_eztv(pages=3)` | L788 | `pages=3` | `[{"hash":"cd34…","raw_name":"The.Bear.S03E01.1080p.WEB.H264-NTb","seeders":812,"source":"eztv","category":"Series"}, …]` |
| `fetch_yts(pages=3)` | L812 | `pages=3` | `[{"hash":"ef56…","raw_name":"Dune: Part Two (2024) [1080p WEB]","seeders":903,"source":"yts","category":"Movies","imdb_id":"tt1160419"}, …]` |
| `_fetch_nyaa_rss_url(url, source)` | L855 | RSS url + tag | `[{"hash":"77aa…","raw_name":"[SubsPlease] Frieren - 12 (1080p)","seeders":1500,"source":"nyaa","category":"Anime"}, …]` |
| `fetch_nyaa_rss()` | L880 | — | same shape, `source="nyaa"` |
| `fetch_subsplease_rss()` | L887 | — | same shape, `source="subsplease"` |

Note: only YTS sets `imdb_id` here (enables IMDB-direct match in `main`); the others rely on fuzzy matching.

### TMDB / search helpers

| Function | Line | Input | Sample return |
|---|---|---|---|
| `_tmdb_get(path, key, **params)` | L901 | `("/trending/movie/week", key, page=1)` | `{"results":[{"id":693134,"title":"Dune: Part Two","release_date":"2024-02-27"}, …]}` |
| `_tmdb_external_ids(id, media, key)` | L907 | `(693134, "movie", key)` | `{"imdb_id":"tt1160419","wikidata_id":"Q110319188"}` |
| `_search_yts_by_imdb(imdb_id)` | L919 | `"tt1160419"` | `[{"hash":"ef56…","raw_name":"Dune: Part Two (2024) [1080p]","seeders":903}, …]` (no source/category yet) |
| `_search_eztv_by_imdb(imdb_id)` | L942 | `"tt11280740"` | `[{"hash":"cd34…","raw_name":"The.Bear.S03E01…","seeders":812}, …]` (strips `tt`, needs numeric) |
| `_search_tpb_by_title(title, cat)` | L965 | `("The Bear", "205,208")` | `[{"hash":"99ff…","raw_name":"The Bear S03 COMPLETE 1080p","seeders":340}, …]` |
| `_torznab_attr(item, name)` | L989 | `(xml_item, "infohash")` | `"a1b2c3…40hex"` or `""` |
| `_follow_redirect_hash(url)` | L1001 | Jackett proxy link | `"a1b2c3…40hex"` (btih pulled from 302 `Location`) or `""` |
| `_search_jackett(query, indexer, cat_id, limit)` | L1024 | `("Dune", "torrentgalaxyclone", "2000")` | `[{"hash":"…","raw_name":"Dune Part Two 2024 2160p","seeders":120,"source":"torrentgalaxy"}, …]` or `[]` if no key |
| `_search_nyaa_by_title(title)` | L1072 | `"Frieren 2023"` | `[{"hash":"77aa…","raw_name":"[SubsPlease] Frieren - 12","seeders":1500}, …]` |
| `_resolve_ip_id(catalog, prefix, imdb_id, wikidata)` | L1098 | `(cat, "film", "tt1160419")` | `"film-tt1160419"` (catalog imdb-only; `None` if untracked — never minted) |
| `_gather_deadline(workers, fn, items, deadline_s, label, on_result)` | L1119 | concurrency runner | `int` count of completed futures, e.g. `487` (stragglers abandoned at deadline) |

### `fetch_tmdb_movies(api_key, pages=25, catalog=None)` — TMDB trending movies → matched rows (L1150)
Trending IDs → enrich with external_ids → resolve ip_id → multi-source hash search (YTS + apibay +
TorrentGalaxy), `_name_matches_title`-gated, deduped by hash.
```python
fetch_tmdb_movies(key, pages=25, catalog=cat)
# → [{"hash":"ef56…","raw_name":"Dune: Part Two (2024) [1080p]","seeders":903,
#     "source":"yts","category":"Movies","ip_id":"film-tt1160419",
#     "matched_title":"Dune: Part Two","imdb_id":"tt1160419"}, …]
```
Nested: `_get_ext(item)` → item with `imdb_id`/`wikidata_id` added; `_search_one(item)` → list of the
above rows for one movie (returns `[]` if not in catalog or no imdb_id).

### `fetch_tmdb_tv(api_key, pages=25, catalog=None)` — TMDB trending TV → matched rows (L1234)
Same shape; sources are EZTV-by-imdb + apibay-by-title; `source="tmdb"`, `category="Series"`.
```python
fetch_tmdb_tv(key, pages=25, catalog=cat)
# → [{"hash":"cd34…","raw_name":"The.Bear.S03E01.1080p…","seeders":812,
#     "source":"tmdb","category":"Series","ip_id":"series-tt11280740",
#     "matched_title":"The Bear","imdb_id":"tt11280740"}, …]
```

### `fetch_anilist_top(pages=10, catalog=None)` — AniList top anime → matched rows (L1326)
Top anime by popularity → ip_id from `anime-<idMal>` → Nyaa title search with ambiguity guard +
bidirectional reverse-validation (drops "Another"-matching-"Re:ZERO" type noise).
```python
fetch_anilist_top(pages=10, catalog=cat)
# → [{"hash":"77aa…","raw_name":"[SubsPlease] Frieren - 12 (1080p)","seeders":1500,
#     "source":"anilist","category":"Anime","ip_id":"anime-52991",
#     "matched_title":"Frieren: Beyond Journey's End"}, …]
```

### Alias override helpers (inline, at insert time)
| Function | Line | Input | Sample return |
|---|---|---|---|
| `_alias_words(s)` | L1493 | `"Tongari Boushi no Atelier"` | `{"tongari","boushi","atelier"}` (drops "no", stop/tech tokens) |
| `_alias_twords(raw)` | L1499 | `"[Group] Witch Hat Atelier - 03"` | `{"witch","hat","atelier"}` (PTN title, falls back to stripped name) |
| `_load_alias_index()` | L1504 | — | `(postings, by_ip)` defaultdicts; `({}, {})` if `title_aliases.db` absent (no-op) |
| `alias_best_match(raw_name, cur_ip_id)` | L1527 | `("[Grp] Witch Hat Atelier 03", "anime-OLD")` | `"anime-NEW"` if a strictly-more-specific alias matches, else `None` |

### `upsert_hashes(conn, rows)` — write new / bump existing → int new-insert count (L1546)
For each row: if the hash exists, bump `seeders`+`last_seen` only when the new seeder count is higher;
else INSERT (applying `alias_best_match` redirect first). Returns the number of brand-new rows.
```python
upsert_hashes(conn, [
  {"hash":"ef56…","ip_id":"film-tt1160419","matched_title":"Dune: Part Two",
   "raw_name":"Dune…","category":"Movies","seeders":903,"source":"yts"},
  …  # 1,200 rows, 340 of which are new
])
# → 340       (and the hashes table now has those 340 new rows + bumped seeders on the rest)
```

### `ensure_title_row(conn, ip_id, title, category, imdb_id=None, mal_id=None)` → None (L1596)
Inserts a `titles` row if absent; backfills `imdb_id`/`mal_id` on existing rows but **never overwrites**
a non-empty value. Side-effecting; no return.
```python
ensure_title_row(conn, "film-tt1160419", "Dune: Part Two", "Movies", imdb_id="tt1160419")
# → None   (titles row created/backfilled)
```

### `main()` — entrypoint: fetch → dedupe → match → write → audit (L1631)
Parses args, loads keys from `.env` (existence only logged, never the value), builds `Catalog`, loads
the titles-table year map, fetches all sources, dedupes by hash, resolves each to an ip_id, writes
hashes + titles, runs the post-ingest audit. Prints a summary; in `--dry-run` writes nothing.
Nested `_drop(raw_name, reason)` records an unmatched torrent under a tagged reason for diagnostics
(reasons: `collection`, `no-title`, `ambiguous-no-year`, `year-xcheck`, `no-catalog-match`).
```
$ python trending_hash_collector.py --tmdb --anilist
…
Matched: 9,842  |  Unmatched: 3,110  |  YTS direct IMDB: 412
Unmatched breakdown: no-catalog-match=2104  collection=701  ambiguous-no-year=190  …
✅  Done — 1,530 new hashes added, 11,372 total matched
```

### `post_ingest_audit(conn, new_ip_ids)` — false-positive tripwire → list[str] flagged ip_ids (L333)
After insert, for each touched ip_id with ≥3 hashes: (1) title-word consistency <60% → flag; (2) for
movies only, >30% of dated hashes off the canonical year → flag. Writes to `data/suspicious_matches.log`.
```python
post_ingest_audit(conn, {"film-tt1160419", "film-tt0000001"})
# → ["film-tt0000001"]     (and a line appended to suspicious_matches.log)
# returns [] when everything is consistent
```

---

## Deep-dive: how a torrent becomes an `ip_id` (the matching path, step by step)

This is the core of `main()`'s per-torrent loop (L1778–L1872), the most complex decision path. Take
one torrent row and walk it through:

**Input row** (from a fetcher):
```python
{"hash":"cd34…", "raw_name":"Dune.Part.Two.2024.2160p.WEB-DL.x265-GROUP",
 "seeders":903, "source":"tpb_top100", "category":"Movies"}
```

**1. IMDB-direct short-circuit.** If the row carries `imdb_id` (YTS/TMDB rows do), try the O(1) exact
lookup first — highest confidence, skips all fuzzy logic:
```python
catalog.find_by_imdb("tt1160419")   # → ("film-tt1160419", "Dune: Part Two", "Movies")
```
Our sample row has no `imdb_id`, so fall through.

**2. Collection gate.** Box sets / ranges can't map to one ip_id → dropped with reason `collection`:
```python
is_collection("Dune.Part.Two.2024.2160p.WEB-DL.x265-GROUP")   # → False, continue
```

**3. Parse the name.** PTN extracts a clean title + year:
```python
parse_torrent_name("Dune.Part.Two.2024.2160p.WEB-DL.x265-GROUP")   # → ("Dune Part Two", "2024")
```
If no title → drop `no-title`.

**4. Ambiguity gate.** If the title is ambiguous (`is_ambiguous_title`) AND there's no year → drop
`ambiguous-no-year`. "Dune Part Two" is distinctive, so continue.

**5. Fuzzy match.** Hand title+category+year to the catalog:
```python
catalog.fuzzy_match("Dune Part Two", "Movies", year="2024")
# internally:  exact-title? no → word-index candidates on {"dune","part","two"}
#              → SequenceMatcher on the ≤500 best candidates
#              → hard year filter (±1): keeps 2024 entry, rejects a hypothetical 1984 same-name
#              → sequel guard: q_seq=2 must match candidate's seq number
#              → bidirectional coverage: candidate can't be a sparse 1-word title
# → ("film-tt1160419", "Dune: Part Two", "Movies")
```

**6. Year cross-check (ground truth).** The parquet year is often missing, so re-check against the
`titles` table year map loaded at startup. For movies always; for series only when the title is
ambiguous (a series' PTN year is the *episode* air-date, not the premiere):
```python
db_yr = _db_year_map["film-tt1160419"]   # → 2024
gap   = abs(2024 - 2024)                  # → 0
# tolerance = 0 if ambiguous else 2  →  0 <= 2  → keep
# (if this were "The Visitor (2024)" landing on the 2008 film, gap=16 → reject, reason "year-xcheck")
```

**7. Attach + collect.** On success the row gets `ip_id` / `matched_title` / `category` set and joins
`matched`; otherwise `_drop(...)` with the right reason. Matched rows + the pre-matched TMDB/AniList
rows are then written by `upsert_hashes` (which may still redirect via `alias_best_match`) and audited.

---

## Deep-dive: `_name_matches_title` length-scaled rule (the apibay/Jackett guard)

Title-only searches (apibay, TorrentGalaxy) return junk, so every hit is gated. The rule scales with
how many significant words the catalog title has (`n = len(_title_words(title))`):

| Case | Rule | Why |
|---|---|---|
| Movies, `n<=2` | torrent's **PTN title** must equal the title's words exactly | "Michael" must not match "Michael Clayton" / "Michael B. Jordan" |
| Any, `n<=3` | **all** significant words present | "One Piece 4D" needs "4d"; base-franchise "One Piece - 1164" fails |
| `n==4` | `>=3` of 4 present (one drop allowed) | ≈ old 0.60 bar |
| `n>=5` | overlap `>= 0.60` | unchanged |
| Movies fallback | `>=0.40` overlap **and** parsed year within ±1 | recovers subtitle-dropping releases ("Movie: The Subtitle (2025)" → "Movie 2025") |

Plus a **confident year-mismatch reject** (movies only, before the overlap math): if the catalog has a
clean 4-digit year and the torrent's PTN year differs by >1, it's a different film (remake / franchise
entry) → reject regardless of word overlap. This is what separates "Masters of the Universe" 1987 vs
2026, and "Inside Out 2" vs "Inside Out".
```python
_name_matches_title("Inside Out 2 2024 1080p", "Inside Out", "2015", "Movies")  # → False (year 2024 vs 2015)
_name_matches_title("Inside Out 2 2024 1080p", "Inside Out 2", "2024", "Movies")  # → True
```

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Change the title-overlap matcher (length-scaled thresholds) | `_name_matches_title` L253 |
| Change fuzzy title→ip_id matching / year & sequel guards | `Catalog.fuzzy_match` L579, `_best_in` L686, `_seq_number` L655 |
| Change ip_id resolution (imdb-only, never minted) | `_resolve_ip_id` L1098, `Catalog.find_by_imdb` L570, `find_by_mal` L573 |
| Change which sources/feeds are fetched | `sources` list in `main` L1707; fetchers L768–L896 |
| Add/adjust a torrent search source | `_search_*` L919–L1095, wired in `_search_one`/`_search_tv` L1195/L1277 |
| Change TMDB movie/TV pipeline or concurrency | `fetch_tmdb_movies` L1150, `fetch_tmdb_tv` L1234, `_gather_deadline` L1119 |
| Change anime (AniList→Nyaa) matching & reverse-validation | `fetch_anilist_top` L1326 |
| Change ambiguous-title / box-set skipping | `is_ambiguous_title` L177, `is_collection` L147 |
| Change post-ingest false-positive auditing | `post_ingest_audit` L333 |
| Change what gets written to the DB | `upsert_hashes` L1546, `ensure_title_row` L1596; DB block in `main` L1907 |
| Change inline alias→best-match override | `alias_best_match` L1527, `_load_alias_index` L1504 |
| Change the match/drop decision loop & year cross-check | match loop in `main` L1778–L1872, `_drop` L1770 |
| Change Jackett/TorrentGalaxy config or key loading | Config L90–L98, key load in `main` L1665 |
| Change thresholds / page counts / deadlines / source tags | Config L56–L88 |

---

## Quick test recipe (verify behavior with the samples above)
```bash
# fetch + match everything, write nothing — prints top matches + unmatched breakdown:
python trending_hash_collector.py --dry-run --verbose

# include the title-only pipelines (needs TMDB_API_KEY / JACKETT_API_KEY in .env):
python trending_hash_collector.py --tmdb --anilist --dry-run
```
