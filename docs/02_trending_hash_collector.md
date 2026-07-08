# 02 — `trending_hash_collector.py` (Code Reference)

> **Role:** The primary hash-discovery engine. Pulls what's trending from torrent sites
> + TMDB + AniList, **matches each torrent to a known catalog title** (the careful part),
> resolves the Pantheon `ip_id`, and upserts hashes. Also runs a post-ingest audit that
> flags suspicious title→hash matches.

---

## Where it runs
- **cron:** `01:00 / 10:00 / 18:00` (main trending) · `06:00` (`--tmdb`) · `06:30` (`--anilist`).
- **Args:** `--tmdb`, `--anilist` (else: the scraper sources).

## Data flow
```
TPB/EZTV/Nyaa/YTS/SubsPlease  ┐
TMDB trending → YTS/TPB/Jackett search   ├─ raw torrents → _name_matches_title → Catalog
AniList top-500 → search                 ┘                       ↓ ip_id (mapped)
                                            upsert_hashes + ensure_title_row → hashes_v2.db
                                                       ↓
                                            post_ingest_audit → suspicious_matches.log
```

---

## Matching layer (the heart — where false positives are prevented)

### `_name_matches_title(raw_name, title, year="", category="")`  (253)
Gate for title-only searches. Requires ≥60% of the title's significant words to appear in the torrent name (movies: ≥40% **if** the parsed year matches ±1). **Strips the release-group tag first** via `_strip_release_group` so a scene group like `x265-ELiTE` can't match the show "Elite" (§44).

### `_strip_release_group(raw_name)`  (233)
Removes the trailing `-GROUP` scene tag (+ file extension) before tokenizing. Regression-safe: only drops a trailing token, never invents a match; mid-name hyphens (`Spider-Noir`) untouched. (Added §44.)

### `_title_words` (227) / `_normalize` (406) / `parse_torrent_name` (418)
`_normalize` — lowercase, strip accents/punct. `_title_words` — significant words (drops stop-words + tech tokens via `_AUDIT_TECH`). `parse_torrent_name` — PTN wrapper → (clean title, year).

### `is_collection` (147) / `is_ambiguous_title` (177)
Guards: `is_collection` rejects season packs / multi-title bundles; `is_ambiguous_title` flags titles too generic to match safely (all-stopword, known-ambiguous solo words, short acronyms) so they aren't matched on weak signals.

### `post_ingest_audit(conn, new_ip_ids)`  (281)
After insert, for each touched ip_id checks (1) title-word consistency across its hashes (<60% share → flag) and (2) year-spread for movies (multiple distinct years → flag). Writes to `data/suspicious_matches.log`. **Reviewer note:** also strips release groups now (§44) so a group token can't inflate consistency.

---

## Catalog & id resolution

### `class Catalog`  (433)
Loads the Pantheon catalog parquets (movies/series/anime) into in-memory indexes for fast title/imdb/mal → ip_id lookups during matching.

### `_resolve_ip_id(catalog, prefix, imdb_id, wikidata="")`  (1046)
Resolves the authoritative Pantheon `ip_id` from the catalog by `imdb_id` (movies/series) — **never minted**; returns `None` if not in catalog (caller skips). The map-don't-mint rule, enforced at collection time.

---

## Source fetchers

### Scrapers (→ `list[dict]`)
`fetch_tpb_top100` (716), `fetch_eztv` (736), `fetch_yts` (760), `fetch_nyaa_rss` (828)/`_fetch_nyaa_rss_url` (803), `fetch_subsplease_rss` (835). `_fetch` (110) — shared HTTP GET with retries.

### TMDB / AniList (trending → targeted torrent search)
- `fetch_tmdb_movies` (1098) / `fetch_tmdb_tv` (1182): pull `/trending/{movie,tv}/week` → `_tmdb_external_ids` for the imdb id → `_resolve_ip_id` → search torrents (`_search_yts_by_imdb`, `_search_tpb_by_title`, `_search_jackett`), each candidate gated by `_name_matches_title`. **Reviewer note:** no release-date filter — unreleased titles simply have no torrents, so they yield nothing (see discussion in `00_OVERVIEW`/FLOW). Year-match guards stray same-name films.
- `fetch_anilist_top` (1274): AniList GraphQL top-500 anime → search (`_search_nyaa_by_title`, etc.) → ip_id by MAL.
- `_gather_deadline(workers, fn, items, deadline_s, …)` (1067) — bounded-time concurrent map, so a slow source can't hang the whole run (the `TMDB_*_DEADLINE` guards, §31).

### Search helpers
`_search_yts_by_imdb` (867), `_search_eztv_by_imdb` (890), `_search_tpb_by_title` (913), `_search_jackett` (972) + `_torznab_attr`/`_follow_redirect_hash`, `_search_nyaa_by_title` (1020).

---

## Write path
`upsert_hashes(conn, rows)` (1431) — insert/update hashes (dedupe by hash). `ensure_title_row(conn, ip_id, title, …)` (1474) — make sure the `titles` row exists (ip_id, title, category, imdb_id, mal_id). `main()` (1509) — dispatch by flag, run sources, upsert, then `post_ingest_audit`.

---

## Gotchas / invariants (for reviewers)
- **Matching is the risk surface.** `_name_matches_title` + `_strip_release_group` + `is_ambiguous_title` are what stop release-group / generic-word false matches (the "Elite" class, §44). Touch with a regression test batch.
- **`ip_id` is mapped, never minted** (`_resolve_ip_id` returns `None` if not in catalog) — consistent with `export_nbcu.py`.
- **Deadline-bounded fetchers** (§31) — a hung TMDB/search can't stall the cron run.
- TMDB/AniList enrich titles but the real demand counting is downstream (DHT + harvester).

## Change history
`SESSION_CHANGES.md` §7/§8 (broaden TMDB), §16/§18 (no-mint ip_id), §19 (movie sources), §20/§21/§22 (matcher loosening + mining + ranking), §31 (deadline guards), §44 (release-group strip).
