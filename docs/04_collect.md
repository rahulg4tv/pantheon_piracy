# 04 — `collect.py` (Code Reference)

> **Role:** Targeted hash discovery from torrent indexers (Jackett, BitMagnet, EZTV,
> 1337x, TorrentGalaxy, AnimeTosho, Nyaa) **plus** the metadata-enrichment step that
> resolves missing titles/IDs via TMDB + MyAnimeList. Writes to `hashes_v2.db`.
> (The retired `enrich.py` was folded in here.)

---

## Where it runs
- **cron:** `00:10` (`--skip-flare --skip-enrich` — discovery) and `02:00` (`--skip-flare --skip-jackett --skip-bitmagnet` — the **enrich** pass; logs to `enrich.log`).
- **Args:** `--skip-flare` / `--skip-jackett` / `--skip-bitmagnet` / `--skip-enrich` — toggle individual sources / the enrichment step.

## Data flow
```
indexers (Jackett/BitMagnet/EZTV/1337x/TGX/AnimeTosho/Nyaa)  →  upsert → hashes_v2.db.hashes
hashes with missing title/imdb/mal  →  tmdb_enrich / mal_enrich  →  update rows
```

---

## Functions

### DB
`init_db` (53) — schema. `upsert(db, items)` (83) — insert/update hashes (dedupe by hash; fill title/category/seeders/source/imdb/mal).

### Name parsing
`_extract_year` (135) / `_clean_name` (141) — strip release noise (quality tags, groups, years) from a raw torrent name to a searchable title.

### Enrichment (the folded-in `enrich.py` role)
- `tmdb_enrich(raw_name, category)` (165) — query TMDB → returns `{title, imdb_id, year, …}` for movies/series.
- `mal_enrich(raw_name)` (228) — query MyAnimeList/Jikan → `{title, mal_id, …}` for anime.
- `enrich_anime(db)` (262) — find anime hashes with no `mal_id`, fill via `mal_enrich`.
- `enrich_missing(db)` (292) — find hashes with no `imdb_id`, fill via `tmdb_enrich`.
- **Reviewer note:** these run in the 02:00 pass (gated by `--skip-enrich` + `TMDB_KEY` present). This is why `enrich.py` is retired — enrichment lives here now.

### Source fetchers (each → `list[dict]` of hash rows)
`fetch_jackett` (359) + `_parse_jackett` (330) — torznab indexers. `fetch_bitmagnet` (395) — local BitMagnet GraphQL. `fetch_eztv` (455) — EZTV API. `fetch_1337x` (511) + `_flare_get` (502) — 1337x via FlareSolverr (Cloudflare bypass). `fetch_torrentgalaxy` (577) + `_tgx_hash_from_link` (566). `fetch_animetosho` (634), `fetch_nyaa` (690) — anime sources. **Reviewer note:** each fetcher is independently `try/except`-guarded in `main` so one dead indexer doesn't sink the run; `_flare_get` paths are skipped under `--skip-flare`.

### `main()`  (726)
Parse flags → open DB → run the enabled fetchers → `upsert` all results → if enrichment enabled, `enrich_missing` + `enrich_anime` → summary.

---

## Gotchas / invariants (for reviewers)
- **The 02:00 "enrich.log" cron actually runs `collect.py`**, not the retired `enrich.py` — misleading log name, real behavior is `enrich_missing`/`enrich_anime`.
- Enrichment resolves `imdb_id`/`mal_id` but **does not mint `ip_id`** — the catalog mapping happens at export time (`export_nbcu.py`); collect just stores the real external ids.
- Sources are best-effort and isolated; a 0-result fetcher is normal (site down / rate-limited), not a failure.
- `--skip-flare` avoids FlareSolverr dependency when it's not running.

## Change history
`SESSION_CHANGES.md` §4 (direct-API sources), §16/§18 (no-mint ip_id resolution, store imdb/mal), §19 (expanded movie sources). Enrichment absorbed from the retired `enrich.py`.
