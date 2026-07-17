# collect_hashes/ — infohash discovery + title catalog

**Step 1 of the pipeline.** Decide *which* torrents to track. This stage finds the
BitTorrent **infohashes** for titles in our catalog, matches each one to a real
title, and stores it. Everything downstream only ever sees hashes that made it
through here — so this is where coverage (finding every swarm) and accuracy (not
mistagging) are won or lost.

→ **Output:** the `hashes` and `titles` tables in `hashes_v2.db`.
→ Component deep-dives: [`../docs/02_trending_hash_collector.md`](../docs/02_trending_hash_collector.md),
[`../docs/03_bep51_crawler.md`](../docs/03_bep51_crawler.md),
[`../docs/04_collect.md`](../docs/04_collect.md),
[`../docs/MATCHING_QUALITY_DESIGN.md`](../docs/MATCHING_QUALITY_DESIGN.md).

```
                         DISCOVER (cast a wide net)
   ┌──────────────────────────────────────────────────────────────┐
   │  A. Broad trending / daily sweep     B. Trending → targeted   │
   │     apibay · EZTV · YTS · Nyaa          search                │
   │     Jackett · 1337x · BitMagnet         TMDB & AniList lists  │
   │     BEP-51 DHT crawl                    → per-title torrent    │
   │                                           search              │
   └──────────────────────────────┬───────────────────────────────┘
                                   ▼
                         MATCH each release name → catalog title
                         (strip group · year-disambiguate · fuzzy)
                                   ▼
                         RESOLVE ip_id  (map by IMDb/MAL, never mint)
                                   ▼
                         STORE → hashes_v2.db  +  post-ingest audit
```

---

## What we use to collect hashes (full inventory — no blindspots)

We don't crawl all of BitTorrent; we start from a curated **Pantheon catalog**
(movies / series / anime) and attach live infohashes to it. Hashes come from three
complementary strategies:

### A. Broad discovery — "what's out there right now"

Public listings and local indexers, swept on a schedule. `trending_hash_collector.py`
handles the direct-API scrapers; `collect.py` handles the indexer pipeline.

| Source | Host / endpoint | What it gives | Script |
|---|---|---|---|
| The Pirate Bay | `apibay.org` — `/precompiled/data_top100_<cat>.json` | TPB top-100 per category | `trending_hash_collector.py` |
| EZTV | `eztvx.to` — `/api/get-torrents` (3 pages) | TV episodes (direct JSON) | `trending_hash_collector.py`, `collect.py` |
| YTS | `yts.mx` → `yts.am` fallback — `/api/v2/list_movies.json` (3 pages) | Movies (multi-quality) | `trending_hash_collector.py` |
| Nyaa | `nyaa.si` RSS — `c=1_0` (all anime), `c=1_2` (Eng-translated), `u=SubsPlease` | Anime + SubsPlease releases | `trending_hash_collector.py`, `collect.py` |
| Jackett | `http://localhost:9117` — Torznab, **all configured indexers** | Whatever indexers you've added to Jackett (TPB, YTS, showRSS, …) | `collect.py` |
| 1337x | `1337x.to` via **FlareSolverr** `http://localhost:8191` | 1337x listings (Cloudflare-bypassed) | `collect.py` |
| BitMagnet | `http://localhost:3333/graphql` | Your own local DHT-crawled index | `collect.py` |
| TorrentGalaxy | Jackett `torrentgalaxyclone` indexer (Torznab) | Movies beyond YTS/apibay | `collect.py`, `trending_hash_collector.py` |
| DHT (BEP-51) | Mainline DHT `sample_infohashes` | Swarms that never appear on any site listing | `bep51_crawler.py` |

### B. Trending → targeted search — "chase the titles people want"

Take editorial "trending" lists, resolve each to an IMDb/MAL id, then search the
torrent sources **specifically for that title** (fills swarms the broad sweep missed).

| Trending source | Host / endpoint | Then searched on… |
|---|---|---|
| TMDB movies | `api.themoviedb.org` — `/trending/movie/week` (25 pages ≈ 500 titles) | YTS-by-IMDb (`yts.mx/am`), TPB (`apibay.org/q.php`, cats 201,207,202,200), Jackett `torrentgalaxyclone` |
| TMDB TV | `api.themoviedb.org` — `/trending/tv/week` (25 pages ≈ 500 titles) | EZTV-by-IMDb (`eztvx.to`), TPB (`apibay.org/q.php`, cats 205,208) |
| AniList | `graphql.anilist.co` — top ~500 anime (10 pages) | Nyaa-by-title (`nyaa.si` RSS) → ip_id by MAL |

### C. Metadata / enrichment

| Purpose | Host / endpoint | Script |
|---|---|---|
| Title → IMDb id + alt titles | `api.themoviedb.org` — `/find`, `/external_ids`, `/alternative_titles` | `collect.py`, `build_title_aliases.py` |
| Anime alt titles | `graphql.anilist.co`, `api.jikan.moe` (MAL) | `build_title_aliases.py` |
| Magnet/infohash resolve | Jackett 302→magnet (`_follow_redirect_hash`); `torrage.info` / `itorrents.org` torrent-file fallback | `trending_hash_collector.py` |

### Local service + credential dependencies

These must be running / set for the indexer path (`collect.py`); the direct-API
scrapers work without them.

- **Jackett** `JACKETT_HOST` (default `http://localhost:9117`) + `JACKETT_API_KEY`
- **FlareSolverr** `FLARESOLVERR_HOST` (default `http://localhost:8191`) — for 1337x
- **BitMagnet** `BITMAGNET_HOST` (default `http://localhost:3333`)
- **TMDB** `TMDB_API_KEY` (trending + enrichment)

> If a source is down or a credential is missing, that source logs a skip and the run
> continues — coverage degrades, nothing crashes.

---

## How we match a release to a title (the accuracy part)

A release name is messy (`The.Death.of.Robin.Hood.2026.1080p.WEB-DL.x265-ELiTE`);
turning it into "this is catalog title X and nothing else" is where false positives
happen, so the matcher is deliberately strict:

1. **Strip the release-group tag** (`-ELiTE`, etc.) so a scene group can't be read as
   a title word (group `-ELiTE` vs. the show *Elite*).
2. **Parse** the clean title + year out of the release name.
3. **Disambiguate same-title works by year** — many titles share a name (*Obsession*
   1981 vs 2026). The year (parsed, or regex-extracted when the parser misses it)
   picks the right catalog entry; a hard year filter rejects a candidate off by >1yr.
4. **Fuzzy-match** against the catalog: IMDb-id direct hit → exact normalized title
   (year-disambiguated) → word-index + similarity score with a sequel-number guard.
   Too-generic / ambiguous names are rejected rather than guessed.
5. **Fallback retry** — if the strict pass fails, retry once with season/episode and
   stray-year tokens stripped (`Euphoria.US.S03…`), recovering real matches without
   loosening the strict pass.
6. **Resolve the id — map, never mint.** The title resolves to its catalog `ip_id`
   only if it exists (by IMDb / MAL id); an unresolved torrent is skipped, never given
   a new id.
7. **Store + audit** — upsert into `hashes_v2.db` (deduped by infohash) and run a
   **post-ingest audit** that flags suspicious matches (title-word inconsistency
   across a title's hashes, or a spread of release years for one movie) for review.

---

## Files

- **trending_hash_collector.py** — the main engine: runs the direct-API scrapers
  (A) + the trending→targeted search (B), applies the matcher / `ip_id` resolution,
  upserts matched hashes, runs the post-ingest audit. (cron; the heart of this stage)
- **collect.py** — daily indexer sweep: Jackett (all indexers) / 1337x via
  FlareSolverr / BitMagnet / EZTV / Nyaa / TorrentGalaxy, TMDB-enriches titles. (cron)
- **bep51_crawler.py** — DHT infohash discovery via BEP-51 `sample_infohashes`. (cron)
- **build_title_aliases.py** — builds the title-alias DB (TMDB + AniList + Jikan/MAL)
  used to dedup title variants. (cron)
- **alias_remap.py** — remaps legacy / duplicate `ip_id`s to their canonical id
  (imports `trending_hash_collector`).
