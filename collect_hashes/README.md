# collect_hashes/ — infohash discovery + title catalog

**Step 1 of the pipeline.** Decide *which* torrents to track. This stage finds the
BitTorrent **infohashes** that belong to titles in our catalog, matches each one to a
real title, and stores it. Everything downstream (peer collection, the demand feed)
only ever sees hashes that made it through here — so this is where accuracy is won or
lost.

→ **Output:** the `hashes` and `titles` tables in `hashes_v2.db`.
→ Component deep-dives: [`../docs/02_trending_hash_collector.md`](../docs/02_trending_hash_collector.md),
[`../docs/03_bep51_crawler.md`](../docs/03_bep51_crawler.md),
[`../docs/04_collect.md`](../docs/04_collect.md),
[`../docs/MATCHING_QUALITY_DESIGN.md`](../docs/MATCHING_QUALITY_DESIGN.md).

---

## How we pick the hashes

### 1. Start from a catalog, not the open firehose
We don't try to track *all* of BitTorrent. We start from a curated **Pantheon
catalog** of titles (movies / series / anime). Each catalog entry has an
authoritative id (`ip_id`) keyed on its **IMDb id** (or **MAL id** for anime). The
job of this stage is to attach real, live infohashes to those catalog entries.

### 2. Discover candidate torrents (several independent sources)
More sources = more of a title's swarms found, so we cast a wide net:

- **Trending scrapers** — pull what's hot right now from The Pirate Bay, EZTV, Nyaa,
  YTS, SubsPlease.
- **Trending lists → targeted search** — take TMDB (movies/TV) and AniList (anime)
  "trending this week", then search the torrent sites/indexers specifically for those
  titles (YTS-by-IMDb, TPB, Jackett/Torznab, Nyaa).
- **DHT infohash sampling** — crawl the Mainline DHT directly via BEP-51
  `sample_infohashes` to discover swarms that never show up on a site listing.
- **Indexer pipeline** — Jackett / 1337x (via FlareSolverr) / BitMagnet for a broader
  daily sweep.

### 3. Match each torrent to a catalog title (the careful part)
A torrent's name is a messy release string like
`The.Death.of.Robin.Hood.2026.1080p.WEB-DL.x265-ELiTE`. Turning that into "this is
catalog title X and nothing else" is where false positives happen, so the matcher is
deliberately strict:

- **Strip the release-group tag** first (`-ELiTE`, etc.) so a scene group can't be
  mistaken for a title word (e.g. group `-ELiTE` vs. the show *Elite*).
- **Parse** the clean title + year out of the release name.
- **Disambiguate same-title works by year** — many films share a name (*Obsession*
  1981 vs 2026). The year (from the release name, or extracted with a regex when the
  parser misses it) picks the right one; a hard year filter rejects a candidate whose
  catalog year is off by more than a year.
- **Fuzzy-match** against the catalog: IMDb-id direct hit first, then exact
  normalized title (year-disambiguated), then a word-index + similarity score with a
  sequel-number guard. Ambiguous / too-generic names are rejected rather than
  guessed.
- **Fallback retry** — if the strict match fails, retry once with season/episode and
  stray-year tokens stripped (some scene names bury the title, e.g.
  `Euphoria.US.S03...`), so we recover real matches without loosening the strict pass.

### 4. Resolve the id — *map, never mint*
The matched title resolves to its catalog `ip_id` **only** if it exists in the
catalog (looked up by IMDb / MAL id). We never invent a new id for an unmatched
torrent — an unresolved torrent is simply skipped. This keeps the id space clean and
de-fragmented.

### 5. Store + audit
Matched hashes are upserted into `hashes_v2.db` (deduped by infohash), the `titles`
row is ensured, and a **post-ingest audit** flags suspicious title→hash matches
(title-word inconsistency across a title's hashes, or a spread of release years for a
single movie) for review.

---

## Files

- **trending_hash_collector.py** — the main engine: runs the discovery sources above,
  applies the matcher/`ip_id` resolution, upserts matched hashes, and runs the
  post-ingest audit. (cron; the heart of this stage)
- **collect.py** — daily hash-collection pipeline (Jackett / 1337x via FlareSolverr /
  BitMagnet), TMDB-enriches titles. (cron)
- **bep51_crawler.py** — DHT infohash discovery via BEP-51 `sample_infohashes`. (cron)
- **build_title_aliases.py** — builds the title-alias DB used to dedup title variants.
  (cron)
- **alias_remap.py** — remaps legacy / duplicate `ip_id`s to their canonical id
  (imports `trending_hash_collector`).
