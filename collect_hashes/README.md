# collect_hashes/ — infohash discovery + title catalog

Finds the torrent **infohashes** to track and maps them to catalog titles.
Output: the `hashes` / `titles` tables in `hashes_v2.db`.

- **trending_hash_collector.py** — maps trending torrents (TPB / EZTV / Nyaa / YTS) to catalog `ip_id`s and upserts matched hashes → `hashes_v2.db` (cron)
- **collect.py** — daily hash-collection pipeline (Jackett / 1337x via FlareSolverr / BitMagnet), TMDB-enriches titles (cron)
- **bep51_crawler.py** — DHT infohash discovery via BEP-51 `sample_infohashes` (cron)
- **build_title_aliases.py** — builds the title-alias DB used to dedup titles (cron)
- **alias_remap.py** — remaps legacy/duplicate `ip_id`s to canonical (imports `trending_hash_collector`)
