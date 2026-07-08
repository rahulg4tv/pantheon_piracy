# collectors/ — peer-IP collection (non-DHT)

Harvest distinct peer IPs from BitTorrent swarms. Each writes its own SQLite DB;
the sets are unioned + deduped downstream by `../merge_and_upload.py` and
`../export_nbcu.py`. (The primary DHT collector, `dht_peer_count.py`, is in root.)

- **tracker_harvest.py** — tracker announce library (BEP-15 UDP/HTTP); imported by the others
- **tracker_harvest_service.py** — always-on tracker-harvest service → `harvest_peers.db` (systemd `tracker-harvest`)
- **harvest_velocity.py** — high-velocity re-harvest lane for day-0 churn IPs (systemd `harvest-velocity`)
- **pex_harvest.py** — PEX peer-exchange collector (BEP-11 `ut_pex`; systemd `pex-harvest`) → `pex_peers.db`
- **bep51_crawler.py** — BEP-51 DHT infohash-indexing crawl (cron)
- **collect.py** — passive announce/collection helper (cron)

Run from repo root or this folder (imports resolve within `collectors/`).
