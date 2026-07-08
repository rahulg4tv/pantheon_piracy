# collect_peers/ — peer-IP collection (given hashes)

Given the infohashes from `../collect_hashes`, gather **distinct peer IPs** per
hash × country from every P2P source. Each writes its own SQLite DB; the sets are
unioned + deduped downstream by `../merge_and_upload.py` and `../export_nbcu.py`.

- **dht_peer_count.py** — DHT `get_peers` walk + BEP-33 scrape → `hashes_v2.db` peers (systemd `dht-peer-count` / `dht-dormant` workers). The primary peer collector.
- **dht_single_writer.py** / **dht_ipc_writer.py** — optional single-writer queue for `dht_peer_count`. **OFF by default** (`DHT_SINGLE_WRITER=0`); prod writes direct to SQLite. Kept because `dht_peer_count` imports them at module load.
- **tracker_harvest.py** — tracker announce library (BEP-15 UDP/HTTP); imported by the harvesters
- **tracker_harvest_service.py** — always-on tracker harvest → `harvest_peers.db` (systemd `tracker-harvest`)
- **harvest_velocity.py** — high-velocity re-harvest lane → `harvest_velocity_peers.db` (systemd `harvest-velocity`)
- **pex_harvest.py** — PEX peer-exchange collector → `pex_peers.db` (systemd `pex-harvest`). Reuses bencode/handshake helpers from `../collect_hashes/bep51_crawler.py`, so add the repo root to `PYTHONPATH` when running from this layout. (On the production box everything is flat, so it's a non-issue.)
