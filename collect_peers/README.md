# collect_peers/ — peer-IP collection (given hashes)

**Step 2 of the pipeline.** For each infohash from [`../collect_hashes`](../collect_hashes/README.md),
enumerate the **distinct peer IPs** in its swarm, split by **country**. The count of
distinct IPs sharing a title is our demand metric, so this stage is what actually
produces the numbers.

→ **Output:** per-source peer tables in separate SQLite DBs (see below).
→ Component deep-dives: [`../docs/01_dht_peer_count.md`](../docs/01_dht_peer_count.md),
[`../docs/07_tracker_harvest_service.md`](../docs/07_tracker_harvest_service.md),
[`../docs/Peer_Discovery_DHT_Tracker_PEX.pdf`](../docs/Peer_Discovery_DHT_Tracker_PEX.pdf).

---

## How we collect the peers

### 1. Ask the swarm "who else has this?" — from several angles
No single discovery method sees a whole swarm, so we run **independent** sources and
union them. Each finds peers the others miss:

- **DHT** (`dht_peer_count.py`) — walk the Mainline DHT with `get_peers` for each
  infohash to collect the peers the DHT knows, plus a **BEP-33 scrape** for swarm-size
  (seeders/leechers). Runs as an **active** tier (fresh/hot hashes, frequent) and a
  **dormant** tier (long-tail hashes, less often), with tuned timeouts and early-stop
  so slow lookups don't stall a pass.
- **Tracker-harvest** (`tracker_harvest_service.py`) — **announce** to the public
  trackers in each torrent (BEP-15 UDP / HTTP) and read back the peer list the tracker
  returns. This is the **workhorse** — it typically yields the most IPs, because
  trackers return large slices of the swarm directly.
- **PEX** (`pex_harvest.py`) — connect to known peers and use BEP-11 `ut_pex`
  peer-exchange to learn the peers *they* know. Supplementary; fills gaps left by DHT
  and trackers.
- **Velocity lane** (`harvest_velocity.py`) — re-harvests brand-new / fast-moving
  releases much more frequently, so a title spiking today isn't undercounted while
  waiting for the normal rotation.

### 2. Turn peers into per-country distinct-IP counts
- Each discovered peer is an **IP address**. We geo-locate it to a **country** (GeoIP),
  with **datacenter / VPN ASN re-attribution** to reduce the skew from seedboxes and
  commercial VPN exit nodes (which otherwise inflate a handful of hosting countries).
- We record **distinct** IPs — the same IP seen many times, or by several sources,
  counts once per (title × country).
- Rows carry `first_seen` / `last_seen` timestamps so the daily export can slice by
  day and compute day-over-day velocity.

### 3. Each source writes its own DB; union happens downstream
To keep the always-on collectors from contending on one file, **each source writes
its own SQLite DB**. The distinct-IP **union across all four** is done later, once per
day, by [`../export_nbcu.py`](../docs/08_export_nbcu.md) and
[`../merge_and_upload.py`](../docs/06_merge_and_upload.md) — not here.

| Source | DB it writes | Runs as |
|---|---|---|
| DHT | `hashes_v2.db` (`peers` table) | systemd `dht-peer-count` / `dht-dormant` workers |
| Tracker-harvest | `harvest_peers.db` | systemd `tracker-harvest` |
| Velocity | `harvest_velocity_peers.db` | systemd `harvest-velocity` |
| PEX | `pex_peers.db` | systemd `pex-harvest` |

---

## Files

- **dht_peer_count.py** — DHT `get_peers` walk + BEP-33 scrape → `hashes_v2.db` peers
  (systemd `dht-peer-count` / `dht-dormant` workers). The primary peer collector.
- **dht_single_writer.py** / **dht_ipc_writer.py** — optional single-writer queue for
  `dht_peer_count`. **OFF by default** (`DHT_SINGLE_WRITER=0`); prod writes direct to
  SQLite. Kept because `dht_peer_count` imports them at module load.
- **tracker_harvest.py** — tracker announce library (BEP-15 UDP/HTTP); imported by the
  harvesters (not run directly).
- **tracker_harvest_service.py** — always-on tracker harvest → `harvest_peers.db`
  (systemd `tracker-harvest`). The largest-yield source.
- **harvest_velocity.py** — high-velocity re-harvest lane → `harvest_velocity_peers.db`
  (systemd `harvest-velocity`).
- **pex_harvest.py** — PEX peer-exchange collector → `pex_peers.db` (systemd
  `pex-harvest`). Reuses bencode/handshake helpers from
  `../collect_hashes/bep51_crawler.py`, so add the repo root to `PYTHONPATH` when
  running from this grouped layout. (On the production box everything is flat, so it's
  a non-issue.)
