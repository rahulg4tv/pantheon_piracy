# collect_peers/ — peer-IP collection (given hashes)

**Step 2 of the pipeline.** For each infohash from [`../collect_hashes`](../collect_hashes/README.md),
enumerate the **distinct peer IPs** in its swarm, split by **country**. The count of
distinct IPs sharing a title is our demand metric, so this stage produces the numbers.

→ **Output:** per-source peer tables in separate SQLite DBs (see table below).
→ Component deep-dives: [`../docs/01_dht_peer_count.md`](../docs/01_dht_peer_count.md),
[`../docs/07_tracker_harvest_service.md`](../docs/07_tracker_harvest_service.md),
[`../docs/Peer_Discovery_DHT_Tracker_PEX.pdf`](../docs/Peer_Discovery_DHT_Tracker_PEX.pdf).

```
   infohash
      │
      ├─▶ DHT        get_peers walk + BEP-33 scrape ─────────▶ hashes_v2.db
      │   (bootstrap: router.bittorrent/utorrent, libtorrent, transmission)
      │
      ├─▶ TRACKER    announce (BEP-15 UDP + HTTP) to 15 ─────▶ harvest_peers.db
      │   public trackers, read back swarm (numwant=200)
      │
      ├─▶ PEX        seed from tracker peers, BEP-11 ut_pex ─▶ pex_peers.db
      │
      └─▶ VELOCITY   re-harvest HOT set every few min ───────▶ harvest_velocity_peers.db
                                   │
                                   ▼
             geo-locate each IP → country (+ datacenter/VPN re-attribution)
             record DISTINCT ip per (hash, country), first/last_seen
                                   │
                                   ▼   (unioned once/day, downstream — not here)
             ../export_nbcu.py  +  ../merge_and_upload.py
```

---

## What we use to collect peers (full inventory — no blindspots)

No single method sees a whole swarm, so we run **four independent sources** and union
them downstream. Each finds peers the others miss.

### 1. DHT — `dht_peer_count.py` → `hashes_v2.db`
Mainline DHT `get_peers` walk for each infohash + a **BEP-33 scrape** for swarm size
(seeders/leechers). Runs an **active** tier (fresh/hot hashes, frequent) and a
**dormant** tier (long-tail, less often), with tuned timeouts + early-stop.

- **Bootstrap nodes** (IPv4 + IPv6/BEP-32): `router.bittorrent.com:6881`,
  `router.utorrent.com:6881`, `dht.libtorrent.org:6881`, `dht.transmissionbt.com:6881`.
- Each query then seeds from the bootstrap + the 300 XOR-closest nodes in the pool.

### 2. Tracker-harvest — `tracker_harvest_service.py` (lib: `tracker_harvest.py`) → `harvest_peers.db`
The **workhorse** — usually the largest IP yield. It **announces** to public trackers
(BEP-15 UDP + HTTP/compact) with `numwant=200`, paged over rounds, and reads back the
peer list. Announce-only: it claims to listen on port 6881 and never transfers data.

- **UDP trackers (12):** `tracker.opentrackr.org:1337`, `open.stealth.si:80`,
  `tracker.openbittorrent.com:6969`, `exodus.desync.com:6969`,
  `tracker.torrent.eu.org:451`, `open.demonii.com:1337`, `tracker.dler.org:6969`,
  `explodie.org:6969`, `tracker.0x7c0.com:6969`, `opentracker.io:6969`,
  `tracker.tiny-vps.com:6969`, `tracker.bittor.pw:1337`.
- **HTTP trackers (3):** `tracker.tamersunion.org:443`,
  `tracker.openbittorrent.com:80`, `tracker.gbitt.info:443`.

### 3. PEX — `pex_harvest.py` → `pex_peers.db`
BEP-11 `ut_pex` peer-exchange. For each popular infohash it **seeds peers from
tracker-harvest**, connects (BT + BEP-10 extended handshake advertising `ut_pex`), and
collects the peers those peers gossip. Supplementary; fills gaps DHT + trackers leave.
(Reuses bencode/handshake helpers from `../collect_hashes/bep51_crawler.py`.)

### 4. Velocity lane — `harvest_velocity.py` → `harvest_velocity_peers.db`
Re-harvests a small **HOT set** (fresh + high-seed hashes) every few minutes. New
releases churn peers fast (~28% new distinct IPs per re-harvest on a hot title;
6 re-harvests ≈ 3.5× the IPs), so this keeps a surging title from being undercounted
between normal rotations.

### Turning peers into per-country counts
- Each peer is an **IP** → geo-located to a **country** (GeoIP), with **datacenter /
  VPN ASN re-attribution** to reduce skew from seedboxes and VPN exit nodes.
- We store **distinct** IPs — the same IP seen many times, or by several sources,
  counts once per (hash × country). `first_seen` / `last_seen` let the daily export
  slice by day and compute velocity.

### Why four separate DBs
Each always-on collector writes its **own** SQLite DB so they don't contend on one
file (a hard-won lesson — one shared DB caused WAL bloat). The distinct-IP **union
across all four** happens once per day downstream, not here.

| Source | DB it writes | Runs as (systemd) |
|---|---|---|
| DHT | `hashes_v2.db` (`peers` table) | `dht-peer-count` (active) + `dht-dormant` workers |
| Tracker-harvest | `harvest_peers.db` | `tracker-harvest` |
| Velocity | `harvest_velocity_peers.db` | `harvest-velocity` |
| PEX | `pex_peers.db` | `pex-harvest` |

---

## Files

- **dht_peer_count.py** — DHT `get_peers` + BEP-33 scrape → `hashes_v2.db` peers
  (systemd `dht-peer-count` / `dht-dormant` workers). The primary peer collector.
- **dht_single_writer.py** / **dht_ipc_writer.py** — optional single-writer queue for
  `dht_peer_count`. **OFF by default** (`DHT_SINGLE_WRITER=0`); prod writes direct to
  SQLite. Kept because `dht_peer_count` imports them at module load.
- **tracker_harvest.py** — tracker announce library (BEP-15 UDP/HTTP + the tracker
  pool above); imported by the harvesters, not run directly.
- **tracker_harvest_service.py** — always-on tracker harvest → `harvest_peers.db`
  (systemd `tracker-harvest`). Largest-yield source.
- **harvest_velocity.py** — high-velocity re-harvest lane → `harvest_velocity_peers.db`
  (systemd `harvest-velocity`).
- **pex_harvest.py** — PEX peer-exchange collector → `pex_peers.db` (systemd
  `pex-harvest`). Add the repo root to `PYTHONPATH` when running from this grouped
  layout (it imports `../collect_hashes/bep51_crawler.py`); on the flat production box
  it's a non-issue.
