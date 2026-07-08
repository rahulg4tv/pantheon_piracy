# End-to-End Flow — From DHT to Our Per-Country Demand Feed

> **Read this if you want the whole picture in one place.** The per-script docs
> (`01`–`10`) go deep on each component; this file is the **map that connects
> them** — what feeds what, where data lands, and which path is production vs
> legacy. Current as of **2026-05-30** (single-region / US-only; EU instance
> stopped — see `00_OVERVIEW.md` and `SESSION_CHANGES.md` §30).

---

## The one-paragraph version

We discover BitTorrent **info_hashes** for known titles (and raw DHT hashes we
later identify), then measure how many distinct people are sharing each one and
where they are. Measurement happens on **two layers**: a weak DHT **sample**
(`dht_peer_count.py`) and a strong tracker-**announce** harvest
(`tracker_harvest_service.py`). The harvest is what reaches full real-world
magnitude. Once a day we union the distinct IPs from both layers, bucket them by
title × country, drop noise, and write **our own** per-country
IP_COUNT demand feed (`export_nbcu.py`) — the primary product. A separate, weaker
legacy path (`merge_and_upload.py`) still publishes DHT-only peer counts to the
dashboard.

---

## The flow, end to end

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ STAGE 1 — DISCOVER HASHES                            → hashes_v2.db (.hashes) │
├─────────────────────────────────────────────────────────────────────────────┤
│  trending_hash_collector.py   TPB / EZTV / Nyaa / YTS / TMDB / AniList        │
│  bep51_crawler.py             raw DHT network scan (millions/day, filtered)   │
│  collect.py                   Jackett / 1337x / BitMagnet (watchlist search)  │
│  search.py                    Pantheon catalog → Jackett lookup               │
│                                                                               │
│  Each row ties a hash → ip_id → title/category (via the .titles catalog).     │
│  Raw BEP-51 hashes that arrive unidentified get resolved later by enrich.py.  │
└───────────────────────────────────────┬─────────────────────────────────────┘
                                         │
                ┌────────────────────────┴────────────────────────┐
                ▼                                                  ▼
┌──────────────────────────────────┐        ┌────────────────────────────────────┐
│ STAGE 2a — DHT COUNT (WEAK)      │        │ STAGE 2b — TRACKER HARVEST (STRONG) │
│ dht_peer_count.py  (systemd,     │        │ tracker_harvest_service.py (systemd,│
│   always-on, 7 workers on US)    │        │   always-on continuous loop)        │
│                                  │        │                                     │
│ Kademlia walk → samples a swarm. │        │ Re-announces to each swarm's        │
│ Undercounts distinct IPs 3–39×.  │        │ trackers repeatedly. Captures churn │
│ Tiers: active / dormant / new.   │        │ (~23% IPs new/~3min). Daily UNION = │
│                                  │        │ the real distinct-IP demand.        │
│        ▼                         │        │   • breadth: all hashes of a title  │
│  peers table in hashes_v2.db     │        │   • depth: ROUNDS re-announce passes│
│                                  │        │   • worklist: seeders-first order   │
│                                  │        │        ▼                            │
│                                  │        │  peers table in harvest_peers.db    │
│                                  │        │  (SEPARATE single-writer DB)        │
└──────────────────┬───────────────┘        └─────────────────┬──────────────────┘
                   │                                           │
                   │   (STAGE 3 — ENRICH: enrich.py resolves   │
                   │    title+category for raw BEP-51 hashes)  │
                   │   (STAGE 4 — CLEAN: prune_dead_hashes.py  │
                   │    drops hashes nobody shares anymore)    │
                   │                                           │
                   └─────────────────────┬─────────────────────┘
                                         │  union distinct IP per (title, country)
              ┌──────────────────────────┴───────────────────────────┐
              ▼                                                       ▼
┌──────────────────────────────────────┐   ┌─────────────────────────────────────┐
│ STAGE 5a — PRIMARY OUTPUT            │   │ STAGE 5b — LEGACY DASHBOARD         │
│ export_nbcu.py                       │   │ merge_and_upload.py (systemd timer, │
│                                      │   │   05/11/17/23 UTC, 4×/day)          │
│ ATTACH harvest_peers.db read-only +  │   │                                     │
│ UNION hashes_v2.db peers, dedupe IP. │   │ DHT-only peer counts. Canonicalize  │
│ Bucket to 14 named countries +       │   │ → CSV → S3 → dashboard.             │
│ "Other"; drop (title,country) < 10.  │   │ US + EU CSVs (EU optional; degrades │
│ Categories Video:TV / Video:Movie.   │   │ gracefully if EU absent).           │
│        ▼                             │   │        ▼                            │
│  nbcu_equiv_<date>.csv               │   │  peer-counts CSV → S3 → Dashboard   │
│  schema: TMDB_TITLE, TMDB_ID, DATE,  │   │                                     │
│  CATEGORY, COUNTRY_4, IP_COUNT       │   │  *** weaker signal — DHT only ***   │
│  *** our own feed ***                │   │                                     │
└──────────────────────────────────────┘   └─────────────────────────────────────┘
```

---

## Stage-by-stage detail

### Stage 1 — Discover hashes  →  `hashes_v2.db` `.hashes`
Four independent discoverers feed the master hash list. They overlap on purpose
(redundancy); the DB dedupes on `hash`.

| Script | What it pulls | Coverage | Doc |
|---|---|---|---|
| `trending_hash_collector.py` | top torrents from TPB/EZTV/Nyaa/YTS + TMDB/AniList trending | most reliable, matched to catalog | `02` |
| `bep51_crawler.py` | every hash seen on the live DHT, media-filtered | widest, but raw/unidentified | `03` |
| `collect.py` | indexer search for a watchlist of titles | targeted | `04` |
| `search.py` | Pantheon catalog → Jackett | targeted | `07` |

Every hash is tied to an **`ip_id`** (Pantheon catalog ID) → title + category via
the `.titles` table. Hashes from BEP-51 that arrive unidentified are resolved in
Stage 3.

### Stage 2 — Measure peers (TWO layers, run in parallel, always-on)
This is the heart of the system and the reason the numbers are now credible.

- **2a DHT count (`dht_peer_count.py`) — weak sample.** Iterative Kademlia walk,
  geolocates responders, stores per-country counts. Cycles active/dormant/new
  tiers. **Undercounts distinct IPs 3–39×** — kept for ranking + the legacy
  dashboard, *not* trusted as the magnitude truth. Writes `peers` in
  `hashes_v2.db`. Doc `01`.
- **2b Tracker harvest (`tracker_harvest_service.py`) — strong signal.**
  Repeatedly announces to each swarm's trackers and unions every distinct peer
  IP across the day. This is what reaches full real-world magnitude. Writes `peers` in the
  **separate** `harvest_peers.db`. Doc `09`.

**Why two DBs:** the harvester's write volume (millions of rows/day) would bloat
`hashes_v2.db`'s WAL, which the 7 DHT reader workers block from checkpointing.
Isolating the harvester in its own single-writer DB keeps both write paths
healthy. They are reunited (read-only) only at export time.

### Stage 3 — Enrich  (`enrich.py`)
Raw BEP-51 hashes with no known title get their real title + category resolved so
they can be bucketed correctly downstream. Doc `05`.

### Stage 4 — Clean  (`prune_dead_hashes.py`, weekly)
Drops hashes nobody is sharing anymore so the worklist stays focused on live
demand. Doc `06`.

### Stage 5 — Publish (TWO outputs)
- **5a `export_nbcu.py` — PRIMARY OUTPUT.** ATTACHes `harvest_peers.db` read-only,
  UNIONs it with `hashes_v2.db` `.peers`, dedupes **distinct IP per (title,
  country)** so an IP seen by both layers counts once. Buckets to the 14 named
  countries + "Other", drops any (title, country) below 10 IPs, folds anime into
  `Video: TV`. Output `nbcu_equiv_<date>.csv` carries the per-country distinct-IP
  fields our feed reports. **US-only** — does not read EU data. Doc `10`.
- **5b `merge_and_upload.py` — LEGACY dashboard.** DHT-only peer counts,
  canonicalized to CSV and pushed to S3 four times a day for the existing
  dashboard. Consumes US + EU DHT CSVs; degrades gracefully without EU. Weaker
  signal, kept for continuity. Doc `08`.

---

## Data stores (where things live)

| Store | Written by | Read by | Notes |
|---|---|---|---|
| `hashes_v2.db` `.hashes` | Stage 1 discoverers | everything | master hash list, tied to `ip_id` |
| `hashes_v2.db` `.titles` | catalog sync | joins everywhere | Pantheon catalog (ip_id, title, imdb_id, mal_id) |
| `hashes_v2.db` `.peers` | `dht_peer_count.py` | export + merge | DHT sample (weak) |
| `harvest_peers.db` `.peers` | `tracker_harvest_service.py` | `export_nbcu.py` | tracker harvest (strong), separate DB |
| `nbcu_equiv_<date>.csv` | `export_nbcu.py` | the team (validated vs the reference feed) | **primary output** |
| peer-counts CSV → S3 | `merge_and_upload.py` | dashboard | legacy output |

On EC2 the DBs live under `/data/db/`; GeoLite2 at
`/data/geoip/GeoLite2-Country.mmdb`. Local dev mirrors under `data/`.

---

## Who runs where (current reality)

| | US — `YOUR_INSTANCE_ID...` (us-east-1, c6in.xlarge) | EU — `YOUR_INSTANCE_ID...` (eu-central-1, t3a.medium) |
|---|---|---|
| Discovery (Stage 1) | ✅ all four | ❌ |
| DHT count (2a) | ✅ 7 workers | ~~2 workers~~ **STOPPED** |
| Harvester (2b) | ✅ | ❌ |
| Enrich / Prune (3,4) | ✅ | ❌ |
| `export_nbcu.py` (5a) | ✅ **primary** | ❌ |
| `merge_and_upload.py` (5b) | ✅ | ~~EU CSV feed~~ **STOPPED** |

**EU was stopped 2026-05-30.** It only ever fed weak DHT samples into the legacy
dashboard and contributed **nothing** to our per-country demand feed. The KR/DE
under-capture is *structural* (German VPN use, Korean private trackers) and is
**not** fixable by an EU vantage point — do not re-add EU expecting to close that
gap. See `SESSION_CHANGES.md` §30 and Task #35.

---

## The schedule (US, UTC) — condensed

| Time | Job | Stage |
|---|---|---|
| 00:10 | `collect.py` | 1 |
| 00:30 | S3 catalog sync | 1 (titles) |
| 01:00 / 10:00 / 18:00 | `trending_hash_collector.py` | 1 |
| 01:05 / 10:05 / 18:05 | `dht_peer_count.py --new-only` | 2a (baseline new hashes) |
| 02:00 | `collect.py --skip-*` (TMDB enrich) | 1/3 |
| 02:30 | `bep51_crawler.py --filter-media --min-num 10 --bep09` | 1 |
| 03:00 | S3 sync (logs + DB backup) | — |
| 05/11/17/23 | `merge_and_upload.py` (timer) | 5b |
| Sun 04:30 | `prune_dead_hashes.py` | 4 |
| continuous | `dht_peer_count.py` (systemd) | 2a |
| continuous | `tracker_harvest_service.py` (systemd) | 2b |

`export_nbcu.py` (5a) is run per-day against the completed UTC day to produce our
per-country demand feed.

---

## Reading guide

- **Just want the concept?** → `00_OVERVIEW.md`
- **The two measurement layers** → `01_dht_peer_count.md` (weak) +
  `07_tracker_harvest_service.md` (strong)
- **Our per-country demand feed + the metric** → `08_export_nbcu.md`
- **The legacy dashboard path** → `06_merge_and_upload.md`
- **Discovery internals** → `02`–`04`, `07`
- **What's archived and why** → `../junk/README.md`
- **What was tried / what worked / TODO** → `../SESSION_CHANGES.md` (source of truth)
