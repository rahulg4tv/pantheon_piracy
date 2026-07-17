> **Shared copy.** Infra identifiers (AWS account id, S3 bucket names, EC2 instance ids, host IP) are replaced with `YOUR_*` placeholders — set your own before running. Internal working notes, competitive research, and the security audit are intentionally excluded from this repo.

# Pantheon Piracy Intelligence

A self-hosted pipeline that measures **per-title, per-country piracy demand** as
**distinct peer-IP counts** — an in-house, NBCU-equivalent demand feed — plus a
second **web-streaming** intelligence channel.

The core idea: for every title we care about, find the torrent swarms carrying it,
count how many **distinct people (IP addresses)** are sharing it, and split that
count by **country** and **day**. Distinct-IP count is a direct proxy for demand —
how many unique people pirate a title, and where.

---

## How the pipeline works, step by step

The whole system is four stages. Each stage has its own folder and a detailed
README; this is the short version so a first-time reader can follow the flow
end to end.

```
 ┌──────────────────┐   ┌────────────────┐   ┌──────────────────────┐   ┌───────────────┐
 │ 1. collect_hashes│──▶│2. collect_peers│──▶│ 3. merge & export    │──▶│ 4. feed +     │
 │ which torrents?  │   │ who is sharing?│   │ union → per-country  │   │ dashboard     │
 └──────────────────┘   └────────────────┘   └──────────────────────┘   └───────────────┘
   catalog + matching     DHT · tracker ·      distinct-IP union,          S3 CSV feed +
   → infohashes           PEX · velocity       one row per title×country    Flask web UI
```

### Step 1 — `collect_hashes/` · *pick which torrents to track*

We track a curated **catalog** of titles (movies, series, anime). This stage finds
the torrent **infohashes** that belong to those titles and nothing else.

- **Discover** candidate torrents from several sources: trending scrapers
  (The Pirate Bay, EZTV, Nyaa, YTS), TMDB/AniList "trending" lists fed into targeted
  torrent search, and direct DHT infohash sampling (BEP-51).
- **Match** each torrent's messy release name to a real catalog title — this is the
  careful part. We strip scene release-group tags, disambiguate same-title films by
  year (e.g. *Obsession* 1981 vs 2026), and fuzzy-match against the catalog, so a
  torrent is tracked only when we're confident which title it is.
- **Resolve** the matched title to its authoritative Pantheon `ip_id` (keyed on
  IMDb / MAL id — mapped, never invented) and store the infohash.

→ **Output:** the `hashes` + `titles` tables in `hashes_v2.db`.
→ **Detail:** [`collect_hashes/README.md`](collect_hashes/README.md).

### Step 2 — `collect_peers/` · *count who is sharing them*

Given those infohashes, we enumerate the **distinct peer IPs** in each swarm, from
several **independent** P2P sources so we don't rely on any single one:

- **DHT** — Mainline DHT `get_peers` walk + BEP-33 scrape (active + dormant tiers).
- **Tracker-harvest** — announce to public trackers (BEP-15 UDP/HTTP) and enumerate
  the swarm the tracker returns. The workhorse — usually the largest IP yield.
- **PEX** — BEP-11 peer-exchange, fills peers the others miss.
- **Velocity lane** — re-harvests brand-new / fast-moving releases more often.

Every peer IP is geo-located to a **country** (with datacenter/VPN re-attribution to
reduce skew). Each source writes its **own** SQLite DB; they are unioned later.

→ **Output:** per-source peer tables (`hashes_v2.db`, `harvest_peers.db`,
`harvest_velocity_peers.db`, `pex_peers.db`).
→ **Detail:** [`collect_peers/README.md`](collect_peers/README.md).

### Step 3 — merge & export · *turn peer sets into the product*

The daily jobs take the four per-source peer sets and produce the demand feed:

- **`export_nbcu.py`** — UNIONs all sources, **dedupes to distinct IPs** per
  (title × country), adds per-source breakdown columns, normalizes country names, and
  applies catalog-official titles + IMDb/MAL de-fragmentation → the daily
  NBCU-equivalent per-country CSV.
- **`merge_and_upload.py`** — the same distinct-IP union built for the dashboard
  feed, plus day-over-day velocity and spike alerts.

→ **Output:** daily per-(title × country) CSVs in S3.
→ **Detail:** [`docs/08_export_nbcu.md`](docs/08_export_nbcu.md),
[`docs/06_merge_and_upload.md`](docs/06_merge_and_upload.md).

### Step 4 — feed + dashboard · *consume it*

- **`pantheon_web.py`** (Flask + gunicorn) — per-title/country demand, source toggle,
  world map, trends, and the streaming-site registry tab. Backed by
  **`intel/pantheon_intel.py`**, which builds the dashboard DB from the daily feed.
- **`streaming/`** — the second channel: ranks piracy *streaming sites* from public
  copyright-takedown data and tracks liveness (foundation for per-title
  streaming-demand parsers).

→ **Detail:** [`intel/README.md`](intel/README.md), [`streaming/README.md`](streaming/README.md).

---

## Repository layout

Production runs from a single flat directory; this repo groups files by function so
the flow is easy to read. **Each folder has its own README.**

| Path | Role |
|---|---|
| [`collect_hashes/`](collect_hashes/README.md) | Step 1 — infohash discovery + title catalog/matching |
| [`collect_peers/`](collect_peers/README.md) | Step 2 — distinct peer-IP collection (DHT / tracker / PEX / velocity) |
| root `*.py` | Step 3 — `export_nbcu.py`, `merge_and_upload.py` (feeds), `pantheon_web.py` (dashboard) |
| [`intel/`](intel/README.md) | Dashboard-DB builder + decoy detection |
| [`streaming/`](streaming/README.md) | Channel 2 — streaming-site registry + AceStream live sports |
| [`output/`](output/README.md) | Secondary export & Parquet compaction |
| [`ops/`](ops/README.md) | Maintenance, monitoring, scheduling (WAL, watchdog, S3 sync) |
| [`deploy/`](deploy/README.md) | systemd units & config |
| `docs/` | Architecture, per-component docs, ops runbook, research |
| `tests/` | Tests |

## Running

Runs on a single Linux host: **systemd** services for the always-on collectors
(DHT workers, tracker-harvest, harvest-velocity, pex-harvest, web) plus **cron** for
periodic jobs (collection, daily export, parquet compaction, dead-hash prune, WAL
maintenance, health watchdog). Full setup and operations:
[`docs/11_NEW_BOX_SETUP.md`](docs/11_NEW_BOX_SETUP.md) and
[`docs/10_ops_runbook.md`](docs/10_ops_runbook.md).

## Where to start reading

1. This README — the whole flow in one page.
2. [`docs/09_END_TO_END_FLOW.md`](docs/09_END_TO_END_FLOW.md) — the flow with data
   stores and cron/systemd wiring.
3. [`docs/00_OVERVIEW.md`](docs/00_OVERVIEW.md) — architecture overview, then the
   numbered per-component docs.

## Notes

- **Secrets and data are not tracked.** `.env`, databases, CSV/parquet, GeoIP, and
  snapshots are git-ignored — this repo is **code + docs only**.
