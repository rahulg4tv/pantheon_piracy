> **Shared copy.** Infra identifiers (AWS account id, S3 bucket names, EC2 instance ids, host IP) are replaced with `YOUR_*` placeholders — set your own before running. Internal working notes, competitive research, and the security audit are intentionally excluded from this repo.

# Pantheon Piracy Intelligence

A self-hosted pipeline that measures **per-title, per-country piracy demand** as
**distinct peer-IP counts** — an in-house, NBCU-equivalent demand feed — plus a
second **web-streaming** intelligence channel.

## What it does

- **Channel 1 — P2P demand (primary).** For each tracked title, count the *distinct
  peer IPs* sharing it on BitTorrent, bucketed by country and day. This is a direct
  demand proxy (how many unique people pirate a title, and where). Output: a daily
  per-(title × country) CSV feed.
- **Channel 2 — Web-streaming registry.** Rank piracy *streaming sites* from public
  copyright-takedown data and track liveness — the foundation for per-title
  streaming-demand parsers.

## Peer discovery — three independent P2P sources

| Source | File | Role |
|---|---|---|
| Tracker-harvest | `tracker_harvest_service.py` | announce to public trackers, enumerate swarm peers (the workhorse) |
| DHT | `dht_peer_count.py` | Mainline DHT `get_peers` sampling (active + dormant tiers; tuned timeout + early-stop) |
| PEX | `pex_harvest.py` | BEP-11 `ut_pex` peer exchange (supplementary; fills gaps the others miss) |

`export_nbcu.py` UNIONs all sources and dedupes distinct IPs per (title, country),
with per-source breakdown columns, country-name normalization, catalog-official
titles, and imdb/MAL-keyed de-fragmentation.

## Pipeline (high level)

```
collect / trending  ->  hash catalog  ->  DHT + tracker-harvest + PEX peer collection
                    ->  daily export (export_nbcu.py)  ->  S3 + dashboard
```

## Dashboard

`pantheon_web.py` (Flask + gunicorn) — per-title/country demand, source toggle
(DHT / Harvest), world map, trends, and the streaming-site registry tab. Backed by
`pantheon_intel.py`, which builds the dashboard DB from the daily feed.

## Repo layout

- root `*.py` — the live pipeline + its dependencies
- `misc/` — superseded / experimental / one-off reference tools (see `misc/README.md`)
- `docs/` — architecture, per-component docs, ops runbook, research, proposals
- `SESSION_CHANGES.md` — detailed change log · `NEXT_SESSION_TODO.md` — roadmap / TODO
- `requirements.txt`, `*.service`, `*.sh` — deps, systemd units, scripts

## Running

Runs on a single Linux host: **systemd** services for the always-on collectors
(DHT workers, tracker-harvest, harvest-velocity, pex-harvest, web) plus **cron** for
periodic jobs (collection, daily export, parquet compaction, dead-hash prune, WAL
maintenance, health watchdog). See `docs/10_ops_runbook.md` and
`docs/11_NEW_BOX_SETUP.md`.

## Notes

- **Secrets and data are not tracked.** `.env`, databases, CSV/parquet, GeoIP, and
  snapshots are git-ignored (see `.gitignore`) — this repo is **code + docs only**.
- Start with `docs/00_OVERVIEW.md` and `docs/09_END_TO_END_FLOW.md`.

## Layout
- **Root** — the scheduled pipeline: every script run by cron or a systemd service, plus the modules they import (`tracker_harvest.py`, `dht_single_writer.py`).
- **`tools/`** — auxiliary / ad-hoc scripts not in the automated schedule (see `tools/README.md`).
- **`docs/`** — architecture, per-component, and end-to-end flow docs.
