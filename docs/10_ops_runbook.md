# hash_trackerv2 — Operations Runbook

> **Purpose:** the on-call reference for the US production box. What runs, where
> the data lives, how we know it's healthy, and how to recover when it isn't.
> Last updated 2026-05-30.

---

## 1. What this system produces

Our **own** per-country distinct-peer-IP piracy demand feed — the independent
replacement for the third-party NBCU feed. The daily deliverable is:

```
/data/daily/<date>.csv  →  s3://YOUR_S3_BUCKET/daily/<date>.csv
schema: TITLE, IMDB_ID, DATE, CATEGORY, COUNTRY_4, IP_COUNT
```

Produced by `export_nbcu.py` at **23:55 UTC** daily. (See `docs/08_export_nbcu.md`
for the metric definition.)

---

## 2. Infrastructure

| | |
|---|---|
| US prod box | `YOUR_INSTANCE_ID` (us-east-1, public IP YOUR_BOX_IP) |
| EU node | `YOUR_INSTANCE_ID` (eu-central-1) — legacy `merge_and_upload` path only |
| Access | `aws ssm start-session --target <id> --profile awsprofile` |
| Instance role | `Ec2instancerole` (has `sns:Publish`, S3, CloudWatch) |
| Python | `/home/ec2-user/venv/bin/python3`; **AWS CLI** needs `/usr/bin/python3.9 /usr/bin/aws` (AL2023 quirk) |
| Code | `/home/ec2-user/hash_trackerv2/` |
| Data volume | `/data` (100G, separate EBS) |

---

## 3. Services, timers, crons

### Long-running systemd services (must always be `active`)
| Unit | Role |
|---|---|
| `dht-peer-count{,-w1,-w2,-w3}` | **Active tier** DHT peer counter, 4 workers (slices 0-3/7), `--active-only` |
| `dht-dormant{,-w1,-w2,-w3}` | **Dormant tier** DHT scan, 4 workers (4-7), `--dormant-only 3`; long-tail coverage |
| `tracker-harvest` | **Heavy lifter** — UDP/HTTP tracker-announce harvester → `harvest_peers.db` |

`dht-new.service` is **disabled / retired** — leave it; the watchdog ignores it.

### Timers
| Timer | When | Action |
|---|---|---|
| `export-nbcu.timer` | 23:55 UTC | `run_export_nbcu_daily.sh` → daily CSV + S3 |
| `merge-and-upload.timer` | 17:00 / 23:00 | legacy dashboard upload (additive, separate) |
| `db-export-eu.timer` | 04:00 | export hashes for EU node sync |

### Crons (ec2-user)
| When | Job |
|---|---|
| `*/15` | **`health_watchdog.py`** — whole-pipeline health + SNS alert |
| `*/5` | `push_metric.py` — CloudWatch `DHTProcessAlive` / `DHTProgressPct` |
| `00:30` | catalog parquet refresh (movies/series/anime) from S3 |
| `01:00 / 10:00 / 18:00` | `trending_hash_collector.py` — hash discovery |
| `06:00` | `trending_hash_collector.py --tmdb` |
| `06:30` | `trending_hash_collector.py --anilist` |
| `00:10` | `collect.py --skip-flare --skip-enrich` |
| `02:00` | `collect.py` enrich |
| `03:00` | `s3_sync.sh` |
| `00:00` | logrotate |
| `Sun 04:00` | `prune_dead_hashes.py --days 7 --vacuum` |

---

## 4. Data stores

| Path | What |
|---|---|
| `/data/db/hashes_v2.db` | Main DB: `hashes`, `titles`, `peers` (DHT collector writes). WAL mode. |
| `/data/db/harvest_peers.db` | Harvester's separate DB (`peers`); retention 4 days. Kept separate so heavy harvest writes don't bloat the collector's WAL. |
| `/data/daily/<date>.csv` | Daily demand feed output. |
| `/data/peer_counts/*.csv` | DHT per-pass peer-count CSVs. |
| `/data/geoip/GeoLite2-Country.mmdb` | Country lookup (harvester globs `/data/geoip/*.mmdb`). |
| `/data/geoip/asn/GeoLite2-ASN.mmdb` | ASN lookup (official MaxMind, build 2026-05-28). **In a subdir on purpose** — see Gotchas. |
| `/data/logs/` | All service/cron logs; logrotate daily. |

---

## 5. Monitoring & alerting stack

Three layers, complementary:

1. **`health_watchdog.py`** (cron `*/15`) — the catch-all. Per-unit health, feed
   freshness, export delivery, disk. Consolidated SNS alert, throttled (6h
   re-alert, recovery notice). State: `/data/logs/health_watchdog_state.json`.
   Run manually: `python3 health_watchdog.py --dry-run --verbose`.
2. **`push_metric.py`** (cron `*/5`) — CloudWatch `HashTracker/DHTProcessAlive`.
   NOTE: only sees `pgrep dht_peer_count`, so it stays green if *any* DHT worker
   is alive. Not a substitute for the watchdog.
3. **`crash_notify.py`** (systemd `OnFailure=` on dht-peer-count / dht-dormant) —
   one-shot SNS at the moment a unit fails. Goes silent after `start-limit-hit`.

**SNS topic:** `arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email`
(verified deliverable 2026-05-30). All three layers publish here.

---

## 6. Failure modes & recovery

### A service is `failed` / `start-limit-hit`
The classic trap (see SESSION_CHANGES §34): a `--loop` worker that exits 0 on a
transient-empty pass trips `Restart=always` → `StartLimitBurst` → systemd stops
retrying **permanently**. Recovery:
```bash
systemctl reset-failed <unit> && systemctl start <unit>
systemctl is-active <unit>            # confirm 'active'
journalctl -u <unit> -n 50 --no-pager # find why it died
```
The code-level guard is in `dht_peer_count.py` (loop sleeps instead of exiting on
empty). If a NEW loop service is added, give it the same guard.

### Daily export CSV missing / stale
```bash
systemctl status export-nbcu.timer       # armed?
journalctl -u export-nbcu.service -n 50   # last run
# manual re-run for a date:
/home/ec2-user/venv/bin/python3 export_nbcu.py --date <date> --out /data/daily/<date>.csv
```

### Harvester stalled (today's harvest rows low)
```bash
systemctl status tracker-harvest
journalctl -u tracker-harvest -n 80 --no-pager
systemctl restart tracker-harvest   # safe; writes to its own DB
```

### Disk pressure on /data (watchdog warns ≥90%)
Check large/stale files first; e.g. pre-remap DB backups:
```bash
du -sh /data/db/*.bak* /data/peer_counts 2>/dev/null | sort -h
```
Remove only confirmed-stale backups. `harvest_peers.db` self-prunes (4-day
retention); the Sunday `prune_dead_hashes.py --vacuum` reclaims main-DB space.

### DB locked / WAL bloat
Collector + harvester use **separate** DBs to avoid this. If the main DB shows
lock contention, check no ad-hoc writer is attached; readers should use
`?mode=ro`. Never run a long write txn against `hashes_v2.db` during a pass.

**DHT workers crash-looping on `database is locked` (see SESSION_CHANGES §36).**
All 8 DHT workers write to `hashes_v2.db`; SQLite WAL allows one writer at a
time. If the **WAL file grows huge** (seen at 686MB), every write slows past the
busy-timeout and workers crash → `Restart=always` churn (watchdog may report a
misleading "RECOVERED" as it samples a worker mid-restart). Code now retries
locked writes + checkpoints the WAL between passes, so this shouldn't recur. To
diagnose / reclaim a bloated WAL manually:
```bash
ls -la /data/db/hashes_v2.db-wal           # how big is the WAL?
sqlite3 /data/db/hashes_v2.db "PRAGMA wal_checkpoint(TRUNCATE);"
# returns 'busy|N|N' (1|..) if readers active and the file won't shrink.
# To force-reclaim, briefly stop the readers (harvester is a SEPARATE DB, safe):
U="dht-peer-count dht-peer-count-w{1,2,3} dht-dormant dht-dormant-w{1,2,3}"
systemctl stop $U && sleep 2
sqlite3 /data/db/hashes_v2.db "PRAGMA wal_checkpoint(TRUNCATE);"  # want 0|0|0
for u in $U; do systemctl reset-failed $u; systemctl start $u; done
```

**Automated reclaim — `wal_maintenance.sh` (cron).** The manual steps above are
automated. `wal_maintenance.sh` runs by cron at **`20 */3 * * *`** (every 3h; was
`20 4,16` / 2×-day until 2026-06-04 — bumped to keep the WAL peak ~1–1.5 GB instead
of multi-GB between reclaims). It is **threshold-gated**: it only acts when the WAL
exceeds `WAL_THRESH_MB` (default 500). When it fires it does the same coordinated
stop-8-workers → `wal_checkpoint(TRUNCATE)` → restart, with a guaranteed restart via
an `EXIT` trap, and it **skips** the cycle if a one-off cron writer (`collect.py`,
`bep51_crawler.py`, `compact_peer_counts.py`, `--new-only`) is mid-run (the truncate
would return BUSY anyway — it retries next cycle). Log: `/data/logs/wal_maintenance.log`.
Tune the cron interval or `WAL_THRESH_MB` to trade WAL-peak size vs worker-restart
frequency — **each run restarts the 8 DHT workers**, which then re-bootstrap (a few-min
DHT dip; the harvester is on a separate DB and unaffected). Backups of the units it
restarts and its own deploys live alongside on the box.

> NOTE (2026-06-04): the single-writer "Phase 2" alternative to this firefighting was
> built, tested lossless, but SHELVED — WAL `TRUNCATE` is blocked by collector *reader*
> connections, not just writers, so consolidating writes alone didn't fix it. See
> `SESSION_CHANGES.md` and `docs/proposals/single_writer_queue/`. `wal_maintenance.sh`
> remains the operating mechanism.

---

## 7. Deploy procedure (how code reaches the box)

No CI; deploys are manual and gated:
1. Edit + `python3 -m py_compile` locally; note local md5.
2. `gzip -c file | base64` → SSM `RunShellScript` → decode on box.
3. Remote `py_compile` + **md5-equality gate** before swapping.
4. `cp -p` the live file to `*.bak.<ts>` first; then swap.
5. Restart only the unit(s) that need the new code; verify `is-active`.

SSM tip: base64 the payload to dodge JSON quoting; guard each step with
`|| true`; stage in `/home/ec2-user/` (not `/tmp`, which may hold root-owned
leftovers).

---

## 8. Routine ops commands

```bash
# Full health snapshot
python3 health_watchdog.py --dry-run --verbose

# Are all workers up?
systemctl is-active dht-peer-count{,-w1,-w2,-w3} dht-dormant{,-w1,-w2,-w3} tracker-harvest

# Today's volumes
sqlite3 'file:/data/db/harvest_peers.db?mode=ro' \
  "SELECT COUNT(*) FROM peers WHERE last_seen=date('now') AND ip!='_queried_';"

# Top titles today
/home/ec2-user/venv/bin/python3 export_nbcu.py --date $(date -u +%F) --out /tmp/peek.csv
```

---

## 9. Future hardening (prioritised, not yet done)

1. **Wire `OnFailure=` on `tracker-harvest` and `export-nbcu.service`** to an
   alert handler — currently only the watchdog covers them. (Watchdog catches it
   within 15 min, so this is defence-in-depth, not urgent.)
2. **CloudWatch alarm** on a watchdog heartbeat metric (so a *dead watchdog*
   itself is detected). Today the watchdog is the backstop but nothing watches it.
3. **Second box / failover** — single-instance SPOF. Document an AMI + restore.
4. **Retire stale backups automatically** once disk policy is set.
5. **End-to-end export validation** alert (row count / title count sanity vs
   trailing 7-day median) to catch a "ran successfully but produced garbage" day.

Keep this file current — it is the source of truth for on-call.
