# hash_trackerv2 — New-Box Setup (from scratch)

> **Purpose:** stand up the whole pipeline on a brand-new EC2 instance from
> nothing — what to provision, where to copy files, how to build the venv,
> which systemd units + crons to install, and the bring-up/verify order.
> Companion to `docs/10_ops_runbook.md` (day-2 on-call). Captured from the live
> US prod box `YOUR_INSTANCE_ID` on 2026-05-31.

Everything below reflects the **real, deployed** configuration. Paths, unit
files, and the crontab are copied verbatim from the running box.

---

## 0. What you end up with

| Layer | Units / jobs |
|---|---|
| **DHT active tier** | 4 systemd services `dht-peer-count{,-w1,-w2,-w3}` (slices 0–3 of /7, worker-ids 0–3) |
| **DHT dormant tier** | 4 systemd services `dht-dormant{,-w1,-w2,-w3}` (slices 0–3 of /4, worker-ids 4–7) |
| **Harvester** | `tracker-harvest.service` (the heavy lifter) |
| **Daily feed** | `export-nbcu.timer` → 23:55 UTC → CSV + S3 |
| **EU sync / legacy** | `db-export-eu.timer`, `merge-and-upload.timer` |
| **Collection/enrich** | crons: `collect.py`, `trending_hash_collector.py` (+`--tmdb`/`--anilist`) |
| **Monitoring** | crons: `health_watchdog.py` (*/15), `push_metric.py` (*/5); `dht-peer-count-alert.service` (OnFailure SNS) |

---

## 1. AWS prerequisites (provision these first)

| Resource | Value on current box |
|---|---|
| **Instance** | Amazon Linux 2023, x86_64. Current box is a general-purpose type with 8 GB RAM (`t3.large`-class is sufficient). |
| **Root EBS** | 8 GB gp3 (OS only). |
| **Data EBS** | **separate 100 GB gp3** volume → mounted at `/data` (see §3). |
| **IAM instance role** | `Ec2instancerole` — needs `sns:Publish`, S3 read/write to the data bucket, S3 read to the catalog bucket, and `cloudwatch:PutMetricData`. Attach **SSM managed policy** (`AmazonSSMManagedInstanceCore`) too, so you can drive it over SSM with no SSH key. |
| **SNS topic** | `arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email` (alerts). Subscribe your email + confirm. |
| **Data bucket** | `s3://YOUR_S3_BUCKET/` (feed output, peer-count CSV/parquet, logs, DB backups). |
| **Catalog bucket (read)** | `s3://YOUR_UI_S3_BUCKET/platform_data/` (movies/series/anime `*_info.parquet`). |
| **Security group** | Outbound all (DHT/tracker traffic is outbound UDP/HTTP). No inbound needed — manage via SSM. If you must SSH, allow 22 from your IP only. |

> Access pattern: `aws ssm start-session --target <id> --profile awsprofile`.
> No public SSH is required because the SSM agent ships with AL2023.

---

## 2. OS packages & AWS CLI

SSH/SSM in as `ec2-user`, then:

```bash
sudo dnf -y update
sudo dnf -y install python3.12 python3.12-pip python3.12-devel \
                    sqlite gcc git tar gzip logrotate
```

- **System python is 3.12** (`python3.12-3.12.13`). The venv is built from it.
- **SSM agent** (`amazon-ssm-agent`) is preinstalled & enabled on AL2023.
- **AWS CLI v2**: install the bundled v2 (it carries its own python3.9 runtime).
  Because the system python was moved to 3.12, the CLI binary must be invoked as
  `**/usr/bin/python3.9 /usr/bin/aws**` — every script and cron already does this.

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
cd /tmp && unzip -q awscliv2.zip && sudo ./aws/install
# sanity (note the python3.9 prefix the whole stack relies on):
/usr/bin/python3.9 /usr/bin/aws --version
```

---

## 3. The `/data` volume

The pipeline keeps **all state** on a dedicated EBS volume so the root disk
can't fill. Current box: `/dev/nvme1n1` (100 GB, XFS).

```bash
# Identify the second volume (likely /dev/nvme1n1):
lsblk
sudo mkfs.xfs /dev/nvme1n1                     # ONLY on a fresh, empty volume
sudo mkdir -p /data
# Persistent mount (matches current /etc/fstab line):
echo '/dev/nvme1n1 /data xfs defaults,nofail 0 2' | sudo tee -a /etc/fstab
sudo mount -a
sudo chown ec2-user:ec2-user /data
```

Create the directory layout:

```bash
mkdir -p /data/{db,daily,peer_counts,peer_counts_parquet,merged,collect,logs} \
         /data/catalog /data/geoip/asn /data/db/backup_tmp
```

| Path | What lives here |
|---|---|
| `/data/db/hashes_v2.db` | Main DB (`hashes`, `titles`, `peers`), WAL mode |
| `/data/db/harvest_peers.db` | Harvester's separate DB (4-day retention) |
| `/data/daily/<date>.csv` | Daily demand feed |
| `/data/peer_counts/*.csv` | DHT per-pass CSVs |
| `/data/peer_counts_parquet/` | Compacted parquet (partitioned by date=) |
| `/data/catalog/*_info.parquet` | Pantheon catalog (refreshed daily by cron) |
| `/data/geoip/GeoLite2-Country.mmdb` | Country lookup |
| `/data/geoip/asn/GeoLite2-ASN.mmdb` | ASN lookup (**subdir on purpose** — the harvester globs `/data/geoip/*.mmdb`, so the ASN db must NOT sit beside the Country db) |
| `/data/logs/` | All logs; logrotate daily |

---

## 4. Code + Python venv

Copy the repo to `/home/ec2-user/hash_trackerv2/`. Use the transfer archive
(`hash_trackerv2_export_*.zip`) or `git clone`:

```bash
cd /home/ec2-user
# from the zip you carried over:
unzip hash_trackerv2_export_*.zip -d hash_trackerv2
# (or) git clone <repo> hash_trackerv2
```

Build the venv (matches the live `pip freeze`):

```bash
python3.12 -m venv /home/ec2-user/venv
/home/ec2-user/venv/bin/python3 -m pip install --upgrade pip
/home/ec2-user/venv/bin/python3 -m pip install \
    aiohttp==3.13.5 bencode.py==4.0.0 boto3==1.43.10 geoip2==5.2.0 \
    maxminddb==3.1.1 numpy==2.4.6 pandas==3.0.3 pyarrow==24.0.0 \
    python-dotenv==1.2.2 requests==2.34.2
```

> `requirements.txt` in the repo is **incomplete** (it predates the harvester +
> GeoIP work). The pinned list above is the authoritative set from the running
> box. Update `requirements.txt` to match if you regenerate it.

Compile-check the entrypoints:

```bash
cd /home/ec2-user/hash_trackerv2
for f in dht_peer_count.py tracker_harvest_service.py export_nbcu.py \
         trending_hash_collector.py collect.py health_watchdog.py; do
  /home/ec2-user/venv/bin/python3 -m py_compile "$f" && echo "OK $f"
done
```

---

## 5. Data assets to copy (not generated)

These are **not** produced by the code — copy them onto the box:

1. **GeoIP databases** (MaxMind GeoLite2, official):
   - `/data/geoip/GeoLite2-Country.mmdb`
   - `/data/geoip/asn/GeoLite2-ASN.mmdb`  ← in the `asn/` subdir (see §3 note).
2. **Catalog parquets** — first run can pull them (the cron does this daily):
   ```bash
   AWS="/usr/bin/python3.9 /usr/bin/aws"
   for f in movies_info series_info anime_info; do
     $AWS s3 cp s3://YOUR_UI_S3_BUCKET/platform_data/$f.parquet /data/catalog/
   done
   ```
3. **Seed DB (optional but recommended)** — to avoid starting cold, restore the
   latest main-DB snapshot the old box uploaded:
   ```bash
   AWS="/usr/bin/python3.9 /usr/bin/aws"
   $AWS s3 cp s3://YOUR_S3_BUCKET/backups/db/<latest>.db.gz /tmp/
   gunzip -c /tmp/<latest>.db.gz > /data/db/hashes_v2.db
   ```
   Without a seed, the collectors rebuild the hash set within a day or two.
   `harvest_peers.db` self-creates on first harvester run.

---

## 6. Secrets / environment

The code reads API keys (TMDB, etc.) from a `.env` in the repo root via
`python-dotenv`. **Copy your existing `.env`** to
`/home/ec2-user/hash_trackerv2/.env` (do not commit it). Confirm presence only:

```bash
test -f /home/ec2-user/hash_trackerv2/.env && echo ".env found" || echo ".env MISSING"
```

To override the SNS topic without editing code, set it in `/etc/environment`:
`SNS_TOPIC_ARN=arn:aws:sns:...` (otherwise `crash_notify.py` / `health_watchdog.py`
use the hardcoded default above).

---

## 7. systemd units

Unit files live in `/etc/systemd/system/`. Below are the **exact** live files.
The worker variants differ only in `Description`, `--slice`, `--worker-id`, and
the log path — generate them with the loops in §7.4.

### 7.1 Active worker — `dht-peer-count.service` (w0; template)

```ini
[Unit]
Description=DHT Peer Counter — Active w0
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=600
StartLimitBurst=5
OnFailure=dht-peer-count-alert.service

[Service]
Type=simple
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/hash_trackerv2
ExecStart=/home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/dht_peer_count.py --workers 1 --concurrency 30 --timeout 5 --loop --loop-delay 60 --skip-dead-days 5 --active-only 1 --active-min-peers 3 --slice 0/7 --worker-id 0
StandardOutput=append:/data/logs/dht_peer.log
StandardError=append:/data/logs/dht_peer.log
Restart=always
RestartSec=30s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

w1/w2/w3 are identical but `--slice 1/7 --worker-id 1` (log `dht_peer_w1.log`),
`2/7`/`2`, `3/7`/`3`.

### 7.2 Dormant worker — `dht-dormant.service` (w4; template)

```ini
[Unit]
Description=DHT Peer Counter — Dormant w4
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=600
StartLimitBurst=5
OnFailure=dht-peer-count-alert.service

[Service]
Type=simple
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/hash_trackerv2
ExecStart=/home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/dht_peer_count.py --concurrency 50 --timeout 5 --loop --loop-delay 120 --skip-dead-days 5 --dormant-only 3 --slice 0/4 --worker-id 4
StandardOutput=append:/data/logs/dht_dormant.log
StandardError=append:/data/logs/dht_dormant.log
Restart=always
RestartSec=30s
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

w1/w2/w3 → `--slice 1/4 --worker-id 5` (log `dht_dormant_w1.log`),
`2/4`/`6`, `3/4`/`7`.

> **Critical correctness note:** every `--loop` worker MUST run the build of
> `dht_peer_count.py` that (a) sleeps-and-continues instead of `exit 0` on an
> empty pass — otherwise `Restart=always` + `StartLimitBurst` trips
> `start-limit-hit` and systemd stops retrying *permanently* (runbook §6 / SESSION
> §34); and (b) has the SQLite lock-retry guard + between-pass `wal_checkpoint`
> (SESSION §36) so writer contention across the 8 workers can't crash-loop them.

### 7.3 Harvester — `tracker-harvest.service`

```ini
[Unit]
Description=Tracker-announce peer harvester (NBCU-scale swarm IP collection)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/hash_trackerv2
Environment=MAX_HASHES=15000
Environment=ROUNDS=4
Environment=CONC=32
Environment=CYCLE_SLEEP=15
ExecStart=/home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/tracker_harvest_service.py
Restart=always
RestartSec=10
StandardOutput=append:/data/logs/tracker_harvest.log
StandardError=append:/data/logs/tracker_harvest.log

[Install]
WantedBy=multi-user.target
```

### 7.4 Crash-alert handler — `dht-peer-count-alert.service`

```ini
[Unit]
Description=DHT Peer Counter — Crash Alert

[Service]
Type=oneshot
User=ec2-user
Group=ec2-user
Environment=UNIT_NAME=dht-peer-count.service
ExecStart=/home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/crash_notify.py
StandardOutput=append:/data/logs/dht_peer.log
StandardError=append:/data/logs/dht_peer.log

[Install]
WantedBy=multi-user.target
```

### 7.5 Timers (oneshot service + timer pairs)

**`export-nbcu.service`**
```ini
[Unit]
Description=Daily per-country demand feed export (harvest+DHT distinct-IP union)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=ec2-user
Group=ec2-user
WorkingDirectory=/home/ec2-user/hash_trackerv2
ExecStart=/bin/bash /home/ec2-user/hash_trackerv2/run_export_nbcu_daily.sh
StandardOutput=append:/data/logs/export_nbcu.log
StandardError=append:/data/logs/export_nbcu.log
```
**`export-nbcu.timer`** → `OnCalendar=*-*-* 23:55:00 UTC`, `Persistent=true`,
`WantedBy=timers.target`.

**`db-export-eu.service`** → `ExecStart=/home/ec2-user/venv/bin/python3 .../db_export_eu.py`
(oneshot). **`db-export-eu.timer`** → `OnCalendar=*-*-* 04,12,20:00:00 UTC`.

**`merge-and-upload.service`** → `ExecStart=.../merge_and_upload.py` (oneshot, legacy).
**`merge-and-upload.timer`** → `OnCalendar=*-*-* 05,11,17,23:00:00 UTC`.

### 7.6 Generate the worker files + enable everything

```bash
cd /etc/systemd/system
# --- active workers w1..w3 from the w0 template ---
for n in 1 2 3; do
  sudo sed -E "s#Active w0#Active w$n#; s#--slice 0/7 --worker-id 0#--slice $n/7 --worker-id $n#; \
       s#dht_peer\.log#dht_peer_w$n.log#g" \
       dht-peer-count.service | sudo tee dht-peer-count-w$n.service >/dev/null
done
# --- dormant workers w1..w3 from the w4 template (worker-ids 5..7) ---
for n in 1 2 3; do wid=$((4+n));
  sudo sed -E "s#Dormant w4#Dormant w$wid#; s#--slice 0/4 --worker-id 4#--slice $n/4 --worker-id $wid#; \
       s#dht_dormant\.log#dht_dormant_w$n.log#g" \
       dht-dormant.service | sudo tee dht-dormant-w$n.service >/dev/null
done

sudo systemctl daemon-reload
# Long-running services:
sudo systemctl enable --now \
  dht-peer-count{,-w1,-w2,-w3} dht-dormant{,-w1,-w2,-w3} tracker-harvest
# Timers (the oneshot services are 'static' — enabled via their timers):
sudo systemctl enable --now export-nbcu.timer db-export-eu.timer merge-and-upload.timer
```

---

## 8. Crontab (ec2-user)

`crontab -e -u ec2-user` and install exactly this (verbatim from the box):

```cron
SHELL=/bin/bash
PATH=/home/ec2-user/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
APP=/home/ec2-user/hash_trackerv2
PYTHON=/home/ec2-user/venv/bin/python3
AWS="/usr/bin/python3.9 /usr/bin/aws"

# Pantheon catalog refresh — 00:30
30 0 * * * $AWS s3 cp s3://YOUR_UI_S3_BUCKET/platform_data/movies_info.parquet /data/catalog/ >> /data/logs/catalog_refresh.log 2>&1 && $AWS s3 cp s3://YOUR_UI_S3_BUCKET/platform_data/series_info.parquet /data/catalog/ >> /data/logs/catalog_refresh.log 2>&1 && $AWS s3 cp s3://YOUR_UI_S3_BUCKET/platform_data/anime_info.parquet /data/catalog/ >> /data/logs/catalog_refresh.log 2>&1

# Trending hash collection — 01:00 / 10:00 / 18:00
0 1  * * * $PYTHON $APP/trending_hash_collector.py >> /data/logs/trending.log 2>&1
0 10 * * * $PYTHON $APP/trending_hash_collector.py >> /data/logs/trending.log 2>&1
0 18 * * * $PYTHON $APP/trending_hash_collector.py >> /data/logs/trending.log 2>&1
0 6  * * * $PYTHON $APP/trending_hash_collector.py --tmdb    >> /data/logs/trending_tmdb.log 2>&1
30 6 * * * $PYTHON $APP/trending_hash_collector.py --anilist >> /data/logs/trending_anilist.log 2>&1

# Collect + enrich
10 0 * * * $PYTHON $APP/collect.py --skip-flare --skip-enrich >> /data/logs/collect.log 2>&1
0 2  * * * $PYTHON $APP/collect.py --skip-flare --skip-jackett --skip-bitmagnet >> /data/logs/enrich.log 2>&1

# S3 sync — 03:00
0 3 * * * $APP/ec2-deploy/instance1/scripts/s3_sync.sh >> /data/logs/s3_sync.log 2>&1

# CloudWatch alive metric — every 5 min
*/5 * * * * $PYTHON $APP/ec2-deploy/shared/cloudwatch/push_metric.py >> /data/logs/metric.log 2>&1

# Logrotate — midnight
0 0 * * * /usr/sbin/logrotate $APP/ec2-deploy/instance1/scripts/logrotate.conf --state /data/logs/logrotate.state

# Weekly prune — Sun 04:00
0 4 * * 0 cd $APP && $PYTHON prune_dead_hashes.py --days 7 --vacuum >> /data/logs/prune.log 2>&1

# Whole-pipeline health watchdog — every 15 min
*/15 * * * * $PYTHON $APP/health_watchdog.py >> /data/logs/health_watchdog.log 2>&1
```

> `merge_and_upload.py` is **retired** as a cron (the `merge-and-upload.timer`
> still runs it; the cron line is left commented on the box). Make `s3_sync.sh`
> executable: `chmod +x $APP/ec2-deploy/instance1/scripts/s3_sync.sh`.

---

## 9. Bring-up order & verification

```bash
# 1. Services up?
systemctl is-active dht-peer-count{,-w1,-w2,-w3} dht-dormant{,-w1,-w2,-w3} tracker-harvest
# 2. Timers armed?
systemctl list-timers export-nbcu.timer db-export-eu.timer merge-and-upload.timer --no-pager
# 3. Whole-pipeline health (expect "OK — all checks passed"):
/home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/health_watchdog.py --dry-run --verbose
# 4. Harvester producing rows?
sqlite3 'file:/data/db/harvest_peers.db?mode=ro' \
  "SELECT COUNT(*) FROM peers WHERE last_seen=date('now') AND ip!='_queried_';"
# 5. Dry-run today's export to a scratch file (does NOT touch the live feed):
/home/ec2-user/venv/bin/python3 export_nbcu.py --date $(date -u +%F) --out /tmp/peek.csv && head /tmp/peek.csv
# 6. Confirm SNS works (send a test from the box):
/usr/bin/python3.9 /usr/bin/aws sns publish --region us-east-1 \
  --topic-arn arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email \
  --subject "new-box bring-up test" --message "hello from $(hostname)"
```

Healthy steady state: 8 DHT workers `active` with `NRestarts` not climbing, the
main-DB WAL oscillating (grows during a pass, truncates to a few MB at each pass
boundary), harvester writing millions of rows/day, disk well under 90%.

---

## 10. Gotchas (carried from the runbook)

- **AWS CLI needs the `python3.9` prefix** — `/usr/bin/python3.9 /usr/bin/aws`.
  A bare `aws` will fail because system python is 3.12.
- **ASN mmdb sits in `/data/geoip/asn/`** on purpose — the harvester globs
  `/data/geoip/*.mmdb` and would mis-load the ASN db as a Country db otherwise.
- **SSM default user is root** — stage deploy files under `/home/ec2-user/`,
  not `/tmp` (root-owned leftovers cause `permission denied` for ec2-user).
- **`dht-new.service` is retired** — do not enable; the watchdog ignores it.
- **Collector & harvester use separate DBs** (`hashes_v2.db` vs
  `harvest_peers.db`) to avoid WAL write contention — keep it that way.
- Deploy/update procedure (gzip+base64+md5-gate+backup) is in runbook §7.

Keep this file in sync with the live box whenever units/crons change.
```
