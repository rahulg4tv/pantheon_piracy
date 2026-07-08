# ops/ — maintenance, monitoring, scheduling

- **s3_sync.sh** — daily push of peer counts / logs / DB backups to S3 (cron)
- **wal_maintenance.sh** — coordinated SQLite WAL reclaim for `hashes_v2.db` (cron, every 2h)
- **run_export_nbcu_daily.sh** — wrapper that runs `../export_nbcu.py` (systemd `export-nbcu`)
- **prune_dead_hashes.py** — prune dead hashes from the catalog (cron)
- **health_watchdog.py** — whole-pipeline health check + SNS alerts (cron)
- **crash_notify.py** — systemd `OnFailure` crash notifier
- **push_metric.py** — CloudWatch custom-metric pusher (cron, every 5 min)
