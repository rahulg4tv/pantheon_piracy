# ops/ — keeping a single always-on host healthy

Everything here exists because the pipeline runs **8 DHT workers, 3 harvesters, a
web server and several cron jobs against one SQLite database on one machine**.
That constraint, not general good practice, is what shaped these scripts.

## The central problem: one WAL, many readers

`hashes_v2.db` is tens of GB in WAL mode. SQLite keeps **one** WAL file per
database, shared by every connection, and it can only be truncated when **no
reader holds a snapshot**. With ~10 long-lived readers that moment never arrives
on its own: the WAL grows until it fills the disk and starves writers.

**`wal_maintenance.sh`** creates the moment. Every 2h, if the WAL is over
threshold, it stops the collectors, waits for zero readers, truncates, and
restarts them. It deliberately does **not** use `set -e` — a mid-script failure
must still reach the restart trap, or the collectors stay down.

It refuses to run while a heavy job holds the DB, and logs `skipping this cycle`.
That refusal is correct, but the WAL keeps growing meanwhile — so
`health_watchdog.py` alerts on **two consecutive** skips or failures, while there
is still headroom to act.

## Monitoring: check outcomes, not processes

The hard-won lesson: **`systemctl is-active` proves nothing.** A collector can be
`active` and collecting zero. The dashboard's reverse proxy once had no unit at
all, so a reboot silently took the site down while every collector looked fine.

**`health_watchdog.py`** therefore checks **end-to-end outcomes** every 15 min:

- the site returns 200
- peer counts **grew** since the last run — comparing between runs, because a
  growing number cannot lie whereas a freshness query can silently return 0
- today's export exists
- WAL reclaim is not being starved
- disk headroom

It excludes the `_queried_` sentinel rows collectors write for a hash that
returned no peers. Counting those made a **total** collection failure look
healthy, because the row count kept climbing regardless.

**`crash_notify.py`** is the complement: a systemd `OnFailure=` hook that fires
once on a unit failure. It cannot cover a unit that has already stopped retrying
— which is exactly why the watchdog exists alongside it.

**`push_metric.py`** publishes a CloudWatch custom metric every 5 minutes for
external alarms.

## Retention and backup

**`prune_dead_hashes.py`** removes hashes with no recent peers, with a minimum
age so newly-discovered hashes are not pruned before they have been queried.
Use `--dry-run` first. Deleting frees pages for reuse but only `--vacuum` shrinks
the file, and a VACUUM needs an exclusive lock the readers rarely permit.

**`s3_sync.sh`** pushes peer counts, logs and database backups to S3 nightly.

## Scheduling

**`run_export_nbcu_daily.sh`** wraps the daily export for systemd. The export
runs near end-of-day **on purpose**: a past day is not reproducible (see
[`../docs/DATA_MODEL.md`](../docs/DATA_MODEL.md) §5), so the run that captures a
day is the one that counts.

## Operating notes

- Maintenance jobs and collectors contend for the same database — stagger them.
- Anything that bulk-deletes from `peers` while collectors run generates WAL
  faster than it can be reclaimed. Do bulk deletion in a maintenance window with
  the collectors stopped.
- When a monitor reports everything is fine, confirm it *can* fail: feed it a
  known-bad input and check it actually fires. Several checks here were silently
  broken until tested that way.

Runbook: [`../docs/10_ops_runbook.md`](../docs/10_ops_runbook.md).
