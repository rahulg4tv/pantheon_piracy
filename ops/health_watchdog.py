#!/usr/bin/env python3
"""health_watchdog.py — periodic whole-pipeline health check for hash_trackerv2.

WHY THIS EXISTS
---------------
Two alerting mechanisms already exist, and BOTH missed the 2026-05-29 incident
where all four dht-dormant workers died and stayed dead for ~1.5 days:

  * push_metric.py (CloudWatch, every 5 min) only checks `pgrep dht_peer_count`
    → 1/0. The ACTIVE workers (w0-w3) were alive, so DHTProcessAlive stayed 1
    and nothing fired even though a whole tier was down.
  * crash_notify.py (SNS, via systemd OnFailure=) fires ONCE on the failure
    transition and is wired only to dht-peer-count / dht-dormant. After a unit
    hits `start-limit-hit` systemd stops retrying and stays silent forever, and
    the harvester / export units have no OnFailure hook at all.

This watchdog is the catch-all: it inspects the WHOLE pipeline every 15 min and
sends ONE consolidated SNS alert when something is actually wrong. It would have
caught that incident within 15 minutes.

WHAT IT CHECKS
--------------
  1. Every critical systemd service is `active` (per-unit, not just "some
     dht_peer_count process exists").
  2. No unexpected unit is in `failed` state (ignores intentionally-retired
     units in IGNORE_FAILED).
  3. export-nbcu.timer is armed AND a recent daily CSV was delivered to
     /data/daily/ (today's after the 23:55 run, else yesterday's).
  4. harvest_peers.db has a healthy number of today's peer rows (heavy lifter
     is actually writing).
  5. Main hashes_v2.db peers are fresh (max(last_seen) == today).
  6. /data disk headroom (alert at >=90% used).

ALERTING
--------
Publishes to the existing SNS topic (same one crash_notify.py uses). Alerts are
THROTTLED via a JSON state file so a standing problem re-alerts at most once per
RE_ALERT_HOURS, and a single RECOVERY notice is sent when a problem clears.
Read-only on the pipeline; the only thing it writes is its own state file.
Always exits 0 (a monitor must never itself trip OnFailure handlers).

Usage:
    python3 health_watchdog.py            # check + alert (throttled)
    python3 health_watchdog.py --dry-run  # check + print, never publish SNS
    python3 health_watchdog.py --verbose  # print every check's result
Cron (every 15 min):
    */15 * * * * $PYTHON $APP/health_watchdog.py >> /data/logs/health_watchdog.log 2>&1
"""
from __future__ import annotations
import argparse
import json
import os
import subprocess
import sqlite3
import sys
import time
from datetime import datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
REGION         = "us-east-1"
SNS_TOPIC_ARN  = os.getenv("SNS_TOPIC_ARN",
                           "arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email")
INSTANCE_ID    = "YOUR_INSTANCE_ID"
STATE_PATH     = "/data/logs/health_watchdog_state.json"
RE_ALERT_HOURS = 6        # re-alert a still-broken thing at most this often
DISK_PATH      = "/data"
DISK_WARN_PCT  = 90

HARVEST_DB     = "/data/db/harvest_peers.db"
MAIN_DB        = "/data/db/hashes_v2.db"
DAILY_DIR      = "/data/daily"

# Long-running services that must always be `active`.
CRITICAL_UNITS = [
    "dht-peer-count.service", "dht-peer-count-w1.service",
    "dht-peer-count-w2.service", "dht-peer-count-w3.service",
    "dht-dormant.service", "dht-dormant-w1.service",
    "dht-dormant-w2.service", "dht-dormant-w3.service",
    "tracker-harvest.service",
    "harvest-velocity.service",
    "pantheon-web.service",
    "pex-harvest.service",
]
# Timer that must be armed (active) for the daily deliverable.
CRITICAL_TIMERS = ["export-nbcu.timer"]

# Units that are KNOWN to be failed/retired on purpose — do not alert on these.
IGNORE_FAILED = {"dht-new.service"}
# Only app-owned units are in scope for the failed-unit scan. OS boot units
# (cloud-init/cloud-config/cloud-final/hibinit-agent) are frequently+benignly
# "failed" on Amazon Linux 2023 EC2 and are NOT this pipeline's concern — that's
# the CloudWatch agent's job. We watch only our own systemd units here.
APP_UNIT_PREFIXES = ("dht-", "tracker-", "export-", "merge-", "db-export")

# Minimum today's harvest peer rows below which we consider the harvester stalled.
HARVEST_MIN_ROWS_TODAY = 5000

# Grace window after the 00:00 UTC date rollover. The freshness checks below ask
# "how much data is stamped *today*?" — but right after midnight the workers are
# mid-pass and the harvester mid-cycle, so neither has written a row for the new
# day yet. That produced nightly false-positive SNS alerts (harvest_stalled +
# main_db_stale at 00:00, auto-recovering by ~00:30). During this window we skip
# the two freshness checks; a genuine stall is still caught from ~01:00 onward
# (and the unit-level checks, which don't depend on the date, run regardless).
MIDNIGHT_GRACE_MIN = 45
# ─────────────────────────────────────────────────────────────────────────────


def _in_midnight_grace() -> bool:
    """True during the first MIDNIGHT_GRACE_MIN minutes after 00:00 UTC."""
    now = datetime.now(timezone.utc)
    return now.hour == 0 and now.minute < MIDNIGHT_GRACE_MIN


def _run(cmd: list[str], timeout: int = 10) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return (r.stdout or "").strip()
    except Exception as e:
        return f"__ERR__:{e}"


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ── individual checks: each returns list of (key, detail) problems ────────────
def _wal_maint_running() -> bool:
    """True while wal_maintenance.sh is mid-run. It stop/restarts the 8 DHT workers
    for the WAL truncate (which can take minutes on a large WAL), so a DHT unit can be
    legitimately 'inactive' through no fault. Suppress those alerts during this window
    — this is the recurring 'crashed' false-positive on our own maintenance."""
    return bool(_run(["pgrep", "-f", "wal_maintenance"]))


def check_units(verbose: bool) -> list[tuple[str, str]]:
    problems = []
    for unit in CRITICAL_UNITS:
        state = _run(["systemctl", "is-active", unit])
        if state != "active":
            # Tolerate transient restart windows (wal_maintenance bouncing the DHT
            # workers, manual deploy restarts): re-check once after a short pause
            # so a brief restart is not misreported as a crash (the 11:43 false alert).
            time.sleep(6)
            state = _run(["systemctl", "is-active", unit])
        if verbose:
            print(f"  unit {unit:32} = {state}")
        if state not in ("active", "activating", "reloading"):
            # A 16GB-WAL truncate keeps the DHT worker stopped far longer than the 6s
            # re-check, so also suppress DHT-unit alerts whenever wal_maintenance is
            # actively running (only DHT workers are bounced by it; harvest/pex/web are not).
            if unit.startswith("dht-") and _wal_maint_running():
                if verbose:
                    print(f"  (suppressed {unit}='{state}': wal_maintenance restart window)")
                continue
            problems.append((f"unit:{unit}", f"{unit} is '{state}' (expected active)"))
    for timer in CRITICAL_TIMERS:
        state = _run(["systemctl", "is-active", timer])
        if verbose:
            print(f"  timer {timer:31} = {state}")
        if state != "active":
            problems.append((f"timer:{timer}", f"{timer} is '{state}' (expected active/armed)"))
    return problems


def check_failed_units(verbose: bool) -> list[tuple[str, str]]:
    out = _run(["systemctl", "list-units", "--type=service", "--state=failed",
                "--no-legend", "--no-pager", "--plain"])
    if out.startswith("__ERR__"):
        return [("failed_units_query", f"could not query failed units: {out}")]
    problems = []
    for line in out.splitlines():
        unit = line.split()[0] if line.split() else ""
        if not unit:
            continue
        app_owned = unit.startswith(APP_UNIT_PREFIXES)
        if verbose:
            print(f"  failed-unit {unit}{'' if app_owned else '  (ignored: not app-owned)'}")
        if app_owned and unit not in IGNORE_FAILED and unit not in CRITICAL_UNITS:
            # CRITICAL_UNITS already covered by check_units; report other app units.
            problems.append((f"failed:{unit}", f"{unit} is in failed state"))
    return problems


def check_export_delivery(verbose: bool) -> list[tuple[str, str]]:
    """A daily CSV for today (after the 23:55 run) or yesterday must exist."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    # Before ~00:05 UTC the day rolls but today's run hasn't happened; accept
    # yesterday. After 23:57 we expect today's file.
    expect_today = now.hour == 23 and now.minute >= 57
    candidates = [today] if expect_today else [today, yesterday]
    found = None
    for d in candidates:
        p = os.path.join(DAILY_DIR, f"{d}.csv")
        if os.path.exists(p) and os.path.getsize(p) > 200:  # header + ≥1 row
            found = (d, os.path.getsize(p))
            break
    if verbose:
        print(f"  export delivery: candidates={candidates} found={found}")
    if not found:
        # The daily export writes yesterday's CSV right at the 00:00 UTC rollover
        # (e.g. 2026-06-03.csv landed at 00:00:18); a watchdog run a few seconds
        # into the new day races ahead of it. Suppress during the midnight grace
        # window — same guard the freshness checks use — so we don't page on the
        # rollover. A genuine export failure still surfaces at the first post-grace
        # run (~00:45 UTC).
        if _in_midnight_grace():
            if verbose:
                print("  (midnight grace: export delivery check skipped)")
            return []
        want = today if expect_today else f"{today} or {yesterday}"
        return [("export_delivery", f"no recent daily CSV in {DAILY_DIR} (wanted {want}.csv)")]
    return []


def check_harvest_freshness(verbose: bool) -> list[tuple[str, str]]:
    if not os.path.exists(HARVEST_DB):
        return [("harvest_db_missing", f"{HARVEST_DB} not found")]
    try:
        c = sqlite3.connect(f"file:{HARVEST_DB}?mode=ro", uri=True, timeout=10)
        n = c.execute(
            "SELECT COUNT(*) FROM peers WHERE last_seen=? AND ip!='_queried_'",
            (today_utc(),)).fetchone()[0]
        c.close()
    except Exception as e:
        return [("harvest_db_read", f"could not read {HARVEST_DB}: {e}")]
    if verbose:
        print(f"  harvest today rows = {n:,}")
    if n < HARVEST_MIN_ROWS_TODAY:
        if _in_midnight_grace():
            if verbose:
                print("  (midnight grace: harvest freshness check skipped)")
            return []
        return [("harvest_stalled",
                 f"only {n:,} harvest peer rows for {today_utc()} "
                 f"(< {HARVEST_MIN_ROWS_TODAY:,}); harvester may be stalled")]
    return []


def check_main_db_freshness(verbose: bool) -> list[tuple[str, str]]:
    if not os.path.exists(MAIN_DB):
        return [("main_db_missing", f"{MAIN_DB} not found")]
    if _wal_maint_running():
        # wal_maintenance's TRUNCATE checkpoint can hold the DB lock long enough to
        # exceed our busy-timeout → spurious "database is locked". Skip this cycle;
        # the next run (after maintenance) checks cleanly. (matches the unit-check suppression)
        if verbose:
            print("  (wal_maintenance running: main DB check skipped)")
        return []
    try:
        c = sqlite3.connect(f"file:{MAIN_DB}?mode=ro", uri=True, timeout=10)
        c.execute("PRAGMA busy_timeout=12000")
        mx = c.execute("SELECT MAX(last_seen) FROM peers").fetchone()[0]
        c.close()
    except Exception as e:
        return [("main_db_read", f"could not read {MAIN_DB}: {e}")]
    if verbose:
        print(f"  main DB max last_seen = {mx}")
    if mx != today_utc():
        if _in_midnight_grace():
            if verbose:
                print("  (midnight grace: main DB freshness check skipped)")
            return []
        return [("main_db_stale",
                 f"main DB peers max(last_seen)={mx}, expected {today_utc()} "
                 f"(DHT collectors may not be writing)")]
    return []


def check_disk(verbose: bool) -> list[tuple[str, str]]:
    out = _run(["df", "-P", DISK_PATH])
    if out.startswith("__ERR__"):
        return [("disk_query", f"could not run df: {out}")]
    try:
        line = out.splitlines()[-1]
        pct = int(line.split()[4].rstrip("%"))
    except Exception as e:
        return [("disk_parse", f"could not parse df output: {e}")]
    if verbose:
        print(f"  {DISK_PATH} used = {pct}%")
    if pct >= DISK_WARN_PCT:
        return [("disk_full", f"{DISK_PATH} is {pct}% full (>= {DISK_WARN_PCT}%)")]
    return []


# ── state / throttling ────────────────────────────────────────────────────────
def load_state() -> dict:
    try:
        with open(STATE_PATH) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
        tmp = STATE_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, STATE_PATH)
    except Exception as e:
        print(f"[watchdog] WARN: could not save state: {e}", file=sys.stderr)


def publish_sns(subject: str, message: str, dry_run: bool) -> None:
    if dry_run:
        print("── [DRY-RUN] would publish SNS ─────────────────────────────")
        print("SUBJECT:", subject)
        print(message)
        print("────────────────────────────────────────────────────────────")
        return
    try:
        import boto3
        boto3.client("sns", region_name=REGION).publish(
            TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)
        print(f"[watchdog] SNS alert published: {subject}")
    except Exception as e:
        print(f"[watchdog] FAILED to publish SNS: {e}", file=sys.stderr)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="check + print; never publish to SNS")
    ap.add_argument("--verbose", action="store_true",
                    help="print each check's result")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    now_s = now.strftime("%Y-%m-%d %H:%M:%S UTC")

    checks = [
        check_units, check_failed_units, check_export_delivery,
        check_harvest_freshness, check_main_db_freshness, check_disk,
    ]
    problems: dict[str, str] = {}
    for fn in checks:
        try:
            for key, detail in fn(args.verbose):
                problems[key] = detail
        except Exception as e:
            problems[f"checkerr:{fn.__name__}"] = f"{fn.__name__} raised {e}"

    state = load_state()           # key -> last_alert_epoch
    now_epoch = now.timestamp()
    re_alert_secs = RE_ALERT_HOURS * 3600

    # Which problems to include in this alert (new, or stale past throttle)?
    to_alert = {}
    for key, detail in problems.items():
        last = state.get(key, 0)
        if now_epoch - last >= re_alert_secs:
            to_alert[key] = detail

    # Recoveries: keys we previously alerted on that are no longer problems.
    recovered = [k for k in state if k not in problems]

    if not problems:
        print(f"[{now_s}] OK — all checks passed.")
    else:
        print(f"[{now_s}] {len(problems)} problem(s): "
              + "; ".join(problems.values()))

    if to_alert:
        body = (f"hash_trackerv2 health watchdog on {INSTANCE_ID}\n"
                f"Time: {now_s}\n\n"
                f"{len(to_alert)} active problem(s):\n"
                + "\n".join(f"  • {d}" for d in to_alert.values())
                + "\n\nInvestigate:\n"
                f"  aws ssm start-session --target {INSTANCE_ID} --profile awsprofile\n"
                f"  systemctl --failed ; tail /data/logs/health_watchdog.log\n")
        subject = f"[ALERT] hash_trackerv2 health: {len(to_alert)} issue(s) on {INSTANCE_ID}"
        publish_sns(subject, body, args.dry_run)
        if not args.dry_run:
            for key in to_alert:
                state[key] = now_epoch

    if recovered:
        body = (f"hash_trackerv2 health watchdog on {INSTANCE_ID}\n"
                f"Time: {now_s}\n\nRECOVERED (no longer failing):\n"
                + "\n".join(f"  • {k}" for k in recovered) + "\n")
        publish_sns(f"[RECOVERED] hash_trackerv2 health on {INSTANCE_ID}",
                    body, args.dry_run)
        if not args.dry_run:
            for k in recovered:
                state.pop(k, None)

    if not args.dry_run:
        save_state(state)

    return 0  # monitor always exits clean


if __name__ == "__main__":
    sys.exit(main())
