#!/usr/bin/env python3
"""
crash_notify.py — Called by systemd OnFailure to publish a crash alert to SNS.

Triggered automatically by:
    dht-peer-count-alert.service  →  OnFailure= in dht-peer-count.service

Usage (systemd calls this directly):
    python3 /home/ec2-user/hash_trackerv2/crash_notify.py

Environment variables injected by systemd unit template:
    UNIT_NAME       — e.g. "dht-peer-count.service"
    EXIT_CODE       — e.g. "exited" or "killed"
    EXIT_STATUS     — e.g. "1"

Setup:
    1. Create an SNS topic in us-east-1 and subscribe your email/phone.
    2. Set SNS_TOPIC_ARN below (or export it as an env var on the instance).
    3. The EC2 instance IAM role must have sns:Publish on the topic.
       Add inline policy: {"Effect":"Allow","Action":"sns:Publish","Resource":"<arn>"}
"""

import os
import sys
import subprocess
import boto3
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
REGION        = "us-east-1"
# Set SNS_TOPIC_ARN as an env var on the instance, or hardcode here after creation:
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email")
INSTANCE_ID   = "YOUR_INSTANCE_ID"
LOG_PATH      = "/data/logs/dht_peer.log"
TAIL_LINES    = 30
# ─────────────────────────────────────────────────────────────────────────────


def tail_log(n: int = TAIL_LINES) -> str:
    try:
        result = subprocess.run(
            ["tail", f"-{n}", LOG_PATH],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() or "(no log output)"
    except Exception as e:
        return f"(could not read log: {e})"


def get_systemd_status(unit: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "status", unit, "--no-pager", "-n", "5"],
            capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() or "(no status)"
    except Exception as e:
        return f"(could not get status: {e})"


def _wal_maint_running() -> bool:
    """True while wal_maintenance.sh is mid-run. It stop/restarts the 8 DHT
    workers to TRUNCATE the WAL; on a heavy (multi-GB) reclaim a worker can be
    blocked on the locked DB and miss its SIGTERM, so systemd SIGKILLs it and
    records 'Failed with result timeout' → OnFailure fires here. That's our own
    maintenance bouncing the worker, NOT a real crash — suppress the page.
    health_watchdog.py (every 15 min) still catches any PERSISTENT real failure
    after the window ends, so we don't lose true-positive coverage."""
    try:
        return subprocess.run(["pgrep", "-f", "wal_maintenance"],
                              capture_output=True, timeout=5).returncode == 0
    except Exception:
        return False


def _unit_active(unit: str) -> bool:
    """True if the unit is already back to 'active' by the time we run — i.e. it
    self-healed (Restart=always) during the OnFailure race. Nothing to page."""
    try:
        out = subprocess.run(["systemctl", "is-active", unit],
                             capture_output=True, text=True, timeout=5).stdout.strip()
        return out == "active"
    except Exception:
        return False


def main():
    unit_name   = os.getenv("UNIT_NAME",   "dht-peer-count.service")
    exit_code   = os.getenv("EXIT_CODE",   "unknown")
    exit_status = os.getenv("EXIT_STATUS", "unknown")
    now_utc     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if not SNS_TOPIC_ARN:
        print("[crash_notify] SNS_TOPIC_ARN not set — skipping notification", file=sys.stderr)
        sys.exit(0)

    # ── Suppress self-inflicted maintenance-window false positives ──────────────
    if _wal_maint_running():
        print(f"[crash_notify] suppressed {unit_name}: wal_maintenance reclaim in "
              f"progress (worker SIGKILL on stop-timeout is expected, not a crash)")
        sys.exit(0)
    if _unit_active(unit_name):
        print(f"[crash_notify] suppressed {unit_name}: already recovered to 'active' "
              f"(self-healed via Restart=always before alert ran)")
        sys.exit(0)

    log_tail = tail_log()
    svc_status = get_systemd_status(unit_name)

    subject = f"[ALERT] {unit_name} crashed on {INSTANCE_ID}"
    message = f"""DHT peer counter crashed on {INSTANCE_ID}

Time       : {now_utc}
Unit       : {unit_name}
Exit code  : {exit_code}
Exit status: {exit_status}

── Last {TAIL_LINES} lines of {LOG_PATH} ──────────────────────────────
{log_tail}

── systemctl status ────────────────────────────────────────────────────
{svc_status}

── Action ──────────────────────────────────────────────────────────────
systemd will auto-restart (Restart=always, RestartSec=30s).
If restarts keep failing, check the log above for the root cause.

To manually check:
  aws ssm start-session --target {INSTANCE_ID} --profile awsprofile
  sudo journalctl -u {unit_name} -n 50
"""

    try:
        sns = boto3.client("sns", region_name=REGION)
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],   # SNS subject max 100 chars
            Message=message,
        )
        print(f"[crash_notify] Alert published to SNS: {subject}")
    except Exception as e:
        print(f"[crash_notify] Failed to publish SNS alert: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
