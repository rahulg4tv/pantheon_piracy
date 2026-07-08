"""
push_metric.py — CloudWatch custom metric pusher (runs every 5 min via cron)

Pushes:
  - DHTProcessAlive: 1 if dht_peer_count.py is running, 0 if crashed
  - DHTProgress: current progress % from log (if parseable)

CloudWatch alarm on DHTProcessAlive < 1 → SNS → email/SMS alert
"""

import subprocess
import re
import boto3
from pathlib import Path
from datetime import datetime, timezone

REGION = "us-east-1"
NAMESPACE = "HashTracker"
LOG_PATH = "/data/logs/dht_peer.log"


def check_process_alive() -> int:
    result = subprocess.run(
        ["pgrep", "-f", "dht_peer_count"],
        capture_output=True
    )
    return 1 if result.returncode == 0 else 0


def get_progress_pct() -> float | None:
    """Parse last progress % from log file."""
    try:
        log = Path(LOG_PATH)
        if not log.exists():
            return None
        # Read last 2KB to find recent progress line
        with open(log, "rb") as f:
            f.seek(max(0, log.stat().st_size - 2048))
            tail = f.read().decode("utf-8", errors="ignore")
        # Match: [ 30400/104322   29.1%]
        matches = re.findall(r'\[\s*\d+/\d+\s+([\d.]+)%\]', tail)
        if matches:
            return float(matches[-1])
    except Exception:
        pass
    return None


def push_metrics(alive: int, progress: float | None):
    cw = boto3.client("cloudwatch", region_name=REGION)
    now = datetime.now(timezone.utc)

    metric_data = [
        {
            "MetricName": "DHTProcessAlive",
            "Value": alive,
            "Unit": "Count",
            "Timestamp": now,
        }
    ]

    if progress is not None:
        metric_data.append({
            "MetricName": "DHTProgressPct",
            "Value": progress,
            "Unit": "Percent",
            "Timestamp": now,
        })

    cw.put_metric_data(Namespace=NAMESPACE, MetricData=metric_data)


def main():
    alive = check_process_alive()
    progress = get_progress_pct()

    status = "ALIVE" if alive else "CRASHED"
    prog_str = f"{progress:.1f}%" if progress is not None else "unknown"
    print(f"[{datetime.now()}] DHT process: {status} | Progress: {prog_str}")

    push_metrics(alive, progress)

    if not alive:
        print("WARNING: dht_peer_count.py is NOT running! CloudWatch alarm should fire.")


if __name__ == "__main__":
    main()
