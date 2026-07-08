#!/bin/bash
# S3 sync script — Instance 1
# Pushes daily CSVs, archived logs, and SQLite DB snapshots to S3

set -euo pipefail

BUCKET="YOUR_S3_BUCKET"
REGION="us-east-1"
DATE=$(date +%Y/%m/%d)

echo "[$(date)] Starting S3 sync..."

# AWS CLI v2 on Amazon Linux 2023 requires python3.9 prefix (system python was changed to 3.12)
AWS="/usr/bin/python3.9 /usr/bin/aws"

# Sync peer count CSVs (today's files)
$AWS s3 sync /data/peer_counts/ s3://$BUCKET/peer_counts/ \
    --storage-class STANDARD_IA \
    --region $REGION \
    --exclude "*.tmp"

echo "[$(date)] Peer count CSVs synced"

# Sync compressed logs (exclude active .log files, only push .gz archives)
$AWS s3 sync /data/logs/ s3://$BUCKET/logs/ \
    --region $REGION \
    --exclude "*.log" \
    --include "*.gz"

echo "[$(date)] Logs synced"

# Sync SQLite DB snapshots (for backup).
# Use SQLite's .backup command — safe online backup that works correctly with WAL
# mode and concurrent writers (unlike raw gzip -c which can capture a torn state).
# Stage on /data (100 GB volume, ~42 GB free), NOT /tmp — root is only 8 GB and a
# multi-GB .backup (e.g. harvest_peers.db = 6.4 GB) would fill root and wedge the box.
# Each DB is isolated: a single DB's failure logs a WARN and the loop continues
# (the `if` condition suspends `set -e`, so one bad backup never aborts the rest).
STAGE="/data/db_backup_tmp"
mkdir -p "$STAGE"
for DB in hashes_v2 pantheon_intel harvest_peers title_aliases stream_demand acestream_pilot pex_peers; do
    SRC="/data/db/${DB}.db"
    [ -f "$SRC" ] || continue
    OUT="$STAGE/${DB}_$(date +%Y%m%d).db"
    if sqlite3 "$SRC" ".backup ${OUT}" \
        && gzip -f "${OUT}" \
        && $AWS s3 cp "${OUT}.gz" \
            "s3://$BUCKET/backups/db/${DB}_$(date +%Y%m%d).db.gz" \
            --region $REGION; then
        echo "[$(date)] DB snapshot uploaded: ${DB}"
    else
        echo "[$(date)] WARN: backup FAILED for ${DB}"
    fi
    rm -f "${OUT}" "${OUT}.gz" 2>/dev/null || true
done

echo "[$(date)] S3 sync complete"
