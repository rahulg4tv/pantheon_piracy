#!/bin/bash
# Daily archival of OUR per-country distinct-IP demand feed (harvest+DHT union).
# Runs at 23:55 UTC for the CURRENT day so all of today's harvest rows are still
# in harvest_peers.db. Writes /data/daily/<date>.csv and pushes it to S3.
set -euo pipefail

D=$(date -u +%F)
OUTDIR=/data/daily
OUT="${OUTDIR}/${D}.csv"
S3_DEST="s3://YOUR_S3_BUCKET/daily/${D}.csv"
# AWS CLI v2 on Amazon Linux 2023 requires the python3.9 prefix
# (system python was changed to 3.12) — same as s3_sync.sh.
AWS="/usr/bin/python3.9 /usr/bin/aws"

mkdir -p "$OUTDIR"

/home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/export_nbcu.py \
    --date "$D" --out "$OUT"

# Publish to S3 so the team can consume it (object named <date>.csv).
$AWS s3 cp "$OUT" "$S3_DEST"

echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') export_nbcu OK -> $OUT ($(wc -l < "$OUT") rows) -> $S3_DEST"

# --- Velocity ranking (fastest-rising / new-release) -------------------------
# Additive, SECONDARY deliverable. Runs AFTER the demand export so today's CSV
# exists, then compares to yesterday. A velocity failure must NEVER fail the
# critical daily feed, so the whole block is guarded.
VEL_OUT="${OUTDIR}/velocity/${D}.csv"
VEL_DEST="s3://YOUR_S3_BUCKET/daily/velocity/${D}.csv"
if /home/ec2-user/venv/bin/python3 /home/ec2-user/hash_trackerv2/velocity_rank.py \
       --date "$D" --out "$VEL_OUT"; then
    $AWS s3 cp "$VEL_OUT" "$VEL_DEST" \
        && echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') velocity OK -> $VEL_OUT -> $VEL_DEST" \
        || echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') velocity upload FAILED (non-critical)" >&2
else
    echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') velocity SKIPPED/FAILED (non-critical)" >&2
fi
