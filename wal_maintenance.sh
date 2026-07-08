#!/bin/bash
# wal_maintenance.sh — threshold-gated coordinated WAL reclaim for hashes_v2.db.
#
# WHY: the 8 always-on DHT workers continuously hold WAL read-marks, so the
# per-pass wal_checkpoint(TRUNCATE) in dht_peer_count.py usually returns BUSY and
# can't fully truncate. During the overnight write surge (collect / bep51 /
# compact / dht --new-only, 01:00-03:00) the WAL can balloon past the ~686MB
# level that historically caused the crash-loop (SESSION_CHANGES §36/§48).
#
# A full truncate only completes with NO readers. The 8 DHT units AND
# tracker-harvest all hold a long-lived hashes_v2.db connection (the harvester
# reads its worklist from it — it is NOT on a separate DB for *reads*), so ALL of
# them must be briefly stopped or the TRUNCATE returns BUSY and the WAL never
# shrinks. This job checks the WAL size and ONLY when it exceeds THRESH_MB does a
# coordinated reclaim: stop those units, TRUNCATE-checkpoint, then ALWAYS restart
# them (guaranteed via an EXIT trap, even if the checkpoint errors). Most runs
# are a cheap no-op.
#
# 2026-06-29: tracker-harvest added to UNITS — it was the unaccounted reader that
# silently blocked every TRUNCATE and let the WAL reach 9GB (78%+ iowait).
#
# Cron (root, consolidated crontab):  20 4,16 * * *  $APP/wal_maintenance.sh >> /data/logs/wal_maintenance.log 2>&1
set -uo pipefail

DB=/data/db/hashes_v2.db
WALF="${DB}-wal"
THRESH_MB=${WAL_THRESH_MB:-500}
UNITS="dht-peer-count dht-peer-count-w1 dht-peer-count-w2 dht-peer-count-w3 dht-dormant dht-dormant-w1 dht-dormant-w2 dht-dormant-w3 tracker-harvest"
TS() { date -u +'%Y-%m-%dT%H:%M:%SZ'; }
wal_mb() { echo $(( ( $(stat -c %s "$WALF" 2>/dev/null || echo 0) ) / 1048576 )); }

before=$(wal_mb)
if [ "$before" -lt "$THRESH_MB" ]; then
    echo "$(TS) WAL=${before}MB < ${THRESH_MB}MB — no action"
    exit 0
fi

# Don't pause workers if another hashes_v2.db writer is mid-run (the truncate
# would be BUSY anyway). Try again next cycle. Does NOT match the always-on
# workers (they have no --new-only flag) — only the cron writers + ad-hoc sqlite3.
# NOTE: `pgrep -fc` already prints "0" when there are no matches; a trailing
# `|| echo 0` (its exit is 1 on no-match) appended a SECOND line, making extra
# "0\n0" and breaking the -gt integer test below (line "integer expression
# expected") — so the guard silently misfired. Capture the count directly.
extra=$(pgrep -fc 'collect\.py|bep51_crawler\.py|compact_peer_counts\.py|export_nbcu\.py|merge_and_upload\.py|--new-only' 2>/dev/null); extra=${extra:-0}
sqlb=$(pgrep -fc 'sqlite3 .*hashes_v2' 2>/dev/null); sqlb=${sqlb:-0}
if [ "${extra:-0}" -gt 0 ] || [ "${sqlb:-0}" -gt 0 ]; then
    echo "$(TS) WAL=${before}MB but ${extra} writer(s)+${sqlb} sqlite3 active — skipping this cycle"
    exit 0
fi

echo "$(TS) WAL=${before}MB >= ${THRESH_MB}MB — coordinated reclaim"

# Guarantee workers come back no matter what happens below.
restart_workers() {
    for u in $UNITS; do systemctl reset-failed "$u" 2>/dev/null; systemctl start "$u"; done
    echo "$(TS) workers restarted ($(pgrep -fc dht_peer_count.py) procs)"
}
trap restart_workers EXIT

for u in $UNITS; do systemctl stop "$u"; done
sleep 5
# Only TRUNCATE in a true zero-reader window — otherwise it returns BUSY and the
# WAL is left bloated (the silent failure that let it reach 9GB). Retry briefly to
# ride out a transient on-demand reader (dashboard) or a unit still exiting.
ok=0
for _ in $(seq 1 24); do   # up to ~2min to ride out a transient reader (export/merge/dashboard)
    if [ "$(lsof -t "$DB" 2>/dev/null | wc -l)" -eq 0 ]; then
        sqlite3 "$DB" "PRAGMA busy_timeout=30000; PRAGMA wal_checkpoint(TRUNCATE);" || true
        ok=1; break
    fi
    sleep 5
done
after=$(wal_mb)
if [ "$ok" -eq 1 ] && [ "$after" -lt "$before" ]; then
    echo "$(TS) reclaim OK: WAL ${before}MB -> ${after}MB"
else
    echo "$(TS) reclaim FAILED: WAL ${before}MB -> ${after}MB — a reader blocked TRUNCATE; holders:"
    for p in $(lsof -t "$DB" 2>/dev/null); do tr '\0' ' ' < "/proc/$p/cmdline" 2>/dev/null | cut -c1-60; echo; done
fi
# trap restarts workers on exit
