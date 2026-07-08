#!/usr/bin/env python3
"""
Updated EU DB export — includes a minimal peers activity stub.

Instead of exporting full peers table (millions of IPs), we export one
synthetic row per active hash per country: ip='_active_marker_'.
This satisfies dht_peer_count.py's --skip-dead-days and --active-only
queries without shipping any real IP data.

Also restricts hashes export to last_seen >= 5 days (no point sending dead hashes).
"""
import sqlite3, boto3, os
from datetime import datetime, timezone

print(f"[db-export] Starting at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")

DB_SRC   = "/data/db/hashes_v2.db"
DST_PATH = "/home/ec2-user/hashes_eu.db"
BUCKET   = "YOUR_S3_BUCKET"
S3_KEY   = "eu-bootstrap/hashes_eu.db"

if os.path.exists(DST_PATH):
    os.remove(DST_PATH)

src = sqlite3.connect(DB_SRC)
src.execute("PRAGMA busy_timeout=30000")
dst = sqlite3.connect(DST_PATH)
dst.execute("PRAGMA journal_mode=DELETE")  # NO WAL — avoids -wal/-shm files that corrupt on S3 round-trip

# ── hashes table (active only — last_seen within 5 days) ──────
dst.execute("""
    CREATE TABLE hashes (
        hash TEXT PRIMARY KEY,
        ip_id TEXT,
        title TEXT,
        category TEXT,
        source TEXT,
        seeders INTEGER,
        first_seen TEXT,
        last_seen TEXT
    )
""")
dst.execute("CREATE INDEX idx_last_seen ON hashes(last_seen)")

hashes = src.execute("""
    SELECT hash, ip_id, title, category, source, seeders, first_seen, last_seen
    FROM hashes
    WHERE last_seen >= date('now', '-5 days')
""").fetchall()
dst.executemany("INSERT OR IGNORE INTO hashes VALUES (?,?,?,?,?,?,?,?)", hashes)
print(f"[db-export] Exported {len(hashes):,} active hashes (last 5 days)")

# ── peers stub — synthetic rows to satisfy --skip-dead-days queries ────────
# Schema matches dht_peer_count.py's init_peers_table exactly so no migration needed.
# ip='_marker_{country}' is unique per (hash, country) to avoid PRIMARY KEY conflicts
# and is not '_queried_' so announce_peer listener won't filter it out.
dst.execute("""
    CREATE TABLE peers (
        hash       TEXT NOT NULL,
        ip         TEXT NOT NULL,
        country    TEXT,
        first_seen TEXT,
        last_seen  TEXT,
        PRIMARY KEY (hash, ip)
    )
""")
dst.execute("CREATE INDEX idx_peers_hash       ON peers(hash)")
dst.execute("CREATE INDEX idx_peers_country    ON peers(country)")
dst.execute("CREATE INDEX idx_peers_seen       ON peers(last_seen)")
dst.execute("CREATE INDEX idx_peers_real_seen  ON peers(ip, hash, last_seen)")

# Get per-hash country + any recent peer activity from the source DB
peer_stubs = src.execute("""
    SELECT DISTINCT p.hash, p.country, p.last_seen
    FROM peers p
    JOIN hashes h ON h.hash = p.hash
    WHERE p.last_seen >= date('now', '-5 days')
      AND p.ip != '_queried_'
      AND h.last_seen >= date('now', '-5 days')
    GROUP BY p.hash, p.country
    HAVING COUNT(DISTINCT p.ip) >= 1
""").fetchall()

# Use '_marker_{country}' as ip so each (hash, country) gets a unique row
stub_rows = [(h, f'_marker_{c or "XX"}', c, d, d) for h, c, d in peer_stubs]
dst.executemany("INSERT OR IGNORE INTO peers VALUES (?,?,?,?,?)", stub_rows)
print(f"[db-export] Exported {len(stub_rows):,} peer activity stubs ({len(set(r[0] for r in stub_rows)):,} hashes)")

dst.commit()
src.close()
dst.close()

size_mb = os.path.getsize(DST_PATH) / 1024 / 1024
print(f"[db-export] DB size: {size_mb:.1f} MB")

s3 = boto3.client("s3", region_name="us-east-1")
s3.upload_file(DST_PATH, BUCKET, S3_KEY)
print(f"[db-export] Uploaded to s3://{BUCKET}/{S3_KEY}")
print("[db-export] Done.")
