#!/usr/bin/env python3
"""
tracker_harvest_service.py — continuous tracker-announce peer harvester.

Runs alongside the DHT collector and feeds the SAME `peers` table. The DHT can
only ever see a structural sample of each swarm (peers announced to the ~8 nodes
closest to the infohash); measured here the all-time DHT union over 16 days ≈
NBCU's single-day IP_COUNT. Tracker announces return the live swarm directly, and
because swarms churn (~23% new IPs per 3 min, measured), repeatedly harvesting
all day accumulates the daily distinct-IP union that matches NBCU's published
per-country numbers.

Validation (2026-05-30, EC2, one ~98s cycle, geo-bucketed vs NBCU 05-28):
  The Boys 19.5% of NBCU's FULL DAY after one cycle; per-country split matched
  (US 24%, CA 35%, AU 50%, Other 17.5%). PHM 17.9%. With continuous looping +
  churn the daily union reaches/exceeds NBCU magnitude.

Writes: peers(hash, ip, country, first_seen, last_seen) via the exact same
upsert semantics as dht_peer_count.upsert_peers (ON CONFLICT update last_seen).
Per-IP geo via the collector's GeoLite2-Country.mmdb. Date is UTC date-only to
match the existing rows the merge reads with `last_seen = <date>`.

Config (env):
  MAX_HASHES   hashes harvested per cycle, by recent peer activity   (default 25000)
  ROUNDS       announce rounds per tracker per hash                  (default 4)
  CONC         concurrent hashes                                     (default 16)
  CYCLE_SLEEP  seconds to sleep between full cycles                  (default 30)
  ONESHOT=1    run a single cycle and exit (for testing)
"""
from __future__ import annotations
import os, sys, time, glob, sqlite3, threading, datetime
from concurrent.futures import ThreadPoolExecutor

import tracker_harvest as th

# READ_DB: the shared DHT collector DB. We ONLY read the hash worklist + title
# metadata from it — we never write peers here, because 7 DHT workers hold
# continuous read locks that prevent WAL checkpoint frame reclamation, so any
# write volume we add inflates the shared WAL unboundedly (measured ~1GB/min).
#
# HARVEST_DB: our OWN database for harvested peers. Single writer process, so a
# periodic TRUNCATE checkpoint actually drains the WAL (no foreign long-lived
# readers). export_nbcu.py ATTACHes both and unions distinct IPs.
READ_DB     = "/data/db/hashes_v2.db"
HARVEST_DB  = "/data/db/harvest_peers.db"
DB_PATH     = HARVEST_DB  # all writes go here
MAX_HASHES  = int(os.environ.get("MAX_HASHES", "25000"))
ROUNDS      = int(os.environ.get("ROUNDS", "4"))
CONC        = int(os.environ.get("CONC", "16"))
CYCLE_SLEEP = int(os.environ.get("CYCLE_SLEEP", "30"))
ONESHOT     = os.environ.get("ONESHOT") == "1"
# Retention: export_nbcu.py only ever reads `last_seen = <a recent date>`, so any
# (hash, ip) row whose last_seen is older than this many days is dead weight —
# the IP churned out of every swarm we track and was never re-seen. The table
# grows ~380 MB/day in NEW (hash,ip) pairs; without a prune it would climb
# unbounded. Keeping a few days lets us still run yesterday's export / backfill.
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "4"))


def _utc_date() -> str:
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")


def _find_geodb() -> str | None:
    for pat in ("/data/geoip/*.mmdb", "/home/ec2-user/hash_trackerv2/*.mmdb"):
        hits = glob.glob(pat)
        if hits:
            return hits[0]
    return None


# ── GeoIP (shared, thread-safe readers are fine for lookups) ────────────────
_GEODB = _find_geodb()
try:
    import geoip2.database
    _reader = geoip2.database.Reader(_GEODB)
    def country_of(ip: str) -> str:
        try:
            return _reader.country(ip).country.iso_code or "XX"
        except Exception:
            return "XX"
except ImportError:
    import maxminddb
    _geo = maxminddb.open_database(_GEODB)
    def country_of(ip: str) -> str:
        try:
            r = _geo.get(ip)
            return r["country"]["iso_code"] if r and "country" in r else "XX"
        except Exception:
            return "XX"


# ── per-thread DB connections (never share an sqlite conn across threads) ────
_db_local = threading.local()
def _get_db() -> sqlite3.Connection:
    if not hasattr(_db_local, "conn"):
        conn = sqlite3.connect(HARVEST_DB, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        _db_local.conn = conn
    return _db_local.conn


def _init_harvest_db() -> None:
    """Create the peers table in our own DB. Same schema/PK as the DHT
    collector's peers table so export_nbcu.py can union them identically."""
    conn = sqlite3.connect(HARVEST_DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS peers (
            hash       TEXT NOT NULL,
            ip         TEXT NOT NULL,
            country    TEXT,
            first_seen TEXT,
            last_seen  TEXT,
            PRIMARY KEY (hash, ip)
        )""")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_peers_lastseen ON peers(last_seen)")
    conn.commit()
    conn.close()


def _upsert(hash_val: str, ip_country: dict[str, str], today: str) -> int:
    """Same semantics as dht_peer_count.upsert_peers; one txn per hash."""
    conn = _get_db()
    if ip_country:
        conn.executemany(
            """INSERT INTO peers (hash, ip, country, first_seen, last_seen)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(hash, ip) DO UPDATE SET
                   country=excluded.country, last_seen=excluded.last_seen""",
            [(hash_val, ip, c, today, today) for ip, c in ip_country.items()])
    else:
        conn.execute(
            """INSERT INTO peers (hash, ip, country, first_seen, last_seen)
               VALUES (?, '_queried_', 'XX', ?, ?)
               ON CONFLICT(hash, ip) DO UPDATE SET last_seen=excluded.last_seen""",
            (hash_val, today, today))
    conn.commit()
    return len(ip_country)


def load_hashes(limit: int) -> list[str]:
    """Highest-seeder (most popular) swarms first, DHT recency as tiebreak.

    Read-only against the shared DHT DB (worklist + seeders only). Ordering is
    POPULARITY-first on purpose: the metric we reproduce (NBCU per-title daily
    distinct-IP union) is dominated by each title's biggest live swarms, so the
    harvest budget must go to high-`seeders` hashes regardless of whether the DHT
    collector happened to see recent peers for them.

    The previous ordering (`p.ls DESC, h.seeders DESC` — DHT-recency first) starved
    popular-but-DHT-quiet titles: e.g. Spider-Noir had 63 hashes with seeders>=50
    but only 24 got harvested in a day, because the rest fell below the MAX_HASHES
    cut on the weak recency signal. Seeders is a real popularity signal (populated
    for ~66% of hashes); ranking on it pulls every title's high-seeder hashes into
    the harvested set each cycle. Recency remains the tiebreak among equal seeders.
    COALESCE so NULL seeders sort last rather than unpredictably.

    NEW-RELEASE TIEBREAK (2nd key): some indexers report a PLACEHOLDER seeder count
    (e.g. YTS hashes all arrive as seeders=100), so a genuinely-popular fresh WEB
    release lands in the huge seeders<=100 tier and, being brand new, has NO peers
    yet -> p.ls is NULL -> it sinks to the BOTTOM of that tier and falls below the
    MAX_HASHES cut for ~a day (observed: Hokum's 1080p WEB-DL was discovered one day
    but not harvested until the next). Fix: within a seeder tier, rank freshly-
    discovered hashes (first_seen within 2 days) ABOVE older ones. Seeders stays the
    PRIMARY key, so established high-seeder titles are never starved by the new-hash
    inflow (mostly low-seed tmdb); this only reorders within a tier."""
    conn = sqlite3.connect("file:" + READ_DB + "?mode=ro", uri=True, timeout=60)
    try:
        rows = conn.execute("""
            SELECT h.hash
            FROM hashes h
            LEFT JOIN (
                SELECT hash, MAX(last_seen) ls
                FROM peers WHERE ip != '_queried_' GROUP BY hash
            ) p ON p.hash = h.hash
            ORDER BY COALESCE(h.seeders, 0) DESC,
                     (h.first_seen >= date('now','-2 days')) DESC,
                     p.ls DESC
            LIMIT ?""", (limit,)).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def harvest_hash(ih: str, today: str) -> int:
    try:
        peers = th._public_only(th.harvest_infohash(ih, rounds=ROUNDS))
    except Exception:
        peers = set()
    ip_country = {ip: country_of(ip) for ip, _ in peers}
    return _upsert(ih, ip_country, today)


def run_cycle() -> tuple[int, int]:
    today = _utc_date()
    hashes = load_hashes(MAX_HASHES)
    total_ips = 0
    done = 0
    with ThreadPoolExecutor(max_workers=CONC) as ex:
        futs = {ex.submit(harvest_hash, ih, today): ih for ih in hashes}
        for f in futs:
            try:
                total_ips += f.result()
            except Exception:
                pass
            done += 1
            if done % 2000 == 0:
                print(f"    .. {done}/{len(hashes)} hashes, {total_ips:,} ip-writes",
                      flush=True)
    return len(hashes), total_ips


def _prune_old() -> int:
    """Delete (hash, ip) rows whose last_seen is older than RETENTION_DAYS.

    Safe: export_nbcu.py queries a specific recent date, never the full history,
    and the ON CONFLICT upsert re-inserts an IP (with today's last_seen) the
    moment it is re-seen — so pruning a churned-out IP costs nothing if it comes
    back. Runs on the single writer, so the periodic TRUNCATE checkpoint reclaims
    the freed WAL frames. Returns rows deleted (best-effort; 0 on any error)."""
    cutoff = (datetime.datetime.now(datetime.UTC)
              - datetime.timedelta(days=RETENTION_DAYS)).strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(HARVEST_DB, timeout=60)
        conn.execute("PRAGMA busy_timeout=60000")
        cur = conn.execute("DELETE FROM peers WHERE last_seen < ?", (cutoff,))
        n = cur.rowcount
        conn.commit()
        conn.close()
        if n:
            print(f"[prune] deleted {n:,} rows with last_seen < {cutoff} "
                  f"(retention {RETENTION_DAYS}d)", flush=True)
        return n
    except Exception as e:
        print(f"[prune] error: {e}", flush=True)
        return 0


def _checkpoint_loop(interval: int = 90):
    """Time-based WAL checkpoint thread for HARVEST_DB only. This DB has a single
    writer process (us), so no foreign reader holds a continuous read lock — a
    TRUNCATE checkpoint actually reclaims all WAL frames and resets it to zero.
    This is the whole point of the separate-DB design: it isolates our heavy
    write volume from the DHT collector's shared WAL (which we could never
    checkpoint because the 7 DHT workers never release their read locks)."""
    ticks = 0
    # prune roughly once an hour (3600/interval ticks); the DELETE is cheap and
    # the very next checkpoint reclaims its WAL frames.
    prune_every = max(1, 3600 // interval)
    while True:
        time.sleep(interval)
        ticks += 1
        if ticks % prune_every == 0:
            _prune_old()
        try:
            ck = sqlite3.connect(HARVEST_DB, timeout=60)
            ck.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            ck.close()
        except Exception:
            pass


def main():
    if not _GEODB:
        print("FATAL: no GeoLite2 mmdb found", file=sys.stderr)
        sys.exit(1)
    _init_harvest_db()
    print(f"tracker_harvest_service starting: MAX_HASHES={MAX_HASHES} ROUNDS={ROUNDS} "
          f"CONC={CONC} CYCLE_SLEEP={CYCLE_SLEEP} RETENTION_DAYS={RETENTION_DAYS} "
          f"geodb={_GEODB}\n"
          f"  read_db={READ_DB}  harvest_db={HARVEST_DB}", flush=True)
    _prune_old()  # reclaim stale rows once at startup
    if not ONESHOT:
        threading.Thread(target=_checkpoint_loop, args=(90,), daemon=True).start()
    cycle = 0
    while True:
        cycle += 1
        t0 = time.time()
        n, ips = run_cycle()
        print(f"[cycle {cycle} {_utc_date()}] harvested {n:,} hashes, "
              f"{ips:,} ip-writes in {time.time()-t0:.0f}s", flush=True)
        if ONESHOT:
            break
        time.sleep(CYCLE_SLEEP)


if __name__ == "__main__":
    main()
