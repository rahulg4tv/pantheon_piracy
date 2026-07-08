#!/usr/bin/env python3
"""
harvest_velocity.py — high-velocity re-harvest lane for SURGING new releases.

Why: the main tracker_harvest_service hits each hash once per ~15-min cycle. A
day-0 surge churns peers fast, so one harvest captures only a fraction of the
distinct IPs (measured ~28% on In the Grey's top hash; 6 re-harvests = 3.5x).
This lane re-harvests a small HOT SET (fresh + high-seed hashes) every few minutes
— MANY passes/hour — to capture that churn and close the gap vs NBCU on surges.

Isolation (today's WAL lesson): writes to its OWN db `harvest_velocity_peers.db`
with a SINGLE writer connection + periodic TRUNCATE checkpoint + retention prune,
so its WAL self-manages and it adds ZERO contention to harvest_peers.db /
hashes_v2.db. export_nbcu.py UNIONs this db as a third peer source.
"""
import os, sys, time, sqlite3, datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from concurrent.futures import ThreadPoolExecutor

import tracker_harvest as th                 # harvest_infohash / _public_only
import tracker_harvest_service as ths        # country_of (GeoLite2)

READ_DB      = "/data/db/hashes_v2.db"
VEL_DB       = os.environ.get("VEL_DB", "/data/db/harvest_velocity_peers.db")
HOT_K        = int(os.environ.get("HOT_K", "400"))
HOT_NEW_DAYS = int(os.environ.get("HOT_NEW_DAYS", "3"))
HOT_MIN_SEED = int(os.environ.get("HOT_MIN_SEED", "150"))
CONC         = int(os.environ.get("VEL_CONC", "24"))
ROUNDS       = int(os.environ.get("VEL_ROUNDS", "4"))
LOOP_SLEEP   = int(os.environ.get("VEL_LOOP_SLEEP", "8"))
REFRESH_SEC  = int(os.environ.get("VEL_REFRESH_SEC", "300"))
RETENTION_DAYS = int(os.environ.get("VEL_RETENTION_DAYS", "4"))
CKPT_EVERY   = int(os.environ.get("VEL_CKPT_EVERY", "5"))   # passes between TRUNCATE+prune

DDL = ("CREATE TABLE IF NOT EXISTS peers(hash TEXT NOT NULL, ip TEXT NOT NULL, "
       "country TEXT, first_seen TEXT, last_seen TEXT, PRIMARY KEY(hash,ip))")
IDX = "CREATE INDEX IF NOT EXISTS idx_vel_lastseen ON peers(last_seen)"
UPSERT = ("INSERT INTO peers(hash,ip,country,first_seen,last_seen) VALUES(?,?,?,?,?) "
          "ON CONFLICT(hash,ip) DO UPDATE SET country=excluded.country, last_seen=excluded.last_seen")


def today_utc():
    return datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")


def hot_set():
    c = sqlite3.connect("file:" + READ_DB + "?mode=ro", uri=True, timeout=60)
    try:
        rows = c.execute(
            """SELECT hash FROM hashes
               WHERE first_seen >= date('now', ?) AND COALESCE(seeders,0) >= ?
               ORDER BY seeders DESC LIMIT ?""",
            ("-%d days" % HOT_NEW_DAYS, HOT_MIN_SEED, HOT_K)).fetchall()
    finally:
        c.close()
    return [r[0] for r in rows]


def harvest_one(ih):
    """Network harvest + GeoIP (runs in a thread). Returns (hash, {ip: country})."""
    try:
        peers = th._public_only(th.harvest_infohash(ih, rounds=ROUNDS))
    except Exception:
        peers = set()
    return ih, {ip: ths.country_of(ip) for ip, _ in peers}


def run():
    conn = sqlite3.connect(VEL_DB, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(DDL); conn.execute(IDX); conn.commit()
    print("harvest_velocity starting: VEL_DB=%s HOT_K=%d NEW_DAYS=%d MIN_SEED=%d "
          "CONC=%d LOOP_SLEEP=%d" % (VEL_DB, HOT_K, HOT_NEW_DAYS, HOT_MIN_SEED, CONC, LOOP_SLEEP),
          flush=True)
    hs = hot_set(); last_refresh = time.time(); passes = 0
    while True:
        if not hs:
            time.sleep(30); hs = hot_set(); continue
        today = today_utc()
        with ThreadPoolExecutor(max_workers=CONC) as ex:
            results = list(ex.map(harvest_one, hs))
        rows = []
        for ih, ipc in results:
            for ip, ctry in ipc.items():
                rows.append((ih, ip, ctry, today, today))
        wrote = 0
        if rows:
            conn.executemany(UPSERT, rows); conn.commit(); wrote = len(rows)
        passes += 1
        ndist = sum(len(ipc) for _, ipc in results)
        print("%s pass=%d hot=%d ip_writes=%d" % (
              datetime.datetime.now(datetime.UTC).strftime("%H:%M:%SZ"), passes, len(hs), wrote),
              flush=True)
        if passes % CKPT_EVERY == 0:
            cutoff = (datetime.date.today() - datetime.timedelta(days=RETENTION_DAYS)).isoformat()
            conn.execute("DELETE FROM peers WHERE last_seen < ?", (cutoff,)); conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        if time.time() - last_refresh >= REFRESH_SEC:
            hs = hot_set(); last_refresh = time.time()
        time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    if "--show-hotset" in sys.argv:
        print("HOT SET size:", len(hot_set()))
    elif "--oneshot" in sys.argv:           # one pass, for testing
        conn = sqlite3.connect(VEL_DB, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL"); conn.execute(DDL); conn.execute(IDX); conn.commit()
        hs = hot_set()[: int(os.environ.get("ONESHOT_N", "30"))]
        today = today_utc()
        with ThreadPoolExecutor(max_workers=CONC) as ex:
            results = list(ex.map(harvest_one, hs))
        rows = [(ih, ip, ctry, today, today) for ih, ipc in results for ip, ctry in ipc.items()]
        conn.executemany(UPSERT, rows); conn.commit()
        n = conn.execute("SELECT COUNT(*) FROM peers").fetchone()[0]
        print("oneshot: %d hashes -> %d ip-writes, %d total rows in vel db" % (len(hs), len(rows), n))
    else:
        run()
