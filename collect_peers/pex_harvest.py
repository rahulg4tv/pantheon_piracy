#!/usr/bin/env python3
"""pex_harvest.py — BEP-11 ut_pex peer harvester (P2P source #3, alongside DHT and
tracker-harvest). For each popular info_hash: seed peers from tracker-harvest
(ip:port), connect (BT + BEP-10 ext handshake advertising ut_pex), collect the
gossiped peer IPs, geo-locate, and upsert distinct (hash, ip, country) into an
ISOLATED pex_peers.db. Reuses bep51_crawler's bencode/handshake + the validated
pex_spike logic. Read-only against hashes_v2.db; writes ONLY pex_peers.db.

Usage:
  pex_harvest.py --once --limit 8        # one batch pass (test)
  pex_harvest.py --loop --limit 60 --loop-delay 600   # continuous service
"""
import socket, struct, time, os, sys, sqlite3, binascii, argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
sys.path.insert(0, "/home/ec2-user/hash_trackerv2")
import bep51_crawler as bt
import tracker_harvest as th
try:
    import maxminddb
except Exception:
    maxminddb = None

DB_PATH  = os.environ.get("PEX_DB", "/data/db/pex_peers.db")
GEO_MMDB = "/data/geoip/GeoLite2-Country.mmdb"
HASH_DB  = "file:/data/db/hashes_v2.db?mode=ro"
PSTR = bt._BT_HANDSHAKE_PREFIX
EXTB = b"\x00\x00\x00\x00\x00\x10\x00\x00"
DUR      = int(os.environ.get("PEX_DUR", "60"))
SEED_CAP = int(os.environ.get("PEX_SEED_CAP", "60"))
CONC     = int(os.environ.get("PEX_CONC", "40"))
HASH_CONC = int(os.environ.get("PEX_HASH_CONC", "3"))   # hashes processed in parallel

_geo = maxminddb.open_database(GEO_MMDB) if (maxminddb and os.path.exists(GEO_MMDB)) else None
def country(ip):
    if not _geo:
        return None
    try:
        r = _geo.get(ip)
        return ((r or {}).get("country") or {}).get("iso_code")
    except Exception:
        return None

def _recvn(s, n):
    b = b""
    while len(b) < n:
        try: c = s.recv(n - len(b))
        except Exception: return None
        if not c: return None
        b += c
    return b
def _recv_msg(s):
    h = _recvn(s, 4)
    if h is None: return None
    ln = struct.unpack("!I", h)[0]
    if ln == 0: return b""
    if ln > (1 << 20): return None
    return _recvn(s, ln)
def _compact(blob):
    return [".".join(map(str, blob[i:i+4])) for i in range(0, len(blob) - 5, 6)]
def _pex_peer(ip, port, ih, deadline):
    found = set(); pid = b"-PX0001-" + os.urandom(12)
    try:
        s = socket.socket(); s.settimeout(8); s.connect((ip, int(port)))
        s.sendall(PSTR + EXTB + ih + pid)
        hs = _recvn(s, 68)
        if not hs or hs[:20] != PSTR or not (hs[25] & 0x10):
            s.close(); return found
        ext = b"\x14\x00" + bt._bencode({b"m": {b"ut_pex": 1}})
        s.sendall(struct.pack("!I", len(ext)) + ext)
        their = None
        while time.time() < deadline:
            s.settimeout(max(1.0, deadline - time.time()))
            m = _recv_msg(s)
            if m is None: break
            if len(m) < 2 or m[0] != 20: continue
            if m[1] == 0:
                try: d = bt.bdecode(m[2:])
                except Exception: d = {}
                their = (d.get(b"m") or {}).get(b"ut_pex")
            elif their and m[1] == their:
                try: d = bt.bdecode(m[2:])
                except Exception: continue
                for x in _compact(d.get(b"added") or b""):
                    found.add(x)
        s.close()
    except Exception:
        pass
    return found

def init_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""CREATE TABLE IF NOT EXISTS peers(
        hash TEXT NOT NULL, ip TEXT NOT NULL, country TEXT,
        first_seen TEXT, last_seen TEXT, PRIMARY KEY(hash, ip))""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_pex_lastseen ON peers(last_seen)")
    # Rotation ledger. Records that a hash was HARVESTED, whether or not it
    # yielded peers — `peers` alone cannot express "looked, found nothing", so
    # rotation keyed off it re-picks every zero-yield hash forever.
    con.execute("""CREATE TABLE IF NOT EXISTS visits(
        hash TEXT PRIMARY KEY, last_visit TEXT, last_yield INT)""")
    con.commit()
    return con

def harvest_one(hx):
    """Return set of peer IPs found via PEX for one hash (network only, no DB)."""
    try: ih = binascii.unhexlify(hx)
    except Exception: return set()
    try: seed = list(th.harvest_infohash(hx))[:SEED_CAP]
    except Exception: seed = []
    if not seed:
        return set()
    deadline = time.time() + DUR
    ips = set()
    with ThreadPoolExecutor(max_workers=min(CONC, len(seed))) as ex:
        futs = [ex.submit(_pex_peer, ip, port, ih, deadline) for ip, port in seed]
        for f in as_completed(futs):
            ips |= f.result()
    return ips

POOL = int(os.environ.get("PEX_POOL", "4000"))   # demand-ranked candidates to rotate through


def worklist(limit):
    """Pick the next `limit` hashes: high current demand, LEAST-RECENTLY PEXed first.

    Rewritten 2026-07-27. The old query was

        SELECT hash, COUNT(*) n FROM peers WHERE ip!='_queried_'
        GROUP BY hash ORDER BY n DESC LIMIT ?

    which had three separate faults:

    1. It ranked by ALL-TIME accumulated peer rows — i.e. by how long a hash had
       been tracked, not by how many people are sharing it now. It sat on
       Interstellar and The Godfather while ignoring live releases, and picked
       hashes with seeders=100 over ones with seeders=13,468.
    2. It was DETERMINISTIC with no rotation, so every 15-minute cycle re-queried
       the SAME 50 hashes. Measured on 2026-07-27: pex_peers.db contained 73
       distinct hashes for its entire lifetime, out of 103,100 live ones — 0.07%
       of the catalog, and not one Series or Anime, ever.
    3. It was the unbounded `GROUP BY hash` over all 66M peer rows — the exact
       query that deadlocked every collector on the cold start after the
       2026-07-25 reboot. That was bounded in dht_peer_count.py and
       tracker_harvest_service.py (commit 3b0f919); this file was missed and kept
       running the full aggregate every 15 minutes.

    Now: take a demand-ranked pool of live hashes (seeders DESC, 30-day bound like
    its sibling collectors), then order that pool by when PEX last visited each
    hash. Never-visited hashes sort first, so it sweeps the catalog instead of
    hammering the same 50; among equals, higher demand wins.
    """
    c = sqlite3.connect(HASH_DB, uri=True)
    try:
        c.execute("PRAGMA busy_timeout=30000")
        pool = c.execute(
            "SELECT hash FROM hashes "
            "WHERE last_seen >= date('now','-30 days') AND seeders IS NOT NULL "
            "ORDER BY seeders DESC LIMIT ?", (POOL,)).fetchall()
    finally:
        c.close()
    pool = [r[0] for r in pool]
    if not pool:
        return []

    # When did PEX last LOOK at each of these? Read from `visits`, not from
    # `peers`: a hash that was harvested and yielded ZERO peers writes no peer
    # row, so keying rotation off `peers` left it on the ""/never-visited
    # sentinel and it sorted to the front again every cycle, forever. With a
    # pool of high-seeder hashes, zero-yield ones (no seed advertises ut_pex)
    # are routine, so that silently rebuilt the same fixed-worklist blind spot
    # this rewrite was meant to remove.
    seen = {}
    try:
        p = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        p.execute("PRAGMA busy_timeout=30000")
        seen = {h: ls for h, ls in p.execute("SELECT hash, last_visit FROM visits")}
        p.close()
    except sqlite3.Error:
        pass                      # first run / no visits table yet => all unvisited

    # "" sorts before any date, so never-PEXed hashes go first; pool order (which
    # is seeders DESC) breaks ties because sort is stable.
    return sorted(pool, key=lambda h: seen.get(h, ""))[:limit]

def run_pass(con, limit):
    today = time.strftime("%Y-%m-%d", time.gmtime())
    hs = worklist(limit)
    tot = 0
    with ThreadPoolExecutor(max_workers=HASH_CONC) as ex:
        for hx, ips in zip(hs, ex.map(harvest_one, hs)):
            for ip in ips:
                con.execute("INSERT INTO peers(hash,ip,country,first_seen,last_seen) "
                            "VALUES(?,?,?,?,?) ON CONFLICT(hash,ip) DO UPDATE SET last_seen=excluded.last_seen",
                            (hx, ip, country(ip), today, today))
            con.execute("INSERT INTO visits(hash,last_visit,last_yield) VALUES(?,?,?) "
                        "ON CONFLICT(hash) DO UPDATE SET last_visit=excluded.last_visit,"
                        " last_yield=excluded.last_yield",
                        (hx, today, len(ips)))
            tot += len(ips)
            con.commit()
    print("%s pex pass: %d hashes, %d peer-rows upserted (db=%s)"
          % (time.strftime("%H:%M:%S", time.gmtime()), len(hs), tot, DB_PATH), flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--limit", type=int, default=60)
    ap.add_argument("--loop-delay", type=int, default=600)
    a = ap.parse_args()
    con = init_db()
    while True:
        run_pass(con, a.limit)
        if not a.loop:
            break
        time.sleep(a.loop_delay)

if __name__ == "__main__":
    main()
