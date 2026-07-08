#!/usr/bin/env python3
"""Liveness sweep of the streaming registry. Concurrent HEAD probes of every
stream_demand.db:stream_sites domain; marks status live/dead + last_checked.
Any HTTP response = live; no response (000/timeout/DNS/conn) = dead. Network-only."""
import sqlite3, subprocess, time
from concurrent.futures import ThreadPoolExecutor
DB = "/data/db/stream_demand.db"

def probe(dom):
    try:
        r = subprocess.run(["curl","-sI","-m","8","-L","-k","-A","Mozilla/5.0",
                            "-o","/dev/null","-w","%{http_code}","https://"+dom],
                           capture_output=True, text=True, timeout=14)
        code = (r.stdout or "").strip()
    except Exception:
        code = "000"
    return dom, ("live" if code and code != "000" else "dead"), code

con = sqlite3.connect(DB, timeout=30)
doms = [r[0] for r in con.execute("SELECT domain FROM stream_sites")]
con.close()
today = time.strftime("%Y-%m-%d", time.gmtime())
results = []
with ThreadPoolExecutor(max_workers=25) as ex:
    for dom, status, code in ex.map(probe, doms):
        results.append((dom, status, code))
con = sqlite3.connect(DB, timeout=30)
for dom, status, code in results:
    con.execute("UPDATE stream_sites SET status=?, last_checked=? WHERE domain=?", (status, today, dom))
con.commit()
live = sum(1 for _, s, _ in results if s == "live")
print("probed %d  live=%d  dead=%d" % (len(results), live, len(results) - live))
print("--- top 15 LIVE by takedown rank ---")
for r in con.execute("SELECT domain,rank_signal FROM stream_sites WHERE status='live' ORDER BY rank_signal DESC LIMIT 15"):
    print("  %-26s %d" % (r[0], r[1]))
con.close()
