#!/usr/bin/env python3
"""chao_swarm_estimate.py — "true swarm size" via capture-recapture, using our 3
collection sources (DHT / tracker-harvest / PEX) as independent capture occasions.
READ-ONLY (mode=ro on every DB).

⚠️ RUN OFF-BOX ONLY (against an S3/Parquet snapshot or a copy), NOT on the prod EC2 box.
2026-06-08: running this on the live box with the top-12 biggest swarms pulled millions of
IPs into memory, ran for 45+ min, and PINNED the 16 GB WAL (blocked wal_maintenance truncate)
— a self-inflicted near-incident. If you must run on-box: tiny mid-tier sample only, never the
top titles, and never during the :35 hourly export window. Prefer Parquet/Athena off-box.

For a sample of titles on one day it builds each IP's source-membership and computes:
  * f_k = # IPs seen by EXACTLY k sources (k=1,2,3)
  * Chao1 (bias-corrected, 3 sources):  N_hat = S_obs + f1*(f1-1) / (2*(f2+1))
  * Chapman (2 sources, DHT x Harvest):  N_hat = (n1+1)(n2+1)/(m+1) - 1
Both estimate swarm IPs that NO source caught → how much our observed distinct-IP count
undercounts true demand. NOTE: DHT/Harvest/PEX are positively correlated (a popular peer
is caught by several), which makes these estimators LOWER bounds, not exact.
"""
from __future__ import annotations
import sqlite3, csv, argparse, collections, os

MAIN = "file:/data/db/hashes_v2.db?mode=ro"
HARV = "/data/db/harvest_peers.db"
VEL  = "/data/db/harvest_velocity_peers.db"
PEX  = "/data/db/pex_peers.db"


def setq(cur, sql, params):
    return {r[0] for r in cur.execute(sql, params)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default="2026-06-07")
    ap.add_argument("--csv", default="/data/daily/2026-06-07.csv")
    ap.add_argument("--n", type=int, default=12, help="sample size (titles by demand)")
    a = ap.parse_args()

    dem = collections.Counter()
    with open(a.csv, encoding="utf-8", errors="replace") as f:
        for row in csv.DictReader(f):
            try:
                dem[row["IP_ID"]] += int(row["IP_COUNT"])
            except (KeyError, ValueError):
                pass
    sample = [ip for ip, _ in dem.most_common() if ip][:a.n]

    con = sqlite3.connect(MAIN, uri=True, timeout=60)
    if os.path.exists(HARV): con.execute("ATTACH ? AS hv", ("file:" + HARV + "?mode=ro",))
    if os.path.exists(VEL):  con.execute("ATTACH ? AS vv", ("file:" + VEL + "?mode=ro",))
    if os.path.exists(PEX):  con.execute("ATTACH ? AS pe", ("file:" + PEX + "?mode=ro",))
    cur = con.cursor()

    print("%-30s %9s %9s %9s | %8s %8s %7s" %
          ("title", "obs(S)", "Chao1", "Chapman", "dht", "harv", "pex"))
    print("-" * 86)
    t_obs = t_chao = t_chap = 0.0
    for ip_id in sample:
        hashes = [h for (h,) in cur.execute("SELECT hash FROM hashes WHERE ip_id=?", (ip_id,))]
        if not hashes:
            continue
        ql = ",".join("?" * len(hashes))
        d = setq(cur, "SELECT DISTINCT ip FROM peers WHERE hash IN (%s) AND last_seen=? AND ip!='_queried_'" % ql, (*hashes, a.day))
        h = setq(cur, "SELECT DISTINCT ip FROM hv.peers WHERE hash IN (%s) AND last_seen=?" % ql, (*hashes, a.day))
        try:
            h |= setq(cur, "SELECT DISTINCT ip FROM vv.peers WHERE hash IN (%s) AND last_seen=?" % ql, (*hashes, a.day))
        except sqlite3.Error:
            pass
        try:
            p = setq(cur, "SELECT DISTINCT ip FROM pe.peers WHERE hash IN (%s) AND last_seen=?" % ql, (*hashes, a.day))
        except sqlite3.Error:
            p = set()
        S = d | h | p
        if len(S) < 50:
            continue
        freq = collections.Counter((ip in d) + (ip in h) + (ip in p) for ip in S)
        f1, f2 = freq[1], freq[2]
        chao = len(S) + f1 * (f1 - 1) / (2 * (f2 + 1))           # bias-corrected Chao1
        n1, n2, m = len(d), len(h), len(d & h)
        chap = (n1 + 1) * (n2 + 1) / (m + 1) - 1                 # Chapman (Lincoln-Petersen)
        nm = next((r[0] for r in cur.execute("SELECT title FROM hashes WHERE ip_id=? LIMIT 1", (ip_id,))), ip_id)
        print("%-30s %9d %9.0f %9.0f | %8d %8d %7d" %
              ((nm or ip_id)[:30], len(S), chao, chap, n1, n2, len(p)))
        t_obs += len(S); t_chao += chao; t_chap += chap

    print("-" * 86)
    if t_obs:
        print("SAMPLE TOTAL  observed=%d  Chao1=%.0f (+%.1f%% hidden)  Chapman=%.0f (+%.1f%%)" %
              (t_obs, t_chao, 100 * (t_chao - t_obs) / t_obs, t_chap, 100 * (t_chap - t_obs) / t_obs))
    con.close()


if __name__ == "__main__":
    main()
