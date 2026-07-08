#!/usr/bin/env python3
"""
audit_mismatch.py — read-only mis-map detector (Tier-1 active-audit prototype).

For every catalog ip_id with >=3 hashes, measure CONSISTENCY = fraction of its
hashes whose (punctuation-normalized) significant words contain ALL the catalog
title's significant words. Low consistency ⇒ the hashes are NOT actually this
title (the Witch Hat→Memole signature). Splits results into two tiers:

  TIER 1  cons < 0.15  → fully mis-mapped: the WHOLE title is wrong → remap/quarantine
  TIER 2  0.15..0.50   → contaminated: title is right but wrong hashes leaked → de-contaminate

Punctuation (apostrophes/periods) is stripped so "Bob's"/"Bobs" and "P.D."/"PD"
don't false-flag. The Japanese particle 'no' is ignored. Ranks by IPs at stake.

Read-only. Run on the box:  sudo -u ec2-user venv/bin/python3 audit_mismatch.py
"""
import sqlite3, sys, re, unicodedata
from collections import defaultdict, Counter
sys.path.insert(0, "/home/ec2-user/hash_trackerv2")
import trending_hash_collector as thc

DB = "/data/db/hashes_v2.db"
SW = thc._AUDIT_SW | {"no"}
MIN_HASHES = 3
T1, T2 = 0.15, 0.50

def _fold(s):
    # strip diacritics so "Shogun"=="Shōgun", "Fiance"=="Fiancé" (else false-flagged)
    return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def words(s, strip_group=False):
    if strip_group:
        s = thc._strip_release_group(s or "")
    s = _fold(s).lower().replace("'", "").replace("’", "").replace(".", "").replace("-", " ")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return {w for w in s.split() if w not in SW and not thc._AUDIT_TECH.match(w)}

def main():
    c = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=30)
    c.execute("PRAGMA busy_timeout=30000")
    titles = {ip: (t, cat, yr, words(t))
              for ip, t, cat, yr in c.execute(
                  "SELECT ip_id,title,category,release_year FROM titles")}
    byip = defaultdict(list)
    for ip, raw, seed in c.execute("SELECT ip_id,raw_name,seeders FROM hashes"):
        byip[ip].append((raw or "", seed or 0))

    flags = []
    for ip, hs in byip.items():
        if ip not in titles:
            continue
        title, cat, yr, tw = titles[ip]
        if not tw or len(hs) < MIN_HASHES:
            continue
        ok = 0; wc = Counter()
        for raw, _ in hs:
            hw = words(raw, strip_group=True)
            if tw.issubset(hw):
                ok += 1
            wc.update(hw)
        cons = ok / len(hs)
        if cons < T2:
            n = len(hs)
            extra = [w for w, k in wc.most_common(8) if w not in tw and k / n > 0.5][:4]
            sample = max(hs, key=lambda x: x[1])[0][:60]
            flags.append([ip, title, cat, yr, n, round(cons, 2), extra, sample])

    ids = [f[0] for f in flags]; impact = {}
    for i in range(0, len(ids), 400):
        ch = ids[i:i + 400]; ph = ",".join("?" * len(ch))
        for ip, k in c.execute(
                f"SELECT ip_id,COUNT(DISTINCT ip) FROM peers WHERE ip_id IN ({ph}) GROUP BY ip_id", ch):
            impact[ip] = k
    c.close()
    for f in flags:
        f.append(impact.get(f[0], 0))
    flags.sort(key=lambda f: -f[-1])

    t1 = [f for f in flags if f[5] < T1]
    t2 = [f for f in flags if f[5] >= T1]
    print(f"FLAGGED: {len(flags)}  (Tier1 full mis-map={len(t1)}, Tier2 contaminated={len(t2)})")
    print(f"IPs at stake: Tier1={sum(f[-1] for f in t1):,}  Tier2={sum(f[-1] for f in t2):,}\n")
    for label, group in (("TIER 1 — FULL MIS-MAP (remap)", t1), ("TIER 2 — CONTAMINATED (clean)", t2)):
        print(f"=== {label} — top 25 by IPs ===")
        for f in group[:25]:
            ip, title, cat, yr, n, cons, extra, sample, ips = f
            print(f"  {ips:>6,} | {ip:16} '{title[:26]}' ({cat[:5]}) cons={cons} n={n} say:{extra}")
            print(f"          e.g. {sample}")
        print()

if __name__ == "__main__":
    main()
