#!/usr/bin/env python3
"""
decoy_detect.py — read-only quality/decoy detector (prototype).

Flags titles whose demand may be ARTIFICIALLY inflated, on two independent signals:

  1. DECOY FLOOD — the same raw_name (actual torrent filename) repeated across many
     distinct infohashes. Anti-piracy firms flood swarms with identically-named fake
     torrents; a legit popular title instead has MANY DISTINCT raw_names (different
     releases/qualities/groups). Uses raw_name, NOT the catalog title.
  2. DATACENTER-HEAVY — high share of datacenter/VPN IPs (seedbox/bot inflation vs
     real residential demand), from the demand snapshot's dc_ip_count.

READ-ONLY. Conservative thresholds + samples so false positives can be eyeballed
before any feed suppression / UI badge is built. Run on the box:
  sudo -u ec2-user venv/bin/python3 decoy_detect.py
"""
import os, sys, sqlite3
from collections import defaultdict, Counter

SNS_TOPIC = os.environ.get("SNS_TOPIC_ARN",
                           "arn:aws:sns:us-east-1:YOUR_AWS_ACCOUNT_ID:searchpantheon_Admin_Email")

HASH_DB = "/data/db/hashes_v2.db"
INTEL_DB = "/data/db/pantheon_intel.db"
MIN_HASHES = 5      # ignore tiny titles
DUP_FLAG = 4        # same raw_name across >=4 infohashes -> decoy-suspect (sensitive)
DC_FLAG = 0.60      # >=60% datacenter IPs -> inflation-suspect
DC_MIN_IPS = 500    # only apply dc flag to titles with real volume

def main():
    c = sqlite3.connect(f"file:{HASH_DB}?mode=ro", uri=True, timeout=20)
    c.execute("PRAGMA busy_timeout=20000")
    byip = defaultdict(Counter)          # ip_id -> Counter(raw_name -> #infohashes)
    for ip_id, raw in c.execute("SELECT ip_id, raw_name FROM hashes"):
        rn = (raw or "").strip()
        if len(rn) >= 5:                 # skip empty/junk names (not a real decoy signal)
            byip[ip_id][rn] += 1
    titlecat = {ip: (t, cat) for ip, t, cat in c.execute("SELECT ip_id,title,category FROM titles")}
    c.close()

    demand = {}                          # ip_id -> (ip_count, dc_ip_count)
    try:
        d = sqlite3.connect(f"file:{INTEL_DB}?mode=ro", uri=True, timeout=10)
        dt = d.execute("SELECT MAX(date) FROM title_demand").fetchone()[0]
        for ip, ipc, dc in d.execute(
                "SELECT ip_id, ip_count, dc_ip_count FROM title_demand WHERE date=?", (dt,)):
            demand[ip] = (ipc or 0, dc or 0)
        d.close()
        print(f"demand snapshot date: {dt}  ({len(demand):,} titles)")
    except Exception as e:
        print("demand/dc signal unavailable:", e)

    flags = []
    for ip, cnt in byip.items():
        n = sum(cnt.values())
        if n < MIN_HASHES:
            continue
        topname, topdup = cnt.most_common(1)[0]
        ndist = len(cnt)
        ipc, dc = demand.get(ip, (0, 0))
        dcfrac = (dc / ipc) if ipc else 0.0
        reasons = []
        # real decoy flood = one name DOMINATES (concentration), not just a few
        # re-uploads of a legit release. Require both high dup count AND low variety.
        if topdup >= DUP_FLAG and ndist and (ndist / n) < 0.5:
            reasons.append(f"decoy:rawname×{topdup} ({ndist}/{n} distinct)")
        if dcfrac >= DC_FLAG and ipc >= DC_MIN_IPS:
            reasons.append(f"datacenter:{int(dcfrac*100)}%")
        if reasons:
            t, cat = titlecat.get(ip, (ip, "?"))
            flags.append((ipc, ip, t, cat, n, ndist, topdup, topname, int(dcfrac*100), reasons))

    flags.sort(key=lambda x: -x[0])
    decoy = sum(1 for f in flags if any("decoy" in r for r in f[9]))
    dch = sum(1 for f in flags if any("datacenter" in r for r in f[9]))
    # how many flagged titles are in the TOP-100 by demand? (do decoys distort the headline?)
    top100 = {ip for _, (ip, _t) in zip(range(100),
              sorted(((d[0], (ip, d)) for ip, d in demand.items()), reverse=True))} if demand else set()
    top_flagged = [f for f in flags if f[1] in top100]
    print(f"\nFLAGGED: {len(flags)}  (decoy-flood={decoy}, datacenter-heavy={dch})")
    print(f"  of which in TOP-100 by demand: {len(top_flagged)}  (0 = rankings not distorted)\n")
    # --alert: page SNS ONLY when a high-demand (top-100) title is flagged — i.e. a
    # decoy flood / inflation actually distorting the headline. Quiet otherwise.
    if "--alert" in sys.argv and top_flagged:
        try:
            import boto3
            body = "Decoy/quality: %d TOP-100 title(s) flagged (possible inflation):\n" % len(top_flagged) + \
                   "\n".join("  %s '%s' | %s" % (f[1], str(f[2])[:30], ",".join(f[9])) for f in top_flagged[:12])
            boto3.client("sns", region_name="us-east-1").publish(
                TopicArn=SNS_TOPIC, Subject="[ALERT] decoy/quality flag on top titles"[:100], Message=body)
            print("SNS alert published (%d top-100 flagged)" % len(top_flagged))
        except Exception as e:
            print("SNS publish failed:", e)
    print("Top 30 by demand:")
    for ipc, ip, t, cat, n, ndist, topdup, topname, dcp, reasons in flags[:30]:
        print(f"  {ipc:>7,} IPs | '{str(t)[:26]}' ({str(cat)[:5]}) n={n} distinct={ndist} dc={dcp}% | {','.join(reasons)}")
        if topdup >= DUP_FLAG:
            print(f"         repeated ×{topdup}: {str(topname)[:52]}")

if __name__ == "__main__":
    main()
