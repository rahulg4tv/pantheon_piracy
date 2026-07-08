#!/usr/bin/env python3
"""
alias_remap.py — alias-based best-match correction pass (Pillar-1 enforcer).

Re-maps any hash whose torrent title matches a MORE SPECIFIC catalog alias than its
current title (best-match disambiguation with "keep current on tie"). Fixes the
foreign-romaji / sequel / show-vs-movie mis-maps the inline matcher can't
(Memole←Witch Hat, Law&Order→SVU, Spy x Family S2→base, ...). Shadow-validated
2026-06-11: 4.3% of hashes change, ~all correct, edge cases suppressed by tie-break.

Idempotent: re-running does nothing once consistent. Reversible: every changed row
is dumped to /data/db/backups/ before any write.

Usage:
  alias_remap.py                 # DRY-RUN — print what would change, write nothing
  alias_remap.py --apply         # apply (backup -> batched UPDATE on hashes + peers)
"""
import sqlite3, sys, re, os, json, time, unicodedata
from collections import defaultdict, Counter

APP="/home/ec2-user/hash_trackerv2"
DB="/data/db/hashes_v2.db"; ALIAS_DB="/data/db/title_aliases.db"
SAFETY_CAP=12000           # abort if more than this would change (sanity)
MIN_ALIAS_WORDS=2          # single-word titles handled by the collector's short-title guard
sys.path.insert(0, APP)
import trending_hash_collector as thc
SW=thc._AUDIT_SW | {"no"}

def _fold(s): return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))
def words(s):
    s=_fold(s).lower().replace("'","").replace("’","").replace(".","").replace("-"," ")
    return {w for w in re.sub(r"[^a-z0-9 ]"," ",s).split() if w not in SW and not thc._AUDIT_TECH.match(w)}
def twords(raw):
    t,_=thc.parse_torrent_name(raw or ""); w=words(t)
    return w if w else words(thc._strip_release_group(raw or ""))

def build_index():
    al=sqlite3.connect(f"file:{ALIAS_DB}?mode=ro", uri=True)
    by_ip=defaultdict(list); entries=[]
    for ip,a in al.execute("SELECT ip_id,alias FROM title_aliases"):
        w=frozenset(words(a))
        if len(w)>=MIN_ALIAS_WORDS: entries.append((w,ip)); by_ip[ip].append(w)
    al.close()
    freq=Counter(x for w,_ in entries for x in w)
    postings=defaultdict(list)
    for w,ip in entries: postings[min(w,key=lambda x:freq[x])].append((w,ip))
    return postings, by_ip

def compute_changes(cur, postings, by_ip):
    changes=[]
    for ip_id,h,raw in cur.execute("SELECT ip_id,hash,raw_name FROM hashes"):
        T=twords(raw)
        if not T: continue
        best,bn=None,0
        for w in T:
            for W,ip in postings.get(w,()):
                if W<=T and len(W)>bn: best,bn=ip,len(W)
        if best is None or best==ip_id: continue
        cb=max((len(W) for W in by_ip.get(ip_id,()) if W<=T), default=0)
        if bn<=cb: continue                     # tie-break: keep current
        changes.append((h, ip_id, best))
    return changes

def main():
    apply = "--apply" in sys.argv
    postings, by_ip = build_index()
    con=sqlite3.connect(DB, timeout=40); con.execute("PRAGMA busy_timeout=40000"); cur=con.cursor()
    title={ip:t for ip,t in cur.execute("SELECT ip_id,title FROM titles")}
    changes=compute_changes(cur, postings, by_ip)
    print(f"[alias_remap] {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}  changes={len(changes):,}  mode={'APPLY' if apply else 'DRY-RUN'}")
    if not changes: con.close(); return
    if len(changes) > SAFETY_CAP:
        print(f"[alias_remap] SAFETY ABORT: {len(changes)} > cap {SAFETY_CAP}"); con.close(); sys.exit(1)
    # sample summary
    agg=Counter((c0,c1) for _,c0,c1 in changes)
    for (c0,c1),n in agg.most_common(12):
        print(f"    {n:>4}  '{str(title.get(c0,c0))[:26]}' -> '{str(title.get(c1,c1))[:26]}'")
    if not apply:
        print("[alias_remap] DRY-RUN — no writes. Re-run with --apply to commit."); con.close(); return
    # backup
    os.makedirs("/data/db/backups", exist_ok=True)
    path=f"/data/db/backups/remap_alias_{time.strftime('%Y%m%d', time.gmtime())}.jsonl"
    bk=open(path,"a"); nh=npx=0
    for h,c0,c1 in changes:
        for r in cur.execute("SELECT hash,ip_id,title,raw_name,seeders FROM hashes WHERE hash=? AND ip_id=?",(h,c0)):
            bk.write(json.dumps({"t":"hash","r":r,"to":c1})+"\n"); nh+=1
        for r in cur.execute("SELECT hash,ip,ip_id FROM peers WHERE hash=? AND ip_id=?",(h,c0)):
            bk.write(json.dumps({"t":"peer","r":r,"to":c1})+"\n"); npx+=1
    bk.close(); print(f"[alias_remap] backup -> {path} (hash={nh:,} peer={npx:,})")
    # apply batched (gentle on the DHT writers)
    dh=dp=0
    for i in range(0,len(changes),500):
        con.execute("BEGIN")
        for h,c0,c1 in changes[i:i+500]:
            dh+=con.execute("UPDATE hashes SET ip_id=?,title=? WHERE hash=? AND ip_id=?",(c1,title.get(c1),h,c0)).rowcount
            dp+=con.execute("UPDATE peers  SET ip_id=? WHERE hash=? AND ip_id=?",(c1,h,c0)).rowcount
        con.commit(); time.sleep(0.15)
    con.close(); print(f"[alias_remap] APPLIED hash_rows={dh:,} peer_rows={dp:,}")

if __name__=="__main__": main()
