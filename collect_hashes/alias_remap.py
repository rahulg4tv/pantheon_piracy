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
# Abort if more than this would change. Tightened 12000 -> 500 on 2026-07-26 when
# the cron was re-enabled. Once the DB is consistent this job is idempotent — the
# steady-state dry run is 0 changes and a day's genuine corrections are tens, not
# thousands. The two runs that ever did damage were 621 and 1,070, i.e. BOTH would
# have been caught by this cap. A cap that only trips above 12,000 could never have
# stopped either one.
SAFETY_CAP=500
MIN_ALIAS_WORDS=2          # single-word titles handled by the collector's short-title guard
sys.path.insert(0, APP)
import trending_hash_collector as thc
SW=thc._AUDIT_SW | {"no"}

def _fold(s): return "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))

def _drop(w):
    """True => token is noise and should NOT enter the word-set.

    SEQUEL-NUMBER FIX (2026-07-25). thc._AUDIT_TECH strips ^\\d+$, i.e. EVERY bare
    number, so 'Toy Story 3' / 'Toy Story 4' / 'Toy Story 5' all collapsed to
    {toy, story}. Because compute_changes() picks the longest alias that is a
    SUBSET of the torrent words, those became freely interchangeable and the job
    merged distinct works into one ip_id — it re-created the Toy Story 3/4 ->
    Toy Story 5, Super Troopers, Scary Movie and Robin Hood mis-tags twice a day
    (621 such changes on 2026-07-25, all reverted).

    Keeping standalone numbers 2..99 makes the sequel part of the identity:
    {toy,story,5} is no longer a subset of {toy,story,3}, so the wrong sequel
    stops being a candidate. The range covers '28 Years Later' and "Ocean's 11"
    as well as plain sequels. YEARS (1900-2099) and resolutions (720/1080/2160)
    fall outside it and are still dropped, so nothing else changes.
    """
    if w.isdigit():
        return not (2 <= int(w) <= 99)          # keep sequel/title numbers only
    return bool(thc._AUDIT_TECH.match(w))

# 'BoneTemple' -> 'Bone Temple'. Torrent names routinely drop the space in a
# compound title; without this the words never form and the hash gets pulled
# onto the shorter parent title ('28 Years Later The BoneTemple' -> '28 Years
# Later'). Must run BEFORE _fold()/lower().
_CAMEL = re.compile(r"(?<=[a-z])(?=[A-Z])")

def words(s):
    s=_CAMEL.sub(" ", s or "")
    s=_fold(s).lower().replace("'","").replace("’","").replace(".","").replace("-"," ")
    return {w for w in re.sub(r"[^a-z0-9 ]"," ",s).split() if w not in SW and not _drop(w)}

def has_alpha(W):
    """An alias must carry at least one LETTER word to be usable.

    words() deletes every non-Latin character, so a foreign alias reduces to
    whatever digits it happened to contain: 'Two and a Half Men' has the Russian
    alias '2,5 человека' -> {2,5}. Once _drop() started keeping digits that
    became a live landmine — {2,5} is a subset of any season pack
    ('The Simpsons S01 2 3 4 5 6 7 8 9'), so the whole Simpsons swarm was about
    to be re-tagged as Two and a Half Men. Same for Kaiju No. 8 {2,8},
    Classroom of the Elite {2,4}, Seven Deadly Sins {2,7}. Digit-only aliases
    carry no identity — drop them."""
    return any(not w.isdigit() for w in W)

# A parse that glued the title into one long alphanumeric run
# ('Top Gun Maverick.2022.HD720p...' -> 'maverick2022hd720pfuckadssrt') has LOST
# words. The surviving fragment {top,gun} then looks like a clean match for the
# parent title, so the hash gets demoted off 'Top Gun: Maverick'. Refuse to
# judge these at all rather than remap on a corrupt word-set.
def degraded(W):
    return any(len(w) >= 15 and any(c.isdigit() for c in w) for w in W)

def twords(raw):
    t,_=thc.parse_torrent_name(raw or ""); w=words(t)
    return w if w else words(thc._strip_release_group(raw or ""))

def build_index():
    al=sqlite3.connect(f"file:{ALIAS_DB}?mode=ro", uri=True)
    by_ip=defaultdict(list); entries=[]
    for ip,a in al.execute("SELECT ip_id,alias FROM title_aliases"):
        w=frozenset(words(a))
        if len(w)>=MIN_ALIAS_WORDS and has_alpha(w): entries.append((w,ip)); by_ip[ip].append(w)
    al.close()
    freq=Counter(x for w,_ in entries for x in w)
    postings=defaultdict(list)
    for w,ip in entries: postings[min(w,key=lambda x:freq[x])].append((w,ip))
    return postings, by_ip

def compute_changes(cur, postings, by_ip, cat_of=None, title_of=None):
    """cat_of: ip_id -> category. When supplied, a hash is never moved across
    categories (CATEGORY GUARD, 2026-07-25). The sequel-number fix cannot
    separate a film and a series that genuinely share a title — e.g. 'Robin Hood'
    the 1991 film vs 'Robin Hood' the 2025 series both reduce to {robin, hood} —
    so the 2025 episodes kept being pulled onto the film. Categories are known
    and authoritative, so crossing them is always wrong.

    title_of: ip_id -> title. Enables the SAME-TITLE BLOCK (2026-07-25). Moving a
    hash between two entries with the SAME title is never a rename — it is the
    conflation signature we revert by hand every time ('Masters of the Universe'
    1987 vs 2026, 'Beautiful Boy' 2010 vs 2018). Word-sets cannot tell those
    apart because the titles are literally identical, so refuse the move."""
    cat_of = cat_of or {}
    title_of = title_of or {}
    def tkey(ip):
        return " ".join((title_of.get(ip) or "").split()).casefold()

    def digit_demotion(cur_ip, new_ip):
        """True => refuse. The candidate is the CURRENT title minus only its
        sequel digits ('Toy Story 3' -> 'Toy Story'), which happens when
        parse_torrent_name eats the number: 'Toy Story 3 2010 BRRip' parses to
        'Toy Story', so {toy,story,3} stops being a subset and the base title
        wins — demoting a hash that was already RIGHT. Losing a digit is the
        signature of a lossy parse, not of a better match. A demotion that also
        drops a WORD ('One-Punch Man Season 2' -> 'One-Punch Man') is the
        intended season-to-franchise collapse and stays allowed."""
        c, n = words(title_of.get(cur_ip) or ""), words(title_of.get(new_ip) or "")
        d = c - n
        return bool(d) and n < c and all(w.isdigit() for w in d)
    changes=[]
    for ip_id,h,raw,hcat in cur.execute("SELECT ip_id,hash,raw_name,category FROM hashes"):
        T=twords(raw)
        if not T or degraded(T): continue
        best,bn=None,0
        for w in T:
            for W,ip in postings.get(w,()):
                if not (W<=T and len(W)>bn): continue
                tcat = cat_of.get(ip)
                if hcat and tcat and tcat != hcat: continue   # category guard
                best,bn=ip,len(W)
        if best is None or best==ip_id: continue
        # NO-ALIAS-COVERAGE GUARD (2026-07-26). Only judge a hash when we actually
        # hold alias data for its CURRENT title. title_aliases.db is rebuilt weekly
        # (build_title_aliases.py, Mondays), so a title added since the last build
        # has NO aliases — and cb then comes out 0, which this job would otherwise
        # read as "the current title does not fit" instead of "I have no evidence".
        # It then drags the hashes onto whatever SHORTER title does have aliases:
        # 'The Death of Robin Hood' (film-tt32273171, no aliases yet) lost all 18 of
        # its hashes to 'Robin Hood' (film-Q689658, alias {robin,hood}) — undoing the
        # very correction made on 2026-07-26. Absence of aliases is missing data, not
        # a mismatch, so refuse to move.
        if not by_ip.get(ip_id): continue
        k=tkey(ip_id)
        if k and k==tkey(best): continue         # same-title block (conflation)
        if digit_demotion(ip_id, best): continue # lossy-parse sequel demotion
        cb=max((len(W) for W in by_ip.get(ip_id,()) if W<=T), default=0)
        if bn<=cb: continue                     # tie-break: keep current
        changes.append((h, ip_id, best))
    return changes

def main():
    apply = "--apply" in sys.argv
    postings, by_ip = build_index()
    con=sqlite3.connect(DB, timeout=40); con.execute("PRAGMA busy_timeout=40000"); cur=con.cursor()
    title={ip:t for ip,t in cur.execute("SELECT ip_id,title FROM titles")}
    cat_of={ip:c for ip,c in cur.execute("SELECT ip_id,category FROM titles")}
    changes=compute_changes(cur, postings, by_ip, cat_of, title)
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
