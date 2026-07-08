#!/usr/bin/env python3
"""
build_title_aliases.py — Pillar-1 alias-table builder (MATCHING_QUALITY_DESIGN.md).

For each TRACKED catalog ip_id, fetch every known title variant and store them:
  • anime  (anime-<malId>)  -> AniList GraphQL: title.romaji/english/native + synonyms
  • movie/series (imdb_id)  -> TMDB /find -> /{media}/{id}/alternative_titles + original

Writes an ISOLATED sqlite (does NOT touch hashes_v2.db / its WAL):
  /data/db/title_aliases.db  (table title_aliases)

Run:  sudo -u ec2-user venv/bin/python3 build_title_aliases.py [--sample N] [--limit N]
TMDB key loaded from env or the project .env (never printed).
"""
import os, sys, re, json, time, sqlite3, unicodedata, urllib.request, urllib.parse
HASH_DB="/data/db/hashes_v2.db"; ALIAS_DB="/data/db/title_aliases.db"
APP="/home/ec2-user/hash_trackerv2"; UA="Mozilla/5.0 (PantheonAliasBuilder/1.0)"

def tmdb_key():
    k=os.environ.get("TMDB_API_KEY","")
    if k: return k
    try:
        for ln in open(f"{APP}/.env"):
            if ln.strip().startswith("TMDB_API_KEY"):
                return ln.split("=",1)[1].strip().strip('"').strip("'")
    except Exception: pass
    return ""
TMDB=tmdb_key()

def _norm(s):
    s="".join(c for c in unicodedata.normalize("NFKD",s or "") if not unicodedata.combining(c))
    s=s.lower().replace("'","").replace("’","").replace(".","").replace("-"," ")
    return re.sub(r"\s+"," ",re.sub(r"[^a-z0-9 ]"," ",s)).strip()

def _get(req, retries=5):
    """urlopen+json with exponential backoff on 429/5xx (AniList rate-limits hard)."""
    for i in range(retries):
        try:
            return json.load(urllib.request.urlopen(req, timeout=20))
        except urllib.error.HTTPError as e:
            if e.code in (429,500,502,503,504):
                wait=float(e.headers.get("Retry-After") or 0) or (2**i)
                time.sleep(min(wait,30)); continue
            raise
        except Exception:
            time.sleep(2**i)
    raise RuntimeError("retries exhausted")

def anilist_aliases(mal):
    Q="query($v:Int){Media(idMal:$v,type:ANIME){title{romaji english native} synonyms}}"
    req=urllib.request.Request("https://graphql.anilist.co",
        data=json.dumps({"query":Q,"variables":{"v":mal}}).encode(),
        headers={"Content-Type":"application/json","Accept":"application/json","User-Agent":UA})
    d=(_get(req).get("data") or {}).get("Media")
    if not d: return []
    t=d["title"]; out=[("mal_synonym",a) for a in ([t.get("romaji"),t.get("english"),t.get("native")]+(d.get("synonyms") or [])) if a]
    return out

def tmdb_aliases(imdb, media_hint=None):
    if not TMDB: return []
    find=_get(urllib.request.Request(
        f"https://api.themoviedb.org/3/find/{imdb}?"+urllib.parse.urlencode({"api_key":TMDB,"external_source":"imdb_id"}),
        headers={"User-Agent":UA}))
    for media,key in (("movie","movie_results"),("tv","tv_results")):
        res=find.get(key) or []
        if res:
            tid=res[0]["id"]; out=[]
            orig=res[0].get("original_title") or res[0].get("original_name")
            if orig: out.append(("tmdb_orig",orig))
            alt=_get(urllib.request.Request(
                f"https://api.themoviedb.org/3/{media}/{tid}/alternative_titles?"+urllib.parse.urlencode({"api_key":TMDB}),
                headers={"User-Agent":UA}))
            for a in (alt.get("titles") or alt.get("results") or []):
                if a.get("title"): out.append(("tmdb_alt",a["title"]))
            return out
    return []

def main():
    sample=None; limit=None
    if "--sample" in sys.argv: sample=int(sys.argv[sys.argv.index("--sample")+1])
    if "--limit" in sys.argv:  limit=int(sys.argv[sys.argv.index("--limit")+1])
    c=sqlite3.connect(f"file:{HASH_DB}?mode=ro",uri=True,timeout=20)
    tracked=set(r[0] for r in c.execute("SELECT DISTINCT ip_id FROM hashes"))
    rows=[r for r in c.execute("SELECT ip_id,title,category,imdb_id,mal_id FROM titles") if r[0] in tracked]
    c.close()
    if sample: rows=rows[:sample]
    elif limit: rows=rows[:limit]
    a=sqlite3.connect(ALIAS_DB); a.execute("""CREATE TABLE IF NOT EXISTS title_aliases(
        ip_id TEXT, alias TEXT, alias_norm TEXT, source TEXT, PRIMARY KEY(ip_id,alias_norm))""")
    a.execute("CREATE INDEX IF NOT EXISTS idx_alias_norm ON title_aliases(alias_norm)")
    # resume: skip ip_ids that already have >1 alias (i.e. fetched OK last run);
    # only retry the canonical-only failures. Pass --all to force a full rebuild.
    if "--all" not in sys.argv:
        done=set(ip for (ip,) in a.execute(
            "SELECT ip_id FROM title_aliases GROUP BY ip_id HAVING COUNT(*)>1"))
        before=len(rows); rows=[r for r in rows if r[0] not in done]
        print(f"resume: {len(done)} already populated, {len(rows)} of {before} to (re)fetch")
    n_ip=n_al=err=0
    for ip_id,title,cat,imdb,mal in rows:
        aliases=[("canonical",title)]
        try:
            if str(ip_id).startswith("anime-"):
                m=mal or (ip_id.split("-")[1] if ip_id.split("-")[1].isdigit() else None)
                if m: aliases+=anilist_aliases(int(m)); time.sleep(0.7)
            elif imdb and str(imdb).startswith("tt"):
                aliases+=tmdb_aliases(imdb); time.sleep(0.3)
        except Exception as e:
            err+=1
        seen=set()
        for src,al in aliases:
            nz=_norm(al)
            if nz and nz not in seen:
                seen.add(nz)
                a.execute("INSERT OR IGNORE INTO title_aliases VALUES(?,?,?,?)",(ip_id,al,nz,src)); n_al+=1
        n_ip+=1
        if n_ip%200==0: a.commit(); print(f"  {n_ip}/{len(rows)} ips, {n_al} aliases",flush=True)
    a.commit()
    print(f"DONE: {n_ip} ip_ids -> {n_al} aliases ({err} fetch errors).  db={ALIAS_DB}")
    # sample dump
    for ip_id,_,_,_,_ in rows[:6]:
        al=[r[0] for r in a.execute("SELECT alias FROM title_aliases WHERE ip_id=?",(ip_id,))]
        print(f"   {ip_id}: {al[:6]}")
    a.close()

if __name__=="__main__": main()
