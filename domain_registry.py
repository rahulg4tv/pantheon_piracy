#!/usr/bin/env python3
"""Build the Phase-2 video-streaming site registry from the Google copyright-removal
top-domains list. Classifies /data/transparency/top_domains.tsv, drops torrent /
book / adult / DDL / software / file-host, keeps movie-TV-anime streaming sites,
and writes the ranked candidates to the ISOLATED stream_demand.db (stream_sites).
Touches no P2P DB. Run: python3 domain_registry.py [--write]"""
import sys, sqlite3, time
TSV = "/data/transparency/top_domains.tsv"
STREAM_DB = "/data/db/stream_demand.db"
WRITE = "--write" in sys.argv

TORRENT = ("torrent","magnet","1337","yts","rarbg","nyaa","piratebay","thepiratebay",
           "tpb","eztv","limetorrent","kickass","rutracker","rutor","torrentgalaxy",
           "glodls","bitsearch","zooqle","torlock","ettv","torrentz")
BOOK    = ("book","annas-archive","anna-archive","libgen","z-lib","zlib","zlibrary",
           "sci-hub","scihub","epub","ebook","pdfdrive","-library","oceanofpdf")
ADULT   = ("porn","sex","xxx","xnxx","xvideo","x-video","xhamster","hentai","brazzers",
           "onlyfans","nsfw","redtube","youporn","chaturbate","fapello","erome","camsoda",
           "spankbang","tnaflix","camwhore","camstream","rectube","rec-tube","tubeorigin",
           "webcam","milf","nude","escort","fuck","boobs","hublot","tna")
DDL     = ("rapidgator","1fichier","mediafire","mega.nz","nitroflare","ddownload","dropgalaxy",
           "krakenfiles","turbobit","hitfile","uploadgig","filefactory","dailyuploads",
           "clicknupload","uploaded.","wdupload","katfile","filestube","filesfly","filespace")
SOFT    = ("crack","repack","skidrow","fitgirl","igg-games","warez","keygen","nulled",
           "getintopc","oceanofgames","steamunlocked","gamebra","apkdone","apkpure","mod-apk")
# 'tube' removed (porn-tube false positives); keep specific video-streaming signals
STREAM  = ("flix","stream","watch","movie","series","putlocker","123movie","gomovies","fmovies",
           "soap2","cine","cinema","anime","kdrama","drama","pelis","repelis","cuevana","voir-film",
           "voirfilm","ver-pel","-film","film-","megaflix","hdtoday","sflix","soaper","lookmovie",
           "primewire","9anime","nineanime","nwanime","aniwatch","hianime","goku","movierulz",
           "hdmovie","yesmovies","myflixer","bflix","dramacool","kissasian","watchseries","streamin",
           "uptostream","rabbitstream","vidsrc","showbox","tinyzone","wcofun","gogoanime","zoro",
           "lordfilm","lrdfilm","zfilm","soul-anime","seriesfree","5movies","kinox","seriesflix")
KNOWN_LIVE = ("9anime", "ifmovies", "cuevana")

def classify(d):
    d = d.lower()
    for cat, hints in (("adult",ADULT),("book",BOOK),("torrent",TORRENT),
                       ("software",SOFT),("ddl",DDL),("streaming",STREAM)):
        if any(h in d for h in hints):
            return cat
    return "unknown"

cats, streaming = {}, []
for line in open(TSV):
    p = line.rstrip("\n").split("\t")
    if len(p) != 2: continue
    try: cnt = int(p[0])
    except: continue
    c = classify(p[1]); cats[c] = cats.get(c,0)+1
    if c == "streaming": streaming.append((cnt, p[1]))

print("=== categories (of %d) ===" % sum(cats.values()))
for c in sorted(cats, key=lambda k:-cats[k]): print("  %-10s %d" % (c, cats[c]))
print("=== top 35 streaming ===")
for cnt,dom in streaming[:35]: print("  %12d  %s" % (cnt,dom))
print("=== known-live present? ===")
for k in KNOWN_LIVE:
    hit=[d for _,d in streaming if k in d]; print("  %-9s -> %s" % (k, hit[:3] or "absent from top-5000"))
print("=== streaming candidates: %d ===" % len(streaming))

if WRITE:
    today = time.strftime("%Y-%m-%d", time.gmtime())
    con = sqlite3.connect(STREAM_DB, timeout=30)
    con.execute("CREATE TABLE IF NOT EXISTS stream_sites(domain TEXT PRIMARY KEY, kind TEXT, "
                "rank_signal INTEGER, parser TEXT, status TEXT, first_seen TEXT, last_checked TEXT)")
    for cnt, dom in streaming:
        con.execute("INSERT INTO stream_sites(domain,kind,rank_signal,status,first_seen,last_checked) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(domain) DO UPDATE SET rank_signal=excluded.rank_signal",
                    (dom, "streaming", cnt, "unparsed", today, today))
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM stream_sites").fetchone()[0]
    con.close()
    print("=== WROTE %d streaming sites to %s (total %d) ===" % (len(streaming), STREAM_DB, n))
