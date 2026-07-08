#!/usr/bin/env python3
"""
stream_registry.py — Phase 2 (streaming channel) spike, step 1: build the site
registry from Google's copyright Transparency Report bulk CSV.

The Transparency Report "copyright removals" export includes a per-DOMAIN file
ranking specified websites by how many URLs were requested for removal — i.e. an
authority-published ranking of the largest piracy sites, refreshed continuously.
We ingest that, drop torrent-only hosts (already covered by the P2P feed), keep
likely streaming/DDL hosts, and load the top N into stream_demand.db:stream_sites.

ISOLATION: writes ONLY to its own DB (default /data/db/stream_demand.db). Touches
no P2P DB or service. No pirate URLs live in source — they come from the report.

Usage:
  python3 stream_registry.py /path/to/google_domains_export.csv --top 300
  python3 stream_registry.py --show          # dump current registry summary
"""
import os, sys, csv, sqlite3, datetime

STREAM_DB = os.environ.get("STREAM_DB", "/data/db/stream_demand.db")

DDL = ("CREATE TABLE IF NOT EXISTS stream_sites("
       "domain TEXT PRIMARY KEY, kind TEXT, rank_signal INTEGER, parser TEXT, "
       "status TEXT, first_seen TEXT, last_checked TEXT, last_live TEXT)")

# Heuristic classifier. Torrent hosts are dropped (P2P feed already covers them);
# streaming/DDL hints keep a host; everything else stays as 'mixed' for the parser
# to confirm/kill. Deliberately broad — the registry is a candidate list, not truth.
TORRENT_HINT = ("torrent", "magnet", "1337", "yts", "rarbg", "nyaa", "piratebay",
                "thepiratebay", "tpb", "eztv", "limetorrent", "kickass")
STREAM_HINT = ("flix", "stream", "watch", "movie", "/tv", "series", "putlocker",
               "123", "gomovies", "fmovies", "soap", "cine", "play", "tube", "ddl",
               "hdmovie", "yesmovies", "primewire", "vidsrc", "hd", "anime", "cloud")


def classify(domain: str):
    d = domain.lower()
    if any(h in d for h in TORRENT_HINT):
        return None
    if any(h in d for h in STREAM_HINT):
        return "streaming"
    return "mixed"


def load_csv(path: str):
    """Defensive parse — the export's column names vary; locate a domain col and a
    URL/removal-count col by header keywords."""
    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        r = csv.reader(f)
        header = next(r)
        hl = [h.lower() for h in header]
        dcol = next((i for i, h in enumerate(hl) if "domain" in h or "site" in h), 0)
        ccol = next((i for i, h in enumerate(hl)
                     if "url" in h and ("remov" in h or "request" in h or "delist" in h)), None)
        if ccol is None:
            ccol = next((i for i, h in enumerate(hl) if "count" in h or "urls" in h), None)
        agg = {}
        for row in r:
            if len(row) <= dcol:
                continue
            dom = row[dcol].strip().lower().lstrip("*.")
            if not dom or "." not in dom or " " in dom:
                continue
            cnt = 0
            if ccol is not None and ccol < len(row):
                try:
                    cnt = int(row[ccol].replace(",", "").strip() or 0)
                except ValueError:
                    cnt = 0
            agg[dom] = agg.get(dom, 0) + cnt
    return agg


def build(path: str, topn: int):
    agg = load_csv(path)
    ranked = sorted(agg.items(), key=lambda x: -x[1])
    conn = sqlite3.connect(STREAM_DB)
    conn.execute(DDL)
    today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    kept = 0
    for dom, cnt in ranked:
        kind = classify(dom)
        if kind is None:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO stream_sites"
            "(domain,kind,rank_signal,status,first_seen,last_checked) VALUES(?,?,?,?,?,?)",
            (dom, kind, cnt, "unparsed", today, today))
        kept += 1
        if kept >= topn:
            break
    conn.commit()
    n = conn.execute("SELECT COUNT(*) FROM stream_sites").fetchone()[0]
    print("registry: ingested %d candidate streaming/DDL domains (of %d ranked); "
          "%d total in %s" % (kept, len(ranked), n, STREAM_DB))
    print("(top entries are the registry's highest removal-volume hosts — parsers "
          "get built for these first; inspect with --show on the box)")


def seed_from_list(path: str):
    """Source-agnostic seed: ingest a plain newline-delimited list of domains (any
    source — USTR/EUIPO named markets, an in-house list, a curated starter set).
    Optional `domain,rank` or `domain,rank,kind` per line. Blank lines and `#`
    comments ignored. Lets the spike proceed without Google's bulk export."""
    conn = sqlite3.connect(STREAM_DB)
    conn.execute(DDL)
    today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    kept = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [p.strip() for p in line.replace("\t", ",").split(",")]
            dom = parts[0].lower().lstrip("*.").replace("https://", "").replace("http://", "").split("/")[0]
            if "." not in dom or " " in dom:
                continue
            rank = 0
            if len(parts) > 1 and parts[1].replace(",", "").isdigit():
                rank = int(parts[1].replace(",", ""))
            kind = parts[2] if len(parts) > 2 else (classify(dom) or "mixed")
            conn.execute(
                "INSERT OR IGNORE INTO stream_sites"
                "(domain,kind,rank_signal,status,first_seen,last_checked) VALUES(?,?,?,?,?,?)",
                (dom, kind, rank, "unparsed", today, today))
            kept += 1
    conn.commit()
    print("registry: seeded %d domains from %s -> %s" % (kept, path, STREAM_DB))


def liveness():
    """Probe each registry domain for reachability (NOT content crawling). Marks
    status: 'live' (HTTP <400), 'blocked' (responds but 403/503 — Cloudflare/JS
    challenge; domain exists, needs a real browser/flaresolverr later), or 'dead'
    (DNS failure / refused / timeout). Updates stream_sites in place."""
    import urllib.request, ssl, datetime as _dt
    UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    conn = sqlite3.connect(STREAM_DB)
    conn.execute(DDL)
    domains = [r[0] for r in conn.execute("SELECT domain FROM stream_sites ORDER BY rank_signal DESC")]
    today = _dt.datetime.now(_dt.UTC).strftime("%Y-%m-%d")
    print("liveness probe (%d domains):" % len(domains))
    for dom in domains:
        status, code, final = "dead", None, None
        for scheme in ("https", "http"):
            try:
                req = urllib.request.Request(scheme + "://" + dom, method="GET",
                                             headers={"User-Agent": UA})
                with urllib.request.urlopen(req, timeout=12, context=ctx) as resp:
                    code = resp.status
                    final = resp.geturl()
                status = "live" if (code or 599) < 400 else "blocked"
                break
            except urllib.error.HTTPError as e:
                code = e.code
                status = "blocked" if e.code in (403, 503, 429) else "dead"
                break
            except Exception:
                status = "dead"
        conn.execute(
            "UPDATE stream_sites SET status=?, last_checked=?, "
            "last_live=CASE WHEN ?='live' THEN ? ELSE last_live END WHERE domain=?",
            (status, today, status, today, dom))
        print("  %-22s %-8s %s %s" % (dom, status, code or "", final or ""))
    conn.commit()
    live = conn.execute("SELECT COUNT(*) FROM stream_sites WHERE status IN ('live','blocked')").fetchone()[0]
    print("=> %d/%d reachable (live+blocked); parsers get built for these." % (live, len(domains)))


def show():
    if not os.path.exists(STREAM_DB):
        print("no registry db at", STREAM_DB)
        return
    c = sqlite3.connect("file:" + STREAM_DB + "?mode=ro", uri=True)
    n = c.execute("SELECT COUNT(*) FROM stream_sites").fetchone()[0]
    bykind = c.execute("SELECT kind, COUNT(*) FROM stream_sites GROUP BY kind").fetchall()
    print("stream_sites: %d rows  %s" % (n, dict(bykind)))
    print("top 20 by rank_signal:")
    for dom, rs, st in c.execute(
            "SELECT domain, rank_signal, status FROM stream_sites "
            "ORDER BY rank_signal DESC LIMIT 20"):
        print("  %-32s %12d  %s" % (dom, rs, st))


if __name__ == "__main__":
    if "--show" in sys.argv:
        show()
    elif "--seed" in sys.argv:
        seed_from_list(sys.argv[sys.argv.index("--seed") + 1])
    elif "--liveness" in sys.argv:
        liveness()
    elif len(sys.argv) >= 2 and not sys.argv[1].startswith("--"):
        top = 300
        if "--top" in sys.argv:
            top = int(sys.argv[sys.argv.index("--top") + 1])
        build(sys.argv[1], top)
    else:
        print(__doc__)
