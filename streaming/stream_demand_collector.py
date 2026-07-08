#!/usr/bin/env python3
"""stream_demand_collector.py — Channel-2 (web-streaming) demand collector.

Fetches each LIVE site in stream_demand.db:stream_sites, extracts candidate
title strings from its HTML, and matches them against the Pantheon catalog
(anime/movies/series parquet, keyed by normalized title). A title's streaming
demand signal is the number of distinct live streaming sites it currently
appears on (presence breadth) — a proxy for how widely a title is being pirated
via web streaming, complementary to the P2P distinct-peer-IP feed.

WHY broad-extract + strict-match: extraction is deliberately broad (every
`title=`/`alt=` attribute + anchor/heading text), but ONLY candidates that
normalize to a real catalog ip_id are kept. The catalog is therefore the noise
filter — site navigation / boilerplate never matches a real title and is dropped
automatically. This is the validated spike approach (gogoanimes.fi → 399 clean
catalog matches), productionised across all live sites with persistence.

Per-site parser hooks (SITE_PARSERS) let us tighten extraction for specific
sites later; sites without a hook use the generic extractor, which the match
gate already keeps clean.

ISOLATION: reads stream_sites + the read-only catalog parquet; writes ONLY to
stream_demand.db (new tables stream_title_obs + stream_title_demand). Touches no
P2P DB or service. Fetches are plain GETs to domains already in the registry.

Usage:
  python3 stream_demand_collector.py              # scrape all live sites, store
  python3 stream_demand_collector.py --limit 10   # cap site count (testing)
  python3 stream_demand_collector.py --show        # dump today's demand table
"""
import os, sys, re, json, sqlite3, subprocess, datetime

DB  = os.environ.get("STREAM_DB", "/data/db/stream_demand.db")
CAT = os.environ.get("CATALOG_DIR", "/data/catalog")
ALIAS_DB = os.environ.get("ALIAS_DB", "/data/db/title_aliases.db")
UA  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
TODAY = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")

_STOP = re.compile(
    r'\b(hd|fhd|uhd|4k|1080p|720p|480p|web ?dl|webrip|bluray|bdrip|hdrip|hdtv|cam|'
    r'season|episode|ep|complete|watch|online|free|full|streaming|stream|movie|'
    r'movies|series|tv|show|anime|sub|subbed|dub|dubbed|vostfr|vf|vo|english|eng)\b')

def norm(s):
    """Normalise a raw title to a match key: lowercase, drop year / quality /
    season-episode / generic words, strip punctuation, collapse whitespace."""
    s = s.lower()
    s = re.sub(r'&[a-z]+;|&#\d+;', ' ', s)      # html entities
    s = re.sub(r'\(\d{4}\)', ' ', s)            # (2024)
    s = re.sub(r'\b(19|20)\d{2}\b', ' ', s)     # bare year
    s = _STOP.sub(' ', s)
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    return ' '.join(s.split()).strip()

def load_catalog():
    """norm(title) -> (ip_id, official_title, category). First mapping wins.

    Includes the canonical catalog titles AND every known ALIAS from
    title_aliases.db (romaji / English / foreign-language variants), so a site
    that lists a title under an alternate name (e.g. 'Tongari Boushi no Atelier'
    for Witch Hat Atelier) still matches. This closes the same foreign-title gap
    we fixed on the P2P side. Memory-safe: the alias DB (~12k rows) is streamed
    row-by-row; the parquet read is column-pruned (ip_id, ip only)."""
    import pyarrow.parquet as pq
    m = {}
    ipmeta = {}   # ip_id -> (official_title, category) for attaching aliases
    for fn, cat in (("anime_info", "Anime"), ("movies_info", "Movie"), ("series_info", "Series")):
        p = "%s/%s.parquet" % (CAT, fn)
        if not os.path.exists(p):
            continue
        t = pq.read_table(p, columns=["ip_id", "ip"]).to_pydict()
        for ipid, ip in zip(t["ip_id"], t["ip"]):
            if not ip:
                continue
            ipmeta.setdefault(ipid, (ip, cat))
            n = norm(ip)
            if len(n) >= 3 and n not in m:
                m[n] = (ipid, ip, cat)
    # ── alias keys (foreign / alt titles) — streamed row-by-row, low memory ──
    if os.path.exists(ALIAS_DB):
        try:
            a = sqlite3.connect("file:%s?mode=ro" % ALIAS_DB, uri=True)
            n_alias = 0
            for ipid, alias in a.execute("SELECT ip_id, alias FROM title_aliases"):
                meta = ipmeta.get(ipid)
                if not meta:
                    continue
                n = norm(alias)
                if len(n) >= 3 and n not in m:
                    m[n] = (ipid, meta[0], meta[1]); n_alias += 1
            a.close()
            print("catalog: %d title keys (+%d from aliases)" % (len(m), n_alias))
        except Exception as e:
            print("alias keys skipped:", e)
    return m

_ATTR = re.compile(r'(?:title|alt)="([^"]{3,80})"')
_TEXT = re.compile(r'<(?:a|h1|h2|h3|h4)[^>]*>\s*([^<>{}]{3,80}?)\s*</', re.I)
# Titles embedded as JSON in the static HTML payload (JSON-LD, __NEXT_DATA__,
# __NUXT__, inline API responses). Many "JS-rendered" movie/series sites ship
# their data this way, so curl already has it — no headless browser needed.
# The catalog match-gate keeps this safe even though the pattern is broad.
_JSONTITLE = re.compile(r'"(?:title|name|titleText|originalTitle)"\s*:\s*"((?:[^"\\]|\\.){3,80}?)"')

def _unesc(s):
    try:
        return json.loads('"%s"' % s)                       # decodes \uXXXX, \/, \" etc.
    except Exception:
        return s.replace('\\/', '/').replace('\\u0026', '&')

def extract_json_titles(html):
    return {_unesc(m.group(1)) for m in _JSONTITLE.finditer(html)}

def extract_generic(html):
    c = set()
    for m in _ATTR.finditer(html):
        c.add(m.group(1))
    for m in _TEXT.finditer(html):
        c.add(m.group(1).strip())
    c |= extract_json_titles(html)        # embedded-JSON titles (JS-site payloads)
    return c

def extract_gogoanime(html):
    # broad generic recall PLUS the clean show names on episode anchors (superset)
    c = extract_generic(html)
    c |= set(m.group(1) for m in re.finditer(
        r'<a[^>]*href="[^"]*-episode-[^"]*"[^>]*title="([^"]{3,80})"', html))
    return c

SITE_PARSERS = {
    "gogoanimes.fi": extract_gogoanime,
}

# site-chrome whose normalized form coincides with a catalog title — genre filter
# links + footer/nav labels appear on EVERY site's menu, so they masquerade as
# high-presence "titles". Dropped even on a catalog match. We accept losing the
# rare real title literally named e.g. "Music"/"Romance" — worth it to kill the
# pervasive genre-menu noise. (Multi-word real titles are unaffected.)
NAV_BLOCK = {
    # nav / footer chrome
    "popular", "latest", "trending", "trending now", "most popular", "ongoing",
    "recent", "recently added", "latest episode", "new episode", "recent release",
    "top", "home", "genres", "genre", "schedule", "upcoming", "completed",
    "featured", "random", "search", "login", "register", "new", "list", "az list",
    "ongoing series", "contact", "disclaimer", "about", "dmca", "privacy", "terms",
    "help", "faq", "news", "menu", "more", "movies", "series", "tv shows",
    # genre filter labels (each is also some obscure catalog title)
    "action", "adventure", "comedy", "drama", "fantasy", "horror", "thriller",
    "romance", "mystery", "sci fi", "science fiction", "music", "sports",
    "supernatural", "slice of life", "historical", "harem", "ecchi", "isekai",
    "mecha", "magic", "psychological", "seinen", "shoujo", "shounen", "josei",
    "kids", "family", "animation", "adult", "crime", "war", "western", "space",
    "documentary", "biography", "musical", "martial arts", "demons", "game",
    "military", "parody", "police", "samurai", "school", "vampire", "dementia",
    "cars", "kingdom",
    # pagination / UI / section chrome surfaced by listing-page crawl
    "next", "prev", "previous", "page", "special", "specials", "faq", "faqs",
    "open", "close", "private", "reality", "continue", "play", "see all",
    "view all", "load more", "show more", "comments", "related", "server",
    "mirror", "mirrors", "trailer", "trailers", "cast", "report", "request",
    "watch now", "full movie", "download", "subtitles", "quality", "episodes",
    "seasons", "you may also like", "recommended", "english subbed", "dubbed",
}

_SCRAPER = None   # lazy cloudscraper session (reused across sites)
def _scraper():
    """Cloudflare/anti-bot capable fetcher — only if `cloudscraper` is installed.
    Returns False (cached) when unavailable so we never retry the import."""
    global _SCRAPER
    if _SCRAPER is None:
        try:
            import cloudscraper
            _SCRAPER = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows"})
        except Exception:
            _SCRAPER = False
    return _SCRAPER

def _flaresolverr_get(url):
    """Render a Cloudflare/JS page via FlareSolverr — only if FLARESOLVERR_URL is set
    (e.g. http://172.31.45.15:8191/v1). Returns rendered HTML or "". POSTs the v1 API
    via curl (consistent with the rest of this module). maxTimeout 120s absorbs the
    cold-start: FlareSolverr's FIRST solve launches Chromium and can exceed 60s."""
    base = os.environ.get("FLARESOLVERR_URL", "")
    if not base:
        return ""
    payload = json.dumps({"cmd": "request.get", "url": url, "maxTimeout": 120000})
    try:
        r = subprocess.run(["curl", "-s", "-m", "135", "-X", "POST", base,
                            "-H", "Content-Type: application/json", "-d", payload],
                           capture_output=True, text=True, timeout=145)
        d = json.loads(r.stdout)
        sol = d.get("solution") or {}
        if d.get("status") == "ok" and sol.get("status") == 200:
            return sol.get("response") or ""
    except Exception:
        pass
    return ""

def fetch_url(url):
    # 1) fast path: plain curl (works for the static-HTML sites, mostly anime)
    try:
        r = subprocess.run(["curl", "-s", "-m", "18", "-L", "-k", "--compressed", "-A", UA, url],
                           capture_output=True, text=True, timeout=25)
        if r.stdout and len(r.stdout) > 3000:
            return r.stdout
    except Exception:
        pass
    # 2) FlareSolverr (Cloudflare/JS movie+series sites). No-op until FLARESOLVERR_URL
    #    is set in the collector's env — then it auto-activates, no code change.
    h = _flaresolverr_get(url)
    if h and len(h) > 3000:
        return h
    # 3) fallback: cloudscraper (legacy, dormant). No-op until `pip install cloudscraper`.
    sc = _scraper()
    if sc:
        try:
            h = sc.get(url, timeout=30).text
            if h and len(h) > 3000:
                return h
        except Exception:
            pass
    return ""

def fetch(domain):
    for url in ("https://" + domain + "/", "http://" + domain + "/"):
        h = fetch_url(url)
        if h:
            return h
    return ""

# anchor text that marks a content-listing page (where real titles live, vs the
# genre-nav-heavy homepage of WordPress movie/series sites)
_LISTING_KW = re.compile(
    r'\b(trending|popular|recently added|recent release|latest|new release|'
    r'new movies|top movies|box office|most viewed|tv shows|tv series|series|movies)\b', re.I)

def discover_listings(html, domain, cap=2):
    """Find up to `cap` same-domain listing-page URLs linked from the homepage."""
    out = []
    for m in re.finditer(r'<a[^>]*href="([^"]+)"[^>]*>\s*([^<>{}]{3,40}?)\s*</a>', html):
        href, text = m.group(1), m.group(2).strip()
        if not _LISTING_KW.search(text):
            continue
        u = href
        if u.startswith("/"):
            u = "https://" + domain + u
        if u.startswith("http") and domain in u and u not in out:
            out.append(u)
        if len(out) >= cap:
            break
    return out

DDL = [
 "CREATE TABLE IF NOT EXISTS stream_title_obs(date TEXT,domain TEXT,ip_id TEXT,title TEXT,category TEXT,PRIMARY KEY(date,domain,ip_id))",
 "CREATE TABLE IF NOT EXISTS stream_title_demand(date TEXT,ip_id TEXT,title TEXT,category TEXT,n_sites INT,sites TEXT,PRIMARY KEY(date,ip_id))",
 "CREATE INDEX IF NOT EXISTS idx_sto_date ON stream_title_obs(date)",
 "CREATE INDEX IF NOT EXISTS idx_std_date ON stream_title_demand(date,n_sites)",
]

def collect(limit=None):
    cat = load_catalog()
    print("catalog normalized titles: %d" % len(cat))
    con = sqlite3.connect(DB)
    for d in DDL:
        con.execute(d)
    sites = [r[0] for r in con.execute(
        "SELECT domain FROM stream_sites WHERE status='live' ORDER BY rank_signal DESC")]
    if limit:
        sites = sites[:limit]
    con.execute("DELETE FROM stream_title_obs WHERE date=?", (TODAY,))
    obs = {}        # ip_id -> [title, category, set(domains)]
    scraped = 0
    for dom in sites:
        html = fetch(dom)
        if not html:
            print("  %-28s FETCH-FAIL" % dom)
            continue
        scraped += 1
        parser = SITE_PARSERS.get(dom, extract_generic)
        # homepage + up to 2 discovered listing pages (lifts movie/series sites
        # whose homepage is genre-nav; the title cards live on listing pages)
        cands = set(parser(html))
        for lu in discover_listings(html, dom):
            h2 = fetch_url(lu)
            if h2:
                cands |= parser(h2)
        hits = {}
        for cand in cands:
            n = norm(cand)
            if n in cat and n not in NAV_BLOCK:
                ipid, title, c = cat[n]
                hits[ipid] = (title, c)
        for ipid, (title, c) in hits.items():
            slot = obs.setdefault(ipid, [title, c, set()])
            slot[2].add(dom)
        con.executemany("INSERT OR REPLACE INTO stream_title_obs VALUES(?,?,?,?,?)",
            [(TODAY, dom, ipid, title, c) for ipid, (title, c) in hits.items()])
        print("  %-28s matches=%d" % (dom, len(hits)))
    con.execute("DELETE FROM stream_title_demand WHERE date=?", (TODAY,))
    con.executemany("INSERT OR REPLACE INTO stream_title_demand VALUES(?,?,?,?,?,?)",
        [(TODAY, ipid, v[0], v[1], len(v[2]), ",".join(sorted(v[2]))) for ipid, v in obs.items()])
    con.commit()
    n = con.execute("SELECT COUNT(*) FROM stream_title_demand WHERE date=?", (TODAY,)).fetchone()[0]
    print("\n=== %d distinct catalog titles across %d/%d sites scraped (%s) ===" % (n, scraped, len(sites), TODAY))
    print("top by streaming-site presence:")
    for title, c, ns, _s in con.execute(
            "SELECT title,category,n_sites,sites FROM stream_title_demand WHERE date=? "
            "ORDER BY n_sites DESC,title LIMIT 25", (TODAY,)):
        print("  %2d sites  %-42s [%s]" % (ns, title[:42], c))
    con.close()

def show():
    con = sqlite3.connect(DB)
    try:
        rows = con.execute("SELECT date,COUNT(*),MAX(n_sites) FROM stream_title_demand GROUP BY date ORDER BY date DESC LIMIT 10").fetchall()
    except sqlite3.OperationalError:
        print("no stream_title_demand table yet — run without --show first"); return
    print("date         titles  max_sites")
    for d, n, mx in rows:
        print("  %s   %5d   %d" % (d, n, mx or 0))
    con.close()

if __name__ == "__main__":
    if "--show" in sys.argv:
        show()
    else:
        lim = None
        if "--limit" in sys.argv:
            lim = int(sys.argv[sys.argv.index("--limit") + 1])
        collect(lim)
