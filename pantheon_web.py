#!/usr/bin/env python3
"""
pantheon_web.py — Pantheon Piracy Intelligence (interactive single-page app, Flask).

Reads the DERIVED snapshot DB pantheon_intel.db (rebuilt hourly) — never the live
harvest DBs, so zero write-contention. Vanilla-JS front end (no CDN): tabs, search-as-
you-type, category + country filters, sortable tables, click-a-title detail drawer
(per-country breakdown + daily trend), surge movers, CSV export. Country labels
normalised to full names (CN->China) via pycountry.
"""
import os, io, csv, json, sqlite3
from flask import Flask, request, jsonify, Response

DB = os.environ.get("INTEL_DB", "/data/db/pantheon_intel.db")
STREAM_DB = os.environ.get("STREAM_DB", "/data/db/stream_demand.db")
ACESTREAM_DB = os.environ.get("ACESTREAM_DB", "/data/db/acestream_pilot.db")
PORT = int(os.environ.get("PORT", "8090"))

# Poster lookup: {ip_id: TMDB image path} generated from the synced catalog.
# Static reference (no live-DB contention); loaded once at worker start.
POSTERS_JSON = os.environ.get("POSTERS_JSON", "/data/catalog/posters.json")
TMDB_IMG = "https://image.tmdb.org/t/p/w185"   # movies + series (TMDB paths)
MAL_IMG = "https://cdn.myanimelist.net/images"  # anime (MyAnimeList paths, e.g. /anime/1244/..l.jpg)
try:
    with open(POSTERS_JSON) as _pf:
        _POSTERS = json.load(_pf)
except Exception:
    _POSTERS = {}


def poster_url(ip_id):
    p = _POSTERS.get(ip_id)
    if not p:
        return None
    p = str(p)
    if p.startswith("http"):
        return p
    # anime image_url is a MAL CDN path, not a TMDB path — pick the right origin.
    base = MAL_IMG if str(ip_id).startswith("anime-") else TMDB_IMG
    return base + (p if p.startswith("/") else "/" + p)
try:
    import pycountry
except Exception:
    pycountry = None

LEGACY = {"United States", "United Kingdom", "Canada", "Australia", "Brazil", "France",
          "Germany", "Ireland", "Italy", "Japan", "Mexico", "South Korea", "Spain", "Thailand"}
app = Flask(__name__)


def cname(label):
    if not label or label in LEGACY or label == "Other":
        return label or "Unknown"
    if pycountry and len(label) == 2:
        c = pycountry.countries.get(alpha_2=label)
        if c:
            return getattr(c, "common_name", None) or c.name
    return label


_L2ISO = {"United States": "US", "United Kingdom": "GB", "Canada": "CA", "Australia": "AU",
          "Brazil": "BR", "France": "FR", "Germany": "DE", "Ireland": "IE", "Italy": "IT",
          "Japan": "JP", "Mexico": "MX", "South Korea": "KR", "Spain": "ES", "Thailand": "TH"}
_NAME2ISO = {"Vietnam": "VN", "Laos": "LA", "Syria": "SY", "Brunei": "BN", "Russia": "RU",
             "Taiwan": "TW", "North Korea": "KP"}
def ciso(label):
    """Country label -> ISO-2 for GeoJSON matching. Handles full names (the feed's
    new format), the legacy 14, our export overrides, and raw 2-letter codes."""
    if not label or label == "Other":
        return None
    if len(label) == 2 and label.isupper():
        return label
    if label in _L2ISO:
        return _L2ISO[label]
    if label in _NAME2ISO:
        return _NAME2ISO[label]
    if pycountry:
        try:
            return pycountry.countries.lookup(label).alpha_2
        except Exception:
            return None
    return None


def db():
    c = sqlite3.connect("file:" + DB + "?mode=ro", uri=True, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def latest_date(c):
    r = c.execute("SELECT MAX(date) d FROM daily_totals").fetchone()
    return r["d"] if r else None


SORTS = {"ip": "ip_count DESC", "ip_asc": "ip_count ASC", "title": "title ASC", "title_desc": "title DESC"}


SRC_COL = {"dht": "ip_count_dht", "harv": "ip_count_harv", "pex": "ip_count_pex"}
def _title_rows(c, date, cat, country, q, sort, limit, src="all"):
    col = SRC_COL.get(src, "ip_count")
    dcsel = "dc_ip_count" if col == "ip_count" else "NULL dc_ip_count"
    where, args = ["date=?"], [date]
    if cat and cat != "All":
        where.append("category=?"); args.append(cat)
    if q:
        where.append("title LIKE ?"); args.append("%" + q + "%")
    order = SORTS.get(sort, SORTS["ip"]).replace("ip_count", col)
    if country and country != "All":
        where.append("country=?"); args.append(country)
        sql = (f"SELECT tc.title,tc.category,tc.ip_id,tc.{col} ip_count,NULL dc_ip_count FROM title_country tc WHERE "
               + " AND ".join(where) + f" ORDER BY {order} LIMIT ?")
    else:
        sql = (f"SELECT title,category,ip_id,{col} ip_count,{dcsel} FROM title_demand WHERE "
               + " AND ".join(where) + f" ORDER BY {order} LIMIT ?")
    args.append(limit)
    return c.execute(sql, args).fetchall()


def _sparks(c, ip_ids, ndays=14):
    """Return {ip_id: [ip_count per day over the last ndays]} aligned to a shared
    date axis — for inline sparklines. One query, indexed on ip_id (fast)."""
    ids = [i for i in dict.fromkeys(ip_ids) if i]
    if not ids:
        return {}, []
    dates = [r["date"] for r in c.execute("SELECT date FROM daily_totals ORDER BY date DESC LIMIT ?", (ndays,))][::-1]
    if not dates:
        return {}, []
    ph = ",".join("?" * len(ids))
    rows = c.execute(f"SELECT ip_id,date,ip_count FROM title_demand WHERE date>=? AND ip_id IN ({ph})",
                     [dates[0]] + ids).fetchall()
    by = {}
    for r in rows:
        by.setdefault(r["ip_id"], {})[r["date"]] = r["ip_count"]
    return {i: [by.get(i, {}).get(d, 0) for d in dates] for i in ids}, dates


@app.after_request
def _no_cache(resp):
    # dashboard is fully dynamic — stop browsers serving a stale cached page
    # (was causing "I don't see the fix" after every deploy)
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    return resp


@app.route("/api/meta")
def api_meta():
    c = db(); L = latest_date(c)
    if not L:
        return jsonify({"latest": None, "dates": [], "countries": []})
    dates = [r["date"] for r in c.execute("SELECT date FROM daily_totals ORDER BY date DESC")]
    countries = [{"code": r["country"], "name": cname(r["country"]), "ip": r["s"]}
                 for r in c.execute("SELECT country, SUM(ip_count) s FROM country_demand "
                                    "WHERE date=? AND country!='Other' GROUP BY country ORDER BY s DESC", (L,))]
    return jsonify({"latest": L, "dates": dates, "countries": countries})


@app.route("/api/overview")
def api_overview():
    c = db(); date = request.args.get("date") or latest_date(c)
    if not date:
        return jsonify({})
    tot = c.execute("SELECT * FROM daily_totals WHERE date=?", (date,)).fetchone()
    prev = c.execute("SELECT date,total_ip FROM daily_totals WHERE date<? ORDER BY date DESC LIMIT 1", (date,)).fetchone()
    movers = []
    if prev:
        cur = {r["title"]: r["ip_count"] for r in c.execute("SELECT title,ip_count FROM title_demand WHERE date=?", (date,))}
        pv = {r["title"]: r["ip_count"] for r in c.execute("SELECT title,ip_count FROM title_demand WHERE date=?", (prev["date"],))}
        for t, v in cur.items():
            movers.append({"title": t, "ip": v, "delta": v - pv.get(t, 0)})
        movers = sorted(movers, key=lambda x: -x["delta"])[:12]
    top = {cat: [dict(r) for r in c.execute("SELECT title,ip_id,ip_count,category,dc_ip_count FROM title_demand WHERE date=? AND category=? ORDER BY ip_count DESC LIMIT 10", (date, cat))]
           for cat in ["Movie", "Series", "Anime"]}
    sp, _ = _sparks(c, [r["ip_id"] for cat in top for r in top[cat]])
    for cat in top:
        for r in top[cat]:
            r["spark"] = sp.get(r["ip_id"], [])
    trend = [dict(r) for r in c.execute("SELECT date,total_ip FROM daily_totals ORDER BY date")]
    return jsonify({"date": date, "totals": dict(tot) if tot else {},
                    "prev": dict(prev) if prev else None, "movers": movers, "top": top, "trend": trend})


@app.route("/api/titles")
def api_titles():
    c = db(); date = request.args.get("date") or latest_date(c)
    rows = _title_rows(c, date, request.args.get("category", "All"), request.args.get("country", "All"),
                       request.args.get("q", "").strip(), request.args.get("sort", "ip"),
                       min(int(request.args.get("limit", "200")), 1000), request.args.get("src", "all"))
    data = [dict(r) for r in rows]
    sp, _ = _sparks(c, [d["ip_id"] for d in data])
    for d in data:
        d["spark"] = sp.get(d["ip_id"], [])
    return jsonify(data)


@app.route("/api/title")
def api_title():
    c = db(); ip_id = request.args.get("ip_id"); date = request.args.get("date") or latest_date(c)
    cols = "title,category,imdb_id,anime_id,ip_count,dc_ip_count,ip_count_dht,ip_count_harv,ip_count_pex"
    info = c.execute(f"SELECT {cols} FROM title_demand WHERE ip_id=? AND date=? LIMIT 1", (ip_id, date)).fetchone()
    if not info:
        info = c.execute(f"SELECT {cols} FROM title_demand WHERE ip_id=? ORDER BY date DESC LIMIT 1", (ip_id,)).fetchone()
    countries = [{"code": r["country"], "name": cname(r["country"]), "ip_count": r["ip_count"]}
                 for r in c.execute("SELECT country,ip_count FROM title_country WHERE date=? AND ip_id=? AND country!='Other' "
                                    "ORDER BY ip_count DESC LIMIT 25", (date, ip_id))]
    trend = [dict(r) for r in c.execute("SELECT date,ip_count FROM title_demand WHERE ip_id=? ORDER BY date", (ip_id,))]
    streaming = None  # Channel-2 presence (separate DB)
    try:
        sc = sqlite3.connect("file:" + STREAM_DB + "?mode=ro", uri=True, timeout=5); sc.row_factory = sqlite3.Row
        sd = sc.execute("SELECT n_sites,sites FROM stream_title_demand WHERE ip_id=? ORDER BY date DESC LIMIT 1", (ip_id,)).fetchone()
        if sd:
            streaming = {"n_sites": sd["n_sites"], "sites": sd["sites"]}
        sc.close()
    except Exception:
        pass
    out = dict(info) if info else {}
    out["image_url"] = poster_url(ip_id)
    sp, _ = _sparks(c, [ip_id])   # same 14-day zero-filled series the list uses → WoW matches
    return jsonify({"info": out, "date": date, "countries": countries,
                    "trend": trend, "streaming": streaming,
                    "spark": sp.get(ip_id, [])})


@app.route("/api/title_hashes")
def api_title_hashes():
    # lazy, on-demand: the only read of the big hashes_v2.db. Scoped to one ip_id,
    # seeders-ordered, capped + busy-timeout so it can't pin/stall.
    ip_id = request.args.get("ip_id")
    if not ip_id:
        return jsonify([])
    try:
        h = sqlite3.connect("file:/data/db/hashes_v2.db?mode=ro", uri=True, timeout=8)
        h.execute("PRAGMA busy_timeout=6000")
        rows = h.execute("SELECT raw_name,title,category,seeders FROM hashes WHERE ip_id=? ORDER BY seeders DESC LIMIT 12",
                         (ip_id,)).fetchall()
        h.close()
        # raw_name = the ACTUAL torrent filename (varied per release); title = catalog-normalized.
        # Show raw_name so decoy-cluster detection is meaningful (identical RAW names = real decoy).
        return jsonify([{"name": r[0] or r[1], "category": r[2], "seeders": r[3] or 0} for r in rows])
    except Exception:
        return jsonify([])


@app.route("/api/countries")
def api_countries():
    c = db(); date = request.args.get("date") or latest_date(c)
    rows = c.execute("SELECT country,ip_count FROM country_demand WHERE date=? AND country!='Other' ORDER BY ip_count DESC LIMIT 80", (date,)).fetchall()
    return jsonify([{"code": r["country"], "name": cname(r["country"]), "iso": ciso(r["country"]), "ip_count": r["ip_count"]} for r in rows])


@app.route("/api/surging")
def api_surging():
    # "trending now": today (partial) projected to a full day vs each title's 7-day
    # average. ratio = breakouts, absolute = where the volume actually moved, breakout
    # = titles with little/no history but big today. Intel-DB only (light).
    import datetime, collections
    c = db()
    dates = [r["date"] for r in c.execute(
        "SELECT DISTINCT date FROM title_demand ORDER BY date DESC LIMIT 8")]
    if not dates:
        return jsonify({"today": None, "frac": 0, "ratio": [], "absolute": [], "breakout": []})
    today, prior = dates[0], dates[1:]
    ph = ",".join("?" * len(dates))
    series, meta = collections.defaultdict(dict), {}
    for r in c.execute(f"SELECT ip_id,title,category,date,ip_count FROM title_demand WHERE date IN ({ph})", dates):
        ip = r["ip_id"]
        if ip:
            series[ip][r["date"]] = r["ip_count"]; meta[ip] = (r["title"], r["category"])
    now = datetime.datetime.now(datetime.timezone.utc)
    frac = max(0.10, (now.hour * 60 + now.minute) / 1440.0)
    ratio, absolute, breakout = [], [], []
    for ip, s in series.items():
        tv = s.get(today, 0)
        base = [s.get(d, 0) for d in prior]
        avg = (sum(base) / len(base)) if base else 0
        proj = tv / frac
        title, cat = meta[ip]
        rec = {"title": title, "category": cat, "ip_id": ip,
               "avg": round(avg), "today": tv, "proj": round(proj)}
        if avg >= 1000:
            ratio.append({**rec, "surge": round(proj / avg, 1)})
            absolute.append({**rec, "rise": round(proj - avg)})
        elif avg < 150 and tv >= 1500:
            breakout.append(rec)
    ratio.sort(key=lambda r: r["surge"], reverse=True)
    absolute.sort(key=lambda r: r["rise"], reverse=True)
    breakout.sort(key=lambda r: r["today"], reverse=True)
    return jsonify({"today": today, "frac": round(frac, 2),
                    "ratio": ratio[:40], "absolute": absolute[:40], "breakout": breakout[:40]})


@app.route("/api/trends")
def api_trends():
    c = db()
    days = int(request.args.get("days", "0"))
    dts = c.execute("SELECT date,total_ip FROM daily_totals ORDER BY date").fetchall()
    if days > 0:
        dts = dts[-days:]
    dates = [r["date"] for r in dts]
    cutoff = dates[0] if dates else "0000"
    out = {"dates": dates, "total": [r["total_ip"] for r in dts], "cats": {}}
    for cat in ["Movie", "Series", "Anime"]:
        m = {r["date"]: r["s"] for r in c.execute("SELECT date,SUM(ip_count) s FROM title_demand WHERE category=? AND date>=? GROUP BY date", (cat, cutoff))}
        out["cats"][cat] = [m.get(d, 0) for d in dates]
    return jsonify(out)


@app.route("/export.csv")
def export_csv():
    c = db(); date = request.args.get("date") or latest_date(c)
    rows = _title_rows(c, date, request.args.get("category", "All"), request.args.get("country", "All"),
                       request.args.get("q", "").strip(), request.args.get("sort", "ip"), 1000)
    buf = io.StringIO(); w = csv.writer(buf)
    w.writerow(["date", "category", "title", "ip_id", "ip_count"])
    for r in rows:
        w.writerow([date, r["category"], r["title"], r["ip_id"], r["ip_count"]])
    return Response(buf.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=pantheon_{date}.csv"})


@app.route("/healthz")
def healthz():
    db().execute("SELECT 1"); return "ok"


@app.route("/world.geojson")
def world_geo():
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "world_110m.geojson")
    try:
        return Response(open(p, encoding="utf-8").read(), mimetype="application/json")
    except Exception:
        return jsonify({"type": "FeatureCollection", "features": []})


LOGO_SVG = r'<svg viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="pg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#a7ddf2"/><stop offset="1" stop-color="#5fb0db"/></linearGradient></defs><rect width="100" height="100" rx="26" fill="url(#pg)"/><rect x="30" y="24" width="13" height="58" rx="6" fill="#fff"/><circle cx="53" cy="41" r="20" fill="#fff"/><circle cx="53" cy="41" r="8.5" fill="url(#pg)"/></svg>'


@app.route("/logo.svg")
def logo_svg():
    return Response(LOGO_SVG, mimetype="image/svg+xml")


@app.route("/api/streams")
def api_streams():
    try:
        c = sqlite3.connect("file:" + STREAM_DB + "?mode=ro", uri=True, timeout=5)
        c.row_factory = sqlite3.Row
        rows = [{"domain": r["domain"], "kind": r["kind"], "rank": r["rank_signal"],
                 "status": r["status"]} for r in c.execute(
            "SELECT domain,kind,rank_signal,status FROM stream_sites ORDER BY rank_signal DESC LIMIT 300")]
        c.close()
        return jsonify(rows)
    except Exception:
        return jsonify([])


@app.route("/api/stream_titles")
def api_stream_titles():
    # per-title web-streaming demand (n live sites carrying it), joined to the
    # P2P distinct-peer-IP demand for the same catalog ip_id.
    cat = request.args.get("cat", "all")
    try:
        sc = sqlite3.connect("file:" + STREAM_DB + "?mode=ro", uri=True, timeout=5)
        sc.row_factory = sqlite3.Row
        d = sc.execute("SELECT MAX(date) d FROM stream_title_demand").fetchone()["d"]
        q = ("SELECT ip_id,title,category,n_sites,sites FROM stream_title_demand WHERE date=?")
        args = [d]
        if cat and cat.lower() != "all":
            q += " AND category=?"; args.append(cat)
        srows = sc.execute(q, args).fetchall()
        sc.close()
    except Exception:
        return jsonify({"date": None, "rows": []})
    p2p = {}
    try:
        c = sqlite3.connect("file:" + DB + "?mode=ro", uri=True, timeout=10)
        pd = c.execute("SELECT MAX(date) FROM daily_totals").fetchone()[0]
        for ipid, ipc in c.execute(
                "SELECT ip_id,SUM(ip_count) FROM title_demand WHERE date=? GROUP BY ip_id", (pd,)):
            p2p[ipid] = ipc
        c.close()
    except Exception:
        pass
    rows = [{"title": r["title"], "category": r["category"], "ip_id": r["ip_id"],
             "n_sites": r["n_sites"], "sites": r["sites"], "p2p": p2p.get(r["ip_id"])}
            for r in srows]
    # rank by REAL piracy demand (P2P peer-IPs, our validated signal); titles on
    # streaming sites but not in the P2P feed fall to the end, ties by site reach
    rows.sort(key=lambda r: (r["p2p"] if r["p2p"] is not None else -1, r["n_sites"]), reverse=True)
    return jsonify({"date": d, "rows": rows[:400]})


@app.route("/api/acestream")
def api_acestream():
    # Live-IPTV (AceStream) demand — distinct concurrent peer-IPs per live channel on the
    # mainline DHT, geolocated. A SEPARATE channel from the title feed (linear TV, not titles).
    # NOTE: raw peer geo — VPN/datacenter IPs not yet discounted (planned methodology step),
    # so a global channel's top peer country may be a VPN-heavy region, not its origin.
    try:
        c = sqlite3.connect("file:" + ACESTREAM_DB + "?mode=ro", uri=True, timeout=5)
        c.row_factory = sqlite3.Row
        ts = c.execute("SELECT MAX(run_ts) t FROM acestream_demand").fetchone()["t"]
        if not ts:
            c.close(); return jsonify({"run_ts": None, "rows": []})
        want = (request.args.get("owner") or "").strip().lower()   # "" = all owners
        agg = {}
        for r in c.execute(
            "SELECT infohash,name,categories,ch_country,availability,peer_country,peer_count,"
            "bep33_seeders,bep33_leechers,owner FROM acestream_demand WHERE run_ts=?", (ts,)):
            a = agg.setdefault(r["infohash"], {
                "name": r["name"], "categories": r["categories"],
                "ch_country": r["ch_country"], "availability": r["availability"],
                "owner": r["owner"] or "Other",
                "leechers": 0, "seeders": 0, "sample": 0, "geo": {}})
            # BEP-33 is channel-level (denormalized onto every row) → take the max
            a["leechers"] = max(a["leechers"], r["bep33_leechers"] or 0)
            a["seeders"] = max(a["seeders"], r["bep33_seeders"] or 0)
            n = r["peer_count"] or 0
            a["sample"] += n
            cc = r["peer_country"] or "??"
            if cc != "??":
                a["geo"][cc] = a["geo"].get(cc, 0) + n
        # most recent run_ts where each channel had ANY demand (>0 leechers), across the
        # retained history → lets the UI say "quiet now" vs "never measured".
        last_demand = {ih2: ts2 for ih2, ts2 in c.execute(
            "SELECT infohash, MAX(run_ts) FROM acestream_demand "
            "WHERE bep33_leechers > 0 GROUP BY infohash")}
        c.close()
        rows = []
        for ih, a in agg.items():
            if want and a["owner"].lower() != want:   # owner filter (e.g. ?owner=comcast)
                continue
            top = sorted(a["geo"].items(), key=lambda x: -x[1])[:6]
            rows.append({"infohash": ih, "name": a["name"], "categories": a["categories"],
                         "ch_country": a["ch_country"], "availability": a["availability"],
                         "owner": a["owner"], "last_demand": last_demand.get(ih),
                         # headline demand = BEP-33 leechers (swarm-size estimate, matches title feed);
                         # 'sample' = raw get_peers IPs (weak DHT sample, kept for the geo split)
                         "peers": a["leechers"], "seeders": a["seeders"], "sample": a["sample"],
                         "geo": ", ".join("%s %d" % (k, v) for k, v in top)})
        rows.sort(key=lambda r: -r["peers"])
        return jsonify({"run_ts": ts, "rows": rows[:200]})
    except Exception:
        return jsonify({"run_ts": None, "rows": []})


@app.route("/")
def index():
    return Response(SHELL, mimetype="text/html")


SHELL = r"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pantheon Intelligence</title><link rel="icon" type="image/svg+xml" href="/logo.svg"><style>
:root{--bg:#0a0e16;--pan:#141a28;--pan2:#19202f;--bd:#243044;--mut:#8b95ab;--fg:#eef1f7;
--ac:#5fb0db;--ac2:#7fd0f5;--glow:rgba(95,176,219,.32);--up:#2ee0a0;--dn:#ff6b6b}
*{box-sizing:border-box}
body{margin:0;background:radial-gradient(1100px 560px at 72% -12%,#16243a 0%,var(--bg) 55%);color:var(--fg);font:14px/1.55 -apple-system,Segoe UI,Roboto,Inter,sans-serif;-webkit-font-smoothing:antialiased}
.top{background:rgba(11,15,23,.82);backdrop-filter:blur(11px);border-bottom:1px solid var(--bd);padding:11px 24px;display:flex;align-items:center;gap:16px;position:sticky;top:0;z-index:20}
.brand{display:flex;align-items:center;gap:11px}
.brand .logo{width:34px;height:34px;border-radius:9px;box-shadow:0 4px 16px var(--glow)}
.brand .wm{font-size:16px;font-weight:700;letter-spacing:.2px;line-height:1.1}
.brand .wm span{color:var(--ac2)}
.brand .tag{font-size:10px;color:var(--mut);letter-spacing:.07em;text-transform:uppercase}
.nav{display:flex;gap:4px;margin-left:12px}
.nav button{background:none;border:0;color:var(--mut);font-size:13px;font-weight:500;padding:7px 14px;border-radius:9px;cursor:pointer;transition:.15s}
.nav button:hover{color:var(--fg);background:#1a2233}
.nav button.on{color:#fff;background:linear-gradient(135deg,var(--ac),#4f7cff);box-shadow:0 3px 12px var(--glow)}
.pill{font-size:11px;color:var(--ac2);background:rgba(95,176,219,.12);border:1px solid rgba(95,176,219,.25);border-radius:20px;padding:3px 11px;margin-left:auto}
.wrap{max-width:1200px;margin:0 auto;padding:22px 24px}
.cards{display:flex;gap:13px;flex-wrap:wrap;margin-bottom:18px}
.card{background:linear-gradient(160deg,var(--pan2),var(--pan));border:1px solid var(--bd);border-radius:14px;padding:14px 17px;flex:1;min-width:160px;position:relative;overflow:hidden;transition:.15s}
.card:hover{border-color:rgba(95,176,219,.4);transform:translateY(-1px)}
.card::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--ac),transparent)}
.card .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.05em}
.card .v{font-size:25px;font-weight:700;margin:3px 0;letter-spacing:-.5px}
.card .s{font-size:11px;color:var(--mut)}
.up{color:var(--up)}.dn{color:var(--dn)}
.sgtab{cursor:pointer;font:12px system-ui;color:var(--mut);background:#121826;border:1px solid var(--bd);border-radius:8px;padding:3px 11px;margin-right:5px}.sgtab.on{color:#dbe7f5;background:#1b2740;border-color:#3a4f7a}
.panel{background:linear-gradient(160deg,var(--pan2),var(--pan));border:1px solid var(--bd);border-radius:14px;padding:16px 18px;margin-bottom:16px}
.panel h2{font-size:12px;margin:0 0 12px;color:#cdd5e4;text-transform:uppercase;letter-spacing:.05em;font-weight:600}
.cols{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}@media(max-width:900px){.cols{grid-template-columns:1fr}}
.bar{display:inline-block;vertical-align:middle;height:9px;border-radius:5px;background:linear-gradient(90deg,var(--ac),#7aa0ff)}
table{width:100%;border-collapse:collapse}th,td{text-align:left;padding:6px 9px;font-size:13px}
th{color:var(--mut);font-weight:600;font-size:11px;border-bottom:1px solid var(--bd);cursor:pointer;user-select:none;text-transform:uppercase;letter-spacing:.03em}
td.num,th.num{text-align:right;font-variant-numeric:tabular-nums}
tbody tr{border-bottom:1px solid rgba(36,48,68,.5)}
tr.clk{cursor:pointer;transition:.1s}tr.clk:hover{background:rgba(95,176,219,.08)}
.controls{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px}
select,input[type=text]{background:#0d1320;border:1px solid #2a3447;color:var(--fg);border-radius:9px;padding:9px 11px;font-size:13px;outline:none;transition:.15s}
select:focus,input[type=text]:focus{border-color:var(--ac);box-shadow:0 0 0 3px var(--glow)}
input[type=text]{min-width:230px}
.seg{display:flex;border:1px solid #2a3447;border-radius:9px;overflow:hidden}
.seg button{background:#0d1320;border:0;color:var(--mut);padding:9px 13px;font-size:12px;cursor:pointer;transition:.12s}
.seg button:hover{color:var(--fg)}.seg button.on{background:linear-gradient(135deg,var(--ac),#4f7cff);color:#fff}
.btn{background:#1a2233;border:1px solid #2a3447;color:var(--fg);padding:9px 13px;border-radius:9px;font-size:12px;cursor:pointer;text-decoration:none;transition:.12s}
.btn:hover{border-color:var(--ac);color:var(--ac2)}
.muted{color:var(--mut);font-size:12px}
#drawer{position:fixed;top:0;right:0;height:100%;width:430px;max-width:92vw;background:linear-gradient(180deg,#0e1320,#0b0f17);border-left:1px solid var(--bd);transform:translateX(100%);transition:transform .2s cubic-bezier(.4,0,.2,1);z-index:40;overflow:auto;padding:20px;box-shadow:-12px 0 40px #0007}
#drawer.open{transform:none}#drawer .x{float:right;cursor:pointer;color:var(--mut);font-size:22px;border:0;background:none}
#scrim{position:fixed;inset:0;background:#000a;opacity:0;pointer-events:none;transition:opacity .2s;z-index:30;backdrop-filter:blur(2px)}#scrim.open{opacity:1;pointer-events:auto}
#tip{position:fixed;display:none;background:#0e1320;border:1px solid var(--ac);border-radius:8px;padding:7px 10px;font-size:12px;pointer-events:none;z-index:50;box-shadow:0 6px 24px #0009}
svg text{fill:var(--mut);font-size:10px}
.catpill{font-size:10px;color:var(--ac2);background:rgba(95,176,219,.12);border:1px solid rgba(95,176,219,.2);border-radius:9px;padding:1px 7px}
</style></head><body>
<div class="top"><div class="brand"><svg class="logo" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg"><defs><linearGradient id="pg" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#a7ddf2"/><stop offset="1" stop-color="#5fb0db"/></linearGradient></defs><rect width="100" height="100" rx="26" fill="url(#pg)"/><rect x="30" y="24" width="13" height="58" rx="6" fill="#fff"/><circle cx="53" cy="41" r="20" fill="#fff"/><circle cx="53" cy="41" r="8.5" fill="url(#pg)"/></svg><div><div class="wm">Pantheon<span> Intelligence</span></div><div class="tag">Piracy Demand Signals</div></div></div><div class="nav" id="nav"><button data-t="overview" class="on">Overview</button><button data-t="titles">Titles</button><button data-t="countries">Countries</button><button data-t="trends">Trends</button><button data-t="streams">Streaming</button><button data-t="livesport">Live Sport</button></div><span class="pill" id="datepill"></span></div>
<div class="wrap" id="view"></div>
<div id="scrim"></div><div id="drawer"></div><div id="tip"></div>
<script>
const $=s=>document.querySelector(s), api=(p,q={})=>fetch(p+'?'+new URLSearchParams(q)).then(r=>r.json());
let META={latest:null,dates:[],countries:[]}, T={category:'All',country:'All',q:'',sort:'ip',src:'all'}, tab='overview', TR=0, _sg=null, _sgCat='All';
const fmt=n=>(n||0).toLocaleString();
function barCell(v,vmax,c='#4f7cff'){const w=Math.max(2,Math.round(120*v/(vmax||1)));return `<span class="bar" style="width:${w}px;background:linear-gradient(90deg,${c},${c}aa)"></span>`}
function lineSVG(dates,vals,w=860,h=130,col='#4f7cff'){if(!vals.length)return '';const vmax=Math.max(...vals)||1,n=vals.length,pad=28;
 const fx=i=>pad+(w-2*pad)*(i/Math.max(1,n-1)),fy=v=>h-pad-(h-2*pad)*(v/vmax);
 const today=new Date().toISOString().slice(0,10);
 const partial=n>=2&&dates[n-1]===today;  // last point = in-progress UTC day → mark partial, don't read as a decline
 const solid=partial?n-1:n;
 let pts=vals.slice(0,solid).map((v,i)=>fx(i).toFixed(0)+','+fy(v).toFixed(0)).join(' ');
 let dash=partial?`<polyline fill="none" stroke="${col}" stroke-width="2" stroke-dasharray="4 3" opacity="0.5" points="${fx(n-2).toFixed(0)},${fy(vals[n-2]).toFixed(0)} ${fx(n-1).toFixed(0)},${fy(vals[n-1]).toFixed(0)}"/>`:'';
 let dots=vals.map((v,i)=>(partial&&i===n-1)?`<circle cx="${fx(i).toFixed(0)}" cy="${fy(v).toFixed(0)}" r="3" fill="#fff" stroke="${col}" stroke-width="1.6"/>`:`<circle cx="${fx(i).toFixed(0)}" cy="${fy(v).toFixed(0)}" r="2.4" fill="${col}"/>`).join('');
 let plab=partial?`<text x="${fx(n-1).toFixed(0)}" y="${(fy(vals[n-1])-7).toFixed(0)}" text-anchor="middle" font-size="9" fill="#90a4b8">today (partial)</text>`:'';
 let lab=dates.map((d,i)=>i%Math.max(1,Math.ceil(n/9))?'':`<text x="${fx(i).toFixed(0)}" y="${h-6}" text-anchor="middle">${d.slice(5)}</text>`).join('');
 return `<svg width="${w}" height="${h}"><polyline fill="none" stroke="${col}" stroke-width="2" points="${pts}"/>${dash}${dots}<text x="${pad}" y="13">${fmt(vmax)}</text>${lab}${plab}</svg>`}
function sparkSVG(vals,w=70,h=20){if(!vals||vals.length<2)return '';const mx=Math.max(...vals)||1,n=vals.length;
 const part=!!(META&&META.latest===new Date().toISOString().slice(0,10))&&n>=2;  // last point = in-progress day
 const cmp=part?vals[n-2]:vals[n-1];const col=cmp>=vals[0]?'#27c08a':'#ff6b6b';  // judge up/down on last COMPLETE day
 const fx=i=>1+(w-2)*(i/(n-1)),fy=v=>h-2-(h-4)*(v/mx);
 const pts=vals.slice(0,part?n-1:n).map((v,i)=>fx(i).toFixed(0)+','+fy(v).toFixed(0)).join(' ');
 const dash=part?`<polyline fill="none" stroke="${col}" stroke-width="1.2" stroke-dasharray="2 2" opacity="0.5" points="${fx(n-2).toFixed(0)},${fy(vals[n-2]).toFixed(0)} ${fx(n-1).toFixed(0)},${fy(vals[n-1]).toFixed(0)}"/>`:'';
 return `<svg width="${w}" height="${h}" style="vertical-align:middle"><polyline fill="none" stroke="${col}" stroke-width="1.5" points="${pts}"/>${dash}</svg>`}
// Week-over-week: mean of the last 7 days vs the prior 7 (from the 14-day spark /
// full trend). Averaging both windows avoids the partial-today day dragging it down.
function wowPct(arr){if(!arr||arr.length<4)return null;const n=arr.length,h=Math.min(7,Math.floor(n/2));
 const rec=arr.slice(n-h),pri=arr.slice(n-2*h,n-h);
 const a=rec.reduce((x,y)=>x+(y||0),0)/h,b=pri.reduce((x,y)=>x+(y||0),0)/h;
 if(!b)return a>0?{pct:100,up:true}:null;const p=100*(a-b)/b;return{pct:p,up:p>=0}}
function wowCell(arr){const w=wowPct(arr);if(!w)return '<span class="muted">—</span>';
 return `<span class="${w.up?'up':'dn'}">${w.up?'▲':'▼'} ${w.up?'+':''}${Math.round(w.pct)}%</span>`}
function titleTable(rows,opts={}){const vmax=rows.length?rows[0].ip_count:1;
 const rk={};[...rows].sort((a,b)=>(b.ip_count||0)-(a.ip_count||0)).forEach((r,i)=>{rk[r.ip_id]=i+1});
 let h=`<table><thead><tr><th class="num">#</th><th data-s="title">Title</th><th>Cat</th><th class="num" data-s="ip">IPs ▾</th><th class="num">DC%</th><th class="num">WoW</th><th>Trend</th><th></th></tr></thead><tbody>`;
 rows.forEach(r=>{const dc=(r.dc_ip_count!=null&&r.ip_count)?Math.round(100*r.dc_ip_count/r.ip_count)+'%':'—';
  h+=`<tr class="clk" data-ip="${r.ip_id||''}"><td class="num muted">${rk[r.ip_id]||''}</td><td>${esc(r.title)}</td><td><span class="catpill">${r.category||''}</span></td><td class="num">${fmt(r.ip_count)}</td><td class="num muted">${dc}</td><td class="num">${wowCell(r.spark)}</td><td>${sparkSVG(r.spark)}</td><td>${barCell(r.ip_count,vmax)}</td></tr>`});
 return h+`</tbody></table>`}
const esc=s=>(s==null?'':String(s)).replace(/[&<>]/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[m]));
async function openTitle(ip){if(!ip)return;const d=await api('/api/title',{ip_id:ip,date:curDate()});
 const i=d.info||{},cmax=d.countries.length?d.countries[0].ip_count:1;
 const tot=i.ip_count||0, dcp=tot?Math.round(100*(i.dc_ip_count||0)/tot):0;
 const srcLine=(i.ip_count_dht!=null)?`DHT ${fmt(i.ip_count_dht)} · Harvest ${fmt(i.ip_count_harv)} · PEX ${fmt(i.ip_count_pex||0)}`:'—';
 const st=d.streaming, stArr=st&&st.sites?st.sites.split(','):[];
 const stLine=st?`<b>${st.n_sites}</b> streaming site${st.n_sites==1?'':'s'} <span class="muted">${esc(stArr.slice(0,6).join(', '))}${stArr.length>6?' +'+(stArr.length-6):''}</span>`:'<span class="muted">not seen on streaming sites</span>';
 $('#drawer').innerHTML=`<button class="x" onclick="closeDrawer()">×</button>
  <div style="display:flex;gap:12px;align-items:flex-start;margin-bottom:10px">
    ${i.image_url?`<img src="${i.image_url}" alt="" loading="lazy" onerror="this.style.display='none'" style="width:84px;height:auto;border-radius:8px;flex:none;box-shadow:0 4px 14px #0008">`:''}
    <div><div style="font-size:16px;font-weight:600">${esc(i.title||'')}</div>
    <div class="muted" style="margin-top:3px">${i.category||''} ${i.imdb_id?'· '+i.imdb_id:''} ${i.anime_id?'· MAL '+i.anime_id:''} · <span style="font-family:monospace;font-size:11px">${esc(ip)}</span></div></div>
  </div>
  <div class="cards" style="margin-bottom:4px">
    <div class="card"><div class="k">P2P peer-IPs</div><div class="v">${fmt(tot)}</div><div class="s">${d.date}</div></div>
    <div class="card"><div class="k">Datacenter/VPN</div><div class="v">${dcp}%</div><div class="s">residential ${100-dcp}%</div></div>
    ${(()=>{const w=wowPct(d.spark&&d.spark.length?d.spark:d.trend.map(x=>x.ip_count));return w?`<div class="card"><div class="k">Week-over-week</div><div class="v ${w.up?'up':'dn'}">${w.up?'▲ +':'▼ '}${Math.round(Math.abs(w.pct))}%</div><div class="s">7d vs prior 7d</div></div>`:''})()}
  </div>
  <div class="panel"><h2>Channels &amp; sources</h2>
    <div style="margin:2px 0 6px"><b>Channel&nbsp;1 · P2P:</b> ${srcLine}</div>
    <div><b>Channel&nbsp;2 · streaming:</b> ${stLine}</div></div>
  <div class="panel"><h2>Daily trend</h2>${lineSVG(d.trend.map(x=>x.date),d.trend.map(x=>x.ip_count),370,110)}</div>
  <div class="panel"><h2>By country — ${d.date}</h2><table><tbody>${d.countries.map(c=>`<tr><td>${esc(c.name)} <span class="catpill">${c.code}</span></td><td class="num">${fmt(c.ip_count)}</td><td>${barCell(c.ip_count,cmax,'#8a6cff')}</td></tr>`).join('')}</tbody></table></div>
  <div class="panel"><h2>Torrents feeding this</h2><div id="thashes"><button class="seg" style="cursor:pointer;padding:5px 11px" onclick="loadHashes('${esc(ip)}')">▸ load top torrents</button></div></div>`;
 $('#drawer').classList.add('open');$('#scrim').classList.add('open')}
async function loadHashes(ip){const el=$('#thashes');el.innerHTML='<span class="muted">loading…</span>';
 const h=await api('/api/title_hashes',{ip_id:ip});
 if(!h.length){el.innerHTML='<span class="muted">none found</span>';return}
 const names=h.map(x=>(x.name||'').toLowerCase()),uniq=new Set(names);
 const warn=(h.length>=4&&uniq.size<=2)?'<div style="color:#e0a85f;font-size:12px;margin-bottom:5px">⚠️ many identically-named torrents — possible decoy/spam cluster</div>':'';
 el.innerHTML=warn+`<table><tbody>${h.map(x=>`<tr><td>${esc((x.name||'').slice(0,44))}</td><td class="num muted">${fmt(x.seeders)} seed</td></tr>`).join('')}</tbody></table>`}
function closeDrawer(){$('#drawer').classList.remove('open');$('#scrim').classList.remove('open')}
$('#scrim').onclick=closeDrawer;
function curDate(){return $('#datesel')?$('#datesel').value:META.latest}
document.addEventListener('click',e=>{const tr=e.target.closest('tr.clk');if(tr)openTitle(tr.dataset.ip)});
$('#nav').addEventListener('click',e=>{if(e.target.dataset.t){tab=e.target.dataset.t;[...$('#nav').children].forEach(b=>b.classList.toggle('on',b.dataset.t===tab));render()}});

async function render(){
 if(tab==='overview')return renderOverview();
 if(tab==='titles')return renderTitles();
 if(tab==='countries')return renderCountries();
 if(tab==='trends')return renderTrends();
 if(tab==='streams')return renderStreams();
 if(tab==='livesport')return renderLiveSport()}

async function renderOverview(){const d=await api('/api/overview',{date:curDate()});const t=d.totals||{};
 const sg=await api('/api/surging').catch(()=>({ratio:[],absolute:[],breakout:[],frac:0}));_sg=sg;
 let ch='';const dc=t.total_ip?100*t.dc_ip/t.total_ip:0;
 let delta='';if(d.prev&&d.prev.total_ip){const c=100*(t.total_ip-d.prev.total_ip)/d.prev.total_ip;delta=`<span class="${c>=0?'up':'dn'}">${c>=0?'+':''}${c.toFixed(1)}% vs prev</span>`}
 const dhtp=(t.total_ip&&t.total_dht!=null)?100*t.total_dht/t.total_ip:null,harvp=(t.total_ip&&t.total_harv!=null)?100*t.total_harv/t.total_ip:null,pexp=(t.total_ip&&t.total_pex!=null)?100*t.total_pex/t.total_ip:null;
 const lift=(t.total_ip&&t.total_harv!=null)?Math.max(0,100*(t.total_ip-t.total_harv)/t.total_ip):null;
 const smix=(dhtp!=null)?'DHT '+dhtp.toFixed(0)+'% · Harvest '+harvp.toFixed(0)+'%':'—';
 const smixsub=(lift!=null)?'of IPs · DHT adds '+lift.toFixed(1)+'% uniq'+(pexp!=null?' · PEX '+pexp.toFixed(1)+'%':''):'share of distinct IPs (overlap → sums >100%)';
 const cards=[['Distinct peer-IPs',fmt(t.total_ip),delta],['Titles',fmt(t.n_titles),''],['Countries',fmt(t.n_countries),''],['Datacenter/VPN',dc.toFixed(1)+'%','of IPs'],['Source mix · DHT / Harvest',smix,smixsub]];
 ch=cards.map(c=>`<div class="card"><div class="k">${c[0]}</div><div class="v">${c[1]}</div><div class="s">${c[2]}</div></div>`).join('');
 const mv=d.movers.map(m=>`<tr><td>${esc(m.title)}</td><td class="num">${fmt(m.ip)}</td><td class="num ${m.delta>=0?'up':'dn'}">${m.delta>=0?'▲ +':'▼ '}${fmt(Math.abs(m.delta))}</td></tr>`).join('');
 const cat=(k,col)=>`<div class="panel"><h2>${k==='Movie'?'🎬':k==='Series'?'📺':'🌀'} Top ${k}</h2>${titleTable(d.top[k]||[])}</div>`;
 $('#view').innerHTML=`<div class="cards">${ch}</div>
  <div class="panel"><h2>Daily distinct peer-IPs</h2>${lineSVG(d.trend.map(x=>x.date),d.trend.map(x=>x.total_ip))}</div>
  <div class="panel"><h2>🔥 Surging today <span class="muted" style="font-weight:400;font-size:11px">— full-day projection vs 7-day avg · ${Math.round((sg.frac||0)*100)}% of day in</span></h2>
   <div style="margin:2px 0 9px">${[['All','All'],['Movie','🎬 Movies'],['Series','📺 Series'],['Anime','🌀 Anime']].map(x=>`<button class="sgtab${x[0]===_sgCat?' on':''}" data-c="${x[0]}" onclick="sgPick('${x[0]}')">${x[1]}</button>`).join('')}</div>
   <div id="sgbody"></div>
  </div>
  <div class="cols">${cat('Movie')}${cat('Series')}${cat('Anime')}</div>`;sgPick(_sgCat)}
function sgBody(){if(!_sg)return '';const f=r=>_sgCat==='All'||r.category===_sgCat;
 const R=(_sg.ratio||[]).filter(f).slice(0,7),A=(_sg.absolute||[]).filter(f).slice(0,7),B=(_sg.breakout||[]).filter(f).slice(0,8);
 const row=(r,v)=>`<tr class="clk" data-ip="${r.ip_id||''}"><td>${esc(r.title)}</td><td class="num">${fmt(r.proj)}</td><td class="num up">${v}</td></tr>`;
 const empty='<tr><td class="muted">—</td></tr>';
 return `<div class="cols">
  <div><div class="muted" style="margin:0 0 4px">Top surge ×</div><table><thead><tr><th>Title</th><th class="num">proj</th><th class="num">×</th></tr></thead><tbody>${R.map(r=>row(r,r.surge+'×')).join('')||empty}</tbody></table></div>
  <div><div class="muted" style="margin:0 0 4px">Biggest risers (Δ IPs)</div><table><thead><tr><th>Title</th><th class="num">proj</th><th class="num">Δ</th></tr></thead><tbody>${A.map(r=>row(r,'+'+fmt(r.rise))).join('')||empty}</tbody></table></div>
 </div>
 ${B.length?`<div class="muted" style="margin-top:8px">🌱 Breakout (no history): ${B.map(b=>`<span class="catpill">${esc(b.title)} · ${fmt(b.proj)}</span>`).join(' ')}</div>`:''}`}
function sgPick(c){_sgCat=c;document.querySelectorAll('.sgtab').forEach(b=>b.classList.toggle('on',b.dataset.c===c));const el=document.getElementById('sgbody');if(el)el.innerHTML=sgBody()}

async function renderTitles(){
 const copts=['<option value="All">All countries</option>'].concat(META.countries.map(c=>`<option value="${c.code}" ${c.code===T.country?'selected':''}>${esc(c.name)}</option>`)).join('');
 const dopts=META.dates.map(d=>`<option ${d===curDate()?'selected':''}>${d}</option>`).join('');
 $('#view').innerHTML=`<div class="panel"><div class="controls">
   <select id="datesel">${dopts}</select>
   <div class="seg" id="catseg">${['All','Movie','Series','Anime'].map(x=>`<button data-c="${x}" class="${x===T.category?'on':''}">${x}</button>`).join('')}</div>
   <div class="seg" id="srcseg" title="Filter by collection source">${[['all','All src'],['dht','DHT'],['harv','Harvest'],['pex','PEX']].map(x=>`<button data-s2="${x[0]}" class="${x[0]===T.src?'on':''}">${x[1]}</button>`).join('')}</div>
   <select id="csel">${copts}</select>
   <input type="text" id="q" placeholder="Search title…" value="${esc(T.q)}">
   <a class="btn" id="dl">⬇ CSV</a><span class="muted" id="cnt"></span>
  </div><div id="tbl">loading…</div></div>`;
 $('#datesel').onchange=()=>{loadTitles();$('#datepill').textContent='latest '+META.latest};
 $('#csel').onchange=e=>{T.country=e.target.value;loadTitles()};
 $('#catseg').onclick=e=>{if(e.target.dataset.c){T.category=e.target.dataset.c;[...$('#catseg').children].forEach(b=>b.classList.toggle('on',b.dataset.c===T.category));loadTitles()}};
 $('#srcseg').onclick=e=>{if(e.target.dataset.s2){T.src=e.target.dataset.s2;[...$('#srcseg').children].forEach(b=>b.classList.toggle('on',b.dataset.s2===T.src));loadTitles()}};
 let to;$('#q').oninput=e=>{clearTimeout(to);T.q=e.target.value;to=setTimeout(loadTitles,250)};
 loadTitles()}
async function loadTitles(){const p={date:curDate(),category:T.category,country:T.country,q:T.q,sort:T.sort,src:T.src,limit:300};
 $('#dl').href='/export.csv?'+new URLSearchParams(p);
 const rows=await api('/api/titles',p);$('#cnt').textContent=rows.length+' rows';
 const html=titleTable(rows);$('#tbl').innerHTML=html;
 $('#tbl').querySelectorAll('th[data-s]').forEach(th=>th.onclick=()=>{const s=th.dataset.s;T.sort=(s==='ip')?(T.sort==='ip'?'ip_asc':'ip'):(T.sort==='title'?'title_desc':'title');loadTitles()})}

const CENT={US:[39,-98],CA:[60,-110],MX:[23,-102],BR:[-10,-52],AR:[-35,-65],CL:[-33,-71],CO:[4,-73],PE:[-10,-76],VE:[7,-66],EC:[-1,-78],
GB:[54,-2],IE:[53,-8],FR:[46,2],DE:[51,10],ES:[40,-4],PT:[39,-8],IT:[42,12],NL:[52,5],BE:[50,4],CH:[47,8],AT:[47,14],
PL:[52,19],RO:[46,25],RS:[44,21],GR:[39,22],SE:[62,15],NO:[62,10],FI:[64,26],DK:[56,9],CZ:[49,15],HU:[47,19],BG:[43,25],HR:[45,16],
UA:[49,32],RU:[61,90],TR:[39,35],IN:[22,79],PK:[30,70],BD:[24,90],LK:[7,81],NP:[28,84],CN:[35,103],JP:[36,138],KR:[36,128],TW:[24,121],
PH:[13,122],ID:[-2,118],TH:[15,101],VN:[16,108],MY:[3,102],SG:[1,104],HK:[22,114],AU:[-25,134],NZ:[-42,173],
ZA:[-29,24],KE:[0,38],TZ:[-6,35],NG:[9,8],EG:[27,30],MA:[32,-6],DZ:[28,3],GH:[8,-1],SA:[24,45],AE:[24,54],IL:[31,35],IR:[32,53],IQ:[33,44]};
function worldMap(rows,w=900,h=430){const vmax=rows.length?rows[0].ip_count:1;
 const px=(lat,lon)=>[(lon+180)/360*w,(90-lat)/180*h];
 let g='';for(let lo=-150;lo<=150;lo+=30){const x=px(0,lo)[0];g+=`<line x1="${x.toFixed(0)}" y1="0" x2="${x.toFixed(0)}" y2="${h}" stroke="#1c2230"/>`}
 for(let la=-60;la<=80;la+=30){const y=px(la,0)[1];g+=`<line x1="0" y1="${y.toFixed(0)}" x2="${w}" y2="${y.toFixed(0)}" stroke="#1c2230"/>`}
 const b=rows.map(r=>{const c=CENT[r.code];if(!c)return '';const[x,y]=px(c[0],c[1]);const rad=(4+22*Math.sqrt(r.ip_count/vmax)).toFixed(1);
  return `<circle cx="${x.toFixed(0)}" cy="${y.toFixed(0)}" r="${rad}" fill="#4f7cff" fill-opacity="0.45" stroke="#7aa0ff" stroke-width="1"><title>${esc(r.name)}: ${fmt(r.ip_count)} IPs</title></circle>`}).join('');
 return `<svg viewBox="0 0 ${w} ${h}" width="100%" style="background:#0b0f1a;border-radius:8px">${g}${b}</svg>`}
const L2ISO={'United States':'US','United Kingdom':'GB','Canada':'CA','Australia':'AU','Brazil':'BR','France':'FR','Germany':'DE','Ireland':'IE','Italy':'IT','Japan':'JP','Mexico':'MX','South Korea':'KR','Spain':'ES','Thailand':'TH'};
let GEO=null;async function ensureGeo(){if(!GEO){try{GEO=await fetch('/world.geojson').then(r=>r.json())}catch(e){GEO={features:[]}}}return GEO}
function geoPath(g,W,H){const px=(lon,lat)=>((lon+180)/360*W).toFixed(1)+','+((90-lat)/180*H).toFixed(1);
 const ring=r=>'M'+r.map(c=>px(c[0],c[1])).join('L')+'Z';
 const polys=g.type==='Polygon'?[g.coordinates]:g.coordinates;return polys.map(p=>p.map(ring).join('')).join('')}
async function renderCountries(){const rows=await api('/api/countries',{date:curDate()});const vmax=rows.length?rows[0].ip_count:1;
 const dem={};rows.forEach(r=>{if(r.iso)dem[r.iso]=r.ip_count});await ensureGeo();const W=900,H=460;
 const heat=t=>{const s=[[253,224,107],[249,115,22],[185,28,28]];const x=t*(s.length-1),i=Math.min(s.length-2,Math.floor(x)),f=x-i,a=s[i],b=s[i+1];return `rgb(${Math.round(a[0]+(b[0]-a[0])*f)},${Math.round(a[1]+(b[1]-a[1])*f)},${Math.round(a[2]+(b[2]-a[2])*f)})`};
 const col=v=>!v?'#2b3340':heat(Math.log(v+1)/Math.log((vmax||1)+1));
 const paths=(GEO.features||[]).map(f=>{let iso=f.properties.ISO_A2;if(iso==='-99')iso=f.properties.ISO_A2_EH;const v=dem[iso]||0;
  return `<path d="${geoPath(f.geometry,W,H)}" fill="${col(v)}" stroke="#0d1119" stroke-width="0.5" data-n="${esc(f.properties.NAME)}" data-v="${v}"></path>`}).join('');
 const leg=`<span style="display:inline-flex;align-items:center;gap:6px;vertical-align:middle"><span style="width:14px;height:10px;background:#2b3340;border-radius:2px;display:inline-block"></span>no data&nbsp;&nbsp;<span style="width:64px;height:10px;border-radius:2px;display:inline-block;background:linear-gradient(90deg,rgb(253,224,107),rgb(249,115,22),rgb(185,28,28))"></span>low → high</span>`;
 const map=`<svg viewBox="0 28 ${W} 372" width="100%" style="background:#0b0f17;border-radius:8px">${paths}</svg>`;
 $('#view').innerHTML=`<div class="panel"><h2>🗺️ World demand map — ${curDate()}</h2>${map}<div class="muted" style="margin-top:6px">${leg} &nbsp;·&nbsp; hover a country for its value</div></div>
  <div class="panel"><h2>Demand by country — ${curDate()}</h2><table><thead><tr><th>#</th><th>Country</th><th class="num">IPs</th><th></th></tr></thead><tbody>${rows.map((r,i)=>`<tr><td>${i+1}</td><td>${esc(r.name)} <span class="catpill">${r.iso||r.code}</span></td><td class="num">${fmt(r.ip_count)}</td><td>${barCell(r.ip_count,vmax,'#8a6cff')}</td></tr>`).join('')}</tbody></table></div>`;
 const svg=$('#view svg'),tip=$('#tip');
 if(svg){svg.addEventListener('mousemove',e=>{const p=e.target.closest('path');if(p&&p.hasAttribute('data-n')){const v=p.getAttribute('data-v');tip.innerHTML='<b>'+p.getAttribute('data-n')+'</b><br>'+(v&&v!=='0'?fmt(+v)+' IPs':'no data');tip.style.display='block';tip.style.left=(e.clientX+14)+'px';tip.style.top=(e.clientY+14)+'px'}else tip.style.display='none'});
  svg.addEventListener('mouseleave',()=>tip.style.display='none')}}

async function renderTrends(){const d=await api('/api/trends',{days:TR});
 const col={Movie:'#4f7cff',Series:'#27c08a',Anime:'#c08a27'};
 const seg=`<div class="controls"><span class="muted">Range</span><div class="seg" id="rseg">${[['7','7d'],['14','14d'],['30','30d'],['0','All']].map(x=>`<button data-d="${x[0]}" class="${(''+TR)===x[0]?'on':''}">${x[1]}</button>`).join('')}</div></div>`;
 let h=seg+`<div class="panel"><h2>Total distinct peer-IPs (${d.dates.length} days)</h2>${lineSVG(d.dates,d.total)}</div>`;
 for(const k of ['Movie','Series','Anime'])h+=`<div class="panel"><h2>${k} demand</h2>${lineSVG(d.dates,d.cats[k],860,120,col[k])}</div>`;
 $('#view').innerHTML=h;
 $('#rseg').onclick=e=>{if(e.target.dataset.d!==undefined){TR=+e.target.dataset.d;renderTrends()}}}

let streamCat='All';
async function renderStreams(){const d=await api('/api/stream_titles',{cat:streamCat});const rows=await api('/api/streams');
 const live=rows.filter(r=>r.status==='live').length;const tr=d.rows||[];
 const tbody=tr.map((r,i)=>`<tr><td class="muted">${i+1}</td><td class="name">${esc(r.title)}</td><td><span class="catpill">${esc(r.category)}</span></td><td class="num" title="${fmt(r.p2p||0)} distinct BitTorrent peer-IPs sharing this title (Channel-1 / torrent feed)"><b>${r.p2p!=null?fmt(r.p2p):'<span class="muted">\u2014</span>'}</b></td><td class="num hassites" data-s="On ${r.n_sites} site${r.n_sites==1?'':'s'}:&#10;${(r.sites||'').split(',').map(esc).join('&#10;')}" style="cursor:help;border-bottom:1px dotted #4a5870">${r.n_sites}</td></tr>`).join('');
 $('#view').innerHTML=`<div class="panel"><h2>\ud83d\udcfa Streaming demand \u2014 titles by live-site presence</h2>
  <div class="muted" style="margin-bottom:10px">Titles seen on live streaming sites \u00b7 ${d.date||'\u2014'}, <b>ranked by torrent demand</b> (distinct peer-IPs from the P2P feed \u2014 our validated piracy-demand signal). <b>On # sites</b> = how many live streaming sites also carry it (hover for the list). \u201c\u2014\u201d = on a streaming site but not in the P2P feed.</div>
  <div class="seg" id="stcatseg" style="margin:2px 0 12px">${[['All','All'],['Movie','Movies'],['Series','Series'],['Anime','Anime']].map(x=>`<button data-sc="${x[0]}" class="${x[0]===streamCat?'on':''}">${x[1]}</button>`).join('')}</div>
  <table><thead><tr><th>#</th><th>Title</th><th>Cat</th><th class="num">Torrent demand</th><th class="num">On # sites</th></tr></thead><tbody>${tbody}</tbody></table></div>
  <div class="panel"><h2>\ud83c\udf10 Streaming-piracy site registry</h2>
  <div class="muted" style="margin-bottom:10px">${rows.length} sites \u00b7 <span class="up">${live} live</span> \u00b7 ranked by Google copyright-removal volume.</div>
  <table><thead><tr><th>#</th><th>Domain</th><th>Cat</th><th class="num">Takedown rank</th><th>Status</th></tr></thead><tbody>${
  rows.map((r,i)=>`<tr><td class="muted">${i+1}</td><td>${esc(r.domain)}</td><td><span class="catpill">${esc(r.kind)}</span></td><td class="num">${fmt(r.rank)}</td><td>${r.status==='live'?'<span class="up">\u25cf live</span>':'<span class="dn">\u25cb dead</span>'}</td></tr>`).join('')
  }</tbody></table></div>`;
 const sg=$('#stcatseg');if(sg)sg.onclick=e=>{if(e.target.dataset.sc){streamCat=e.target.dataset.sc;renderStreams()}}}
function agoStr(iso){if(!iso)return '<span class="muted">never</span>';const t=Date.parse(iso.replace(' ','T'));if(isNaN(t))return '<span class="muted">—</span>';const s=(Date.now()-t)/1000;if(s<90)return 'just now';if(s<5400)return Math.round(s/60)+'m ago';if(s<172800)return Math.round(s/3600)+'h ago';return Math.round(s/86400)+'d ago';}
let liveOwner='';
async function renderLiveSport(){const d=await api('/api/acestream',liveOwner?{owner:liveOwner}:{}).catch(()=>({run_ts:null,rows:[]}));const tr=d.rows||[];
 const ownerCell=o=>{if(o==='Comcast')return '<span class="catpill" style="background:#1b2740;border-color:#3a4f7a;color:#dbe7f5">Comcast</span>';if(o==='Versant')return '<span class="catpill" style="background:#2a1f3a;border-color:#5a3f7a;color:#e7dbf5">Versant</span>';return `<span class="muted">${esc(o||'')}</span>`;};
 const total=tr.reduce((s,r)=>s+(r.peers||0),0);
 const tbody=tr.map((r,i)=>`<tr><td class="muted">${i+1}</td><td class="name">${esc(r.name)}</td><td><span class="catpill">${esc((r.categories||'live').split(',')[0]||'live')}</span></td><td>${r.ch_country?`<span class="catpill">${esc((r.ch_country||'').toUpperCase())}</span>`:''}</td><td>${ownerCell(r.owner)}</td><td class="num"><b>${fmt(r.peers)}</b></td><td class="muted" style="font-size:12px">${r.peers>0?'<span class="up">live now</span>':agoStr(r.last_demand)}</td><td class="muted" style="font-size:12px">${esc(r.geo||'')}</td></tr>`).join('');
 const seg=`<div class="seg" id="loseg" style="margin:2px 0 12px">${[['','All owners'],['comcast','Comcast / Sky'],['versant','Versant']].map(x=>`<button data-o="${x[0]}" class="${x[0]===liveOwner?'on':''}">${x[1]}</button>`).join('')}</div>`;
 $('#view').innerHTML=`<div class="panel"><h2>📡 Live Sport / IPTV — AceStream P2P demand</h2>
  <div class="muted" style="margin-bottom:10px">Live-channel demand from the mainline BitTorrent DHT — the <i>live</i> signal our VOD feed can't see · ${d.run_ts||'—'} · <b>pilot</b>. <b>Demand</b> = BEP-33 leechers; <b>Owner</b> = parent media group (filter for a portfolio). <b>Top geo</b> = sampled peer countries (raw — VPN/datacenter not yet discounted).</div>
  ${seg}
  <div class="muted" style="margin-bottom:8px">${tr.length} channels · <b>${fmt(total)}</b> total demand${liveOwner?` · ${liveOwner.charAt(0).toUpperCase()+liveOwner.slice(1)} portfolio`:''}</div>
  <table><thead><tr><th>#</th><th>Channel</th><th>Cat</th><th>Origin</th><th>Owner</th><th class="num">Demand ▾</th><th>Last demand</th><th>Top peer geo</th></tr></thead><tbody>${tbody||'<tr><td colspan=8 class="muted">no data yet — collector run pending</td></tr>'}</tbody></table></div>`;
 const sg=$('#loseg');if(sg)sg.onclick=e=>{if(e.target.dataset.o!==undefined){liveOwner=e.target.dataset.o;renderLiveSport()}}}
const _tip=document.createElement('div');_tip.style.cssText='position:fixed;z-index:99;background:#0f1420;border:1px solid #2a3550;color:#cfe3f5;padding:6px 9px;border-radius:6px;font:12px/1.55 system-ui;max-width:330px;box-shadow:0 6px 20px rgba(0,0,0,.55);pointer-events:none;display:none;white-space:pre-line';document.body.appendChild(_tip);
document.addEventListener('mouseover',e=>{const t=e.target.closest&&e.target.closest('.hassites');if(t){_tip.textContent=t.dataset.s;_tip.style.display='block'}});
document.addEventListener('mousemove',e=>{if(_tip.style.display==='block'){_tip.style.left=Math.min(e.clientX+14,window.innerWidth-342)+'px';_tip.style.top=(e.clientY+16)+'px'}});
document.addEventListener('mouseout',e=>{if(e.target.closest&&e.target.closest('.hassites'))_tip.style.display='none'});
(async()=>{META=await api('/api/meta');$('#datepill').textContent='latest '+(META.latest||'—');render()})();
</script></body></html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, threaded=True)
