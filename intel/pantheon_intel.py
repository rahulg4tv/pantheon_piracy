#!/usr/bin/env python3
"""
pantheon_intel.py — Pantheon Piracy Intelligence: build a queryable SQLite DB from
the demand feed AND render a category-split HTML dashboard from it.

  * Builds   pantheon_intel.db  — tidy tables you can open in any SQLite GUI / Datasette:
        title_demand(date,category,title,ip_id,imdb_id,anime_id,ip_count,dc_ip_count)
        country_demand(date,country,ip_count)
        daily_totals(date,total_ip,n_titles,n_countries,dc_ip)
  * Renders  pantheon_intel.html — summary, daily trend, Top Countries, and SEPARATE
        Top Movies / Top Series / Top Anime tables. Self-contained (inline SVG, no CDN).

  python3 pantheon_intel.py             # rebuild DB from /data/daily + render HTML
  python3 pantheon_intel.py --live      # also regenerate TODAY's feed first (intra-day live)
"""
import os, csv, glob, sqlite3, subprocess, datetime, html, sys

DAILY_DIR = os.environ.get("DAILY_DIR", "/data/daily")
DB        = os.environ.get("INTEL_DB", "/data/db/pantheon_intel.db")
OUT       = os.environ.get("OUT", "/tmp/pantheon_intel.html")
NDAYS     = int(os.environ.get("NDAYS", "30"))
TOPN      = int(os.environ.get("TOPN", "12"))
EXPORT    = "/home/ec2-user/hash_trackerv2/export_nbcu.py"
PY        = "/home/ec2-user/venv/bin/python3"
CATMAP    = {"Video: Movie": "Movie", "Video: TV": "Series", "Video: Anime": "Anime"}

DDL = [
 ("CREATE TABLE IF NOT EXISTS title_demand(date TEXT,category TEXT,title TEXT,ip_id TEXT,"
  "imdb_id TEXT,anime_id TEXT,ip_count INT,dc_ip_count INT,PRIMARY KEY(date,ip_id,title))"),
 "CREATE TABLE IF NOT EXISTS country_demand(date TEXT,country TEXT,ip_count INT,PRIMARY KEY(date,country))",
 "CREATE TABLE IF NOT EXISTS daily_totals(date TEXT PRIMARY KEY,total_ip INT,n_titles INT,n_countries INT,dc_ip INT)",
 ("CREATE TABLE IF NOT EXISTS title_country(date TEXT,country TEXT,category TEXT,title TEXT,ip_id TEXT,"
  "ip_count INT,PRIMARY KEY(date,country,ip_id,title))"),
 "CREATE INDEX IF NOT EXISTS idx_td_cat ON title_demand(category,ip_count)",
 "CREATE INDEX IF NOT EXISTS idx_td_date ON title_demand(date)",
 "CREATE INDEX IF NOT EXISTS idx_tc ON title_country(date,country,category,ip_count)",
 "CREATE INDEX IF NOT EXISTS idx_tc_title ON title_country(date,title)",
 "CREATE INDEX IF NOT EXISTS idx_tc_ipid ON title_country(date,ip_id)",
 "CREATE INDEX IF NOT EXISTS idx_td_ipid ON title_demand(ip_id)",
]


def build_db(live=False):
    today = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    files = sorted(glob.glob(DAILY_DIR + "/*.csv"))
    live_today = "/tmp/%s_live.csv" % today
    if live:
        subprocess.run([PY, EXPORT, "--date", today, "--out", live_today],
                       env={**os.environ, "TMPDIR": "/tmp"}, check=False)
        if os.path.exists(live_today):
            files.append(live_today)
    con = sqlite3.connect(DB)
    for d in DDL:
        con.execute(d)
    for _t, _cols in (("title_demand", ("ip_count_dht", "ip_count_harv", "ip_count_pex")),
                      ("title_country", ("ip_count_dht", "ip_count_harv", "ip_count_pex")),
                      ("daily_totals", ("total_dht", "total_harv", "total_pex"))):
        for _col in _cols:
            try:
                con.execute("ALTER TABLE %s ADD COLUMN %s INT DEFAULT 0" % (_t, _col))
            except Exception:
                pass
    for f in files:
        date = os.path.basename(f).replace("_live.csv", "").replace(".csv", "")
        if len(date) < 8 or not date[:4].isdigit():
            continue
        titles, tdc, meta = {}, {}, {}
        titles_dht, titles_harv, titles_pex = {}, {}, {}
        countries = {}
        tc = {}            # (country, ip_id, title) -> ip_count  (for per-country filtering)
        tc_dht, tc_harv, tc_pex = {}, {}, {}
        tot = dc = tot_dht = tot_harv = tot_pex = 0
        cset = set()
        with open(f, newline="", encoding="utf-8", errors="replace") as fh:
            r = csv.reader(fh); next(r, None)
            for row in r:
                if len(row) < 10:
                    continue
                t, ipid, imdb, anime, dt, cat, country, ipc, dcc, unm = row[:10]
                try:
                    ipc = int(ipc); dcc = int(dcc)
                except ValueError:
                    continue
                try:
                    dht = int(row[10]); harv = int(row[11])
                except (IndexError, ValueError):
                    dht = harv = 0
                try:
                    pex = int(row[12])
                except (IndexError, ValueError):
                    pex = 0
                k = (ipid, t)
                titles[k] = titles.get(k, 0) + ipc
                titles_dht[k] = titles_dht.get(k, 0) + dht
                titles_harv[k] = titles_harv.get(k, 0) + harv
                titles_pex[k] = titles_pex.get(k, 0) + pex
                tdc[k] = tdc.get(k, 0) + dcc
                meta[k] = (CATMAP.get(cat, cat), imdb, anime)
                countries[country] = countries.get(country, 0) + ipc
                tc[(country, ipid, t)] = tc.get((country, ipid, t), 0) + ipc
                tc_dht[(country, ipid, t)] = tc_dht.get((country, ipid, t), 0) + dht
                tc_harv[(country, ipid, t)] = tc_harv.get((country, ipid, t), 0) + harv
                tc_pex[(country, ipid, t)] = tc_pex.get((country, ipid, t), 0) + pex
                tot += ipc; dc += dcc; tot_dht += dht; tot_harv += harv; tot_pex += pex; cset.add(country)
        con.execute("DELETE FROM title_demand WHERE date=?", (date,))
        con.execute("DELETE FROM country_demand WHERE date=?", (date,))
        con.execute("DELETE FROM title_country WHERE date=?", (date,))
        con.executemany(
            "INSERT OR REPLACE INTO title_country VALUES(?,?,?,?,?,?,?,?,?)",
            [(date, c, meta[(ipid, t)][0], t, ipid, v,
              tc_dht.get((c, ipid, t), 0), tc_harv.get((c, ipid, t), 0),
              tc_pex.get((c, ipid, t), 0))
             for (c, ipid, t), v in tc.items()])
        con.executemany(
            "INSERT OR REPLACE INTO title_demand VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            [(date, meta[k][0], k[1], k[0], meta[k][1], meta[k][2], v, tdc[k],
              titles_dht.get(k, 0), titles_harv.get(k, 0), titles_pex.get(k, 0))
             for k, v in titles.items()])
        con.executemany("INSERT OR REPLACE INTO country_demand VALUES(?,?,?)",
                        [(date, c, v) for c, v in countries.items()])
        con.execute("INSERT OR REPLACE INTO daily_totals VALUES(?,?,?,?,?,?,?,?)",
                    (date, tot, len(titles), len(cset), dc, tot_dht, tot_harv, tot_pex))
    con.commit()
    con.close()


# ---- HTML rendering (reads from the SQLite DB) ----
def fmt(n): return f"{n:,}"

def bar(v, vmax, w=150, color="#4f7cff"):
    p = 0 if vmax == 0 else max(2, int(w * v / vmax))
    return (f'<svg width="{w}" height="12" class="bar"><rect width="{w}" height="12" rx="3" '
            f'fill="#1e2433"/><rect width="{p}" height="12" rx="3" fill="{color}"/></svg>')

def ttable(rows, color):
    if not rows:
        return '<div class="empty">no data</div>'
    vmax = rows[0][1]
    out = ['<table><thead><tr><th>Title</th><th class="num">IPs</th><th></th></tr></thead><tbody>']
    for title, val in rows:
        out.append(f'<tr><td class="name">{html.escape(title[:40])}</td>'
                   f'<td class="num">{fmt(val)}</td><td>{bar(val, vmax, color=color)}</td></tr>')
    out.append("</tbody></table>")
    return "".join(out)

def line_chart(dates, values, w=720, h=130, color="#4f7cff"):
    if not values:
        return ""
    vmax = max(values) or 1; n = len(values); pad = 26
    fx = lambda i: pad + (w - 2*pad) * (i/max(1, n-1))
    fy = lambda v: h - pad - (h - 2*pad) * (v/vmax)
    pts = " ".join(f"{fx(i):.0f},{fy(v):.0f}" for i, v in enumerate(values))
    dots = "".join(f'<circle cx="{fx(i):.0f}" cy="{fy(v):.0f}" r="2.5" fill="{color}"/>' for i, v in enumerate(values))
    labs = "".join(f'<text x="{fx(i):.0f}" y="{h-6}" class="ax" text-anchor="middle">{dates[i][5:]}</text>'
                   for i in range(0, n, max(1, n//8)))
    return (f'<svg width="{w}" height="{h}" class="line"><polyline fill="none" stroke="{color}" '
            f'stroke-width="2" points="{pts}"/>{dots}<text x="{pad}" y="13" class="ax">{fmt(vmax)}</text>{labs}</svg>')

def render():
    con = sqlite3.connect("file:" + DB + "?mode=ro", uri=True)
    dts = con.execute("SELECT date,total_ip,n_titles,n_countries,dc_ip FROM daily_totals ORDER BY date").fetchall()
    dts = dts[-NDAYS:]
    if not dts:
        print("no data in DB"); return
    latest = dts[-1][0]
    L = {r[0]: r for r in dts}[latest]
    prev = dts[-2] if len(dts) > 1 else None
    delta = ""
    if prev and prev[1]:
        ch = 100*(L[1]-prev[1])/prev[1]
        delta = f'<span class="{"up" if ch>=0 else "dn"}">{"+" if ch>=0 else ""}{ch:.1f}% vs prev</span>'
    dc_pct = 0 if L[1] == 0 else 100*L[4]/L[1]
    cards = [("Distinct peer-IPs", fmt(L[1]), delta), ("Titles", fmt(L[2]), ""),
             ("Countries", fmt(L[3]), ""), ("Datacenter/VPN", f"{dc_pct:.1f}%", "of IPs")]
    card_html = "".join(f'<div class="card"><div class="k">{k}</div><div class="v">{v}</div><div class="s">{s}</div></div>' for k, v, s in cards)

    def topcat(cat):
        return con.execute("SELECT title,ip_count FROM title_demand WHERE date=? AND category=? "
                           "ORDER BY ip_count DESC LIMIT ?", (latest, cat, TOPN)).fetchall()
    movies, series, anime = topcat("Movie"), topcat("Series"), topcat("Anime")
    countries = con.execute("SELECT country,ip_count FROM country_demand WHERE date=? AND country!='Other' "
                            "ORDER BY ip_count DESC LIMIT ?", (latest, TOPN)).fetchall()
    dates = [r[0] for r in dts]; totals = [r[1] for r in dts]
    gen = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M UTC")
    HTML = f"""<!doctype html><html><head><meta charset="utf-8"><meta http-equiv="refresh" content="900">
<title>Pantheon Piracy Intelligence</title><style>
 body{{margin:0;background:#0d1017;color:#e6e9ef;font:14px/1.5 -apple-system,Segoe UI,Roboto,sans-serif}}
 .wrap{{max-width:1120px;margin:0 auto;padding:24px}} h1{{font-size:20px;margin:0 0 2px}}
 .sub{{color:#8a93a6;font-size:12px;margin-bottom:18px}}
 .cards{{display:flex;gap:12px;flex-wrap:wrap;margin-bottom:20px}}
 .card{{background:#151a25;border:1px solid #232a3a;border-radius:10px;padding:12px 16px;flex:1;min-width:150px}}
 .card .k{{color:#8a93a6;font-size:11px;text-transform:uppercase;letter-spacing:.04em}}
 .card .v{{font-size:23px;font-weight:600;margin:2px 0}} .card .s{{font-size:11px;color:#8a93a6}}
 .up{{color:#27c08a}} .dn{{color:#ff6b6b}}
 .panel{{background:#151a25;border:1px solid #232a3a;border-radius:10px;padding:14px 16px;margin-bottom:18px}}
 .panel h2{{font-size:13px;margin:0 0 10px;color:#c5ccda;text-transform:uppercase;letter-spacing:.04em}}
 .cols{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px}}
 .col2{{display:grid;grid-template-columns:1fr 1fr;gap:16px}}
 table{{width:100%;border-collapse:collapse}} th,td{{text-align:left;padding:4px 8px;font-size:13px}}
 th{{color:#8a93a6;font-weight:500;font-size:11px;border-bottom:1px solid #232a3a}}
 td.num,th.num{{text-align:right;font-variant-numeric:tabular-nums}}
 td.name{{max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
 tr:hover{{background:#1a2030}} .bar{{vertical-align:middle}} .line .ax{{fill:#8a93a6;font-size:10px}}
 .empty{{color:#8a93a6;font-size:12px;padding:8px}}
 @media(max-width:900px){{.cols,.col2{{grid-template-columns:1fr}}}}
</style></head><body><div class="wrap">
 <h1>Pantheon Piracy Intelligence</h1>
 <div class="sub">Latest day {latest} · {gen} · auto-refresh 15m · source: SQLite pantheon_intel.db</div>
 <div class="cards">{card_html}</div>
 <div class="panel"><h2>Daily distinct peer-IPs ({len(dts)} days)</h2>{line_chart(dates, totals)}</div>
 <div class="cols">
   <div class="panel"><h2>🎬 Top Movies — {latest}</h2>{ttable(movies, "#4f7cff")}</div>
   <div class="panel"><h2>📺 Top Series — {latest}</h2>{ttable(series, "#27c08a")}</div>
   <div class="panel"><h2>🌀 Top Anime — {latest}</h2>{ttable(anime, "#c08a27")}</div>
 </div>
 <div class="panel"><h2>🌍 Top Countries — {latest}</h2>{ttable(countries, "#8a6cff")}</div>
 <div class="sub">DB: {DB} — connect any SQLite tool / Datasette for live queries.</div>
</div></body></html>"""
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(HTML)
    con.close()
    print("DB %s rebuilt; HTML %s written (latest %s)" % (DB, OUT, latest))


if __name__ == "__main__":
    build_db(live="--live" in sys.argv)
    render()
