# `pantheon_web.py` — Navigation Map + Endpoint Reference (with sample JSON)

Goal of this doc: make the Flask dashboard easy to understand **without reading the
code** — for every route, show *what it does*, *what params it takes*, and **what it
RETURNS, with a concrete sample value**. Pairs with the source at `pantheon_web.py`
(703 lines as of 2026-06-15). Style mirrors `ACESTREAM_PILOT_GUIDE.md`.

> Mental model: a single-page Flask app, served by gunicorn (bind `0.0.0.0:8090`).
> The `/api/*` endpoints return JSON; `/` returns one big inline vanilla-JS + SVG
> page (`SHELL` — no CDN, no chart lib). All DB reads are READ-ONLY (`mode=ro`).
> Three DBs: the derived snapshot `pantheon_intel.db` (rebuilt hourly — the main feed),
> `stream_demand.db` (web-streaming sites), `acestream_pilot.db` (live IPTV pilot),
> plus the live `hashes_v2.db` (lazy, only on torrent drill-down).

---

## The app in one picture
```
                          pantheon_intel.db  ← main P2P demand snapshot (hourly)
                          stream_demand.db   ← web-streaming sites + per-title presence
   browser                acestream_pilot.db ← live IPTV (AceStream) DHT demand pilot
   GET /  ───────────►    hashes_v2.db (live)← raw torrents, read ONLY on drill-down
        │  returns SHELL          ▲
        ▼  (HTML+JS+SVG)          │  read-only (mode=ro)
   JS calls /api/* ──────────► Flask routes ─┘
        │  JSON
        ▼
   render*() tab fns paint tabs: Overview · Titles · Countries · Trends · Streaming · Live Sport
```
"ip_count" everywhere = **distinct peer-IPs** (the demand unit). `dc_ip_count` =
the datacenter/VPN subset of those IPs.

---

## Endpoint reference (request → sample JSON response)

All routes are GET. Dates are `YYYY-MM-DD`; when a `date` param is omitted the app
uses `latest_date()` (the newest `daily_totals.date`).

### `GET /api/meta` → `api_meta` (L147)
Bootstrap payload the page loads first: latest date, every available date, and the
country list (codes + display names + today's totals) used to populate dropdowns.
Params: none.
```json
{
  "latest": "2026-06-15",
  "dates": ["2026-06-15", "2026-06-14", "2026-06-13", "..."],
  "countries": [
    {"code": "United States", "name": "United States", "ip": 412903},
    {"code": "GB", "name": "United Kingdom", "ip": 188204},
    {"code": "VN", "name": "Vietnam", "ip": 95110}
  ]
}
```
Empty DB → `{"latest": null, "dates": [], "countries": []}`.

### `GET /api/overview` → `api_overview` (L159)
Overview tab data: day totals, previous-day row (for the delta), top-12 movers by
absolute change, top-10 per category (each with a 14-day `spark` array), and the
full daily trend.
Params: `date` (optional).
```json
{
  "date": "2026-06-15",
  "totals": {"date":"2026-06-15","total_ip":1840221,"dc_ip":221044,
             "n_titles":7412,"n_countries":171,
             "total_dht":1502119,"total_harv":1390550,"total_pex":88210},
  "prev": {"date":"2026-06-14","total_ip":1798004},
  "movers": [
    {"title":"Dune: Part Two","ip":52310,"delta":14820},
    {"title":"The Last of Us","ip":40912,"delta":-3110}
  ],
  "top": {
    "Movie":  [{"title":"Dune: Part Two","ip_id":"tt15239678","ip_count":52310,
                "category":"Movie","dc_ip_count":6120,"spark":[41020,43110,"...14 vals"]}],
    "Series": [{"title":"The Last of Us","ip_id":"tt3581920","ip_count":40912,
                "category":"Series","dc_ip_count":4880,"spark":["..."]}],
    "Anime":  [{"title":"One Piece","ip_id":"anime-21","ip_count":33001,
                "category":"Anime","dc_ip_count":2510,"spark":["..."]}]
  },
  "trend": [{"date":"2026-05-16","total_ip":1610002}, {"date":"2026-06-15","total_ip":1840221}]
}
```
No data for the date → `{}`. `prev` is `null` on the earliest date.

### `GET /api/titles` → `api_titles` (L184)
Titles-tab table: filtered/sorted title rows (built by `_title_rows`), each with a
14-day `spark`. When a country filter is set, rows come from `title_country` and
`dc_ip_count` is `null`.
Params: `date`, `category` (All|Movie|Series|Anime), `country` (All|code), `q`
(search substring), `sort` (`ip`|`ip_asc`|`title`|`title_desc`), `src`
(`all`|`dht`|`harv`|`pex`), `limit` (default 200, capped 1000).
```json
[
  {"title":"Dune: Part Two","category":"Movie","ip_id":"tt15239678",
   "ip_count":52310,"dc_ip_count":6120,"spark":[41020,43110,"...14 vals"]},
  {"title":"The Last of Us","category":"Series","ip_id":"tt3581920",
   "ip_count":40912,"dc_ip_count":4880,"spark":["..."]}
]
```

### `GET /api/title` → `api_title` (L197)
Title-detail drawer: catalog info + poster URL, per-country breakdown (top 25), full
daily trend, web-streaming presence (from `stream_demand.db`), and the 14-day spark.
Params: `ip_id` (required), `date` (optional).
```json
{
  "info": {"title":"Dune: Part Two","category":"Movie","imdb_id":"tt15239678",
           "anime_id":null,"ip_count":52310,"dc_ip_count":6120,
           "ip_count_dht":48010,"ip_count_harv":44120,"ip_count_pex":3110,
           "image_url":"https://image.tmdb.org/t/p/w185/abc123.jpg"},
  "date": "2026-06-15",
  "countries": [
    {"code":"United States","name":"United States","ip_count":12044},
    {"code":"GB","name":"United Kingdom","ip_count":8120}
  ],
  "trend": [{"date":"2026-05-16","ip_count":41020}, {"date":"2026-06-15","ip_count":52310}],
  "streaming": {"n_sites": 7, "sites": "fmovies.to,123movies.net,..."},
  "spark": [41020, 43110, "...14 vals"]
}
```
`streaming` is `null` if the title isn't on any streaming site. `image_url` is `null`
if no poster is known. For anime, `anime_id` holds the MAL id and `image_url` points
at the MAL CDN instead of TMDB.

### `GET /api/title_hashes` → `api_title_hashes` (L225)
Lazy drill-down: the top torrents feeding one title. **The only read of the big live
`hashes_v2.db`** — scoped to one `ip_id`, seeders-ordered, capped at 12, busy-timeout
so it can't stall. `name` is the *raw* torrent filename (used by the JS to flag
decoy/spam clusters where many torrents share an identical name).
Params: `ip_id` (required).
```json
[
  {"name":"Dune.Part.Two.2024.2160p.UHD.BluRay.x265-GROUP","category":"Movie","seeders":4120},
  {"name":"Dune Part Two (2024) 1080p WEBRip","category":"Movie","seeders":2890}
]
```
Missing `ip_id` or any DB error → `[]`.

### `GET /api/countries` → `api_countries` (L245)
Countries tab: demand by country (top 80), with ISO-2 codes for the choropleth map.
Params: `date` (optional).
```json
[
  {"code":"United States","name":"United States","iso":"US","ip_count":412903},
  {"code":"GB","name":"United Kingdom","iso":"GB","ip_count":188204},
  {"code":"VN","name":"Vietnam","iso":"VN","ip_count":95110}
]
```
`iso` may be `null` for unmappable labels (those rows simply won't shade the map).

### `GET /api/surging` → `api_surging` (L252)
Overview "Surging today" panel: today's partial volume projected to a full day
(`proj = today / frac`, where `frac` = fraction of the UTC day elapsed, floored at
0.10) compared to each title's 7-day average. Splits into three lists:
`ratio` (biggest multiplier, needs avg ≥ 1000), `absolute` (biggest projected rise),
`breakout` (little history: avg < 150 but today ≥ 1500). Each list capped at 40.
Params: none.
```json
{
  "today": "2026-06-15",
  "frac": 0.62,
  "ratio": [
    {"title":"New Movie X","category":"Movie","ip_id":"tt99",
     "avg":12000,"today":18600,"proj":30000,"surge":2.5}
  ],
  "absolute": [
    {"title":"Big Series","category":"Series","ip_id":"tt77",
     "avg":40000,"today":31000,"proj":50000,"rise":10000}
  ],
  "breakout": [
    {"title":"Surprise Hit","category":"Movie","ip_id":"tt55",
     "avg":80,"today":4200,"proj":6774}
  ]
}
```
No data → `{"today":null,"frac":0,"ratio":[],"absolute":[],"breakout":[]}`.

### `GET /api/trends` → `api_trends` (L293)
Trends tab: total daily series + a per-category series, on a shared date axis.
Params: `days` (window of most-recent N days; `0`/omitted = all).
```json
{
  "dates": ["2026-06-09","2026-06-10","...","2026-06-15"],
  "total": [1701002, 1722110, "...", 1840221],
  "cats": {
    "Movie":  [610000, 622000, "...", 660100],
    "Series": [880000, 890200, "...", 910500],
    "Anime":  [211002, 209900, "...", 269621]
  }
}
```
Each `cats[*]` array is the same length as `dates` (zero-filled for missing days).

### `GET /export.csv` → `export_csv` (L309)
CSV download of the current title filter (reuses `_title_rows`, limit 1000). Not
JSON — returns `text/csv` with `Content-Disposition: attachment`.
Params: same filter params as `/api/titles` (`date`,`category`,`country`,`q`,`sort`).
```
date,category,title,ip_id,ip_count
2026-06-15,Movie,Dune: Part Two,tt15239678,52310
2026-06-15,Series,The Last of Us,tt3581920,40912
```

### `GET /api/streams` → `api_streams` (L344)
Streaming tab: the streaming-piracy site registry from `stream_demand.db`
(domain/kind/rank/status, top 300 by takedown rank signal).
Params: none.
```json
[
  {"domain":"fmovies.to","kind":"movies","rank":982041,"status":"live"},
  {"domain":"123movies.net","kind":"mixed","rank":640112,"status":"dead"}
]
```
Any DB error → `[]`.

### `GET /api/stream_titles` → `api_stream_titles` (L358)
Streaming tab: per-title web-streaming demand (how many live sites carry it) **joined
to** the P2P distinct-peer-IP demand for the same catalog `ip_id`, ranked by P2P
demand (titles with no P2P signal fall to the end, ties broken by site reach). Top 400.
Params: `cat` (`all`|Movie|Series|Anime).
```json
{
  "date": "2026-06-15",
  "rows": [
    {"title":"Dune: Part Two","category":"Movie","ip_id":"tt15239678",
     "n_sites":7,"sites":"fmovies.to,123movies.net,...","p2p":52310},
    {"title":"Obscure Film","category":"Movie","ip_id":"tt42",
     "n_sites":3,"sites":"siteA,siteB,siteC","p2p":null}
  ]
}
```
`p2p` is `null` when the title is on streaming sites but absent from the P2P feed.
Error reading the stream DB → `{"date":null,"rows":[]}`.

### `GET /api/acestream` → `api_acestream` (L394)
Live Sport tab: live-IPTV (AceStream) demand from the mainline BitTorrent DHT — a
SEPARATE channel from the title feed (linear TV, not catalog titles). Aggregates the
latest collector run per channel (`infohash`). `peers` = BEP-33 leechers (swarm-size
estimate, the headline number); `sample` = raw `get_peers` IP count (weak, kept only
for the geo split); `geo` = top-6 sampled peer countries as a display string.
Caveat baked into the comment: peer geo is raw — VPN/datacenter IPs not yet discounted.
Top 200, sorted by `peers` desc.
Params: none.
```json
{
  "run_ts": "2026-06-15T14:00:11Z",
  "rows": [
    {"infohash":"ab12cd…40hex","name":"Sky Sports Main Event [UK]",
     "categories":"sport","ch_country":"gb","availability":1,
     "peers":4640,"seeders":0,"sample":312,"geo":"GB 180, VN 60, US 41"}
  ]
}
```
No run yet or DB error → `{"run_ts":null,"rows":[]}`.

### `GET /healthz` → `healthz` (L322)
Liveness probe — runs `SELECT 1` against the intel DB. Returns plain text `ok`.

### `GET /world.geojson` → `world_geo` (L327)
Serves the bundled `world_110m.geojson` (sits next to the source file) for the
choropleth map. Returns `application/json`. Missing file → empty FeatureCollection
`{"type":"FeatureCollection","features":[]}`.

### `GET /logo.svg` → `logo_svg` (L339)
Serves the inline `LOGO_SVG` string (defined L336) as `image/svg+xml`.

### `GET /` → `index` (L438)
Serves the entire `SHELL` HTML page (the whole frontend) as `text/html`. No params.

### `@app.after_request` → `_no_cache` (L138)
Not a route — a hook on *every* response. Sets `Cache-Control: no-cache, no-store,
must-revalidate` + `Pragma: no-cache` so browsers never serve a stale page after a deploy.

---

## Non-route helpers (Python, L31–L135)
- `poster_url(ip_id)` (L31) — `{ip_id → TMDB/MAL poster URL}`, or `None`. Anime ids
  (`anime-…`) resolve against the MAL CDN, everything else against TMDB. Backed by the
  static `posters.json` loaded once at worker start (no live-DB contention).
- `cname(label)` (L51) — country label → full display name (e.g. `CN` → `China`) via
  pycountry; passes through the legacy 14 and `Other`/`Unknown`.
- `ciso(label)` (L66) — country label → ISO-2 for GeoJSON matching. Handles full
  names, the legacy 14, name overrides (`Vietnam`→`VN`, …), and raw 2-letter codes.
- `db()` (L85) — open a READ-ONLY (`mode=ro`) SQLite connection to the intel DB, `Row` factory.
- `latest_date(c)` (L91) — `MAX(date)` from `daily_totals`.
- `SORTS` (L96) / `SRC_COL` (L99) — sort-key and source-column lookup dicts.
- `_title_rows(...)` (L100) — **shared title-query builder** for `/api/titles` +
  `/export.csv` (date/cat/country/q/sort/limit/src → SQL; switches between
  `title_demand` and `title_country` when a country filter is set).
- `_sparks(c, ip_ids, ndays=14)` (L120) — `{ip_id: [per-day ip_count]}` over the last
  N days on a shared date axis, for inline sparklines. One indexed query.

---

## Frontend (`SHELL`, L443–L698) — high level
`SHELL` is one raw string: `<style>` (dark theme) + body shell + `<script>` (vanilla
JS, no libs). Bootstrap IIFE (L697) loads `/api/meta`, then `render()` dispatches by tab.

**Shared JS utilities:** `api(path, params)` (fetch→JSON, L500), state objects
`META`/`T`/`tab`, `barCell` (L503), `lineSVG` (L504, line chart with a dashed
"today (partial)" segment), `sparkSVG` (L515), `wowPct`/`wowCell` (L524, week-over-week),
`titleTable` (L530, the reusable rank/IPs/DC%/WoW/spark table).

**Tab render functions** (one per nav button), each calls the matching `/api/*`:
| Tab | Fn (line) | Data source |
|---|---|---|
| Overview | `renderOverview` L581 | `/api/overview` + `/api/surging` |
| Titles | `renderTitles`/`loadTitles` L611/628 | `/api/titles` (+ `/export.csv` link) |
| Countries | `renderCountries` L652 (+ `worldMap` L640, `geoPath` L649) | `/api/countries` + `/world.geojson` |
| Trends | `renderTrends` L666 | `/api/trends` |
| Streaming | `renderStreams` L675 | `/api/stream_titles` + `/api/streams` |
| Live Sport | `renderLiveSport` L688 | `/api/acestream` |

**Title-detail drawer:** `openTitle` (L537, calls `/api/title`) + `loadHashes` (L561,
calls `/api/title_hashes`, flags decoy clusters); `closeDrawer`, row-click delegation
(L570), nav-tab switching (L571).

---

## Where to look for common tasks
| You want to… | Go to |
|---|---|
| Add / modify an API endpoint | Endpoint reference above (L147–L435); shared builder `_title_rows` L100 |
| Change the frontend HTML/CSS | `SHELL` `<style>` L444–L495, body L496–L498 |
| Change the frontend JS / charts | `SHELL` `<script>` L499–L698; `lineSVG` L504, `sparkSVG` L515, `titleTable` L530 |
| Change the **Overview** tab | `renderOverview` L581; data `api_overview` L159 + `api_surging` L252 |
| Change the **Titles** tab | `renderTitles`/`loadTitles` L611/628; data `api_titles` L184 / `_title_rows` L100 |
| Change the **Countries** tab / map | `renderCountries` L652, `worldMap` L640, `geoPath` L649; data `api_countries` L245; geojson `world_geo` L327 |
| Change the **Trends** tab | `renderTrends` L666; data `api_trends` L293 |
| Change the **Streaming** tab | `renderStreams` L675; data `api_streams` L344 + `api_stream_titles` L358 |
| Change the **Live Sport** tab | `renderLiveSport` L688; data `api_acestream` L394 |
| Change the **title-detail drawer** | `openTitle` L537 + `loadHashes` L561; data `api_title` L197 + `api_title_hashes` L225 |
| Change posters | `poster_url` L31, `POSTERS_JSON`/`TMDB_IMG`/`MAL_IMG` L21–23 |
| Change CSV export | `export_csv` L309 (reuses `_title_rows` L100) |
| Change caching headers | `_no_cache` L138 (`@app.after_request`) |
| Change country name / ISO normalisation | `cname` L51, `ciso` L66 (Python); `L2ISO` L647 + `CENT` L634 (JS) |
| Change which DB is read | `DB`/`STREAM_DB`/`ACESTREAM_DB` L14–16, `db()` L85; live hashes only in `api_title_hashes` L225 |
| Change bind / port / dev run | `PORT` L17, `app.run(...)` L702 (prod = gunicorn, not in this file) |
