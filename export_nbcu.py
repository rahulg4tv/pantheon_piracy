#!/usr/bin/env python3
"""
export_nbcu.py — produce OUR daily per-country distinct-IP demand feed
from the peers table.

(The `nbcu` in the filename is historical — NBCU is the third-party feed we are
replacing and the benchmark we validate against, NOT a feed we copy. The data
here is ours, measured from our own crawl.)

Methodology:
  * metric  = distinct peer IPs (IP_COUNT), one number per title x country x day
  * window  = a single day (peers.last_seen = DATE; an IP seen at any point that
              day has last_seen=DATE, so this is the day's distinct-IP union)
  * geo     = every country that clears the per-(title,country) floor gets its
              own row; the sub-floor tail (+ any ungeolocated IPs) rolls into a
              small per-title "Other". (Was 14 named countries + a 60%+ "Other";
              expanded 2026-06-05 — "Other" was real demand from ~160 unreported
              countries, led by IN/PH/ZA/PT/CN, not noise. Totals are unchanged;
              this only adds geographic resolution underneath each title.)
  * floor   = drop any (title, country) below 10 IPs (noise floor)
  * cats    = "Video: TV" / "Video: Movie" / "Video: Anime"

Source of IPs is the peers table, fed by BOTH the DHT collector and the
tracker-announce harvester (tracker_harvest_service.py). DHT alone undercounts
~3-39x; with tracker harvesting the daily union reaches full real-world
magnitude.

The harvester writes to a SEPARATE database (/data/db/harvest_peers.db) so its
heavy write volume never inflates the DHT collector's shared WAL. We ATTACH that
DB and UNION its peers with the main DB's peers, deduping distinct IP per
(title, country bucket) in Python so an IP harvested by BOTH paths counts once.

Title identity columns:
  * IP_ID    — the PANTHEON catalog ip_id. We NEVER mint it: it is mapped from
               the catalog parquet, keyed by imdb_id (movies/series) or taken
               directly when the raw id is already a valid catalog id (anime
               MAL ids, legit series-Q). Blank when the title is not in the
               Pantheon catalog.
  * IMDB_ID  — real IMDb tt id for movies/series (blank for anime/unmatched).
  * ANIME_ID — MyAnimeList id for anime (the `anime-<malid>` suffix; blank else).
  * UNMAPPED — 1 when the title has NO Pantheon ip_id (IP_ID blank): real demand
               for a title not (yet) in the catalog — surfaces catalog gaps and
               keeps us honest (we never fabricate an ip_id).
  * DC_IP_COUNT — how many of that row's IP_COUNT distinct IPs are datacenter/VPN
               (seedbox / VPN-exit / cloud), flagged via GeoLite2-ASN. Residential
               = IP_COUNT - DC_IP_COUNT. We do NOT drop datacenter IPs — they are
               real demand routed through seedboxes/VPNs and dropping them widens
               the NBCU gap (§40); the column lets a consumer filter if they want a
               residential-only view. 0 if the ASN db is missing.
Schema: TITLE, IP_ID, IMDB_ID, ANIME_ID, DATE, CATEGORY, COUNTRY_4, IP_COUNT, DC_IP_COUNT, UNMAPPED.
This is ADDITIVE — it does not modify merge_and_upload.py or the legacy upload.

Usage:
  /home/ec2-user/venv/bin/python3 export_nbcu.py --date 2026-05-30 \
      --out /data/daily/2026-05-30.csv
"""
from __future__ import annotations
import argparse, csv, sqlite3, datetime

DB = "file:/data/db/hashes_v2.db?mode=ro"
HARVEST_DB = "/data/db/harvest_peers.db"  # ATTACHed read-only if present
VELOCITY_DB = "/data/db/harvest_velocity_peers.db"  # high-velocity re-harvest lane (ATTACHed read-only if present)
PEX_DB = "/data/db/pex_peers.db"  # BEP-11 ut_pex peer-exchange lane (ATTACHed read-only if present)

NAMED = {  # ISO2 -> bucket label
 "US":"United States","GB":"United Kingdom","CA":"Canada","AU":"Australia",
 "BR":"Brazil","FR":"France","DE":"Germany","IE":"Ireland","IT":"Italy",
 "JP":"Japan","MX":"Mexico","KR":"South Korea","ES":"Spain","TH":"Thailand"}

# --- Country label normalization (2026-06-06): ISO-2 -> full name for ALL
# countries so COUNTRY_4 is uniform (legacy NAMED stays authoritative). Runtime
# pycountry, no embedded dict. Falls back to raw ISO if unavailable/unknown.
try:
    import pycountry as _pycountry
except Exception:
    _pycountry = None
_LABEL_CACHE = {}
_LABEL_OVERRIDES = {"VN": "Vietnam", "LA": "Laos", "SY": "Syria", "BN": "Brunei",
                    "RU": "Russia", "TW": "Taiwan", "KP": "North Korea"}
def _country_label(iso):
    if not iso or iso == "??":
        return iso
    if iso in NAMED:
        return NAMED[iso]
    if iso in _LABEL_CACHE:
        return _LABEL_CACHE[iso]
    label = _LABEL_OVERRIDES.get(iso)
    if label is None and _pycountry is not None:
        try:
            c = _pycountry.countries.get(alpha_2=iso)
            if c is not None:
                label = getattr(c, "common_name", None) or c.name
        except Exception:
            label = None
    if label is None:
        label = iso
    _LABEL_CACHE[iso] = label
    return label


# our hashes.category -> CATEGORY label
CAT = {"Series":"Video: TV", "Anime":"Video: Anime", "Movies":"Video: Movie",
       "Movie":"Video: Movie", "TV":"Video: TV"}

FLOOR = 10
CATALOG_DIR = "/data/catalog"

# Datacenter / VPN / hosting detection (GeoLite2-ASN). Curated cloud/VPN endpoint
# ASNs + hosting/VPN org-name keywords; residential-ISP words are deliberately
# NOT keywords so consumer ISPs are never swept in. Mirrors export_asn_ab.py.
ASN_DB = "/data/geoip/asn/GeoLite2-ASN.mmdb"
DC_ASNS = {16509, 14618, 15169, 396982, 8075, 8068, 14061, 16276, 24940, 20473,
 63949, 9009, 212238, 60068, 136787, 51852, 36352, 53667, 46844, 49981, 20454,
 30633, 40676, 62240, 206092, 9370, 398324, 3214}
DC_KW = ("hosting", "host europe", " host ", "cloud", "datacenter", "data center",
 "colocation", "vpn", "proxy", "dedicated", "leaseweb", "ovh", "m247",
 "datacamp", "choopa", "vultr", "digitalocean", "digital ocean", "hetzner",
 "linode", "contabo", "scaleway", "g-core", "gcore", "hostwinds", "quadranet",
 "psychz", "serverius", "worldstream", "ip volume", "packethub", "tefincom",
 "nordvpn", "expressvpn", "surfshark", "private internet", "frantech", "ponynet",
 "constant company", "amazon", "google llc", "microsoft", "oracle", "alibaba",
 "tencent", "akamai", "fastly", "limelight", "servers", "server ")


def _make_is_dc():
    """Return is_dc(ip)->bool backed by GeoLite2-ASN, with per-IP caching so each
    distinct IP is looked up once across the whole export. If the ASN db is
    missing/unreadable, returns a function that's always False (DC_IP_COUNT=0
    everywhere; the feed stays valid)."""
    import os
    try:
        import maxminddb
        asn = maxminddb.open_database(ASN_DB) if os.path.exists(ASN_DB) else None
    except Exception:
        asn = None
    cache: dict[str, bool] = {}

    def is_dc(ip: str) -> bool:
        if asn is None:
            return False
        v = cache.get(ip)
        if v is not None:
            return v
        dc = False
        try:
            r = asn.get(ip)
            if r:
                num = r.get("autonomous_system_number")
                org = (r.get("autonomous_system_organization") or "").lower()
                dc = (num in DC_ASNS) or any(k in org for k in DC_KW)
        except Exception:
            dc = False
        cache[ip] = dc
        return dc

    return is_dc


def _load_catalog() -> tuple[dict[str, str], set[str], dict[str, str]]:
    """Pantheon catalog mappings — the AUTHORITATIVE source of ip_id.

    Returns (imdb_id -> ip_id, set of all valid catalog ip_ids). We map to these;
    we never mint an ip_id ourselves. movies/series carry imdb_id; anime is keyed
    on MAL (ip_id only). If the parquets are missing, returns empties (every title
    then falls through to UNMAPPED rather than emitting a fabricated id).
    """
    import os
    import pyarrow.parquet as pq
    imdb2ipid: dict[str, str] = {}
    valid: set[str] = set()
    ip2title: dict[str, str] = {}   # catalog ip_id -> official IP name (proper casing)
    for fname in ("movies_info", "series_info"):
        p = f"{CATALOG_DIR}/{fname}.parquet"
        if not os.path.exists(p):
            continue
        d = pq.read_table(p, columns=["ip_id", "imdb_id", "ip"]).to_pydict()
        for ipid, im, ttl in zip(d["ip_id"], d["imdb_id"], d["ip"]):
            if ipid:
                valid.add(ipid)
                if im:
                    imdb2ipid.setdefault(im, ipid)
                if ttl:
                    ip2title.setdefault(ipid, ttl)
    pa = f"{CATALOG_DIR}/anime_info.parquet"
    if os.path.exists(pa):
        da = pq.read_table(pa, columns=["ip_id", "ip"]).to_pydict()
        for ipid, ttl in zip(da["ip_id"], da["ip"]):
            if ipid:
                valid.add(ipid)
                if ttl:
                    ip2title.setdefault(ipid, ttl)
    return imdb2ipid, valid, ip2title


def _resolve_ip_id(raw_ip_id: str, imdb_id: str,
                   imdb2ipid: dict[str, str], valid: set[str]) -> tuple[str, int]:
    """Map a title to its Pantheon ip_id. Returns (ip_id, unmapped_flag).

    Order: (1) imdb_id -> catalog ip_id (authoritative for movies/series);
    (2) the raw id is itself a valid catalog id (anime MAL ids, legit series-Q);
    (3) not in the catalog -> blank ip_id + UNMAPPED=1 (we never mint one).
    """
    if imdb_id and imdb_id in imdb2ipid:
        return imdb2ipid[imdb_id], 0
    if raw_ip_id in valid:
        return raw_ip_id, 0
    return "", 1


def _category(category: str | None, ip_id: str) -> str:
    """Map a title to its feed CATEGORY.

    Anime gets its own "Video: Anime" bucket. The most reliable anime signal is
    the ip_id: anime titles are keyed on MAL and carry an `anime-<malid>` (or
    `mal-`) ip_id, so we key off that first — robust even if a hash's category
    column was mis-tagged — then fall back to the category column.
    """
    if (ip_id or "").startswith(("anime-", "mal-")):
        return "Video: Anime"
    return CAT.get(category, "Video: TV")


def _ids(imdb_id: str | None, ip_id: str) -> tuple[str, str]:
    """Return (IMDB_ID, ANIME_ID) for a title.

    The internal canonical id is always emitted separately as IP_ID, so these two
    public ids are blank when not applicable (no more ip_id fallback):
      * Anime  -> ANIME_ID = the MyAnimeList id (the `anime-<malid>` ip_id
                  suffix; this is reliable even where titles.mal_id is NULL),
                  IMDB_ID blank.
      * Movie / Series -> IMDB_ID = the real tt id (titles.imdb_id, or the tt
                  embedded in a `film-tt...` ip_id), ANIME_ID blank.
      * Unmatched (e.g. `series-Q...`) -> both blank; IP_ID still identifies it.
    """
    if (ip_id or "").startswith(("anime-", "mal-")):
        return "", ip_id.split("-", 1)[1]          # 'anime-21' -> ('', '21')
    if imdb_id:
        return imdb_id, ""
    if ip_id and ip_id.startswith("film-tt"):
        return ip_id[len("film-"):], ""            # 'film-tt123' -> ('tt123','')
    return "", ""


def _build_canon(c) -> dict:
    """Map a leftover legacy Q-style ip_id to the canonical (anime-/film-tt/series-tt)
    id of the SAME title, so a title split across a stale `Q` duplicate and its real
    catalog id is reported as ONE row instead of two. CONSERVATIVE: only merges when a
    title has EXACTLY ONE canonical id and one+ legacy `Q` ids — ambiguous cases (two
    `Q`s, or film-tt vs series-tt) are left untouched to avoid false merges. Built from
    the `hashes` catalog, so it is independent of which titles have peers today."""
    import collections
    CANON = ("anime-", "mal-", "film-tt", "series-tt")
    LEGACY = ("film-Q", "series-Q")
    ti: dict[str, set] = collections.defaultdict(set)
    for title, ipid in c.execute(
            "SELECT DISTINCT title, ip_id FROM hashes "
            "WHERE ip_id IS NOT NULL AND title IS NOT NULL"):
        ti[title].add(ipid)
    canon: dict[str, str] = {}
    for title, ids in ti.items():
        if len(ids) < 2:
            continue
        canon_ids = [i for i in ids if i.startswith(CANON)]
        legacy_ids = [i for i in ids if i.startswith(LEGACY)]
        if len(canon_ids) == 1 and legacy_ids:
            for lid in legacy_ids:
                canon[lid] = canon_ids[0]
    return canon


def _build_canon_v2(titlemeta: dict) -> dict:
    """Merge map keyed on the AUTHORITATIVE id: imdb_id (movies/series) or MAL
    anime_id (anime), NOT the title string. All raw ip_ids resolving to the SAME
    imdb (or MAL) collapse to one target; same-title-different-content stays apart
    because it carries a different imdb_id (film vs series, remakes). Same-title
    UNMAPPED fragments fold into the lone resolved id of that title, but ONLY when
    the title has exactly one resolved id (so e.g. One Piece anime vs live-action
    stays split). Operates on titlemeta (ids present in today's feed) — no full
    hashes scan, so it is also faster than the old title-keyed _build_canon."""
    import collections
    by_imdb = collections.defaultdict(list)
    by_anime = collections.defaultdict(list)
    unmapped_by_title = collections.defaultdict(list)
    resolved_by_title = collections.defaultdict(set)
    for rid, m in titlemeta.items():
        title, imdb, anime, resolved = m[0], m[2], m[3], m[4]
        tkey = (title or "").strip().lower()
        if imdb:
            by_imdb[imdb].append(rid)
        elif anime:
            by_anime[anime].append(rid)
        elif not resolved:
            unmapped_by_title[tkey].append(rid)
        if resolved:
            resolved_by_title[tkey].add(resolved)
    canon: dict = {}
    def _target(rids):
        # deterministic: prefer a mapped id (resolved non-empty, not unmapped), then by id
        return sorted(rids, key=lambda r: (titlemeta[r][4] == "", titlemeta[r][5], r))[0]
    for grp in list(by_imdb.values()) + list(by_anime.values()):
        if len(grp) < 2:
            continue
        tgt = _target(grp)
        for r in grp:
            if r != tgt:
                canon[r] = tgt
    res_to_rid: dict = {}
    for rid, m in titlemeta.items():
        if m[4]:
            res_to_rid.setdefault(m[4], rid)
    for tkey, rids in unmapped_by_title.items():
        rset = resolved_by_title.get(tkey, set())
        if len(rset) == 1:
            tgt = res_to_rid.get(next(iter(rset)))
            if tgt is None:
                continue
            tgt = canon.get(tgt, tgt)   # follow to the group's target (no chains)
            for r in rids:
                if r != tgt and r not in canon:
                    canon[r] = tgt
    return canon


def export(date: str, out: str) -> None:
    import os
    c = sqlite3.connect(DB, uri=True)
    # The hash->title/category/ip_id mapping lives only in the main DB's `hashes`
    # table; the IMDb id lives in `titles`. Peer IPs come from TWO sources: the
    # main DB's peers (DHT collector) and the harvester's separate DB. ATTACH the
    # harvest DB (read-only) and UNION both peers tables, joining each to the main
    # `hashes` (and LEFT JOIN `titles` for imdb_id). DISTINCT ip per (ip_id,
    # country bucket) is the IP_COUNT — Python aggregation dedupes across the two
    # sources so an IP seen by both counts once.
    # Peer IPs come from up to THREE separate DBs, each ATTACHed read-only and
    # UNIONed: the main DB's `peers` (DHT collector), the tracker harvester
    # (harvest_peers.db), and the high-velocity re-harvest lane
    # (harvest_velocity_peers.db). Python aggregation dedupes DISTINCT ip per
    # (ip_id, country) so an IP seen by several sources counts once — the velocity
    # lane only ADDS the day-0 churn IPs the others miss.
    union = ["SELECT hash, country, ip, last_seen, 'dht' AS src FROM peers"]
    if os.path.exists(HARVEST_DB):
        c.execute("ATTACH DATABASE ? AS hv", ("file:" + HARVEST_DB + "?mode=ro",))
        union.append("SELECT hash, country, ip, last_seen, 'harv' AS src FROM hv.peers")
    if os.path.exists(VELOCITY_DB):
        c.execute("ATTACH DATABASE ? AS vv", ("file:" + VELOCITY_DB + "?mode=ro",))
        union.append("SELECT hash, country, ip, last_seen, 'harv' AS src FROM vv.peers")
    if os.path.exists(PEX_DB):
        c.execute("ATTACH DATABASE ? AS pe", ("file:" + PEX_DB + "?mode=ro",))
        union.append("SELECT hash, country, ip, last_seen, 'pex' AS src FROM pe.peers")
    rows = c.execute("""
        SELECT h.ip_id, h.title, h.category, t.imdb_id, p.country, p.ip, p.src FROM (
            %s
        ) p
        JOIN hashes h ON h.hash = p.hash
        LEFT JOIN titles t ON t.ip_id = h.ip_id
        WHERE p.last_seen = ? AND p.ip != '_queried_'
    """ % "\n            UNION ALL\n            ".join(union), (date,))

    imdb2ipid, valid_ipids, ip2title = _load_catalog()
    is_dc = _make_is_dc()

    # Group by the RAW hashes ip_id (the title key) so IP_COUNTs are identical to
    # before; the emitted IP_ID is then resolved to the Pantheon catalog id.
    agg: dict[str, dict[str, set]] = {}
    agg_dht: dict[str, dict[str, set]] = {}
    agg_harv: dict[str, dict[str, set]] = {}
    agg_pex: dict[str, dict[str, set]] = {}
    ipsrc: dict[tuple, set] = {}
    # raw ip_id -> (title, cat_label, imdb, anime, resolved_ip, unmapped)
    titlemeta: dict[str, tuple] = {}
    # Assign each (title, ip) EXACTLY ONE country before bucketing. The same IP can
    # geolocate to different countries across the 3 source DBs (DHT / harvest /
    # velocity); the old 14-bucket export hid this because every non-named country
    # collapsed into one deduped "Other" set. Splitting per-country re-exposes it —
    # without this dedup an IP would be counted in several country buckets and
    # inflate the title's distinct-IP total (seen: JUJUTSU KAISEN 3.7x). First
    # valid (non-"??") country wins; ties are deterministic in source/query order.
    ipctry: dict[tuple, str] = {}
    for ip_id, title, category, imdb_id, iso, ip, src in rows:
        # titlemeta is set every row (last-wins) — identical to the pre-expansion
        # export, so the emitted TITLE/IP_ID labels don't shift; the ONLY intended
        # change is the per-country bucketing below.
        imdb, anime = _ids(imdb_id, ip_id)
        resolved, unmapped = _resolve_ip_id(ip_id, imdb, imdb2ipid, valid_ipids)
        titlemeta[ip_id] = (title, _category(category, ip_id), imdb, anime,
                            resolved, unmapped)
        k = (ip_id, ip)
        ipsrc.setdefault(k, set()).add(src)
        cur = ipctry.get(k)
        if cur is None or (cur == "??" and iso):
            ipctry[k] = iso or "??"
    for (ip_id, ip), iso in ipctry.items():
        agg.setdefault(ip_id, {}).setdefault(iso, set()).add(ip)
        _s = ipsrc.get((ip_id, ip), ())
        if "dht" in _s:
            agg_dht.setdefault(ip_id, {}).setdefault(iso, set()).add(ip)
        if "harv" in _s:
            agg_harv.setdefault(ip_id, {}).setdefault(iso, set()).add(ip)
        if "pex" in _s:
            agg_pex.setdefault(ip_id, {}).setdefault(iso, set()).add(ip)

    # De-fragment titles: merge leftover legacy Q-style ip_ids into their canonical
    # twin (same title) so a show split across e.g. anime-40748 + series-Q98836216
    # is one row, not two. Unions per-country IP sets, so distinct IPs dedupe across
    # the merged ids; the canonical id's titlemeta (real imdb/anime id) is kept.
    for src, tgt in _build_canon_v2(titlemeta).items():
        if src not in agg or src == tgt:
            continue
        for _D in (agg, agg_dht, agg_harv, agg_pex):
            if src in _D:
                for country, ips in _D.pop(src).items():
                    _D.setdefault(tgt, {}).setdefault(country, set()).update(ips)
        if tgt not in titlemeta and src in titlemeta:
            titlemeta[tgt] = titlemeta[src]
        titlemeta.pop(src, None)

    out_rows = []
    by_title: dict[str, int] = {}
    name: dict[str, str] = {}
    for ip_id, buckets in agg.items():
        title, cat_label, imdb, anime, resolved, unmapped = titlemeta[ip_id]
        title = ip2title.get(resolved) or title  # catalog official-cased title (2026-06-06)
        # Per-country reporting (2026-06-05): emit every country that clears the
        # floor — label = the original full name for the 14 legacy buckets, else
        # the ISO-2 code — and roll the sub-floor tail + ungeolocated IPs into one
        # small per-title "Other" so distinct-IP totals are preserved.
        other_ips: set = set()
        other_isos: list = []
        emit: list = []
        for iso, ips in buckets.items():
            if iso and iso != "??" and len(ips) >= FLOOR:
                emit.append((_country_label(iso), ips, [iso]))
            else:
                other_ips |= ips
                other_isos.append(iso)
        if len(other_ips) >= FLOOR:
            emit.append(("Other", other_ips, other_isos))
        _dh = agg_dht.get(ip_id, {})
        _hv = agg_harv.get(ip_id, {})
        _px = agg_pex.get(ip_id, {})
        for label, ips, isos in emit:
            n = len(ips)
            dc = sum(1 for ip in ips if is_dc(ip))
            dht = len(set().union(*[_dh.get(c, set()) for c in isos])) if isos else 0
            harv = len(set().union(*[_hv.get(c, set()) for c in isos])) if isos else 0
            pex = len(set().union(*[_px.get(c, set()) for c in isos])) if isos else 0
            out_rows.append({
                "TITLE": title,
                "IP_ID": resolved,
                "IMDB_ID": imdb,
                "ANIME_ID": anime,
                "DATE": date,
                "CATEGORY": cat_label,
                "COUNTRY_4": label,
                "IP_COUNT": n,
                "DC_IP_COUNT": dc,
                "IP_COUNT_DHT": dht,
                "IP_COUNT_HARVEST": harv,
                "IP_COUNT_PEX": pex,
                "UNMAPPED": unmapped,
            })
            by_title[ip_id] = by_title.get(ip_id, 0) + n
            name[ip_id] = title

    out_rows.sort(key=lambda r: (-r["IP_COUNT"],))
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["TITLE","IP_ID","IMDB_ID","ANIME_ID",
                                          "DATE","CATEGORY","COUNTRY_4","IP_COUNT",
                                          "DC_IP_COUNT","UNMAPPED",
                                          "IP_COUNT_DHT","IP_COUNT_HARVEST","IP_COUNT_PEX"])
        w.writeheader()
        w.writerows(out_rows)

    # summary
    n_titles = len(by_title)
    total_ip = sum(r["IP_COUNT"] for r in out_rows)
    total_dc = sum(r["DC_IP_COUNT"] for r in out_rows)
    n_unmapped_rows = sum(1 for r in out_rows if r["UNMAPPED"])
    n_unmapped_titles = sum(1 for tid in by_title if titlemeta[tid][5])
    print(f"wrote {len(out_rows):,} rows  ({n_titles:,} titles, "
          f"sum IP_COUNT={total_ip:,}) -> {out}")
    print(f"  datacenter/VPN IPs: {total_dc:,} "
          f"({100*total_dc/total_ip:.1f}% of IP_COUNT)  residential={total_ip-total_dc:,}")
    print(f"  unmapped (no Pantheon ip_id): {n_unmapped_titles:,} titles / "
          f"{n_unmapped_rows:,} rows")
    print("\nTop 15 titles by summed IP_COUNT:")
    for tid, v in sorted(by_title.items(), key=lambda x: -x[1])[:15]:
        flag = "  [UNMAPPED]" if titlemeta[tid][5] else ""
        print(f"  {v:>10,}  {name[tid][:40]}{flag}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d"))
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    out = a.out or f"/data/daily/{a.date}.csv"
    export(a.date, out)
