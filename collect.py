#!/usr/bin/env python3
"""
collect.py
----------
Daily hash collection pipeline. Pulls top-seeded Movies + Series from:
  - Jackett (TPB, YTS, showRSS, and any other configured indexers)
  - 1337x via FlareSolverr
  - BitMagnet (local DHT crawler)

Enriches each hash with a clean title + IMDB ID via TMDB where missing.
Appends new hashes to data/hashes.db (SQLite). Deduplicates by infohash.

Usage:
    python collect.py                  # full run
    python collect.py --skip-jackett
    python collect.py --skip-flare
    python collect.py --skip-bitmagnet
    python collect.py --skip-enrich    # skip TMDB enrichment
    python collect.py --min-seeders 10
"""

import argparse
import re
import sqlite3
import time
from datetime import date
from pathlib import Path

import requests
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")

JACKETT_HOST   = os.getenv("JACKETT_HOST", "http://localhost:9117")
JACKETT_KEY    = os.getenv("JACKETT_API_KEY")
FLARE_HOST     = os.getenv("FLARESOLVERR_HOST", "http://localhost:8191/v1")
BITMAGNET_HOST = os.getenv("BITMAGNET_HOST", "http://localhost:3333")
TMDB_KEY       = os.getenv("TMDB_API_KEY")

DB_PATH  = Path(__file__).parent / "data" / "hashes.db"
HEADERS  = {"User-Agent": "Mozilla/5.0"}

MOVIE_CATS = [2000, 2010, 2020, 2030, 2040, 2045, 2050, 2060]
TV_CATS    = [5000, 5010, 5020, 5030, 5040, 5045, 5050, 5060, 5080]
ANIME_CATS = [5070, 2070]  # Nyaa: TV Anime / Anime Movies


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db: sqlite3.Connection):
    db.execute("""
        CREATE TABLE IF NOT EXISTS hashes (
            hash        TEXT PRIMARY KEY,
            title       TEXT,
            raw_name    TEXT,
            category    TEXT,
            imdb_id     TEXT,
            tmdb_id     TEXT,
            mal_id      TEXT,
            seeders     INTEGER DEFAULT 0,
            trackers    TEXT,
            first_seen  TEXT,
            last_seen   TEXT,
            peer_count  INTEGER
        )
    """)
    # Add columns if upgrading from old schema
    for col in ["raw_name TEXT", "mal_id TEXT"]:
        try:
            db.execute(f"ALTER TABLE hashes ADD COLUMN {col}")
        except Exception:
            pass
    db.execute("CREATE INDEX IF NOT EXISTS idx_seeders   ON hashes(seeders)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_category  ON hashes(category)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON hashes(last_seen)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_imdb      ON hashes(imdb_id)")
    db.commit()


def upsert(db: sqlite3.Connection, items: list[dict]):
    today = str(date.today())
    inserted = updated = 0

    for item in items:
        h = item["hash"].lower()
        if not h or len(h) != 40:
            continue

        trackers_str = ",".join(sorted(set(item.get("trackers", []))))
        existing = db.execute(
            "SELECT seeders, trackers FROM hashes WHERE hash=?", (h,)
        ).fetchone()

        if existing is None:
            db.execute("""
                INSERT INTO hashes (hash, title, raw_name, category, imdb_id, tmdb_id, mal_id,
                                    seeders, trackers, first_seen, last_seen)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (h, item.get("title",""), item.get("raw_name",""),
                  item.get("category",""), item.get("imdb_id",""), item.get("tmdb_id",""),
                  item.get("mal_id",""), item.get("seeders",0), trackers_str, today, today))
            inserted += 1
        else:
            existing_trackers = set(existing[1].split(",")) if existing[1] else set()
            new_trackers = existing_trackers | set(trackers_str.split(","))
            new_seeders  = max(existing[0], item.get("seeders", 0))
            db.execute("""
                UPDATE hashes SET seeders=?, trackers=?, last_seen=?,
                    title=COALESCE(NULLIF(title,''), ?),
                    raw_name=COALESCE(NULLIF(raw_name,''), ?),
                    imdb_id=COALESCE(NULLIF(imdb_id,''), ?),
                    tmdb_id=COALESCE(NULLIF(tmdb_id,''), ?),
                    mal_id=COALESCE(NULLIF(mal_id,''), ?)
                WHERE hash=?
            """, (new_seeders, ",".join(sorted(new_trackers)), today,
                  item.get("title",""), item.get("raw_name",""),
                  item.get("imdb_id",""), item.get("tmdb_id",""),
                  item.get("mal_id",""), h))
            updated += 1

    db.commit()
    return inserted, updated


# ---------------------------------------------------------------------------
# TMDB enrichment
# ---------------------------------------------------------------------------

_tmdb_cache: dict[str, dict] = {}


def _extract_year(raw: str) -> str:
    """Extract the first 4-digit year found in a torrent name."""
    m = re.search(r'\b(19|20)\d{2}\b', raw)
    return m.group(0) if m else ""


def _clean_name(raw: str) -> str:
    """Strip resolution/codec tags from torrent name to get a searchable title."""
    t = raw.strip()
    # Remove file extension
    t = re.sub(r'\.(mkv|mp4|avi|mov|wmv)$', '', t, flags=re.I)
    # Replace dots with spaces if no spaces present
    if '.' in t and ' ' not in t[:40]:
        t = t.replace('.', ' ')
    # Cut at quality/codec markers
    t = re.sub(
        r'\b(2160p?|1080p?|720p?|480p?|4K|UHD|WEB[\-. ]?DL|WEBRip|BluRay|BDRip|'
        r'BDRemux|HDRip|DVDRip|x264|x265|HEVC|H\.?264|H\.?265|AVC|AAC|DDP?5?|'
        r'AMZN|NF|DSNP|HULU|MAX|IMAX|HDR|DV|DUAL|MULTi|PROPER|REPACK|EXTENDED).*',
        '', t, flags=re.I
    )
    # Strip S01E01 and beyond
    t = re.sub(r'\s*S\d{1,2}(E\d{1,2})?.*', '', t, flags=re.I)
    # Strip year (trailing or standalone)
    t = re.sub(r'\s*\(?\b(19|20)\d{2}\b\)?', '', t)
    # Strip release group tags like [EZTVx.to], (YTS.BZ)
    t = re.sub(r'[\[\(][^\]\)]{2,20}[\]\)]', '', t)
    return t.strip(' .-_[]()').strip()


def tmdb_enrich(raw_name: str, category: str) -> dict:
    """Search TMDB for clean title + IMDB ID. Returns dict with title, imdb_id, tmdb_id."""
    if not TMDB_KEY:
        return {"title": "", "imdb_id": "", "tmdb_id": ""}

    clean = _clean_name(raw_name)
    if not clean:
        return {"title": "", "imdb_id": "", "tmdb_id": ""}

    # Skip if cleaned name has no Latin characters (e.g. Cyrillic/CJK-only titles)
    # or is too short to be a reliable search query
    latin_chars = re.sub(r"[^a-zA-Z]", "", clean)
    if len(latin_chars) < 3 or len(clean) < 4:
        return {"title": "", "imdb_id": "", "tmdb_id": ""}

    year = _extract_year(raw_name)  # use for validation, not search

    cache_key = f"{clean}|{category}"
    if cache_key in _tmdb_cache:
        return _tmdb_cache[cache_key]

    media = "movie" if category == "Movies" else "tv"
    result = {"title": "", "imdb_id": "", "tmdb_id": ""}

    try:
        r = requests.get(
            f"https://api.themoviedb.org/3/search/{media}",
            params={"api_key": TMDB_KEY, "query": clean, "language": "en-US"},
            timeout=8
        )
        hits = r.json().get("results", [])
        if hits:
            # If year found in torrent name, prefer the hit whose release year matches
            hit = hits[0]
            if year and len(hits) > 1:
                for h in hits:
                    date = h.get("release_date") or h.get("first_air_date") or ""
                    if date.startswith(year):
                        hit = h
                        break

            tmdb_id = str(hit.get("id", ""))
            title   = hit.get("title") or hit.get("name", "")

            result["tmdb_id"] = tmdb_id
            result["title"]   = title

            # Fetch IMDB ID via external_ids endpoint
            ext = requests.get(
                f"https://api.themoviedb.org/3/{media}/{tmdb_id}/external_ids",
                params={"api_key": TMDB_KEY}, timeout=8
            ).json()
            result["imdb_id"] = ext.get("imdb_id", "")
        time.sleep(0.15)
    except Exception:
        pass

    _tmdb_cache[cache_key] = result
    return result


_mal_cache: dict[str, dict] = {}

def mal_enrich(raw_name: str) -> dict:
    """Search Jikan (MAL) for anime title + MAL ID. Returns dict with title, mal_id."""
    clean = _clean_name(raw_name)
    if not clean:
        return {"title": "", "mal_id": ""}

    latin_chars = re.sub(r"[^a-zA-Z]", "", clean)
    if len(latin_chars) < 3:
        return {"title": "", "mal_id": ""}

    if clean in _mal_cache:
        return _mal_cache[clean]

    result = {"title": "", "mal_id": ""}
    try:
        r = requests.get(
            "https://api.jikan.moe/v4/anime",
            params={"q": clean, "limit": 3, "type": "tv"},
            timeout=10
        )
        hits = r.json().get("data", [])
        if hits:
            hit   = hits[0]
            title = hit.get("title_english") or hit.get("title", "")
            result["mal_id"] = str(hit.get("mal_id", ""))
            result["title"]  = title
        time.sleep(0.5)  # Jikan rate limit: 3 req/sec
    except Exception:
        pass

    _mal_cache[clean] = result
    return result


def enrich_anime(db: sqlite3.Connection):
    """Find anime hashes with no mal_id and enrich them via Jikan (MAL)."""
    rows = db.execute("""
        SELECT hash, raw_name, title FROM hashes
        WHERE category = 'Anime'
          AND (mal_id IS NULL OR mal_id = '')
          AND (raw_name IS NOT NULL AND raw_name != '')
    """).fetchall()

    if not rows:
        print("  No anime to enrich.")
        return

    print(f"  Enriching {len(rows)} anime hashes via MAL ...")
    enriched = 0
    for h, raw_name, title in rows:
        info = mal_enrich(raw_name)
        if info["mal_id"] or info["title"]:
            db.execute("""
                UPDATE hashes SET
                    mal_id = COALESCE(NULLIF(mal_id,''), ?),
                    title  = CASE WHEN ? != '' THEN ? ELSE title END
                WHERE hash = ?
            """, (info["mal_id"], info["title"], info["title"], h))
            enriched += 1

    db.commit()
    print(f"  Enriched {enriched} anime hashes with MAL data.")


def enrich_missing(db: sqlite3.Connection):
    """Find hashes with no imdb_id and enrich them via TMDB."""
    rows = db.execute("""
        SELECT hash, raw_name, title, category FROM hashes
        WHERE (imdb_id IS NULL OR imdb_id = '')
          AND category IN ('Movies', 'Series')
          AND (raw_name IS NOT NULL AND raw_name != '')
    """).fetchall()

    if not rows:
        print("  Nothing to enrich.")
        return

    print(f"  Enriching {len(rows)} hashes via TMDB ...")
    enriched = 0
    for h, raw_name, title, category in rows:
        name_to_search = raw_name or title
        info = tmdb_enrich(name_to_search, category)
        if info["imdb_id"] or info["title"]:
            db.execute("""
                UPDATE hashes SET
                    imdb_id = COALESCE(NULLIF(imdb_id,''), ?),
                    tmdb_id = COALESCE(NULLIF(tmdb_id,''), ?),
                    title   = CASE WHEN ? != '' THEN ? ELSE title END
                WHERE hash = ?
            """, (info["imdb_id"], info["tmdb_id"],
                  info["title"], info["title"], h))
            enriched += 1
        time.sleep(0.05)

    db.commit()
    print(f"  Enriched {enriched} hashes with TMDB data.")


# ---------------------------------------------------------------------------
# Jackett
# ---------------------------------------------------------------------------

def _parse_jackett(raw: list, tracker: str) -> list[dict]:
    items = []
    for r in raw:
        h = (r.get("InfoHash") or "").strip().lower()
        if not h or len(h) != 40:
            continue
        cat_ids  = r.get("Category") or []
        is_anime = any(c in ANIME_CATS for c in cat_ids)
        is_movie = any(2000 <= c < 3000 for c in cat_ids)
        is_tv    = any(5000 <= c < 6000 for c in cat_ids)
        if not is_movie and not is_tv and not is_anime:
            continue  # skip software, games, adult, music, etc.
        raw_imdb = str(r.get("Imdb") or "").strip()
        imdb_id  = (raw_imdb if raw_imdb.startswith("tt") else f"tt{raw_imdb}") \
                   if raw_imdb and raw_imdb not in ("0", "") else ""
        raw_name = (r.get("Title") or "").strip()
        items.append({
            "hash":     h,
            "title":    raw_name,   # Jackett titles are already clean enough
            "raw_name": raw_name,
            "category": "Anime" if is_anime else ("Movies" if is_movie else "Series"),
            "imdb_id":  imdb_id,
            "tmdb_id":  "",
            "seeders":  int(r.get("Seeders") or 0),
            "trackers": [tracker or (r.get("Tracker") or "").strip()],
        })
    return items


def fetch_jackett() -> list[dict]:
    if not JACKETT_KEY:
        print("  JACKETT_API_KEY not set — skipping")
        return []

    indexer_dir = Path.home() / "Library/Application Support/Jackett/Indexers"
    indexers = [f.stem for f in indexer_dir.glob("*.json")
                if not f.name.endswith(".bak.json")] if indexer_dir.exists() else []

    all_items = []
    cat_qs = "&".join(f"Category[]={c}" for c in MOVIE_CATS + TV_CATS + ANIME_CATS)

    print(f"  Querying {len(indexers)} Jackett indexers ...")
    for idx in indexers:
        try:
            url  = f"{JACKETT_HOST}/api/v2.0/indexers/{idx}/results"
            # Parse cat_qs into a dict so the API key never appears in the URL string
            import urllib.parse as _up
            params = dict(_up.parse_qsl(cat_qs))
            params.update({"apikey": JACKETT_KEY, "Query": "", "Limit": "100"})
            resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                items = _parse_jackett(resp.json().get("Results", []), idx)
                if items:
                    print(f"    {idx:<35} {len(items):>5} items")
                all_items += items
        except Exception:
            pass

    return all_items


# ---------------------------------------------------------------------------
# BitMagnet (local DHT) — Movies + Series only
# ---------------------------------------------------------------------------

def fetch_bitmagnet(min_seeders: int = 1) -> list[dict]:
    query = """
    {
      torrentContent {
        search(input: {limit: 500, orderBy: [{field: seeders, descending: true}]}) {
          items {
            torrent { infoHash name }
            contentType
            seeders
          }
        }
      }
    }
    """
    try:
        r = requests.post(
            f"{BITMAGNET_HOST}/graphql",
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        items_raw = r.json()["data"]["torrentContent"]["search"]["items"]
    except Exception as e:
        print(f"  BitMagnet unavailable: {e}")
        return []

    items = []
    for item in items_raw:
        ct = (item.get("contentType") or "").lower()
        # Only Movies and Series — skip everything else
        if "movie" not in ct and "tv" not in ct:
            continue
        t        = item.get("torrent", {})
        h        = (t.get("infoHash") or "").lower()
        if not h or len(h) != 40:
            continue
        seeders  = item.get("seeders") or 0
        if seeders < min_seeders:
            continue
        raw_name = t.get("name", "")
        items.append({
            "hash":     h,
            "title":    "",        # will be filled by TMDB enrichment
            "raw_name": raw_name,
            "category": "Movies" if "movie" in ct else "Series",
            "imdb_id":  "",
            "tmdb_id":  "",
            "seeders":  seeders,
            "trackers": ["bitmagnet-dht"],
        })

    return items


# ---------------------------------------------------------------------------
# EZTV (TV Series — direct JSON API)
# ---------------------------------------------------------------------------

EZTV_URL = "https://eztvx.to/api/get-torrents"

def fetch_eztv(pages: int = 3) -> list[dict]:
    items = []
    seen  = set()
    for page in range(1, pages + 1):
        try:
            r = requests.get(EZTV_URL, params={"limit": 100, "page": page},
                             timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            torrents = r.json().get("torrents") or []
            if not torrents:
                break
            for t in torrents:
                h = (t.get("hash") or "").strip().lower()
                if not h or len(h) != 40 or h in seen:
                    continue
                seen.add(h)
                raw_name = (t.get("filename") or t.get("title") or "").strip()
                raw_imdb = str(t.get("imdb_id") or "").strip()
                imdb_id  = f"tt{raw_imdb}" if raw_imdb and raw_imdb not in ("0", "") \
                           and not raw_imdb.startswith("tt") else raw_imdb
                items.append({
                    "hash":     h,
                    "title":    "",
                    "raw_name": raw_name,
                    "category": "Series",
                    "imdb_id":  imdb_id if imdb_id.startswith("tt") else "",
                    "tmdb_id":  "",
                    "seeders":  int(t.get("seeds") or 0),
                    "trackers": ["eztvx.to"],
                })
        except Exception as e:
            print(f"  EZTV page {page} error: {e}")
            break
    return items


# ---------------------------------------------------------------------------
# 1337x (direct FlareSolverr scrape)
# ---------------------------------------------------------------------------

FLARE_URL      = os.getenv("FLARESOLVERR_HOST", "http://localhost:8191")
LEET_BASE      = "https://1337x.to"
LEET_CATEGORIES = {
    "Movies": f"{LEET_BASE}/cat/Movies/1/",
    "Series": f"{LEET_BASE}/cat/TV/1/",
    "Anime":  f"{LEET_BASE}/cat/Anime/1/",
}

def _flare_get(url: str, timeout_ms: int = 40000) -> str:
    r = requests.post(f"{FLARE_URL}/v1", json={
        "cmd": "request.get", "url": url, "maxTimeout": timeout_ms,
    }, timeout=timeout_ms // 1000 + 10)
    sol = r.json().get("solution", {})
    if sol.get("status") != 200:
        raise Exception(f"FlareSolverr HTTP {sol.get('status')}")
    return sol.get("response", "")

def fetch_1337x(pages: int = 1, max_per_cat: int = 20) -> list[dict]:
    items = []
    seen  = set()

    for category, base_url in LEET_CATEGORIES.items():
        cat_items = []
        for page in range(1, pages + 1):
            page_url = re.sub(r'/\d+/$', f'/{page}/', base_url)
            try:
                html  = _flare_get(page_url)
                links = list(dict.fromkeys(re.findall(r'href="(/torrent/\d+/[^"]+)"', html)))
                for link in links[:max_per_cat]:
                    detail_url = f"{LEET_BASE}{link}"
                    try:
                        detail = _flare_get(detail_url)
                        m = re.search(r'magnet:\?xt=urn:btih:([a-fA-F0-9]{40})', detail, re.I)
                        if not m:
                            continue
                        h = m.group(1).lower()
                        if h in seen:
                            continue
                        seen.add(h)
                        t = re.search(r'<h1[^>]*>([^<]+)</h1>', detail)
                        raw_name = t.group(1).strip() if t else link.split("/")[-2].replace("-", " ")
                        cat_items.append({
                            "hash":     h,
                            "title":    "",
                            "raw_name": raw_name,
                            "category": category,
                            "imdb_id":  "",
                            "tmdb_id":  "",
                            "mal_id":   "",
                            "seeders":  0,
                            "trackers": ["1337x"],
                        })
                        time.sleep(0.3)
                    except Exception:
                        time.sleep(0.3)
            except Exception as e:
                print(f"  1337x {category} page {page} error: {e}")
            time.sleep(0.3)

        print(f"  {category}: {len(cat_items)} items")
        items += cat_items

    return items


# ---------------------------------------------------------------------------
# TorrentGalaxy (via Jackett torrentgalaxyclone indexer)
# ---------------------------------------------------------------------------

TGX_INDEXER = "torrentgalaxyclone"
TGX_CATS    = {"Movies": "2000", "Series": "5000", "Anime": "5070"}

def _tgx_hash_from_link(download_url: str) -> str:
    """Follow Jackett proxy link — Jackett redirects to magnet, extract hash."""
    try:
        r = requests.get(download_url, timeout=30, allow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308):
            m = re.search(r'btih:([a-fA-F0-9]{40})', r.headers.get("location", ""), re.I)
            return m.group(1).lower() if m else ""
    except Exception:
        pass
    return ""

def fetch_torrentgalaxy() -> list[dict]:
    if not JACKETT_KEY:
        print("  JACKETT_API_KEY not set — skipping")
        return []

    items = []
    seen  = set()

    for category, cat_id in TGX_CATS.items():
        url = f"{JACKETT_HOST}/api/v2.0/indexers/{TGX_INDEXER}/results/torznab/api"
        params = {"apikey": JACKETT_KEY, "t": "search", "q": "", "cat": cat_id, "limit": 50}
        try:
            import xml.etree.ElementTree as ET
            r    = requests.get(url, params=params, timeout=60)
            root = ET.fromstring(r.content)
            raw_items = root.findall(".//item")
        except Exception as e:
            print(f"  TorrentGalaxy {category} error: {e}")
            continue

        cat_count = 0
        for item in raw_items:
            title   = item.findtext("title", "")
            dl_link = item.findtext("link", "")
            seeders_el = item.find(".//{urn:torznab:schema}attr[@name='seeders']")
            seeders    = int(seeders_el.get("value", 0)) if seeders_el is not None else 0

            h = _tgx_hash_from_link(dl_link)
            if not h or h in seen:
                continue
            seen.add(h)

            items.append({
                "hash":     h,
                "title":    "",
                "raw_name": title,
                "category": category,
                "imdb_id":  "",
                "tmdb_id":  "",
                "mal_id":   "",
                "seeders":  seeders,
                "trackers": ["torrentgalaxy"],
            })
            cat_count += 1
            time.sleep(0.1)

        print(f"  {category}: {cat_count} items")

    return items


# ---------------------------------------------------------------------------
# AnimeTosho (Anime RSS — hashes in magnet links inside description)
# ---------------------------------------------------------------------------

ANIMETOSHO_RSS = "https://feed.animetosho.org/rss2"

def fetch_animetosho() -> list[dict]:
    import xml.etree.ElementTree as ET
    import base64

    items = []
    seen  = set()
    try:
        r    = requests.get(ANIMETOSHO_RSS, headers=HEADERS, timeout=15)
        root = ET.fromstring(r.content)
    except Exception as e:
        print(f"  AnimeTosho fetch error: {e}")
        return []

    for item in root.findall(".//item"):
        title = item.findtext("title", "").strip()
        desc  = item.findtext("description", "")

        m = re.search(r'btih:([a-fA-F0-9]{40})', desc, re.I)
        if m:
            h = m.group(1).lower()
        else:
            m32 = re.search(r'btih:([A-Z2-7]{32})', desc)
            if not m32:
                continue
            try:
                h = base64.b32decode(m32.group(1)).hex()
            except Exception:
                continue

        if not h or len(h) != 40 or h in seen:
            continue
        seen.add(h)
        items.append({
            "hash":     h,
            "title":    "",
            "raw_name": title,
            "category": "Anime",
            "imdb_id":  "",
            "tmdb_id":  "",
            "mal_id":   "",
            "seeders":  0,
            "trackers": ["animetosho"],
        })

    return items


# ---------------------------------------------------------------------------
# Nyaa.si (Anime RSS)
# ---------------------------------------------------------------------------

NYAA_FEEDS = [
    "https://nyaa.si/?page=rss&c=1_2&f=0",  # Anime - English-translated
    "https://nyaa.si/?page=rss&c=1_0&f=0",  # Anime - All
]

def fetch_nyaa() -> list[dict]:
    import xml.etree.ElementTree as ET

    items = []
    seen  = set()
    for url in NYAA_FEEDS:
        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            root = ET.fromstring(r.content)
            ns   = {"nyaa": "https://nyaa.si/xmlns/nyaa"}
            for item in root.findall(".//item"):
                h = (item.findtext("nyaa:infoHash", namespaces=ns) or "").strip().lower()
                if not h or len(h) != 40 or h in seen:
                    continue
                seen.add(h)
                raw_name = (item.findtext("title") or "").strip()
                seeders  = int(item.findtext("nyaa:seeders", "0", namespaces=ns) or 0)
                items.append({
                    "hash":     h,
                    "title":    "",
                    "raw_name": raw_name,
                    "category": "Anime",
                    "imdb_id":  "",
                    "tmdb_id":  "",
                    "seeders":  seeders,
                    "trackers": ["nyaa.si"],
                })
        except Exception as e:
            print(f"  Nyaa feed error ({url}): {e}")
    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-jackett",   action="store_true")
    parser.add_argument("--skip-flare",     action="store_true")
    parser.add_argument("--skip-1337x",     action="store_true")
    parser.add_argument("--skip-bitmagnet", action="store_true")
    parser.add_argument("--skip-nyaa",      action="store_true")
    parser.add_argument("--skip-animetosho", action="store_true")
    parser.add_argument("--skip-eztv",      action="store_true")
    parser.add_argument("--skip-tgx",       action="store_true")
    parser.add_argument("--skip-enrich",    action="store_true",
                        help="Skip TMDB enrichment step")
    parser.add_argument("--min-seeders",    type=int, default=0)
    args = parser.parse_args()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    init_db(db)

    total_before = db.execute("SELECT COUNT(*) FROM hashes").fetchone()[0]
    print(f"\n{'='*55}")
    print(f"Hash Collector — {date.today()}")
    print(f"DB: {DB_PATH}  ({total_before:,} hashes so far)")
    print(f"{'='*55}")

    all_items: list[dict] = []

    if not args.skip_jackett:
        print("\n[Jackett]")
        items = fetch_jackett()
        print(f"  → {len(items)} items")
        all_items += items

    if not args.skip_1337x and not args.skip_flare:
        print("\n[1337x — direct FlareSolverr scrape]")
        try:
            requests.get(f"{FLARE_URL}/health", timeout=3).json()
            items = fetch_1337x(pages=1, max_per_cat=20)
            print(f"  → {len(items)} total items")
            all_items += items
        except Exception:
            print("  FlareSolverr not available — skipping")

    if not args.skip_eztv:
        print("\n[EZTV — Series]")
        items = fetch_eztv(pages=3)
        print(f"  → {len(items)} items")
        all_items += items

    if not args.skip_animetosho:
        print("\n[AnimeTosho — Anime]")
        items = fetch_animetosho()
        print(f"  → {len(items)} anime items")
        all_items += items

    if not args.skip_nyaa:
        print("\n[Nyaa.si — Anime]")
        items = fetch_nyaa()
        print(f"  → {len(items)} anime items")
        all_items += items

    if not args.skip_tgx:
        print("\n[TorrentGalaxy — Movies + Series + Anime]")
        items = fetch_torrentgalaxy()
        print(f"  → {len(items)} total items")
        all_items += items

    if not args.skip_bitmagnet:
        print("\n[BitMagnet DHT — Movies + Series only]")
        items = fetch_bitmagnet(min_seeders=max(args.min_seeders, 1))
        print(f"  → {len(items)} items (software/other filtered out)")
        all_items += items

    if args.min_seeders > 0:
        before = len(all_items)
        all_items = [i for i in all_items if i.get("seeders", 0) >= args.min_seeders]
        print(f"\nFiltered to {args.min_seeders}+ seeders: {before} → {len(all_items)}")

    print(f"\n[Saving] {len(all_items)} items → {DB_PATH.name}")
    inserted, updated = upsert(db, all_items)

    # TMDB enrichment — fill missing titles + IMDB IDs (Movies + Series)
    if not args.skip_enrich and TMDB_KEY:
        print("\n[TMDB Enrichment]")
        enrich_missing(db)
    elif not TMDB_KEY:
        print("\n  TMDB_API_KEY not set — skipping enrichment")

    # MAL enrichment — fill missing titles + MAL IDs (Anime)
    if not args.skip_enrich:
        print("\n[MAL Enrichment — Anime]")
        enrich_anime(db)

    total_after = db.execute("SELECT COUNT(*) FROM hashes").fetchone()[0]
    print(f"\n{'='*55}")
    print(f"  New inserted  : {inserted:,}")
    print(f"  Updated       : {updated:,}")
    print(f"  Total in DB   : {total_after:,}  (+{total_after - total_before:,})")

    for cat in ("Movies", "Series"):
        n     = db.execute("SELECT COUNT(*) FROM hashes WHERE category=?", (cat,)).fetchone()[0]
        n_id  = db.execute("SELECT COUNT(*) FROM hashes WHERE category=? AND imdb_id!=''", (cat,)).fetchone()[0]
        print(f"  {cat:<8}: {n:,} hashes  ({n_id:,} with IMDB ID)")

    top = db.execute("""
        SELECT title, raw_name, seeders, category, imdb_id
        FROM hashes WHERE category IN ('Movies','Series')
        ORDER BY seeders DESC LIMIT 15
    """).fetchall()
    print("\nTop 15 Movies + Series by seeders:")
    print(f"  {'Seeders':>7}  {'Cat':<8}  {'IMDB':<12}  {'Title'}")
    print("  " + "-"*75)
    for title, raw_name, seeders, cat, imdb in top:
        display = title or raw_name or ""
        print(f"  {seeders:>7,}  {cat:<8}  {(imdb or '-'):<12}  {display[:50]}")

    db.close()


if __name__ == "__main__":
    main()
