#!/usr/bin/env python3
"""
trending_hash_collector.py
--------------------------
Fetches trending torrents from TPB top100, EZTV, Nyaa RSS, and YTS.
Maps each torrent to an ip_id using the full local parquet catalog
(movies=41K, series=17K, anime=28K).
Upserts matched hashes into hashes_v2.db.

Sources:
  - TPB cat 207 (Movies HD)
  - TPB cat 208 (TV HD)
  - EZTV API   (TV episodes)
  - Nyaa RSS   (Anime)
  - YTS API    (Movies — IMDB ID direct match, no fuzzy needed)

Usage:
  python trending_hash_collector.py [--dry-run] [--verbose]
"""

import argparse
import json
import logging
import os
import re
import socket
import sqlite3
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path

import PTN
import pandas as pd

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
DATA_DIR   = BASE_DIR / "data"
DB_PATH    = DATA_DIR / "hashes_v2.db"

# Catalog parquets — on EC2 downloaded from S3 to /data/catalog/,
# fall back to local data/ dir for development.
# S3 source: s3://YOUR_UI_S3_BUCKET/platform_data/
_CATALOG_DIR = Path("/data/catalog") if Path("/data/catalog").exists() else DATA_DIR
MOVIE_PQ   = _CATALOG_DIR / "movies_info.parquet"
SERIES_PQ  = _CATALOG_DIR / "series_info.parquet"
ANIME_PQ   = _CATALOG_DIR / "anime_info.parquet"

# ─── Config ───────────────────────────────────────────────────────────────────
MATCH_THRESHOLD = 0.82      # minimum similarity score to accept a title match
MAX_EZTV_PAGES  = 3         # pages × 100 = up to 300 EZTV torrents
HEADERS         = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

SOURCE_TPB_MOVIES = "tpb_top100"
SOURCE_TPB_TV     = "tpb_top100"
SOURCE_EZTV       = "eztv"
SOURCE_NYAA       = "nyaa"
SOURCE_SUBSPLEASE = "subsplease"
SOURCE_YTS        = "yts"
SOURCE_TMDB       = "tmdb"
SOURCE_ANILIST    = "anilist"
SOURCE_TGX        = "torrentgalaxy"

YTS_DOMAIN        = "yts.am"   # fallback: yts.mx is blocked in some regions
YTS_PAGES         = 3          # 3 pages × 50 = 150 movies, each with 2-3 quality hashes

TMDB_API_KEY      = os.environ.get("TMDB_API_KEY", "")
TMDB_WORKERS      = 8          # concurrent external_ids fetches
TMDB_RATE_SLEEP   = 0.25       # seconds between batches to stay under 40 req/10s
TMDB_PAGES        = 25         # 25 × 20 = 500 titles per category
# Wall-clock deadlines for the concurrent gather steps. A single wedged worker
# (e.g. a CLOSE-WAIT socket on a slow-drip read that never trips the per-socket
# timeout) used to hang the whole --tmdb run forever, because the implicit
# ThreadPoolExecutor context-manager exit does shutdown(wait=True). These bound
# each gather; stragglers are abandoned and we proceed with partial results.
# Worst case 2*ENRICH + 2*SEARCH ≈ 1320s, under the cron `timeout 1500` guard.
TMDB_ENRICH_DEADLINE = int(os.environ.get("TMDB_ENRICH_DEADLINE", "240"))
TMDB_SEARCH_DEADLINE = int(os.environ.get("TMDB_SEARCH_DEADLINE", "420"))

ANILIST_PAGES     = 10         # 10 × 50 = 500 anime
NYAA_SEARCH_DELAY = 0.4        # seconds between Nyaa title searches

# ─── Jackett (gateway to TorrentGalaxy etc.) — broadens MOVIE coverage ───────
# Movies are under-covered by YTS+apibay alone (median ~4 hashes/title vs ~63 for
# series). TorrentGalaxy carries movie torrents the others miss; we reach it via
# the Jackett torznab API already running on this host (same indexer collect.py
# uses). Key loaded from .env in main(); never logged.
JACKETT_HOST           = os.environ.get("JACKETT_HOST", "http://localhost:9117")
JACKETT_API_KEY        = os.environ.get("JACKETT_API_KEY", "")
JACKETT_MOVIE_INDEXERS = ["torrentgalaxyclone"]  # proven movie source beyond YTS/apibay
JACKETT_CAT_MOVIES     = "2000"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _fetch(url: str, retries: int = 3, delay: float = 2.0) -> bytes:
    import time
    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.read()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(delay)
    raise last_err


# Patterns that identify unmappable multi-title torrents (box sets, packs, complete series).
# These cannot be linked to a single ip_id so we skip them at ingest.
# NOTE: "Criterion Collection" is a single-film label — explicitly excluded.
_COLLECTION_PATTERNS = [
    re.compile(r'\b\d+\s+to\s+\d+\s+films?\b', re.I),       # "1 to 6 Films"
    re.compile(r'\bfilms?\s+\d+[-–]\d+\b', re.I),            # "Films 1-6"
    re.compile(r'\b\d+\s+films?\b', re.I),                    # "6 Films"
    re.compile(r'\bcomplete\s+series\b', re.I),
    re.compile(r'\bcomplete\s+seasons?\b', re.I),
    re.compile(r'\bseasons?\s+\d+[-–]\d+\b', re.I),           # "Seasons 1-5" / "S01-S03"
    re.compile(r'\bs\d{2}[-–]s?\d{2}\b', re.I),               # "S01-S03"
    re.compile(r'\btrilogy\b', re.I),
    re.compile(r'\bquadrilogy\b', re.I),
    re.compile(r'\bpentalogy\b', re.I),
    re.compile(r'\bbox\s+set\b', re.I),
    re.compile(r'\ball\s+\d+\s+(episodes?|films?|movies?)\b', re.I),
    re.compile(r'\d{4}[-–]\d{4}\b'),                          # year range "1984-2019"
    re.compile(r'\b\d+\s*[-–&+]\s*\d+\s*(pack|film|movie)\b', re.I),  # "4-film pack"
    # "Collection" only when NOT preceded by "Criterion"
    re.compile(r'(?<!criterion )\bcollection\b', re.I),
]

def is_collection(raw_name: str) -> bool:
    """Returns True if torrent is a multi-title pack/box-set that can't map to one ip_id."""
    return any(p.search(raw_name) for p in _COLLECTION_PATTERNS)


# ─── Layer 1: Ambiguous title detection ───────────────────────────────────────
# Titles that are short, common English words, acronyms, or contain only
# stop-words cannot be safely matched by fuzzy score alone — year is required.
#
# Three ambiguity classes:
#   A. All-stop-word titles      → "Another", "It", "Up", "You"
#   B. Known ambiguous singles   → "One", "Last", "New" (appear in many titles)
#   C. Short / acronym titles    → "CIA", "FBI", "M.I.A.", "X", "Go"
#      (≤ 4 chars after stripping dots/spaces, OR all-uppercase acronym pattern)
#
# For all ambiguous titles:
#   • Nyaa search appends the year to narrow results
#   • Main matcher REQUIRES a PTN year in the torrent name
#   • Year tolerance tightened to ±0 (exact match required)

_TITLE_SW = {"the","a","an","of","in","and","to","is","for","on","with","at","by",
             "from","or","another","one","two","three","new","last","first","next",
             "my","me","you","us","him","her","it","its","this","that","your","our"}

_KNOWN_AMBIGUOUS_SOLO = {"another", "one", "new", "last", "first", "next", "you",
                          "it", "up", "go", "run", "cut", "hit", "war", "raw"}

# Acronym pattern: 2-5 uppercase letters optionally separated by dots/dashes
_ACRONYM_RE = re.compile(r'^[A-Z]{2,5}$|^([A-Z]\.){2,}[A-Z]?$|^[A-Z]{2,3}-[A-Z]{1,3}$')

def is_ambiguous_title(title: str) -> bool:
    """
    Returns True when a title requires year confirmation to match safely.

    Class A — all stop-words:        "Another", "It", "Up", "You"
    Class B — known ambiguous solo:  "One", "Last", "War", "Cut"
    Class C — short / acronym:       "CIA", "FBI", "M.I.A.", "X", "Go"

    Distinctive titles like "Bleach", "Demon Slayer", "Attack on Titan" → False.
    """
    stripped = title.strip()

    # Class C: acronym pattern on original (pre-normalize) title
    # e.g. "CIA", "FBI", "M.I.A.", "NCIS"
    acronym_candidate = stripped.replace(".", "").replace("-", "").replace(" ", "")
    if _ACRONYM_RE.match(stripped) or (acronym_candidate.isupper() and len(acronym_candidate) <= 5):
        return True

    # Class C: very short title (≤ 3 chars after stripping punctuation/spaces)
    core = re.sub(r"[^a-zA-Z0-9]", "", stripped)
    if len(core) <= 3:
        return True

    norm = _normalize(stripped)
    words = norm.split()
    sig = [w for w in words if w not in _TITLE_SW and not w.isdigit()]

    # Class A: all stop-words
    if len(sig) == 0:
        return True

    # Class B: exactly one significant word AND it's a known ambiguous word
    if len(sig) == 1 and sig[0] in _KNOWN_AMBIGUOUS_SOLO:
        return True

    return False


# ─── Layer 3: Post-ingest audit ───────────────────────────────────────────────

_AUDIT_SW = {"the","a","an","of","in","and","to","is","for","on","with","at","by","from","or"}
_AUDIT_TECH = re.compile(
    r"^\d+$|^s\d+e\d+$|^e\d+$|^ep\d+$|"
    r"^1080p$|^720p$|^480p$|^2160p$|^4k$|"
    r"^hevc$|^avc$|^h264$|^h265$|^x264$|^x265$|"
    r"^aac$|^ac3$|^flac$|^opus$|"
    r"^web$|^dl$|^webrip$|^webdl$|^bluray$|^bd$|^bdrip$|"
    r"^hdr$|^sdr$|^10bit$|^8bit$|^multi$|^dual$|^subs$"
)

def _title_words(title: str) -> set[str]:
    """Significant words from a title (no stop-words, no tech tokens)."""
    words = _normalize(title).split()
    return {w for w in words if w not in _AUDIT_SW and not _AUDIT_TECH.match(w)}


def _strip_release_group(raw_name: str) -> str:
    """Remove the trailing scene release-group tag (and any file extension).

    Scene convention puts the encoder/group last, after a final hyphen, with no
    spaces — e.g. 'x265-ELiTE', '-NTb', 'H264-SuccessfulCrab'. Without this, the
    group token leaks into the word set and a short title can match on it: the
    'ELiTE' in 'Widows.Bay.S01E04.1080p.x265-ELiTE' was matching the show 'Elite'
    and lumping unrelated shows under it.

    Regression-safe: it only DROPS a trailing token, so it can never invent a
    match — at worst it makes matching slightly stricter. A mid-name hyphen in a
    real title (e.g. 'Spider-Noir') is untouched because only the final '-token'
    at the very end of the string is removed.
    """
    n = (raw_name or "").strip()
    n = re.sub(r"\.(mkv|mp4|avi|ts|m4v|mov|srt)$", "", n, flags=re.IGNORECASE)
    n = re.sub(r"-[A-Za-z0-9]{2,}$", "", n)   # trailing -GROUP
    return n


def _name_matches_title(raw_name: str, title: str,
                        year: str = "", category: str = "") -> bool:
    """Guard for title-only searches (apibay/TPB, Jackett/TorrentGalaxy).

    Default: require >=60% of the title's significant words to appear in the
    torrent name, to avoid false positives.

    Movie-only loosening: for category=='Movies' a matching release year is a
    strong corroborating signal, so a >=40% word overlap that ALSO carries the
    correct year (±1, parsed from the torrent name) is accepted. This recovers
    subtitle-dropping torrents (e.g. 'Movie: The Subtitle (2025)' listed simply
    as 'Movie 2025') without opening the door to unrelated films, because the
    year must line up. Series keep the strict 60% — for TV the PTN year is the
    episode air-date, not a reliable title signal."""
    tw = _title_words(title)
    if not tw:
        return False
    # Confident year-mismatch reject (2026-06-09): for MOVIES, if the catalog has a
    # clean 4-digit year AND the torrent name carries a parseable year that differs by
    # >1, it is a DIFFERENT film — a remake or another entry in the franchise — so
    # reject regardless of word overlap. This splits two cases the word-overlap rule
    # cannot: (a) same-title different-year pairs ("Masters of the Universe" 1987 vs
    # 2026, "Michael" 1996 vs 2026), and (b) franchise NUMBERS that _AUDIT_TECH strips
    # from both sides ("Toy Story 5" vs "Toy Story 3", "Despicable Me 4" vs "…2",
    # "Inside Out 2" vs "Inside Out") — there the title words are identical and only the
    # year distinguishes them. Conservative on purpose: fires ONLY when BOTH years are
    # present and clearly differ, so a missing/unparseable year just falls through (this
    # is what avoids the false-reject blowup — read-only scan: a hard year-gate flagged
    # 88%, this confident-mismatch rule flags 9.4% and every sampled reject is genuine).
    # Movies only — a series' PTN year is the episode air-date, not a title signal.
    if category == "Movies" and year and str(year).isdigit() and len(str(year)) == 4:
        _, _ptn_year = parse_torrent_name(raw_name or "")
        if _ptn_year and str(_ptn_year).isdigit() and abs(int(_ptn_year) - int(year)) > 1:
            return False
    rw = {w for w in _normalize(_strip_release_group(raw_name)).split() if not _AUDIT_TECH.match(w)}
    present = len(tw & rw)
    overlap = present / len(tw)
    n = len(tw)
    # Short-title MOVIE guard (2026-06-09): a 1-2 significant-word movie title is
    # dangerously generic ("Michael", "Beast", "Dracula", "Mermaid") — under the
    # length-scaled rule below it matches ANY torrent that merely CONTAINS the word,
    # so "Michael Clayton (2007)", "Beast Of War", "The Little Mermaid", sequels like
    # "Resident Evil: Apocalypse", and even an actor's name ("…Michael B. Jordan…")
    # all wrongly land on the wrong film. Fix: require the torrent's PARSED TITLE
    # (PTN strips quality/codec/group/year/cast) to carry the SAME significant words
    # as the catalog title — i.e. no extra distinguishing word. Scoped to Movies with
    # n<=2; series (episode structure), anime, and longer movie titles keep the logic
    # below. Year-INDEPENDENT: PTN year parsing is too unreliable on scene names
    # ("Michael.2026.HDTS…Dual.YG⭐") and many catalog movies have no year, so gating
    # on year produced massive false rejects (read-only scan 2026-06-09: 88% vs 34.5%).
    # KNOWN COST (small): foreign-language original titles that share no English words
    # (e.g. "Culpa nuestra" == "Our Fault", "8-ban deguchi" == "Exit 8") are dropped;
    # acceptable, fixable later via a title-alias table.
    if category == "Movies" and n <= 2:
        ptn_title, _ = parse_torrent_name(raw_name or "")
        return _title_words(ptn_title or "") == tw
    # Length-scaled requirement (2026-06-08 franchise-variant fix): for SHORT titles
    # every significant word is distinguishing — the "4d" in "One Piece 4D", the "usa"
    # in "Love Island USA" — so a base-franchise torrent must NOT clear the bar for a
    # qualified sibling on the shared words alone. The old flat 60% let "One Piece - 1164"
    # match "One Piece 4D" at 2/3=67% (ep-1164 demand landed on anime-54196 not anime-21).
    # Only the 3-word case actually tightens (was: need 2 of 3 → now: need all 3); n<=2,
    # n==4 and n>=5 keep their previous effective thresholds.
    # LIMITATION: a purely-numeric qualifier ("Jujutsu Kaisen 0", "Avatar 2") is stripped
    # by _AUDIT_TECH from both sides, so this does not disambiguate those (tracked in TODO).
    if n <= 3:
        ok = (present == n)            # all significant words
    elif n == 4:
        ok = (present >= 3)            # allow one drop (== old 0.60 for n=4)
    else:
        ok = (overlap >= 0.60)         # unchanged for longer titles
    if ok:
        return True
    if category == "Movies" and year and len(str(year)) == 4 and overlap >= 0.40:
        _, ptn_year = parse_torrent_name(raw_name or "")
        if ptn_year and ptn_year.isdigit() and abs(int(ptn_year) - int(year)) <= 1:
            return True
    return False


def post_ingest_audit(conn: sqlite3.Connection, new_ip_ids: set[str]) -> list[str]:
    """
    After inserting hashes, check each newly-touched ip_id for two problems:

    1. Title-word consistency: what fraction of hashes share at least one
       significant word with the canonical title?  Flag if < 60%.

    2. Year-spread consistency: if hashes carry explicit years (from PTN), do
       they cluster around the canonical year?  Flag if multiple distinct years
       span > 3 years AND more than one year cluster is present — this catches
       the "The Visitor 1979/2022/2024 all mapped to The Visitor 2008" pattern.

    Writes flagged entries to data/suspicious_matches.log.
    Returns list of flagged ip_ids.
    """
    if not new_ip_ids:
        return []

    audit_log = DATA_DIR / "suspicious_matches.log"
    flagged: list[str] = []
    today = str(date.today())

    for ip_id in sorted(new_ip_ids):
        # Fetch canonical title + known year
        row = conn.execute(
            "SELECT title, release_year FROM titles WHERE ip_id = ?", (ip_id,)
        ).fetchone()
        if not row:
            continue
        canonical_title, canonical_year = row[0], row[1]
        canon_words = _title_words(canonical_title)

        # Fetch all hashes for this ip_id
        hashes = conn.execute(
            "SELECT raw_name FROM hashes WHERE ip_id = ?", (ip_id,)
        ).fetchall()
        if len(hashes) < 3:
            continue  # too few hashes to be statistically meaningful

        # ── Check 1: title-word consistency ──────────────────────────────────
        title_fail = False
        if canon_words:
            hits = 0
            misses_sample: list[str] = []
            for (raw_name,) in hashes:
                raw_name = raw_name or ""
                raw_words = {w for w in _normalize(_strip_release_group(raw_name)).split()
                             if not _AUDIT_TECH.match(w)}
                if canon_words & raw_words:
                    hits += 1
                else:
                    if len(misses_sample) < 3:
                        misses_sample.append((raw_name or "")[:80])

            consistency = hits / len(hashes)
            if consistency < 0.60:
                title_fail = True
                msg = (
                    f"[{today}] AUDIT FAIL (title)  ip_id={ip_id}  "
                    f"title='{canonical_title}'  "
                    f"consistency={consistency:.0%}  ({hits}/{len(hashes)} hashes match)\n"
                )
                for s in misses_sample:
                    msg += f"           mismatch: {s}\n"
                log.warning(msg.rstrip())
                with open(audit_log, "a") as f:
                    f.write(msg + "\n")
                flagged.append(ip_id)

        # ── Check 2: year-spread consistency ─────────────────────────────────
        # Only meaningful for Movies where the PTN year = the film's release year.
        # For TV series the PTN year is the episode air date — a 2025 episode of
        # Jeopardy (premiere 1984) is correct, NOT a mismatch.
        # Anime entries have no catalog year at all — skip year check for both.
        if not title_fail and ip_id.startswith("film-"):
            parsed_years: list[int] = []
            year_mismatches: list[str] = []
            for (raw_name,) in hashes:
                _, ptn_year = parse_torrent_name(raw_name or "")
                if ptn_year and ptn_year.isdigit():
                    parsed_years.append(int(ptn_year))

            if parsed_years and canonical_year:
                try:
                    canon_yr_int = int(canonical_year)
                    wrong_year = [y for y in parsed_years
                                  if abs(y - canon_yr_int) > 2]
                    wrong_frac = len(wrong_year) / len(parsed_years)
                    if wrong_frac >= 0.30:  # >30% of dated hashes have wrong year
                        # Collect sample of offending raw names
                        for (raw_name,) in hashes:
                            raw_name = raw_name or ""
                            _, ptn_year = parse_torrent_name(raw_name)
                            if ptn_year and ptn_year.isdigit():
                                if abs(int(ptn_year) - canon_yr_int) > 2:
                                    if len(year_mismatches) < 3:
                                        year_mismatches.append(
                                            f"{raw_name[:70]}  [year={ptn_year}]"
                                        )
                        msg = (
                            f"[{today}] AUDIT FAIL (year)   ip_id={ip_id}  "
                            f"title='{canonical_title}'  canonical_year={canonical_year}  "
                            f"wrong_year_hashes={len(wrong_year)}/{len(parsed_years)}\n"
                        )
                        for s in year_mismatches:
                            msg += f"           wrong year: {s}\n"
                        log.warning(msg.rstrip())
                        with open(audit_log, "a") as f:
                            f.write(msg + "\n")
                        if ip_id not in flagged:
                            flagged.append(ip_id)
                except ValueError:
                    pass

    if flagged:
        log.warning(
            f"⚠️  Post-ingest audit: {len(flagged)} suspicious ip_id(s) — "
            f"check {audit_log.name} for details"
        )
    else:
        log.info("✅ Post-ingest audit: all ip_ids look consistent")

    return flagged


def _normalize(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    if not isinstance(text, str):
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_torrent_name(raw: str) -> tuple[str, str | None]:
    """
    Returns (clean_title, year_or_None).
    Uses parse-torrent-title (PTN) for accurate title extraction —
    handles dots-as-spaces, 'Season 2', codec/quality tags, release groups, etc.
    """
    parsed = PTN.parse(raw)
    title  = (parsed.get("title") or "").strip()
    year   = str(parsed.get("year")) if parsed.get("year") else None
    return title, year



# ─── Catalog loader ───────────────────────────────────────────────────────────

class Catalog:
    """
    Loads ip_id + title from all three parquets.
    Provides fuzzy title matching per category.
    """

    def __init__(self):
        log.info("Loading parquet catalog…")
        movies   = pd.read_parquet(MOVIE_PQ,  columns=["ip_id", "ip", "imdb_id", "release_date"])
        series   = pd.read_parquet(SERIES_PQ, columns=["ip_id", "ip", "imdb_id", "release_date"])
        anime_df = pd.read_parquet(ANIME_PQ,  columns=["ip_id", "ip"])
        anime_df["imdb_id"]     = None
        anime_df["release_date"] = None

        movies["category"]   = "Movies"
        series["category"]   = "Series"
        anime_df["category"] = "Anime"

        df = pd.concat([movies, series, anime_df], ignore_index=True)
        df = df.rename(columns={"ip": "title"})
        # Extract 4-digit year from release_date (e.g. "2001-12-19" → "2001")
        df["year"] = df["release_date"].astype(str).str[:4].where(
            df["release_date"].notna(), None
        )
        df["title_norm"] = df["title"].apply(_normalize)

        self.df = df

        # Build imdb_id → (ip_id, title, category) lookup
        self.imdb_map: dict[str, tuple] = {}
        for _, row in df[df["imdb_id"].notna()].iterrows():
            iid = str(row["imdb_id"]).strip()
            if iid and iid not in self.imdb_map:
                self.imdb_map[iid] = (row["ip_id"], row["title"], row["category"])

        # Category-partitioned for faster search
        self.by_cat = {
            cat: sub.reset_index(drop=True)
            for cat, sub in df.groupby("category")
        }

        # ── Word inverted index for fast candidate pre-filtering ──────────────
        # Maps normalized word → list of (df_index, category)
        # Stopwords excluded so common words don't flood candidates
        _STOPWORDS = {"the", "a", "an", "of", "in", "and", "to", "is",
                      "for", "on", "with", "at", "by", "from", "or"}
        self._word_idx: dict[str, list[int]] = defaultdict(list)
        self._records: list[tuple] = []  # (ip_id, title, category, title_norm, year, imdb_id)

        for i, row in df.iterrows():
            idx = len(self._records)
            yr     = str(row["year"])[:4] if row.get("year") and str(row["year"]) != "None" else None
            imdb   = str(row["imdb_id"]).strip() if row.get("imdb_id") and str(row["imdb_id"]) != "None" else None
            self._records.append((row["ip_id"], row["title"], row["category"], row["title_norm"], yr, imdb))
            words = set(row["title_norm"].split()) - _STOPWORDS
            for w in words:
                self._word_idx[w].append(idx)

        # Exact normalized title → {category: index} for O(1) category-aware lookup
        self._exact: dict[str, dict[str, int]] = defaultdict(dict)
        for i, (ip_id, title, cat, norm_t, yr, imdb) in enumerate(self._records):
            if cat not in self._exact[norm_t]:
                self._exact[norm_t][cat] = i

        # IMDB ID → record index for direct lookup (overrides fuzzy entirely)
        self._imdb_idx: dict[str, int] = {}
        for i, (ip_id, title, cat, norm_t, yr, imdb) in enumerate(self._records):
            if imdb and imdb not in self._imdb_idx:
                self._imdb_idx[imdb] = i

        # ip_id → reference ids (imdb_id, mal_id) — authoritative back-reference.
        # mal_id is derived from the anime ip_id scheme (anime-<malid>).
        self.by_ipid: dict[str, dict] = {}
        for ip_id, title, cat, norm_t, yr, imdb in self._records:
            if ip_id and ip_id not in self.by_ipid:
                mal = ip_id.split("-", 1)[1] if ip_id.startswith("anime-") else None
                clean_imdb = imdb if (imdb and imdb not in ("nan", "None", "")) else None
                self.by_ipid[ip_id] = {"imdb_id": clean_imdb, "mal_id": mal}

        log.info(
            f"Catalog: {len(movies):,} movies  {len(series):,} series  "
            f"{len(anime_df):,} anime  |  {len(self.imdb_map):,} with imdb_id  "
            f"|  word-index: {len(self._word_idx):,} tokens"
        )

    def find_by_imdb(self, imdb_id: str) -> tuple | None:
        return self.imdb_map.get(imdb_id.strip())

    def find_by_mal(self, mal_id) -> str | None:
        """Return the catalog's anime ip_id for a MAL id, or None if Pantheon
        doesn't have it. ip_id is Pantheon's identifier — never minted here."""
        ip_id = f"anime-{str(mal_id).strip()}"
        return ip_id if ip_id in self.by_ipid else None

    def fuzzy_match(
        self,
        title: str,
        category: str,
        year: str | None = None,
        threshold: float = MATCH_THRESHOLD,
        imdb_id: str | None = None,
    ) -> tuple | None:
        """
        Returns (ip_id, catalog_title, category) or None.
        Uses word-index to pre-filter candidates before SequenceMatcher.

        If imdb_id is provided, try a direct _imdb_idx lookup first —
        this guarantees correct disambiguation (e.g. LOTR parts, sequels).
        Year is used as a tiebreaker when multiple candidates share the same title.
        """
        _STOPWORDS = {"the", "a", "an", "of", "in", "and", "to", "is",
                      "for", "on", "with", "at", "by", "from", "or"}

        # 0. IMDB direct lookup — highest confidence, skips all fuzzy logic
        if imdb_id:
            clean_imdb = imdb_id.strip()
            if clean_imdb in self._imdb_idx:
                ip_id, t, cat, norm_t, yr, imdb = self._records[self._imdb_idx[clean_imdb]]
                return (ip_id, t, cat)

        norm = _normalize(title)

        # 1. Exact match — O(1), category-aware
        if norm in self._exact:
            cat_map = self._exact[norm]
            # Prefer same category, fall back to any
            chosen_idx = cat_map.get(category) or next(iter(cat_map.values()))
            ip_id, t, cat, norm_t, yr, imdb = self._records[chosen_idx]
            return (ip_id, t, cat)

        # 2. Word-index candidate lookup
        query_words = set(norm.split()) - _STOPWORDS
        if not query_words:
            return None

        candidate_counts: dict[int, int] = defaultdict(int)
        for w in query_words:
            for idx in self._word_idx.get(w, []):
                candidate_counts[idx] += 1

        if not candidate_counts:
            return None

        # Keep top-N candidates by word overlap (max 300 to stay fast)
        min_overlap = max(1, len(query_words) // 2)
        candidates = [
            idx for idx, cnt in candidate_counts.items()
            if cnt >= min_overlap
        ]
        if not candidates:
            # Relax: any single word overlap
            candidates = list(candidate_counts.keys())

        # Cap at 500 candidates (sort by overlap desc)
        candidates = sorted(candidates, key=lambda i: -candidate_counts[i])[:500]

        # 3. SequenceMatcher only on candidates — two passes:
        #    Pass A: same-category only
        #    Pass B: any category (if pass A found nothing)
        same_cat  = [i for i in candidates if self._records[i][2] == category]
        other_cat = [i for i in candidates if self._records[i][2] != category]

        # Two thresholds: string similarity vs token containment
        CONTAIN_THRESHOLD = 0.75   # for token containment (≥3 specific words overlap)
        SEQ_THRESHOLD     = threshold
        YEAR_TOLERANCE    = 1      # allow ±1 year for release date fuzziness

        # Extract sequel/part numbers from title for hard matching
        # Normalises "2", "II", "Part 2", "Chapter 2" → integer
        _NUM_WORDS = {"i":1,"ii":2,"iii":3,"iv":4,"v":5,"vi":6,"vii":7,"viii":8,"ix":9,"x":10}
        def _seq_number(norm_title: str) -> int | None:
            """Extract the first sequel number from a normalised title, or None."""
            # "part 2", "chapter 3", "volume 2"
            m = re.search(r'\b(?:part|chapter|volume|vol)\s+(\d+|[ivxlc]+)\b', norm_title)
            if m:
                tok = m.group(1)
                return int(tok) if tok.isdigit() else _NUM_WORDS.get(tok)
            # standalone digit at word boundary not year-like e.g. "iron man 2"
            m = re.search(r'(?<!\d)(\d{1,2})(?!\d)', norm_title)
            if m:
                n = int(m.group(1))
                if 2 <= n <= 20:   # sequel numbers; skip 1 (often missing)
                    return n
            # Roman numeral standalone
            tokens = norm_title.split()
            for tok in reversed(tokens):
                if tok in _NUM_WORDS and tok not in ("i",):  # skip "i" — too ambiguous
                    return _NUM_WORDS[tok]
            return None

        q_seq = _seq_number(norm)   # sequel number of the query title

        def _scores(norm_q: str, norm_t: str, q_words: set, t_words: set) -> tuple[float, float]:
            """Returns (seq_score, containment_score)."""
            seq = SequenceMatcher(None, norm_q, norm_t).ratio()
            if q_words and t_words and len(q_words) >= 3:
                containment = len(q_words & t_words) / len(q_words)
            else:
                containment = 0.0
            return seq, containment

        def _best_in(pool: list[int]) -> tuple[float, tuple | None]:
            best_s, best_r = 0.0, None
            year_match_s, year_match_r = 0.0, None   # best year-matching result

            for idx in sorted(pool, key=lambda i: -candidate_counts[i]):
                ip_id, t, cat, norm_t, rec_yr, rec_imdb = self._records[idx]

                # ── Hard year filter ──────────────────────────────────────────
                # If both query and catalog have a year, reject if gap > tolerance.
                # This prevents T3 (2003) matching T1 (1984), sequels matching originals, etc.
                if year and rec_yr:
                    try:
                        if abs(int(year) - int(rec_yr)) > YEAR_TOLERANCE:
                            continue   # hard reject — wrong year
                    except ValueError:
                        pass

                # ── Sequel number guard ───────────────────────────────────────
                # "Iron Man 2" must not match "Iron Man 3".
                # If query has a sequel number and candidate has a DIFFERENT one → skip.
                if q_seq is not None:
                    t_seq = _seq_number(norm_t)
                    if t_seq is not None and t_seq != q_seq:
                        continue   # hard reject — different part number

                t_words = set(norm_t.split()) - _STOPWORDS
                seq, containment = _scores(norm, norm_t, query_words, t_words)

                # ── Bidirectional word coverage ───────────────────────────────
                # Prevents "The Terminator" (1 keyword) matching
                # "Terminator 3 Rise of the Machines" (4 keywords).
                # The candidate's words must also overlap well with the query.
                if t_words and query_words:
                    reverse_containment = len(query_words & t_words) / len(t_words)
                    # If candidate is much shorter than query (e.g. 1-word title matching
                    # a 4-word query), require high reverse containment too.
                    if len(t_words) < len(query_words) * 0.5 and reverse_containment < 0.85:
                        continue   # candidate title too sparse relative to query

                # Effective score: whichever signal qualifies
                if seq >= SEQ_THRESHOLD:
                    eff = seq
                elif containment >= CONTAIN_THRESHOLD:
                    eff = containment
                else:
                    eff = seq  # below both thresholds

                if eff > best_s:
                    best_s = eff
                    best_r = (ip_id, t, cat)

                # Track best year-matching result for tiebreaking
                if year and rec_yr and rec_yr == year and eff >= CONTAIN_THRESHOLD:
                    if eff > year_match_s:
                        year_match_s = eff
                        year_match_r = (ip_id, t, cat)

                if seq == 1.0:
                    break

            # Prefer year-matched result when query year was provided
            if year and year_match_r and best_s >= CONTAIN_THRESHOLD:
                return best_s, year_match_r
            return best_s, best_r

        # Try same category first
        best_score, best_rec = _best_in(same_cat)

        # Fall back to other categories only if we found nothing good
        if best_score < SEQ_THRESHOLD and best_score < CONTAIN_THRESHOLD and other_cat:
            cross_score, cross_rec = _best_in(other_cat)
            if cross_score >= min(SEQ_THRESHOLD + 0.10, 0.96):
                best_score, best_rec = cross_score, cross_rec

        if best_score >= SEQ_THRESHOLD or best_score >= CONTAIN_THRESHOLD:
            return best_rec

        return None


# ─── Source fetchers ──────────────────────────────────────────────────────────

def fetch_tpb_top100(category_id: int, category: str) -> list[dict]:
    """Fetch TPB top-100 for a category (207=Movies, 208=TV)."""
    url  = f"https://apibay.org/precompiled/data_top100_{category_id}.json"
    raw  = json.loads(_fetch(url))
    out  = []
    for item in raw:
        info_hash = item.get("info_hash", "").lower().strip()
        if len(info_hash) != 40:
            continue
        out.append({
            "hash":      info_hash,
            "raw_name":  item.get("name", ""),
            "seeders":   int(item.get("seeders", 0)),
            "source":    SOURCE_TPB_MOVIES,
            "category":  category,
        })
    log.info(f"TPB cat {category_id} ({category}): {len(out)} hashes")
    return out


def fetch_eztv(pages: int = MAX_EZTV_PAGES) -> list[dict]:
    """Fetch recent EZTV torrents (TV episodes)."""
    out = []
    for page in range(1, pages + 1):
        url  = f"https://eztvx.to/api/get-torrents?page={page}&limit=100"
        data = json.loads(_fetch(url))
        torrents = data.get("torrents", [])
        if not torrents:
            break
        for t in torrents:
            info_hash = (t.get("hash") or "").lower().strip()
            if len(info_hash) != 40:
                continue
            out.append({
                "hash":     info_hash,
                "raw_name": t.get("filename", ""),
                "seeders":  int(t.get("seeds", 0)),
                "source":   SOURCE_EZTV,
                "category": "Series",
            })
    log.info(f"EZTV: {len(out)} hashes ({pages} pages)")
    return out


def fetch_yts(pages: int = YTS_PAGES) -> list[dict]:
    """
    Fetch recent YTS movies (date_added sort).
    Returns one entry per torrent quality (720p / 1080p / 2160p).
    Each entry has imdb_id pre-set — no fuzzy matching needed.
    Falls back yts.mx → yts.am automatically.
    """
    out = []
    for domain in ["yts.mx", YTS_DOMAIN]:
        try:
            for page in range(1, pages + 1):
                url = (
                    f"https://{domain}/api/v2/list_movies.json"
                    f"?sort_by=date_added&limit=50&page={page}"
                )
                data = json.loads(_fetch(url))
                movies = data.get("data", {}).get("movies", [])
                if not movies:
                    break
                for m in movies:
                    imdb_id  = m.get("imdb_code", "").strip()
                    title    = m.get("title", "")
                    year     = str(m.get("year", ""))
                    for t in m.get("torrents", []):
                        info_hash = t.get("hash", "").lower().strip()
                        if len(info_hash) != 40 or not imdb_id:
                            continue
                        out.append({
                            "hash":     info_hash,
                            "raw_name": f"{title} ({year}) [{t.get('quality','')} {t.get('type','')}]",
                            "seeders":  int(t.get("seeds", 0)),
                            "source":   SOURCE_YTS,
                            "category": "Movies",
                            "imdb_id":  imdb_id,   # direct — skip fuzzy match
                        })
            log.info(f"YTS ({domain}): {len(out)} hashes ({pages} pages)")
            return out
        except Exception as e:
            log.warning(f"YTS {domain} failed: {e}")
    log.error("YTS: all domains failed")
    return out


def _fetch_nyaa_rss_url(url: str, source: str) -> list[dict]:
    """Shared Nyaa RSS parser — fetches any Nyaa RSS URL and returns hash dicts."""
    xml  = _fetch(url)
    root = ET.fromstring(xml)
    ns   = {"nyaa": "https://nyaa.si/xmlns/nyaa"}
    out  = []
    for item in root.findall(".//item"):
        title_el   = item.find("title")
        hash_el    = item.find("nyaa:infoHash", ns)
        seeders_el = item.find("nyaa:seeders", ns)
        if title_el is None or hash_el is None:
            continue
        info_hash = hash_el.text.lower().strip()
        if len(info_hash) != 40:
            continue
        out.append({
            "hash":     info_hash,
            "raw_name": title_el.text or "",
            "seeders":  int(seeders_el.text or 0) if seeders_el is not None else 0,
            "source":   source,
            "category": "Anime",
        })
    return out


def fetch_nyaa_rss() -> list[dict]:
    """Fetch Nyaa anime RSS (top ~75 items, general anime category)."""
    out = _fetch_nyaa_rss_url("https://nyaa.si/?page=rss&c=1_0&f=0", SOURCE_NYAA)
    log.info(f"Nyaa RSS: {len(out)} hashes")
    return out


def fetch_subsplease_rss() -> list[dict]:
    """
    Fetch SubsPlease uploads via Nyaa user RSS.
    SubsPlease is the dominant source for same-day simulcast seasonal anime.
    Uses Nyaa user RSS (nyaa.si/?u=SubsPlease) — gives infoHash + seeders directly.
    Tested: 75 hashes, only 3 overlap with general Nyaa RSS → 72 net new per run.
    """
    out = _fetch_nyaa_rss_url("https://nyaa.si/?page=rss&u=SubsPlease", SOURCE_SUBSPLEASE)
    log.info(f"SubsPlease RSS (via Nyaa): {len(out)} hashes")
    return out


# ─── TMDB ─────────────────────────────────────────────────────────────────────

def _tmdb_get(path: str, api_key: str, **params) -> dict:
    qs = urllib.parse.urlencode({"api_key": api_key, **params})
    url = f"https://api.themoviedb.org/3{path}?{qs}"
    return json.loads(_fetch(url))


def _tmdb_external_ids(tmdb_id: int, media: str, api_key: str) -> dict:
    """Returns {imdb_id, wikidata_id} for a movie or tv show."""
    try:
        data = _tmdb_get(f"/{media}/{tmdb_id}/external_ids", api_key)
        return {
            "imdb_id":     data.get("imdb_id", ""),
            "wikidata_id": data.get("wikidata_id", ""),
        }
    except Exception:
        return {"imdb_id": "", "wikidata_id": ""}


def _search_yts_by_imdb(imdb_id: str) -> list[dict]:
    """Search YTS for all quality hashes of a movie by IMDB ID."""
    out = []
    for domain in ["yts.mx", YTS_DOMAIN]:
        try:
            url = f"https://{domain}/api/v2/list_movies.json?query_term={imdb_id}&limit=10"
            data = json.loads(_fetch(url))
            movies = data.get("data", {}).get("movies") or []
            for m in movies:
                for t in m.get("torrents", []):
                    h = t.get("hash", "").lower().strip()
                    if len(h) == 40:
                        out.append({
                            "hash":     h,
                            "raw_name": f"{m['title']} ({m.get('year','')}) [{t.get('quality','')}]",
                            "seeders":  int(t.get("seeds", 0)),
                        })
            return out
        except Exception:
            continue
    return out


def _search_eztv_by_imdb(imdb_id: str) -> list[dict]:
    """Search EZTV for all episode hashes of a series by IMDB ID (numeric, no 'tt')."""
    out = []
    num = (imdb_id or "").lower().replace("tt", "").strip()
    if not num.isdigit():
        return out
    try:
        url  = f"https://eztvx.to/api/get-torrents?imdb_id={num}&limit=100"
        data = json.loads(_fetch(url))
        for t in data.get("torrents", []) or []:
            h = (t.get("hash") or "").lower().strip()
            if len(h) != 40 or h == "0" * 40:
                continue
            out.append({
                "hash":     h,
                "raw_name": t.get("filename", "") or t.get("title", ""),
                "seeders":  int(t.get("seeds", 0)),
            })
    except Exception:
        pass
    return out


def _search_tpb_by_title(title: str, cat: str = "208") -> list[dict]:
    """Search apibay/TPB for a title. cat may be comma-separated:
    movies = '201,207,202,200', TV = '205,208'."""
    out = []
    try:
        q = urllib.parse.quote(title)
        data = json.loads(_fetch(f"https://apibay.org/q.php?q={q}&cat={cat}"))
        for item in data:
            h = item.get("info_hash", "").lower().strip()
            if len(h) != 40 or h == "0" * 40:
                continue
            out.append({
                "hash":    h,
                "raw_name": item.get("name", ""),
                "seeders":  int(item.get("seeders", 0)),
            })
    except Exception:
        pass
    return out


_no_redirect_opener = None


def _torznab_attr(item, name: str) -> str:
    """Namespace-agnostic torznab <*:attr name=.. value=..> lookup. Jackett emits
    a version-dependent namespace (urn:torznab:schema OR torznab.com/schemas/...),
    so we match by local tag name 'attr' rather than a hard-coded namespace."""
    for el in item.iter():
        tag = el.tag
        if tag == "attr" or tag.endswith("}attr"):
            if el.get("name") == name:
                return el.get("value", "") or ""
    return ""


def _follow_redirect_hash(url: str) -> str:
    """Follow a Jackett proxy link WITHOUT auto-redirect; pull btih from Location.
    Required for indexers (e.g. torrentgalaxyclone) that expose no infohash attr —
    Jackett 302-redirects the proxy link to the real magnet."""
    global _no_redirect_opener
    try:
        if _no_redirect_opener is None:
            class _NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, *a, **k):
                    return None
            _no_redirect_opener = urllib.request.build_opener(_NoRedirect)
        req = urllib.request.Request(url, headers=HEADERS)
        try:
            resp = _no_redirect_opener.open(req, timeout=15)
            loc = resp.headers.get("Location", "")
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location", "") if e.headers else ""
        m = re.search(r"btih:([a-fA-F0-9]{40})", loc or "", re.I)
        return m.group(1).lower() if m else ""
    except Exception:
        return ""


def _search_jackett(query: str, indexer: str, cat_id: str = "2000",
                    limit: int = 25) -> list[dict]:
    """Search a Jackett indexer (e.g. torrentgalaxyclone) via torznab for a title.

    Hash resolution order: infohash torznab attr (cheap) → btih in <link> →
    follow the Jackett proxy redirect (needed for TGX, which has no infohash attr).
    Returns [] silently if no key is configured or the indexer errors — the run
    must never die because TorrentGalaxy is unreachable. source is tagged so we
    can tell TGX hashes apart from YTS/apibay in the DB."""
    if not JACKETT_API_KEY:
        return []
    out = []
    try:
        url = (
            f"{JACKETT_HOST}/api/v2.0/indexers/{indexer}/results/torznab/api"
            f"?apikey={JACKETT_API_KEY}&t=search&q={urllib.parse.quote(query)}"
            f"&cat={cat_id}&limit={limit}"
        )
        raw = _fetch(url)
        if b"<error" in raw[:512]:
            return []
        root = ET.fromstring(raw)
        for item in root.findall(".//item"):
            raw_name = item.findtext("title", "") or ""
            dl_link  = item.findtext("link", "") or ""
            h = (_torznab_attr(item, "infohash") or "").lower()
            if len(h) != 40:
                m = re.search(r"btih:([a-fA-F0-9]{40})", dl_link, re.I)
                h = m.group(1).lower() if m else ""
            if len(h) != 40 and dl_link:
                h = _follow_redirect_hash(dl_link)
            if len(h) != 40 or h == "0" * 40:
                continue
            try:
                seeders = int(_torznab_attr(item, "seeders") or 0)
            except ValueError:
                seeders = 0
            out.append({
                "hash":     h,
                "raw_name": raw_name,
                "seeders":  seeders,
                "source":   SOURCE_TGX,
            })
    except Exception:
        pass
    return out


def _search_nyaa_by_title(title: str) -> list[dict]:
    """Search Nyaa RSS for an anime title."""
    out = []
    try:
        q = urllib.parse.quote(title)
        xml = _fetch(f"https://nyaa.si/?page=rss&q={q}&c=1_0&f=0")
        root = ET.fromstring(xml)
        ns = {"nyaa": "https://nyaa.si/xmlns/nyaa"}
        for item in root.findall(".//item"):
            h_el = item.find("nyaa:infoHash", ns)
            t_el = item.find("title")
            s_el = item.find("nyaa:seeders", ns)
            if h_el is None or t_el is None:
                continue
            h = h_el.text.lower().strip()
            if len(h) == 40:
                out.append({
                    "hash":    h,
                    "raw_name": t_el.text or "",
                    "seeders":  int(s_el.text or 0) if s_el is not None else 0,
                })
    except Exception:
        pass
    return out


def _resolve_ip_id(catalog, prefix: str, imdb_id: str, wikidata: str = "") -> str | None:
    """Resolve the authoritative Pantheon ip_id for a TMDB title.

    The ip_id is the identifier Pantheon mints for its OWN catalog — we must
    never fabricate one. We only ever return the catalog's own ip_id, matched
    on imdb_id (the catalog ip_id scheme is mid-migration wikidata→imdb, a MIX
    of {prefix}-Q<wiki> and {prefix}-tt<imdb>, so the column — not the string —
    is the source of truth). If the title isn't in the Pantheon catalog we
    return None (NA): it is simply not tracked under a made-up id.

    `prefix`/`wikidata` are kept for signature stability but no longer used to
    construct ids.
    """
    imdb_id = (imdb_id or "").strip()
    if catalog is not None and imdb_id:
        hit = catalog.find_by_imdb(imdb_id)
        if hit:
            return hit[0]
    return None


def _gather_deadline(workers, fn, items, deadline_s, label, on_result):
    """Run fn over items concurrently, but never block past deadline_s wall-clock.

    Replaces the `with ThreadPoolExecutor(...) as ex:` pattern, whose context-
    manager exit calls shutdown(wait=True) and so hangs forever if any worker
    thread is wedged (a CLOSE-WAIT socket on a slow-drip read won't trip the
    per-socket timeout). Here as_completed(..., timeout=deadline_s) bounds the
    TOTAL gather time; on the deadline we log how many were abandoned, shut the
    pool down WITHOUT waiting (cancel queued futures), and proceed with partial
    results. on_result(result, idx) is called for each completed future in order.
    Returns the number of futures that completed."""
    ex = ThreadPoolExecutor(max_workers=workers)
    futs = [ex.submit(fn, it) for it in items]
    done = 0
    try:
        for fut in as_completed(futs, timeout=deadline_s):
            try:
                on_result(fut.result(), done)
            except Exception as e:
                log.warning(f"{label}: task error: {e}")
            done += 1
    except TimeoutError:
        log.warning(f"{label}: deadline {deadline_s}s hit — {done}/{len(futs)} "
                    f"done, {len(futs) - done} abandoned (proceeding with partial)")
    try:
        ex.shutdown(wait=False, cancel_futures=True)
    except TypeError:
        ex.shutdown(wait=False)
    return done


def fetch_tmdb_movies(api_key: str, pages: int = TMDB_PAGES, catalog=None) -> list[dict]:
    """
    Fetch TMDB trending movies (top pages×20).
    Gets external_ids concurrently; resolves ip_id from the authoritative Pantheon
    catalog by imdb_id (NA / skipped if not in the catalog — ip_id is never minted).
    imdb_id is also used for the YTS search. Returns list of dicts ready for DB insert.
    """
    if not api_key:
        log.warning("TMDB_API_KEY not set — skipping TMDB movies")
        return []

    # Step 1: collect TMDB movie IDs from trending pages
    tmdb_items = []
    for page in range(1, pages + 1):
        try:
            data = _tmdb_get("/trending/movie/week", api_key, page=page)
            for m in data.get("results", []):
                tmdb_items.append({
                    "tmdb_id": m["id"],
                    "title":   m.get("title", ""),
                    "year":    (m.get("release_date") or "")[:4],
                })
        except Exception as e:
            log.warning(f"TMDB movies page {page}: {e}")
        time.sleep(TMDB_RATE_SLEEP)

    log.info(f"TMDB trending movies: {len(tmdb_items)} titles fetched")

    # Step 2: fetch external_ids concurrently
    def _get_ext(item):
        ext = _tmdb_external_ids(item["tmdb_id"], "movie", api_key)
        item.update(ext)
        return item

    enriched = []
    def _on_ext(item, idx):
        enriched.append(item)
        if idx % 50 == 0:
            time.sleep(TMDB_RATE_SLEEP * 2)
    _gather_deadline(TMDB_WORKERS, _get_ext, tmdb_items,
                     TMDB_ENRICH_DEADLINE, "TMDB movies enrich", _on_ext)

    log.info(f"TMDB movies: external_ids enriched — {sum(1 for e in enriched if e.get('imdb_id'))} with IMDB ID")

    # Step 3: search YTS for hashes concurrently
    def _search_one(item):
        wikidata = item.get("wikidata_id", "")
        imdb_id  = item.get("imdb_id", "")
        title    = item.get("title", "")
        ip_id    = _resolve_ip_id(catalog, "film", imdb_id, wikidata)
        if not ip_id or not imdb_id:
            return []
        # Multi-source: YTS (by imdb) + apibay (movie cats) + TorrentGalaxy (via
        # Jackett), deduped by infohash. TGX carries movie torrents YTS/apibay
        # miss — the main lever for under-covered new releases.
        by_hash = {}
        for h in _search_yts_by_imdb(imdb_id):
            by_hash.setdefault(h["hash"], h)
        if title:
            myear = item.get("year", "")
            for h in _search_tpb_by_title(title, cat="201,207,202,200"):
                if _name_matches_title(h["raw_name"], title, myear, "Movies"):
                    by_hash.setdefault(h["hash"], h)
            for indexer in JACKETT_MOVIE_INDEXERS:
                for h in _search_jackett(title, indexer, JACKETT_CAT_MOVIES):
                    if _name_matches_title(h["raw_name"], title, myear, "Movies"):
                        by_hash.setdefault(h["hash"], h)
        return [{
            **h,
            "source":        h.get("source", SOURCE_TMDB),
            "category":      "Movies",
            "ip_id":         ip_id,
            "matched_title": title,
            "imdb_id":       imdb_id,
        } for h in by_hash.values()]

    out = []
    _gather_deadline(10, _search_one, enriched, TMDB_SEARCH_DEADLINE,
                     "TMDB movies search", lambda r, i: out.extend(r))

    log.info(f"TMDB movies → {len(out)} hashes from YTS searches")
    return out


def fetch_tmdb_tv(api_key: str, pages: int = TMDB_PAGES, catalog=None) -> list[dict]:
    """
    Fetch TMDB trending TV shows (top pages×20).
    Gets external_ids; resolves ip_id from the authoritative Pantheon catalog by
    imdb_id (NA / skipped if not in the catalog — ip_id is never minted), then
    searches EZTV (by imdb) and TPB (by title) for hashes.
    """
    if not api_key:
        log.warning("TMDB_API_KEY not set — skipping TMDB TV")
        return []

    # Step 1: collect TMDB TV IDs
    tmdb_items = []
    for page in range(1, pages + 1):
        try:
            data = _tmdb_get("/trending/tv/week", api_key, page=page)
            for t in data.get("results", []):
                tmdb_items.append({
                    "tmdb_id": t["id"],
                    "title":   t.get("name", ""),
                    "year":    (t.get("first_air_date") or "")[:4],
                })
        except Exception as e:
            log.warning(f"TMDB TV page {page}: {e}")
        time.sleep(TMDB_RATE_SLEEP)

    log.info(f"TMDB trending TV: {len(tmdb_items)} titles fetched")

    # Step 2: external_ids concurrently
    def _get_ext(item):
        ext = _tmdb_external_ids(item["tmdb_id"], "tv", api_key)
        item.update(ext)
        return item

    enriched = []
    def _on_ext_tv(item, idx):
        enriched.append(item)
        if idx % 50 == 0:
            time.sleep(TMDB_RATE_SLEEP * 2)
    _gather_deadline(TMDB_WORKERS, _get_ext, tmdb_items,
                     TMDB_ENRICH_DEADLINE, "TMDB TV enrich", _on_ext_tv)

    # Step 3: search TPB by title for hashes concurrently
    def _search_tv(item):
        wikidata = item.get("wikidata_id", "")
        imdb_id  = item.get("imdb_id", "")
        title    = item.get("title", "")
        ip_id    = _resolve_ip_id(catalog, "series", imdb_id, wikidata)
        if not ip_id or not title:
            return []
        # Multi-source: EZTV (exact, by imdb) + apibay (TV cats), deduped by infohash
        by_hash = {}
        for h in _search_eztv_by_imdb(imdb_id):
            by_hash.setdefault(h["hash"], h)
        for h in _search_tpb_by_title(title, cat="205,208"):
            if _name_matches_title(h["raw_name"], title):
                by_hash.setdefault(h["hash"], h)
        return [{
            **h,
            "source":        SOURCE_TMDB,
            "category":      "Series",
            "ip_id":         ip_id,
            "matched_title": title,
            "imdb_id":       imdb_id,
        } for h in by_hash.values()]

    out = []
    _gather_deadline(10, _search_tv, enriched, TMDB_SEARCH_DEADLINE,
                     "TMDB TV search", lambda r, i: out.extend(r))

    log.info(f"TMDB TV → {len(out)} hashes from TPB searches")
    return out


# ─── AniList ──────────────────────────────────────────────────────────────────

ANILIST_QUERY = """
query ($page: Int, $perPage: Int) {
  Page(page: $page, perPage: $perPage) {
    pageInfo { hasNextPage }
    media(sort: POPULARITY_DESC, type: ANIME, isAdult: false) {
      idMal
      title { english romaji native }
      startDate { year }
      popularity
      averageScore
    }
  }
}
"""


def fetch_anilist_top(pages: int = ANILIST_PAGES, catalog=None) -> list[dict]:
    """
    Fetch top anime from AniList (no auth needed).
    ip_id is taken from the Pantheon catalog (anime-<idMal>); anime not present
    in the catalog are skipped (NA) rather than minting a self-built id.
    Searches Nyaa by title for hashes.
    """
    # Step 1: collect anime titles from AniList
    anime_items = []
    for page in range(1, pages + 1):
        try:
            body = json.dumps({
                "query": ANILIST_QUERY,
                "variables": {"page": page, "perPage": 50},
            }).encode()
            req = urllib.request.Request(
                "https://graphql.anilist.co",
                data=body,
                headers={**HEADERS, "Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
            media = data["data"]["Page"]["media"]
            for m in media:
                if not m.get("idMal"):
                    continue
                title   = m["title"].get("english") or m["title"].get("romaji") or ""
                romaji  = m["title"].get("romaji") or ""
                year    = (m.get("startDate") or {}).get("year")
                anime_items.append({
                    "mal_id": m["idMal"],
                    "title":  title,
                    "romaji": romaji,
                    "year":   year,
                })
            if not data["data"]["Page"]["pageInfo"]["hasNextPage"]:
                break
        except Exception as e:
            log.warning(f"AniList page {page}: {e}")
        time.sleep(0.5)  # AniList rate limit: 90 req/min

    log.info(f"AniList: {len(anime_items)} anime titles fetched")

    # Step 2: search Nyaa for hashes per title, with reverse validation
    # Problem: Nyaa search for "Another" returns Re:ZERO and "another world" anime.
    # Fix: fuzzy-match each result's raw_name back — only keep if it resolves to the
    # same ip_id (or if fuzzy match fails, require a strict title prefix match).
    out = []
    skipped = 0
    not_in_catalog = 0

    for i, item in enumerate(anime_items):
        mal_id      = item["mal_id"]
        title       = item["title"]
        romaji      = item.get("romaji", "")
        year        = item.get("year")
        # ip_id is Pantheon's own identifier — take it from the catalog, never mint.
        ip_id       = catalog.find_by_mal(mal_id) if catalog is not None else None
        if not ip_id:
            not_in_catalog += 1
            continue
        norm_expect = _normalize(title)
        norm_romaji = _normalize(romaji) if romaji else ""

        # Layer 1: ambiguous title guard.
        # Short/common-word titles (e.g. "Another", "One") produce massive false-positive
        # Nyaa results.  Append the air year to narrow the search query so only actual
        # releases of that title appear in results.
        ambiguous = is_ambiguous_title(title)
        nyaa_query = f"{title} {year}" if (ambiguous and year) else title
        if ambiguous:
            log.debug(f"  ⚠ Ambiguous title '{title}' — searching Nyaa as '{nyaa_query}'")

        hashes = _search_nyaa_by_title(nyaa_query)
        for h in hashes:
            raw = h.get("raw_name", "")
            parsed_title, _ = parse_torrent_name(raw)
            norm_parsed = _normalize(parsed_title) if parsed_title else ""

            # Reverse validation: bidirectional word overlap check.
            # Both directions must pass:
            #   forward:  how much of expected title is in torrent title  (≥50%)
            #   reverse:  how much of torrent title is in expected title  (≥35%)
            # This catches "Starting Life in Another World" matching "Another":
            #   forward:  {"another"} ∩ {life,another,world,...} / 1 = 100% ✓
            #   reverse:  {"another"} ∩ {life,another,world,...} / N = low  ✗ → rejected
            # Note: for single-word titles, reverse is checked against content words only
            # (episode numbers, group names, quality tags stripped before reverse calc)
            if norm_parsed and norm_expect:
                _SW = {"the","a","an","of","in","and","to","is","for",
                       "on","with","at","by","from","or","another"}
                # Technical tokens to strip from parsed side for reverse calc
                _TECH = re.compile(
                    r"^\d+$|^s\d+e\d+$|^e\d+$|^ep\d+$|^vol\d*$|"
                    r"^1080p$|^720p$|^480p$|^2160p$|^4k$|"
                    r"^hevc$|^avc$|^h264$|^h265$|^x264$|^x265$|"
                    r"^aac$|^ac3$|^flac$|^opus$|"
                    r"^web$|^dl$|^webrip$|^webdl$|^bluray$|^bd$|^bdrip$|"
                    r"^hdr$|^sdr$|^10bit$|^8bit$|"
                    r"^multi$|^dual$|^audio$|^subs$|^sub$|"
                    r"^weekly$|^uncensored$|^batch$"
                )
                expect_words = set(norm_expect.split()) - _SW
                parsed_words_full = set(norm_parsed.split())
                # For reverse calc: strip stop words AND technical tokens
                parsed_content = {w for w in parsed_words_full if not _TECH.match(w)} - _SW

                # If expected title is entirely stop-words (e.g. "Another"),
                # include those words back for forward matching only
                if not expect_words:
                    expect_words = set(norm_expect.split())
                    parsed_content = {w for w in parsed_words_full if not _TECH.match(w)}

                if expect_words and parsed_content:
                    shared = len(expect_words & parsed_content)
                    forward  = shared / len(expect_words)   # coverage of expected in result
                    reverse  = shared / len(parsed_content) # coverage of result in expected

                    if forward < 0.5 or reverse < 0.30:
                        skipped += 1
                        continue

                    # Layer 2: alt-title anchor for ambiguous titles.
                    # If the English title is ambiguous (e.g. "Another"), also check that
                    # the result doesn't match a different franchise better.
                    # We do this by verifying the romaji title shares words with the
                    # parsed title OR the parsed title doesn't introduce franchise words
                    # absent from both expected titles.
                    if ambiguous and norm_romaji:
                        romaji_words = set(norm_romaji.split()) - _SW - _TITLE_SW
                        parsed_extra = parsed_content - expect_words - romaji_words
                        # If parsed title has many words not in either title, likely wrong show
                        if parsed_extra and len(parsed_extra) >= 3:
                            skipped += 1
                            continue

            out.append({
                **h,
                "source":        SOURCE_ANILIST,
                "category":      "Anime",
                "ip_id":         ip_id,
                "matched_title": title,
            })

        if (i + 1) % 20 == 0:
            log.info(f"  AniList Nyaa search: {i+1}/{len(anime_items)} done, "
                     f"{len(out)} kept, {skipped} rejected")
        time.sleep(NYAA_SEARCH_DELAY)

    log.info(f"AniList → {len(out)} hashes from Nyaa searches "
             f"({skipped} rejected by reverse validation, "
             f"{not_in_catalog} skipped — not in Pantheon anime catalog)")
    return out


# ─── DB operations ────────────────────────────────────────────────────────────

# ── Pillar-2: alias best-match override (inline) ───────────────────────────────
# Applies the SAME alias→best-match logic the scheduled alias_remap.py enforcer
# uses, but at INSERT time so new torrents land on the right ip_id immediately
# (fixes foreign-romaji / sequel / show-vs-movie at birth, e.g. "Tongari Boushi no
# Atelier"→Witch Hat). Fail-safe: if title_aliases.db is absent/empty it is a no-op
# and collection behaves exactly as before. Tie-break = keep current unless the
# alias match is STRICTLY more specific (longer) than the current title's own alias.
_ALIAS_DB = Path("/data/db/title_aliases.db")
_ALIAS_IDX = None  # lazily-built (postings, by_ip)

def _alias_words(s: str) -> set:
    s = "".join(c for c in unicodedata.normalize("NFKD", s or "") if not unicodedata.combining(c))
    s = s.lower().replace("'", "").replace("’", "").replace(".", "").replace("-", " ")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return {w for w in s.split() if w not in _AUDIT_SW and w != "no" and not _AUDIT_TECH.match(w)}

def _alias_twords(raw: str) -> set:
    t, _ = parse_torrent_name(raw or "")
    w = _alias_words(t)
    return w if w else _alias_words(_strip_release_group(raw or ""))

def _load_alias_index():
    global _ALIAS_IDX
    if _ALIAS_IDX is not None:
        return _ALIAS_IDX
    postings, by_ip = defaultdict(list), defaultdict(list)
    if _ALIAS_DB.exists():
        try:
            a = sqlite3.connect(f"file:{_ALIAS_DB}?mode=ro", uri=True)
            entries = []
            for ip, al in a.execute("SELECT ip_id, alias FROM title_aliases"):
                w = frozenset(_alias_words(al))
                if len(w) >= 2:
                    entries.append((w, ip)); by_ip[ip].append(w)
            a.close()
            freq = Counter(x for w, _ in entries for x in w)
            for w, ip in entries:
                postings[min(w, key=lambda x: freq[x])].append((w, ip))
            log.info("alias index: %d entries (>=2 words) for inline best-match", len(entries))
        except Exception as e:
            log.warning("alias index load failed (inline override disabled): %s", e)
    _ALIAS_IDX = (postings, by_ip)
    return _ALIAS_IDX

def alias_best_match(raw_name: str, cur_ip_id: str) -> str | None:
    """Return a strictly-more-specific catalog ip_id for this torrent, or None."""
    postings, by_ip = _load_alias_index()
    if not postings:
        return None
    T = _alias_twords(raw_name)
    if not T:
        return None
    best, bn = None, 0
    for w in T:
        for W, ip in postings.get(w, ()):
            if W <= T and len(W) > bn:
                best, bn = ip, len(W)
    if best is None or best == cur_ip_id:
        return None
    cb = max((len(W) for W in by_ip.get(cur_ip_id, ()) if W <= T), default=0)
    return best if bn > cb else None


def upsert_hashes(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """
    Upserts rows into `hashes` table.
    Returns count of new inserts.
    """
    today   = str(date.today())
    inserted = 0

    for row in rows:
        existing = conn.execute(
            "SELECT hash, seeders FROM hashes WHERE hash = ?", (row["hash"],)
        ).fetchone()

        if existing:
            # Update seeders + last_seen if we have better seeder count
            if row["seeders"] > (existing[1] or 0):
                conn.execute(
                    "UPDATE hashes SET seeders=?, last_seen=? WHERE hash=?",
                    (row["seeders"], today, row["hash"]),
                )
        else:
            ip_id, mtitle = row["ip_id"], row["matched_title"]
            # Pillar-2 inline override: redirect to a more-specific alias match.
            ov = alias_best_match(row["raw_name"], ip_id)
            if ov:
                t = conn.execute("SELECT title FROM titles WHERE ip_id=?", (ov,)).fetchone()
                if t:
                    ip_id, mtitle = ov, t[0]
            conn.execute(
                """INSERT INTO hashes
                   (hash, ip_id, title, raw_name, category, seeders, source, first_seen, last_seen)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    row["hash"],
                    ip_id,
                    mtitle,
                    row["raw_name"],
                    row["category"],
                    row["seeders"],
                    row["source"],
                    today,
                    today,
                ),
            )
            inserted += 1

    conn.commit()
    return inserted


def ensure_title_row(conn: sqlite3.Connection, ip_id: str, title: str,
                     category: str, imdb_id: str | None = None,
                     mal_id=None):
    """Add a titles row if absent, and store the Pantheon reference ids
    (imdb_id for series/movies, mal_id for anime) when available. For rows
    that already exist with those ids missing, backfill them — never overwrite
    a non-empty value."""
    imdb_id = (imdb_id or None)
    if imdb_id in ("nan", "None", ""):
        imdb_id = None
    mal_id  = (str(mal_id) if mal_id else None)
    if mal_id in ("nan", "None", ""):
        mal_id = None
    row = conn.execute(
        "SELECT imdb_id, mal_id FROM titles WHERE ip_id = ?", (ip_id,)
    ).fetchone()
    if row is None:
        conn.execute(
            """INSERT OR IGNORE INTO titles (ip_id, title, category, imdb_id, mal_id)
               VALUES (?,?,?,?,?)""",
            (ip_id, title, category, imdb_id, mal_id),
        )
    else:
        cur_imdb, cur_mal = row
        new_imdb = cur_imdb or imdb_id
        new_mal  = cur_mal or mal_id
        if new_imdb != cur_imdb or new_mal != cur_mal:
            conn.execute(
                "UPDATE titles SET imdb_id = ?, mal_id = ? WHERE ip_id = ?",
                (new_imdb, new_mal, ip_id),
            )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Collect trending hashes and map to ip_ids")
    ap.add_argument("--dry-run",   action="store_true", help="Fetch + match but don't write to DB")
    ap.add_argument("--verbose",   action="store_true", help="Show per-torrent match details")
    ap.add_argument("--threshold", type=float, default=MATCH_THRESHOLD,
                    help=f"Fuzzy match threshold (default {MATCH_THRESHOLD})")
    ap.add_argument("--eztv-pages", type=int, default=MAX_EZTV_PAGES,
                    help=f"EZTV pages to fetch (default {MAX_EZTV_PAGES})")
    ap.add_argument("--tmdb",      action="store_true", help="Fetch TMDB trending movies + TV (needs TMDB_API_KEY)")
    ap.add_argument("--anilist",   action="store_true", help="Fetch AniList top 500 anime (no auth needed)")
    ap.add_argument("--tmdb-pages", type=int, default=TMDB_PAGES,
                    help=f"TMDB pages per category (default {TMDB_PAGES}, 1 page=20 titles)")
    ap.add_argument("--anilist-pages", type=int, default=ANILIST_PAGES,
                    help=f"AniList pages (default {ANILIST_PAGES}, 1 page=50 anime)")
    args = ap.parse_args()

    # Belt-and-suspenders: bound every socket in this process so a stalled
    # connect/read can't sit indefinitely even where an explicit urlopen timeout
    # was missed. The per-gather deadlines (TMDB_*_DEADLINE) are the real guard;
    # this just keeps individual sockets from drip-feeding forever.
    socket.setdefaulttimeout(20)

    tmdb_key = TMDB_API_KEY or os.environ.get("TMDB_API_KEY", "")
    if not tmdb_key:
        # Try loading from .env file (value never logged or printed)
        env_file = BASE_DIR / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("TMDB_API_KEY="):
                    tmdb_key = line.split("=", 1)[1].strip()
                    break
    if args.tmdb:
        log.info(f"TMDB key: {'✅ found' if tmdb_key else '❌ missing — set TMDB_API_KEY in .env'}")

    # Jackett key (TorrentGalaxy gateway — broadens movie coverage). Never logged.
    global JACKETT_API_KEY
    if not JACKETT_API_KEY:
        env_file = BASE_DIR / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("JACKETT_API_KEY="):
                    JACKETT_API_KEY = line.split("=", 1)[1].strip()
                    break
    if args.tmdb:
        log.info(
            f"Jackett: {'✅ key found' if JACKETT_API_KEY else '❌ no key — TorrentGalaxy movie search disabled'}"
            f"  @ {JACKETT_HOST}"
        )

    catalog = Catalog()

    # ── Pre-load titles table years as ground-truth for year cross-check ──────
    # The parquet files often have NaT for release_date (especially Wikidata entries).
    # The titles SQLite table has release_year populated for most movies/series.
    # We use this as a fallback year source during matching — if PTN extracts a year
    # from the torrent name AND the catalog ip_id has a known year that differs by
    # more than 2 years → reject the match (it's a different movie with the same name).
    _db_year_map: dict[str, int] = {}
    try:
        _yconn = sqlite3.connect(DB_PATH, timeout=10)
        for _ip_id, _yr in _yconn.execute(
            "SELECT ip_id, release_year FROM titles WHERE release_year IS NOT NULL"
        ):
            try:
                _db_year_map[_ip_id] = int(_yr)
            except (ValueError, TypeError):
                pass
        _yconn.close()
        log.info(f"Loaded {len(_db_year_map)} title years from DB for cross-validation")
    except Exception as _e:
        log.warning(f"Could not load title years from DB: {_e}")

    # ── Fetch all sources (each wrapped so one failure doesn't kill the run) ──
    all_torrents: list[dict] = []
    pre_matched: list[dict] = []  # TMDB/AniList entries already have ip_id

    sources = [
        ("TPB Movies HD",  lambda: fetch_tpb_top100(207, "Movies")),
        ("TPB Movies All", lambda: fetch_tpb_top100(201, "Movies")),  # +100 new vs HD-only
        ("TPB TV",         lambda: fetch_tpb_top100(208, "Series")),
        ("EZTV",           lambda: fetch_eztv(pages=args.eztv_pages)),
        ("Nyaa RSS",       fetch_nyaa_rss),
        ("SubsPlease RSS", fetch_subsplease_rss),                     # simulcast anime, ~72 net new
        ("YTS",            lambda: fetch_yts(pages=YTS_PAGES)),
    ]
    for name, fn in sources:
        try:
            results = fn()
            all_torrents += results
        except Exception as e:
            log.warning(f"{name} source failed (skipping): {e}")

    # TMDB + AniList — already have ip_id, skip fuzzy matching
    if args.tmdb:
        for name, fn in [
            ("TMDB Movies", lambda: fetch_tmdb_movies(tmdb_key, pages=args.tmdb_pages, catalog=catalog)),
            ("TMDB TV",     lambda: fetch_tmdb_tv(tmdb_key, pages=args.tmdb_pages, catalog=catalog)),
        ]:
            try:
                pre_matched += fn()
            except Exception as e:
                log.warning(f"{name} failed (skipping): {e}")

    if args.anilist:
        try:
            pre_matched += fetch_anilist_top(pages=args.anilist_pages, catalog=catalog)
        except Exception as e:
            log.warning(f"AniList failed (skipping): {e}")

    log.info(f"Total fetched: {len(all_torrents)} torrents  |  pre-matched (TMDB/AniList): {len(pre_matched)}")

    # Deduplicate by hash across all sources
    seen_hashes: set[str] = set()
    unique: list[dict] = []
    for t in all_torrents:
        if t["hash"] not in seen_hashes:
            seen_hashes.add(t["hash"])
            unique.append(t)

    # Add pre_matched entries (TMDB/AniList) — skip if hash already seen
    pre_matched_unique: list[dict] = []
    for t in pre_matched:
        if t["hash"] not in seen_hashes:
            seen_hashes.add(t["hash"])
            pre_matched_unique.append(t)

    log.info(f"Unique hashes: {len(unique)} (torrent sources)  +  {len(pre_matched_unique)} (TMDB/AniList)")

    # ── Match to catalog ──
    from collections import Counter
    matched:   list[dict] = []
    unmatched: list[str]  = []
    # Fix #3 instrumentation: tag WHY each torrent is dropped so we can tell
    # genuinely out-of-catalog noise apart from catalog titles our matcher
    # missed (the recoverable bucket). Purely diagnostic — does not change which
    # torrents are kept.
    unmatched_reasons: "Counter[str]" = Counter()
    unmatched_examples: dict[str, list[str]] = {}

    def _drop(raw_name: str, reason: str):
        unmatched.append(raw_name)
        unmatched_reasons[reason] += 1
        ex = unmatched_examples.setdefault(reason, [])
        if len(ex) < 8:
            ex.append(raw_name[:90])

    yts_direct = 0
    for t in unique:
        raw_name = t["raw_name"]
        category = t["category"]
        result   = None

        # ── YTS: try IMDB direct lookup first (no fuzzy needed) ──
        imdb_id = t.get("imdb_id", "")
        if imdb_id:
            result = catalog.find_by_imdb(imdb_id)
            if result:
                yts_direct += 1
                if args.verbose:
                    log.debug(f"  ✓ [IMDB] '{result[1]}' ({result[0]}) via {imdb_id}")

        # ── Skip collections / box-sets — cannot map to a single ip_id ──
        if is_collection(raw_name):
            if args.verbose:
                log.debug(f"  ⊘ [collection] skipped: {raw_name[:70]}")
            _drop(raw_name, "collection")
            continue

        # ── Fallback: fuzzy title match ──
        if not result:
            title, year = parse_torrent_name(raw_name)
            if not title:
                _drop(raw_name, "no-title")
                continue

            # ── Ambiguous title guard ─────────────────────────────────────────
            # Short titles, acronyms, and all-stop-word titles (CIA, M.I.A., It,
            # Another, Up…) cannot be safely fuzzy-matched without a year anchor.
            # Year is the only reliable signal when the title itself is too short
            # or too common to discriminate between different shows/movies.
            xcheck_rejected = False
            title_ambiguous = is_ambiguous_title(title)
            if title_ambiguous and not year:
                if args.verbose:
                    log.debug(f"  ⊘ [ambiguous-no-year] '{title}' — skipped (needs year to match safely)")
                _drop(raw_name, "ambiguous-no-year")
                continue

            result = catalog.fuzzy_match(title, category, year, threshold=args.threshold)

            # ── Year cross-check (Layer 1b): parquet rec_yr is often None/NaT
            # for Wikidata entries.  Use the titles table as ground truth.
            # If PTN found an explicit year AND the DB knows the canonical year
            # for this ip_id AND they're far apart → this is a different movie
            # with the same name.  Reject to prevent "The Visitor (2024)" from
            # mapping to film-Q1048360 (The Visitor, 2008).
            #
            # ⚠️  ONLY apply to Movies (film-*), NOT Series.
            # For TV series the PTN year is the episode air date (e.g. "Jeopardy
            # 2025 07 23"), not the series premiere year (1984).  Applying this
            # check to series would incorrectly reject all current-season episodes
            # of long-running shows.
            # Apply year cross-check to Movies always; to Series only when title
            # is ambiguous (for long-running shows the PTN year = episode date,
            # not premiere year — so we skip it for non-ambiguous series titles).
            _apply_year_xcheck = (
                result and year and (
                    result[0].startswith("film-") or
                    (result[0].startswith("series-") and title_ambiguous)
                )
            )
            if _apply_year_xcheck:
                ip_id_cand = result[0]
                db_yr = _db_year_map.get(ip_id_cand)
                if db_yr:
                    gap = abs(int(year) - db_yr)
                    # Ambiguous titles (CIA, M.I.A., It, Up…) require EXACT year
                    # match — no tolerance.  Distinctive titles allow ±2 years.
                    tolerance = 0 if title_ambiguous else 2
                    if gap > tolerance:
                        if args.verbose:
                            log.debug(
                                f"  ✗ [year-xcheck] '{title}' ({year}) → '{result[1]}' "
                                f"({ip_id_cand}, db_year={db_yr})  gap={gap}y  "
                                f"tol={tolerance}  rejected"
                            )
                        result = None
                        xcheck_rejected = True

            if args.verbose and result:
                log.debug(f"  ✓ [{result[2]}] '{title}' → '{result[1]}' ({result[0]})")
            elif args.verbose and not result:
                log.debug(f"  ✗ [{category}] '{title}' — no match")

        if result:
            ip_id, matched_title, matched_cat = result
            t["ip_id"]         = ip_id
            t["matched_title"] = matched_title
            t["category"]      = matched_cat
            matched.append(t)
        else:
            _drop(raw_name, "year-xcheck" if xcheck_rejected else "no-catalog-match")

    log.info(
        f"Matched: {len(matched)}  |  Unmatched: {len(unmatched)}  "
        f"|  YTS direct IMDB: {yts_direct}"
    )
    # Fix #3: unmatched breakdown — 'no-catalog-match' is the recoverable bucket
    # (catalog titles our matcher missed); the rest are out-of-catalog / unsafe.
    if unmatched_reasons:
        log.info("Unmatched breakdown: " + "  ".join(
            f"{r}={n}" for r, n in unmatched_reasons.most_common()))

    # Summary by category
    from collections import Counter
    cat_counts = Counter(t["category"] for t in matched)
    for cat, cnt in sorted(cat_counts.items()):
        log.info(f"  {cat}: {cnt}")

    all_matched = matched + pre_matched_unique

    if args.dry_run:
        log.info("Dry-run — skipping DB writes.")
        print("\nTop matched titles (torrent sources):")
        for t in sorted(matched, key=lambda x: -x["seeders"])[:15]:
            print(f"  {t['seeders']:>6} seeds  [{t['category']}]  {t['matched_title']}  ({t['ip_id']})")
        print(f"\nTMDB/AniList pre-matched: {len(pre_matched_unique)} hashes")
        for t in sorted(pre_matched_unique, key=lambda x: -x["seeders"])[:10]:
            print(f"  {t['seeders']:>6} seeds  [{t['category']}]  {t['matched_title']}  ({t['ip_id']})  [{t['source']}]")
        print("\nUnmatched breakdown (reason = count):")
        for reason, n in unmatched_reasons.most_common():
            print(f"  {n:>5}  {reason}")
            for ex in unmatched_examples.get(reason, [])[:5]:
                print(f"           ✗ {ex}")
        return

    # ── Write to DB ──
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    # Ensure the titles table has the reference-id columns (idempotent)
    _cols = {r[1] for r in conn.execute("PRAGMA table_info(titles)")}
    if "imdb_id" not in _cols:
        conn.execute("ALTER TABLE titles ADD COLUMN imdb_id TEXT")
    if "mal_id" not in _cols:
        conn.execute("ALTER TABLE titles ADD COLUMN mal_id TEXT")

    for t in all_matched:
        meta = catalog.by_ipid.get(t["ip_id"], {})
        ensure_title_row(
            conn, t["ip_id"], t["matched_title"], t["category"],
            imdb_id=meta.get("imdb_id") or t.get("imdb_id"),
            mal_id=meta.get("mal_id"),
        )

    new_hashes = upsert_hashes(conn, all_matched)

    # Layer 3: post-ingest audit — flag ip_ids whose hashes look inconsistent
    # with the canonical title (catches false-positive clusters before they grow).
    touched_ip_ids = {t["ip_id"] for t in all_matched}
    post_ingest_audit(conn, touched_ip_ids)

    conn.close()

    log.info(f"DB: {new_hashes} new hashes inserted  |  {len(all_matched) - new_hashes} updated/existing")
    print(f"\n✅  Done — {new_hashes} new hashes added, {len(all_matched)} total matched")


if __name__ == "__main__":
    main()
