#!/usr/bin/env python3
"""
acestream_pilot.py — AceStream (P2P live IPTV / sports) demand pilot
--------------------------------------------------------------------
Channel-1 extension: measure LIVE-streaming piracy demand the same way we measure
on-demand torrents — by counting DISTINCT PEER IPs per swarm — but for AceStream
live channels (sports, news, regional TV) that our VOD torrent pipeline can't see.

WHY THIS WORKS (de-risked 2026-06-13):
  AceStream live channels are BitTorrent swarms whose infohashes ARE announced to the
  PUBLIC MAINLINE BitTorrent DHT. So we DON'T need an AceStream engine (no Chromium-class
  RAM, no uploading infringing chunks, no new box): we resolve channels to infohashes
  via AceStream's server-side search API, then scrape the swarm with mainline-DHT tooling.

PIPELINE (each stage is its own function — see docs/code_maps/ACESTREAM_PILOT_GUIDE.md):
  1. ENUMERATE  — fetch_channels(): AceStream search API → {infohash: channel metadata}
  2. SCRAPE     — probe_dht():      one infohash → (sampled peers, BEP-33 swarm estimate)
  3. GEOLOCATE  — country_of():     peer IP → country via GeoLite2
  4. PERSIST    — main():           one row per (run_ts, infohash, peer_country) → SQLite

DEMAND UNIT: distinct concurrent peer IPs per channel — true live demand, geo-located.
Memory-safe: processes one channel at a time; nothing large held in RAM.

Usage:
  python3 acestream_pilot.py                      # default sport/news queries, snapshot
  python3 acestream_pilot.py --limit 15 --budget 12
  python3 acestream_pilot.py --queries "sky sports,espn,dazn,nba,ufc"
  python3 acestream_pilot.py --db /data/db/acestream_pilot.db --geo /path/GeoLite2-Country.mmdb
"""
import argparse
import math
import os
import socket
import sqlite3
import struct
import sys
import time
import json
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

SEARCH_HOST = "https://search.acestream.net/"   # 301 -> api.acestream.me
API_KEY = os.environ.get("ACESTREAM_API_KEY", "test_api_key")

# Broad seed queries spanning the high-value live verticals. The search API is
# query-based; this set is a pilot seed, not an exhaustive catalog crawl.
DEFAULT_QUERIES = [
    "sport", "sports", "sky sports", "espn", "dazn", "bt sport", "tnt sport",
    "bein", "nba", "nfl", "ufc", "f1", "football", "soccer", "premier league",
    "champions league", "cricket", "tennis", "boxing", "news",
    # hockey (AceStream is football-skewed, so name these explicitly)
    "nhl", "hockey", "ice hockey", "khl", "sportsnet", "tsn",
    # Comcast/NBCU + Sky entertainment portfolio (for owner-tagged portfolio coverage)
    "nbc", "bravo", "telemundo", "universo", "sky atlantic", "sky max", "sky comedy",
    "sky witness", "sky crime", "sky documentaries", "sky nature", "sky arts",
    "sky showcase", "sky cinema", "universal tv", "studio universal", "13th street",
    # Versant networks (spun off from Comcast Jan-2026 — tracked as their own owner)
    "usa network", "cnbc", "msnbc", "e! entertainment", "syfy", "oxygen", "golf channel",
]

# Public mainline-DHT bootstrap routers — where every DHT walk starts.
BOOTSTRAP = [
    ("router.bittorrent.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.libtorrent.org", 6881),
    ("dht.transmissionbt.com", 6881),
]
MY_ID = os.urandom(20)   # our own random 20-byte DHT node id for this run

# DHT-walk tuning — named so probe_dht() reads as plain English.
BATCH_NODES = 16          # how many of the closest nodes we ask each round
RECV_WINDOW = 1.4         # seconds we listen for replies after each send round
SOCK_TIMEOUT = 1.2        # per-recv socket timeout (seconds)
FILTER_LEN = 256          # BEP-33 bloom filter size in bytes (2048 bits)
FAR_DISTANCE = 1 << 161   # placeholder "distance" so bootstrap nodes get asked first


# --------------------------------------------------------------------------- #
# tiny bencode (the DHT/KRPC wire format is bencoded)                          #
# --------------------------------------------------------------------------- #
def _benc(o):
    """Encode a Python value (int / bytes / str / list / dict) to bencode bytes."""
    if isinstance(o, int):
        return b"i%de" % o
    if isinstance(o, bytes):
        return b"%d:%s" % (len(o), o)
    if isinstance(o, str):
        return _benc(o.encode())
    if isinstance(o, list):
        return b"l" + b"".join(_benc(x) for x in o) + b"e"
    if isinstance(o, dict):
        return b"d" + b"".join(_benc(k) + _benc(v) for k, v in sorted(o.items())) + b"e"
    raise TypeError(type(o))


def _bdec(b):
    """Decode bencode bytes back to Python. Strings stay BYTES (not str)."""
    def p(i):
        c = b[i:i + 1]
        if c == b"i":
            j = b.index(b"e", i)
            return int(b[i + 1:j]), j + 1
        if c.isdigit():
            j = b.index(b":", i)
            n = int(b[i:j])
            return b[j + 1:j + 1 + n], j + 1 + n
        if c == b"l":
            i += 1
            out = []
            while b[i:i + 1] != b"e":
                v, i = p(i)
                out.append(v)
            return out, i + 1
        if c == b"d":
            i += 1
            out = {}
            while b[i:i + 1] != b"e":
                k, i = p(i)
                v, i = p(i)
                out[k] = v
            return out, i + 1
        raise ValueError("bad bencode @%d" % i)
    value, _ = p(0)
    return value


def _parse_nodes(blob):
    """Compact node info → list of (node_id, ip, port).
    Each node is 26 bytes: 20-byte node id + 4-byte IPv4 + 2-byte port."""
    out = []
    for i in range(0, len(blob) - 25, 26):
        node_id = blob[i:i + 20]
        ip = socket.inet_ntoa(blob[i + 20:i + 24])
        port = struct.unpack("!H", blob[i + 24:i + 26])[0]
        if port:
            out.append((node_id, ip, port))
    return out


def _parse_values(vals):
    """Compact peer list → set of (ip, port). Each peer is 6 bytes: 4-byte IP + 2-byte port."""
    out = set()
    for v in vals:
        if isinstance(v, bytes) and len(v) >= 6:
            ip = socket.inet_ntoa(v[0:4])
            port = struct.unpack("!H", v[4:6])[0]
            out.add((ip, port))
    return out


def _estimate_bloom(bf):
    """BEP-33 swarm-size estimate from a 256-byte (2048-bit) bloom filter.
    Returns an integer count. n ≈ log(c/m) / (k·log(1−1/m)), c=zero bits, m=2048, k=2.
    (Lifted from dht_peer_count so AceStream uses the SAME demand metric as the title feed.)"""
    m = 2048
    set_bits = sum(bin(b).count("1") for b in bf)
    c = m - set_bits   # number of zero bits
    if c == 0:
        return 50_000   # filter full — overflowed, cap the estimate
    if c == m:
        return 0        # filter empty — no peers announced
    try:
        return max(0, int(math.log(c / m) / (2 * math.log(1 - 1 / m))))
    except (ValueError, ZeroDivisionError):
        return 0


def _or_into(acc, new):
    """Merge one BEP-33 bloom filter into the running total, in place.
    A bloom union is just a bitwise OR (per the BEP-33 spec)."""
    for i in range(len(new)):
        acc[i] |= new[i]


# --------------------------------------------------------------------------- #
# owner tagging — which media group owns a channel (for portfolio filtering)   #
# --------------------------------------------------------------------------- #
# Sky is Comcast ONLY in its European footprint. Sky News Australia (News Corp)
# and Sky Latin America (DirecTV) are NOT Comcast — exclude those regions.
SKY_NON_COMCAST_CC = {"AU", "BR", "MX", "CL", "CO", "AR", "PE", "NZ"}
# US NBCU (retained post-Versant Jan-2026) + Universal international feeds.
COMCAST_NAME_KEYS = ("nbc", "telemundo", "universo", "bravo",
                     "universal tv", "studio universal", "13th street", "dreamworks")
# Versant Media Group — networks spun off from Comcast on 2026-01-02 (now a SEPARATE owner).
# Checked BEFORE Comcast so e.g. "CNBC"/"MSNBC" (which contain 'nbc') resolve to Versant, not Comcast.
VERSANT_NAME_KEYS = ("usa network", "cnbc", "msnbc", "ms now", "e!", "e entertainment",
                     "syfy", "oxygen", "golf channel")


def owner_of(name, country):
    """Classify a channel's parent media group for portfolio filtering.
    Returns 'Comcast', 'Versant', or 'Other'.
    (Extend with more owners — Disney, WBD, beIN, DAZN … — by adding more key tuples.)"""
    n = (name or "").lower()
    cc = (country or "").upper().split(",")[0].replace("UK", "GB")
    # Sky-branded → Comcast, except Sky Australia / Sky LatAm
    if n.startswith("sky ") or " sky " in n:
        return "Other" if cc in SKY_NON_COMCAST_CC else "Comcast"
    # Versant FIRST (its 'cnbc'/'msnbc' contain 'nbc', which would otherwise hit Comcast)
    if any(k in n for k in VERSANT_NAME_KEYS):
        return "Versant"
    if any(k in n for k in COMCAST_NAME_KEYS):
        return "Comcast"
    return "Other"


# --------------------------------------------------------------------------- #
# 1. ENUMERATE — AceStream search API                                          #
# --------------------------------------------------------------------------- #
def fetch_channels(queries, page_size=50):
    """Look up live channels via the AceStream search API.

    Returns {infohash: {name, categories, country, language, availability, bitrate}},
    deduped across all seed queries (keeping the highest-availability sighting).
    Server-side API; no AceStream engine required."""
    chans = {}
    for q in queries:
        url = SEARCH_HOST + "?" + urllib.parse.urlencode({
            "method": "search",
            "api_version": "1",
            "api_key": API_KEY,
            "query": q,
            "page_size": page_size,
        })
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            d = json.load(urllib.request.urlopen(req, timeout=20))
        except Exception as e:
            print(f"  [enumerate] query '{q}' failed: {e}", file=sys.stderr)
            continue

        for r in d.get("results", []):
            ih = (r.get("infohash") or "").lower()
            if len(ih) != 40:
                continue
            # keep the highest-availability sighting of each channel
            prev = chans.get(ih)
            if prev and (prev.get("availability") or 0) >= (r.get("availability") or 0):
                continue
            chans[ih] = {
                "name": r.get("name") or "",
                "categories": ",".join(r.get("categories") or []),
                "country": ",".join(r.get("country") or []),
                "language": ",".join(r.get("language") or []),
                "availability": r.get("availability"),
                "bitrate": r.get("bitrate"),
            }
    return chans


# --------------------------------------------------------------------------- #
# 2. SCRAPE — mainline-DHT get_peers walk (self-contained pilot prober)        #
# --------------------------------------------------------------------------- #
def _ingest(data, ih_int, peers, bfsd, bfpe, frontier, queried):
    """Decode ONE reply datagram and fold what it tells us into our running state.

    A reply can carry any of: peers (`values`), BEP-33 filters (`BFsd`/`BFpe`), and/or
    closer nodes (`nodes`). Everything is updated IN PLACE; this returns nothing."""
    try:
        message = _bdec(data)
    except Exception:
        return
    reply = message.get(b"r")
    if not isinstance(reply, dict):
        return

    # (a) actual swarm peers
    values = reply.get(b"values")
    if isinstance(values, list):
        peers |= _parse_values(values)

    # (b) BEP-33 bloom filters → OR-merge into our running estimate
    seed_filter = reply.get(b"BFsd")
    if isinstance(seed_filter, bytes) and len(seed_filter) == FILTER_LEN:
        _or_into(bfsd, seed_filter)
    leech_filter = reply.get(b"BFpe")
    if isinstance(leech_filter, bytes) and len(leech_filter) == FILTER_LEN:
        _or_into(bfpe, leech_filter)

    # (c) closer nodes → add to the frontier, keyed by XOR distance to the infohash
    nodes_blob = reply.get(b"nodes", b"")
    for node_id, ip, port in _parse_nodes(nodes_blob):
        already_known = (ip, port) in queried or (ip, port) in frontier
        if not already_known:
            frontier[(ip, port)] = int.from_bytes(node_id, "big") ^ ih_int


def probe_dht(ih_hex, budget=14):
    """Walk the mainline DHT toward one infohash and measure its swarm.

    Returns a tuple (peers, bep33):
        peers  — set of (ip, port) actually seen in the swarm (used for the geo split)
        bep33  — {"seeders": int, "leechers": int}, the BEP-33 swarm-size estimate
                 (leechers is our headline DEMAND number)

    It is a CONVERGING Kademlia walk: start at the bootstrap routers, then keep asking the
    nodes CLOSEST (by XOR distance) to the infohash, because those hold the announces and
    return the BEP-33 filters. Runs until `budget` seconds elapse."""
    infohash = bytes.fromhex(ih_hex)
    ih_int = int.from_bytes(infohash, "big")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(SOCK_TIMEOUT)

    peers = set()             # (ip, port) peers actually sampled from the swarm
    queried = set()           # nodes we've already asked (never re-ask)
    bfsd = bytearray(FILTER_LEN)   # running OR of every seeder bloom filter seen
    bfpe = bytearray(FILTER_LEN)   # running OR of every leecher bloom filter seen
    frontier = {}             # (ip, port) -> XOR distance to the infohash (smaller = closer)

    # seed the frontier with the bootstrap routers (asked first, at FAR distance)
    for host, port in BOOTSTRAP:
        try:
            ip = socket.gethostbyname(host)
        except Exception:
            continue
        frontier[(ip, port)] = FAR_DISTANCE

    start = time.time()
    tid = 0
    while frontier and time.time() - start < budget:
        # 1. pick the closest nodes we haven't asked yet
        closest = sorted(frontier.items(), key=lambda kv: kv[1])[:BATCH_NODES]

        # 2. send each one a get_peers query (scrape=1 asks for the BEP-33 filters)
        for (ip, port), _distance in closest:
            frontier.pop((ip, port), None)
            if (ip, port) in queried:
                continue
            queried.add((ip, port))
            tid += 1
            query = {
                b"t": struct.pack("!H", tid & 0xffff),
                b"y": b"q",
                b"q": b"get_peers",
                b"a": {b"id": MY_ID, b"info_hash": infohash, b"scrape": 1},
            }
            try:
                sock.sendto(_benc(query), (ip, port))
            except Exception:
                pass

        # 3. collect replies for a short window, folding each into our running state
        deadline = time.time() + RECV_WINDOW
        while time.time() < deadline:
            try:
                data, _addr = sock.recvfrom(2048)
            except socket.timeout:
                break
            except Exception:
                continue
            _ingest(data, ih_int, peers, bfsd, bfpe, frontier, queried)

    sock.close()
    bep33 = {"seeders": _estimate_bloom(bfsd), "leechers": _estimate_bloom(bfpe)}
    return peers, bep33


# --------------------------------------------------------------------------- #
# 3. GEOLOCATE                                                                 #
# --------------------------------------------------------------------------- #
def make_geo(mmdb_path):
    """Open the GeoLite2 mmdb and return a reader, or None if it's missing/unreadable."""
    if not mmdb_path or not os.path.exists(mmdb_path):
        return None
    try:
        import geoip2.database
        return geoip2.database.Reader(mmdb_path)
    except Exception as e:
        print(f"  [geo] disabled ({e})", file=sys.stderr)
        return None


def country_of(reader, ip):
    """Return the 2-letter country code for an IP, or '??' if unknown / no reader."""
    if reader is None:
        return "??"
    try:
        return reader.country(ip).country.iso_code or "??"
    except Exception:
        return "??"


# --------------------------------------------------------------------------- #
# 4. PERSIST                                                                   #
# --------------------------------------------------------------------------- #
DDL = """
CREATE TABLE IF NOT EXISTS acestream_demand(
    run_ts         TEXT,
    infohash       TEXT,
    name           TEXT,
    categories     TEXT,
    ch_country     TEXT,    -- channel's declared country (geo prior)
    availability   REAL,
    peer_country   TEXT,    -- geolocated peer country (from the raw get_peers sample)
    peer_count     INTEGER, -- raw sampled peers in that country
    bep33_seeders  INTEGER, -- channel-level BEP-33 swarm estimate (denormalized onto every row)
    bep33_leechers INTEGER, -- channel-level BEP-33 leechers = the DEMAND metric (matches title feed)
    owner          TEXT,    -- parent media group (e.g. 'Comcast' / 'Other') for portfolio filtering
    PRIMARY KEY (run_ts, infohash, peer_country)
);
CREATE INDEX IF NOT EXISTS idx_ace_ih ON acestream_demand(infohash);
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default=",".join(DEFAULT_QUERIES),
                    help="comma-separated search seeds")
    ap.add_argument("--limit", type=int, default=0,
                    help="cap channels probed (0 = all enumerated)")
    ap.add_argument("--budget", type=float, default=14, help="DHT seconds per channel")
    ap.add_argument("--min-availability", type=float, default=0.0)
    ap.add_argument("--db", default="acestream_pilot.db")
    ap.add_argument("--geo", default=os.environ.get(
        "GEOIP_DB", "/data/geoip/GeoLite2-Country.mmdb"))
    ap.add_argument("--csv", default="", help="optional CSV dump path")
    args = ap.parse_args()

    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    queries = [q.strip() for q in args.queries.split(",") if q.strip()]

    # --- stage 1: enumerate channels ---
    print(f"[1/4] enumerate via search API ({len(queries)} seed queries)…")
    chans = fetch_channels(queries)
    items = [(ih, m) for ih, m in chans.items()
             if (m.get("availability") or 0) >= args.min_availability]
    # portfolio-first: tagged owners (Comcast/Versant/…) always probed before the --limit
    # cap kicks in, so a few hundred generic sports channels can't crowd them out.
    items.sort(key=lambda x: (owner_of(x[1]["name"], x[1]["country"]) == "Other",
                              -(x[1].get("availability") or 0)))
    if args.limit:
        items = items[:args.limit]
    print(f"      {len(chans)} distinct channels enumerated, probing {len(items)}")

    # --- stage 2 setup: geo reader ---
    geo = make_geo(args.geo)
    print(f"[2/4] GeoIP: {'ON' if geo else 'OFF (no mmdb — peer_country=??)'}")

    con = sqlite3.connect(args.db)
    con.executescript(DDL)
    # migrate older DBs created before owner tagging: add the column if missing
    try:
        con.execute("ALTER TABLE acestream_demand ADD COLUMN owner TEXT")
    except sqlite3.OperationalError:
        pass   # column already exists

    rows_out = []
    summary = []   # (leechers, sample_peers, name, ch_country, categories, top_geo)

    # --- stages 2+3: probe each channel, geolocate its peers ---
    print(f"[3/4] DHT scrape + BEP-33 ({args.budget}s/channel)…")
    for i, (ih, m) in enumerate(items, 1):
        peers, bep = probe_dht(ih, budget=args.budget)
        leech = bep["leechers"]
        seed = bep["seeders"]
        owner = owner_of(m["name"], m["country"])

        by_country = defaultdict(int)
        for ip, _port in peers:
            cc = country_of(geo, ip)
            by_country[cc] += 1
        total = len(peers)

        # still record the channel (+ its BEP-33) even with 0 sampled peers
        if not by_country:
            by_country["??"] = 0

        for cc, n in by_country.items():
            rows_out.append((run_ts, ih, m["name"], m["categories"], m["country"],
                             m["availability"], cc, n, seed, leech, owner))

        top_pairs = sorted(by_country.items(), key=lambda x: -x[1])[:4]
        top_geo = ",".join(f"{cc}:{n}" for cc, n in top_pairs if cc != "??")
        summary.append((leech, total, m["name"], m["country"], m["categories"], top_geo))
        print(f"  [{i:>3}/{len(items)}] leechers={leech:<5} seeders={seed:<5} sample={total:<4} "
              f"[{m['country'] or '?'}] {m['name'][:34]:34} {top_geo}")

    # --- stage 4: persist ---
    con.executemany(
        "INSERT OR REPLACE INTO acestream_demand VALUES (?,?,?,?,?,?,?,?,?,?,?)", rows_out)
    con.commit()
    con.close()

    if args.csv and rows_out:
        import csv as _csv
        with open(args.csv, "w", newline="") as f:
            w = _csv.writer(f)
            w.writerow(["run_ts", "infohash", "name", "categories", "ch_country",
                        "availability", "peer_country", "peer_count",
                        "bep33_seeders", "bep33_leechers", "owner"])
            w.writerows(rows_out)
        print(f"      CSV → {args.csv}")

    print(f"\n[4/4] wrote {len(rows_out)} rows → {args.db}")
    summary.sort(reverse=True)
    live = [s for s in summary if s[0] > 0]
    print(f"\n=== TOP LIVE CHANNELS BY BEP-33 LEECHERS ({len(live)}/{len(summary)} with demand) ===")
    for leech, total, name, cc, cats, top_geo in summary[:20]:
        print(f"  {leech:>6} leechers (sample {total:>3}) | [{cc or '?':<5}] "
              f"{name[:36]:36} | {cats[:18]:18} | {top_geo}")


if __name__ == "__main__":
    main()
