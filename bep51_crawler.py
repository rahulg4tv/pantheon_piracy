#!/usr/bin/env python3
"""
bep51_crawler.py — DHT info_hash discovery via BEP-51
------------------------------------------------------
Crawls the BitTorrent DHT network using the sample_infohashes extension
(BEP-51) to discover new torrent info_hashes being actively shared.

BEP-51: each DHT node stores hashes announced to it. When asked, it
returns up to 20 random samples from its local storage plus routing
nodes for further crawling.

Flow:
  1. Bootstrap from known DHT nodes + saved node pool
  2. Walk the keyspace: send sample_infohashes to nodes covering all
     256 possible first-byte prefixes (ensures full DHT coverage)
  3. Collect samples, deduplicate, filter already-known hashes
  4. Write new hashes to CSV; optionally insert into hashes_v2.db
  5. Respect per-node rate limits (interval field in responses)

Output:
  data/discovered/YYYY-MM-DD.csv   — all new hashes found
  data/discovered/YYYY-MM-DD.db    — optional SQLite staging table

Usage:
  python bep51_crawler.py                        # crawl 30 min, CSV only
  python bep51_crawler.py --duration 60          # crawl 60 minutes
  python bep51_crawler.py --insert               # also insert into hashes_v2.db
  python bep51_crawler.py --sockets 4            # 4 parallel UDP sockets
  python bep51_crawler.py --concurrency 300      # concurrent queries (default 200)
  python bep51_crawler.py --min-num 10           # only keep hashes where node
                                                  # reports num >= 10 (busy nodes, higher
                                                  # resolution hit rate via torrent caches)
  python bep51_crawler.py --filter-media --bep09  # enable BEP-09 direct peer metadata
                                                  # fetch as 2nd-pass resolver (~30-50%
                                                  # hit rate vs 0.1% cache-only)

BEP-51 spec: https://www.bittorrent.org/beps/bep_0051.html
"""

import argparse
import asyncio
import concurrent.futures
import csv
import hashlib
import json
import re
import random
import socket
import sqlite3
import struct
import time
from collections import deque
from datetime import date, datetime
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_PATH        = Path(__file__).parent / "data" / "hashes_v2.db"
NODE_POOL_PATH = Path(__file__).parent / "data" / "node_pool.json"
DISCOVERED_DIR = Path(__file__).parent / "data" / "discovered"
DISCOVERED_DIR.mkdir(parents=True, exist_ok=True)

_BOOTSTRAP_HOSTS = [
    ("router.bittorrent.com",  6881),
    ("router.utorrent.com",    6881),
    ("dht.libtorrent.org",     6881),
    ("dht.transmissionbt.com", 6881),
]

# ---------------------------------------------------------------------------
# Media resolution — inline enrichment before DB insert
# ---------------------------------------------------------------------------

# Torrent cache sites — download .torrent and extract name via bencode
# itorrents.org and torrage.info both serve raw .torrent files for known hashes.
# Note: hit rate is ~1-5% of all DHT hashes (most are uncached junk).
# Use --min-num 10+ to pre-filter to hashes from busy nodes, improving hit rate.
TORRENT_CACHES = [
    "https://itorrents.org/torrent/{HASH}.torrent",
    "https://torrage.info/torrent.php?h={HASH}",
]

_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

# Max hashes to attempt resolution per run — prevents multi-hour resolve phases.
# BEP-51 typically finds 500K-1M new hashes; we sample the most promising ones.
# Hashes are pre-filtered by --min-num so these are from nodes with many torrents.
_RESOLVE_BATCH_CAP = 50_000

CATEGORY_RULES = [
    ("Anime", [
        r"\b(anime|crunchyroll|funimation|subsplease|horriblesubs|erai-raws)\b",
        r"\b(one[ .]piece|naruto|bleach|dragon[ .]ball|attack on titan|demon slayer)\b",
        r"\b(shingeki|boku no|ore no|kimetsu|jujutsu|chainsaw man)\b",
        r"\[(?:SubsPlease|HorribleSubs|Erai-raws|ASW|DKB|NC-Raws)\]",
    ]),
    ("Series", [
        r"\bS\d{1,2}E\d{1,2}\b",
        r"\bSeason[\s._]*\d+\b",
        r"\bEpisode[\s._]*\d+\b",
        r"\b\d+x\d+\b",
        r"\bComplete[\s._]Series\b",
        r"\bTV[\s._]Series\b",
        r"\bS\d{1,2}[\s._](?:COMPLETE|Complete)\b",
        r"\bS\d{2,}[\s._]",
    ]),
    ("Movies", [
        r"\b(?:1080p|2160p|4K|720p|480p)\b",
        r"\b(?:BluRay|Blu-Ray|BDRip|BRRip|BDRIP)\b",
        r"\b(?:WEB-DL|WEBRip|WEBRIP|HDRip|DVDRip|DVDScr)\b",
        r"\b(?:HDCAM|CAM|TS|TC|SCR|R5)\b",
        r"\b(?:x264|x265|HEVC|AVC|H\.264|H\.265|XviD|DivX)\b",
        r"\b(?:DTS|Dolby|Atmos|TrueHD|AAC|AC3|DD5\.1)\b",
        r"\(\d{4}\)",
    ]),
]

MEDIA_CATEGORIES = {"Movies", "Series", "Anime"}

# Adult content keywords — any match → reject the torrent before media classification.
# These appear frequently in adult site names embedded in torrent file names.
_ADULT_PATTERNS = re.compile(
    r"\b(?:XXX|Blacked|BlackedRaw|Tushy|TushyRaw|Brazzers|Bangbros|"
    r"Mofos|Nubiles|Vixen|Deeper|Slayed|Girlfriend|OnlyFans|"
    r"Babes|Wicked|NewSensations|ClubSweethearts|HandsOnHardcore|"
    r"FamilyStrokes|DaughterSwap|Pornhub|xHamster|XNXX|xvideos)\b"
    r"|\bHardcore\b|\bCreampie\b|\bCumshot\b",
    re.IGNORECASE,
)


def _is_adult(title: str) -> bool:
    """Return True if the title looks like adult content — filter before classification."""
    return bool(_ADULT_PATTERNS.search(title))


def _guess_category(title: str) -> str:
    if not title:
        return "Unknown"
    if _is_adult(title):
        return "Adult"   # Not in MEDIA_CATEGORIES → will be filtered out
    for category, patterns in CATEGORY_RULES:
        for pattern in patterns:
            if re.search(pattern, title, re.IGNORECASE):
                return category
    return "Unknown"


def _bdecode_name_from_torrent(data: bytes) -> str | None:
    """Extract torrent name from raw .torrent bytes via bencode fast-path."""
    try:
        idx = data.find(b"4:name")
        if idx == -1:
            return None
        idx += 6
        colon = data.index(b":", idx)
        length = int(data[idx:colon])
        name_bytes = data[colon + 1: colon + 1 + length]
        return name_bytes.decode("utf-8", errors="replace").strip()
    except Exception:
        return None


def _resolve_via_torrent_cache(hash_hex: str) -> dict | None:
    """
    Download .torrent from cache sites (itorrents.org, torrage.info) and extract
    name via bencode fast-path. Hit rate is low (~1-5%) so use --min-num 10+ to
    pre-filter to hashes from busy nodes where resolution is more likely to succeed.
    """
    for url_template in TORRENT_CACHES:
        url = url_template.format(HASH=hash_hex.upper())
        try:
            req = Request(url, headers=_HTTP_HEADERS)
            with urlopen(req, timeout=6) as resp:
                body = resp.read()
            if not body or len(body) < 50 or not body.startswith(b"d"):
                continue
            name = _bdecode_name_from_torrent(body)
            if not name or len(name) < 3:
                continue
            category = _guess_category(name)
            if category in MEDIA_CATEGORIES:
                return {"name": name, "category": category, "seeders": 0}
            return None  # found but not media — skip
        except (URLError, HTTPError, Exception):
            continue
    return None


# ---------------------------------------------------------------------------
# BEP-09 / BEP-10 — Metadata directly from DHT peers
# ---------------------------------------------------------------------------
# Rather than hoping a hash is cached on itorrents.org (0.1% hit rate),
# we can fetch the torrent metadata directly from peers in the DHT network.
# Protocol:
#   1. DHT get_peers → find IP:port peers holding the torrent
#   2. TCP connect → BitTorrent handshake (BEP-05)
#   3. Extension handshake (BEP-10) → negotiate ut_metadata
#   4. Request metadata pieces (BEP-09) → reassemble → verify SHA1 → parse name
# Expected hit rate: 30–50% for actively shared hashes vs 0.1% for caches.
# ---------------------------------------------------------------------------

_BT_HANDSHAKE_PREFIX  = b"\x13BitTorrent protocol"
_BT_EXTENSION_BYTES   = b"\x00\x00\x00\x00\x00\x10\x00\x00"  # bit 20 = ext protocol
_BEP09_MAX_METADATA   = 10 * 1024 * 1024   # 10 MB — reject absurdly large info dicts
_BEP09_PEER_TIMEOUT   = 8.0                # seconds per peer TCP attempt
_BEP09_DHT_TIMEOUT    = 5.0                # seconds for get_peers UDP walk


def _send_bt_msg(sock: socket.socket, payload: bytes) -> None:
    """Send a length-prefixed BitTorrent protocol message."""
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def _recv_bt_msg(sock: socket.socket, deadline: float) -> bytes | None:
    """
    Receive one length-prefixed BitTorrent message.
    Returns None on timeout, disconnect, or oversized message.
    """
    remaining = deadline - time.time()
    if remaining <= 0:
        return None
    sock.settimeout(min(remaining, 10.0))
    try:
        # Read 4-byte length prefix
        buf = b""
        while len(buf) < 4:
            chunk = sock.recv(4 - len(buf))
            if not chunk:
                return None
            buf += chunk
        length = struct.unpack("!I", buf)[0]
        if length == 0 or length > _BEP09_MAX_METADATA + 1024:
            return None

        # Read payload
        buf = b""
        while len(buf) < length:
            chunk = sock.recv(min(length - len(buf), 65536))
            if not chunk:
                return None
            buf += chunk
        return buf
    except Exception:
        return None


def _get_peers_dht(info_hash: bytes, timeout: float = _BEP09_DHT_TIMEOUT) -> list[tuple]:
    """
    Two-round DHT get_peers walk. Returns list of (ip, port) peer tuples.

    Round 1: Query 16 nodes from the pool (random selection for spread).
    Round 2: If no peers found, query closer nodes returned in round-1 responses.

    Uses 1 s per-recv timeout — does NOT break on timeout, keeps trying until
    the overall deadline. This is critical since DHT nodes respond asynchronously.
    Synchronous/blocking — safe to call from ThreadPoolExecutor workers.
    """
    nodes = load_node_pool()
    if not nodes:
        for host, port in _BOOTSTRAP_HOSTS[:2]:
            try:
                nodes.append((socket.gethostbyname(host), port))
            except Exception:
                pass
    if not nodes:
        return []

    # Pick 16 random nodes for spread (avoid clustering)
    sample = random.sample(nodes, min(16, len(nodes)))

    my_id = random.randbytes(20)
    tid   = random.randbytes(4)
    query = _bencode({
        b"t": tid, b"y": b"q", b"q": b"get_peers",
        b"a": {b"id": my_id, b"info_hash": info_hash},
    })

    peers: list[tuple]  = []
    closer_nodes: list[tuple] = []
    seen_addrs: set[tuple]    = set()
    deadline = time.time() + timeout

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1.0)          # per-recv timeout; loop continues until deadline
    try:
        # Round 1 — query initial sample
        for ip, port in sample:
            try:
                sock.sendto(query, (ip, port))
                seen_addrs.add((ip, port))
            except Exception:
                pass

        round2_sent = False
        while time.time() < deadline:
            try:
                data, _ = sock.recvfrom(4096)
                resp = bdecode(data)
                if resp.get(b"y") != b"r":
                    continue
                r = resp.get(b"r", {})

                # "values" = compact peer list (6 bytes each: 4 IP + 2 port)
                for v in r.get(b"values", []):
                    if isinstance(v, bytes) and len(v) == 6:
                        p_ip   = socket.inet_ntoa(v[:4])
                        p_port = struct.unpack_from("!H", v, 4)[0]
                        if p_port > 0:
                            peers.append((p_ip, p_port))
                if peers:
                    break     # got peers — done

                # "nodes" = closer routing nodes to query in round 2
                node_data = r.get(b"nodes", b"")
                if isinstance(node_data, bytes):
                    for i in range(0, len(node_data) - 25, 26):
                        n_ip   = socket.inet_ntoa(node_data[i + 20:i + 24])
                        n_port = struct.unpack_from("!H", node_data, i + 24)[0]
                        addr   = (n_ip, n_port)
                        if n_port > 0 and addr not in seen_addrs:
                            closer_nodes.append(addr)
                            seen_addrs.add(addr)

            except socket.timeout:
                # On timeout: if we have closer nodes and haven't done round 2, send now
                if not round2_sent and closer_nodes and time.time() < deadline - 1.5:
                    for ip, port in closer_nodes[:8]:
                        try:
                            sock.sendto(query, (ip, port))
                        except Exception:
                            pass
                    round2_sent = True
                    closer_nodes.clear()
                # Keep looping until deadline
                continue
            except Exception:
                continue

    finally:
        sock.close()

    return peers[:10]


def _fetch_bep09_metadata(ip: str, port: int,
                           info_hash: bytes, timeout: float = _BEP09_PEER_TIMEOUT) -> bytes | None:
    """
    Connect to a BitTorrent peer via TCP, perform extension handshake (BEP-10),
    and download the torrent info dict via ut_metadata (BEP-09).
    Returns raw bencoded info-dict bytes, or None on any failure.
    SHA1 of returned bytes is guaranteed to equal info_hash.
    """
    peer_id = b"-CC0001-" + random.randbytes(12)
    deadline = time.time() + timeout

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip, port))

        # --- Handshake ---
        sock.sendall(_BT_HANDSHAKE_PREFIX + _BT_EXTENSION_BYTES + info_hash + peer_id)

        hs = b""
        while len(hs) < 68:
            chunk = sock.recv(68 - len(hs))
            if not chunk:
                return None
            hs += chunk

        if hs[:20] != _BT_HANDSHAKE_PREFIX:
            return None
        if not (hs[25] & 0x10):          # extension protocol bit
            return None
        if hs[28:48] != info_hash:        # wrong torrent
            return None

        # --- Extension handshake (ext_id=0) ---
        ext_hs_payload = _bencode({b"m": {b"ut_metadata": 1}})
        _send_bt_msg(sock, b"\x14\x00" + ext_hs_payload)

        # --- Wait for their extension handshake ---
        ut_id  = None
        meta_size = 0
        while time.time() < deadline:
            msg = _recv_bt_msg(sock, deadline)
            if msg is None:
                return None
            if not msg or msg[0] != 20:   # not an extension message
                continue
            if msg[1] != 0:               # not handshake
                continue
            try:
                d = bdecode(msg[2:])
                ut_id     = d.get(b"m", {}).get(b"ut_metadata")
                meta_size = d.get(b"metadata_size", 0)
                if ut_id and 0 < meta_size <= _BEP09_MAX_METADATA:
                    break
            except Exception:
                return None

        if not ut_id or meta_size <= 0:
            return None

        # --- Request all metadata pieces ---
        num_pieces = (meta_size + 16383) // 16384
        for piece_idx in range(num_pieces):
            req = _bencode({b"msg_type": 0, b"piece": piece_idx})
            _send_bt_msg(sock, bytes([20, ut_id]) + req)

        # --- Collect metadata pieces ---
        pieces: dict[int, bytes] = {}
        while len(pieces) < num_pieces and time.time() < deadline:
            msg = _recv_bt_msg(sock, deadline)
            if msg is None:
                break
            if not msg or msg[0] != 20 or msg[1] != ut_id:
                continue
            try:
                d, consumed = _bdecode_inner(msg[2:], 0)
                if d.get(b"msg_type") == 1:  # data
                    idx = d.get(b"piece", -1)
                    if 0 <= idx < num_pieces:
                        pieces[idx] = msg[2 + consumed:]
            except Exception:
                continue

        if len(pieces) < num_pieces:
            return None

        # --- Reassemble and verify SHA1 ---
        metadata = b"".join(pieces[i] for i in range(num_pieces))
        if hashlib.sha1(metadata).digest() != info_hash:
            return None

        return metadata

    except Exception:
        return None
    finally:
        try:
            sock.close()
        except Exception:
            pass


def _resolve_via_bep09(hash_hex: str) -> dict | None:
    """
    Resolve a hash to media metadata via BEP-09 direct peer metadata fetch.

    Steps:
      1. DHT get_peers walk → find IP:port of peers holding this torrent
      2. TCP connect to each peer → BitTorrent + extension handshake
      3. Download ut_metadata → verify SHA1 → extract torrent name

    Hit rate: ~30–50% for actively shared hashes (vs 0.1% for external caches).
    Time cost: ~5–15 s per hash (dominated by DHT walk + TCP connect).
    """
    try:
        info_hash = bytes.fromhex(hash_hex)
    except ValueError:
        return None

    peers = _get_peers_dht(info_hash)
    if not peers:
        return None

    for ip, port in peers[:5]:
        metadata = _fetch_bep09_metadata(ip, port, info_hash)
        if not metadata:
            continue
        try:
            info_dict = bdecode(metadata)
            name_b = info_dict.get(b"name", b"")
            name   = name_b.decode("utf-8", errors="replace").strip() if name_b else ""
            if not name or len(name) < 3:
                continue
            category = _guess_category(name)
            if category in MEDIA_CATEGORIES:
                return {"name": name, "category": category, "seeders": 0}
            return None  # found but not media — skip
        except Exception:
            continue

    return None


def resolve_hash_media(hash_hex: str, bep09: bool = False) -> dict | None:
    """
    Resolve a hash to media metadata.

    Strategy (in order):
      1. Torrent cache (itorrents.org → torrage.info) — fast, 0.1% hit rate
      2. BEP-09 direct peer fetch — slower but ~30–50% hit rate (opt-in via bep09=True)

    Returns {"name": str, "category": str, "seeders": int} or None.
    """
    result = _resolve_via_torrent_cache(hash_hex)
    if result is not None:
        return result
    if bep09:
        return _resolve_via_bep09(hash_hex)
    return None


_BEP09_BATCH_CAP     = 5_000   # max hashes for BEP-09 pass (slower but high hit rate)
_BEP09_WORKERS       = 15      # concurrent TCP connections for BEP-09


def _run_resolve_pass(
    candidates: list[dict],
    bep09: bool,
    max_workers: int,
    label: str,
) -> tuple[list[dict], set[str]]:
    """
    Run one resolution pass over `candidates`.
    Returns (media_results, resolved_hashes_set).
    """
    media: list[dict] = []
    resolved_hashes: set[str] = set()
    resolved = 0
    checked  = 0
    total    = len(candidates)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(resolve_hash_media, h["hash"], bep09): h
            for h in candidates
        }
        for future in concurrent.futures.as_completed(futures):
            h = futures[future]
            checked += 1
            try:
                result = future.result()
            except Exception:
                result = None
            if result:
                resolved += 1
                resolved_hashes.add(h["hash"])
                media.append({
                    **h,
                    "name":     result["name"],
                    "category": result["category"],
                    "seeders":  result.get("seeders", 0),
                })
            if checked % 1000 == 0:
                pct = resolved / max(checked, 1) * 100
                print(
                    f"  [{label}]  {checked:,}/{total:,} checked  "
                    f"media={resolved:,}  ({pct:.1f}% hit rate)",
                    flush=True,
                )

    return media, resolved_hashes


def resolve_media_batch(
    new_hashes: list[dict],
    max_workers: int = 40,
    cap: int = _RESOLVE_BATCH_CAP,
    enable_bep09: bool = False,
) -> list[dict]:
    """
    Resolve a batch of discovered hashes. Two-pass pipeline:

    Pass 1 — Torrent cache (itorrents.org / torrage.info)
      • Fast (6 s timeout), low hit rate (~0.1%)
      • Run over all `cap` candidates in parallel (40 workers)

    Pass 2 — BEP-09 direct peer fetch  (only when enable_bep09=True)
      • Slower (5–15 s), high hit rate (~30–50%)
      • Run over top _BEP09_BATCH_CAP un-resolved hashes (15 workers)
      • Fetches metadata directly from DHT peers — no external APIs needed

    Hashes sorted by `num` DESC (busier DHT nodes first — higher probability
    the torrent is actively shared and BEP-09 peers can be found).
    """
    candidates = sorted(new_hashes, key=lambda h: h.get("num", 0), reverse=True)
    if len(candidates) > cap:
        print(f"  [resolve]  Capping batch: {len(candidates):,} → {cap:,} "
              f"(highest-num hashes first)", flush=True)
        candidates = candidates[:cap]

    # --- Pass 1: torrent cache ---
    print(f"  [resolve]  Pass 1 — torrent cache ({max_workers} workers) …", flush=True)
    media, cache_hits = _run_resolve_pass(candidates, bep09=False,
                                          max_workers=max_workers, label="cache")
    print(f"  [resolve]  Pass 1 done — {len(media):,} media titles from cache", flush=True)

    # --- Pass 2: BEP-09 (optional) ---
    if enable_bep09:
        unresolved = [h for h in candidates if h["hash"] not in cache_hits]
        bep09_candidates = unresolved[:_BEP09_BATCH_CAP]
        print(
            f"  [resolve]  Pass 2 — BEP-09 direct peer fetch "
            f"({len(bep09_candidates):,} hashes, {_BEP09_WORKERS} workers) …",
            flush=True,
        )
        bep09_media, _ = _run_resolve_pass(bep09_candidates, bep09=True,
                                           max_workers=_BEP09_WORKERS, label="bep09")
        print(f"  [resolve]  Pass 2 done — {len(bep09_media):,} additional media titles", flush=True)
        media.extend(bep09_media)

    print(f"  [resolve]  Total media resolved: {len(media):,} / {len(candidates):,} "
          f"({len(media)/max(len(candidates),1)*100:.1f}%)", flush=True)
    return media


# ---------------------------------------------------------------------------
# Bencode (minimal — no external deps)
# ---------------------------------------------------------------------------

def _bencode(obj) -> bytes:
    if isinstance(obj, bytes):
        return str(len(obj)).encode() + b":" + obj
    if isinstance(obj, int):
        return b"i" + str(obj).encode() + b"e"
    if isinstance(obj, list):
        return b"l" + b"".join(_bencode(v) for v in obj) + b"e"
    if isinstance(obj, dict):
        items = sorted(obj.items(), key=lambda kv: kv[0])
        return b"d" + b"".join(_bencode(k) + _bencode(v) for k, v in items) + b"e"
    raise TypeError(f"Cannot bencode {type(obj)}")


def _bdecode_inner(data: bytes, pos: int):
    ch = data[pos:pos+1]
    if ch == b"i":
        end = data.index(b"e", pos + 1)
        return int(data[pos+1:end]), end + 1
    if ch == b"l":
        pos += 1; lst = []
        while data[pos:pos+1] != b"e":
            obj, pos = _bdecode_inner(data, pos); lst.append(obj)
        return lst, pos + 1
    if ch == b"d":
        pos += 1; d = {}
        while data[pos:pos+1] != b"e":
            k, pos = _bdecode_inner(data, pos)
            v, pos = _bdecode_inner(data, pos)
            d[k] = v
        return d, pos + 1
    colon = data.index(b":", pos)
    length = int(data[pos:colon])
    start  = colon + 1
    return data[start:start + length], start + length


def bdecode(data: bytes):
    obj, _ = _bdecode_inner(data, 0)
    return obj

# ---------------------------------------------------------------------------
# Compact node/peer parsing
# ---------------------------------------------------------------------------

def _compact_to_nodes(data: bytes) -> list[tuple]:
    """IPv4 compact node list (26 bytes each) → [(node_id, ip, port)]."""
    nodes = []
    if not isinstance(data, bytes):
        return nodes
    for i in range(0, len(data) - 25, 26):
        port = struct.unpack_from("!H", data, i + 24)[0]
        if port > 0:
            nodes.append((data[i:i+20], socket.inet_ntoa(data[i+20:i+24]), port))
    return nodes

# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

def _resolve_bootstrap() -> list[tuple]:
    resolved = []
    for host, port in _BOOTSTRAP_HOSTS:
        try:
            ip = socket.gethostbyname(host)
            resolved.append((ip, port))
        except socket.gaierror:
            pass
    return resolved


def load_node_pool() -> list[tuple]:
    """Load [(ip, port)] from the main scanner's node pool."""
    if not NODE_POOL_PATH.exists():
        return []
    try:
        with open(NODE_POOL_PATH) as f:
            data = json.load(f)
        # Format: [node_id_hex, ip, port] or [node_id_hex, ip, port, verified]
        nodes = []
        for n in data:
            if len(n) >= 3:
                nodes.append((n[1], n[2]))
        return nodes
    except Exception:
        return []

# ---------------------------------------------------------------------------
# DHT Transport — BEP-51 sample_infohashes
# ---------------------------------------------------------------------------

class BEP51Transport(asyncio.DatagramProtocol):
    """UDP transport for sending sample_infohashes queries (BEP-51).

    Handles outbound requests and routes responses back to the waiting
    coroutine via asyncio.Queue (same tid-waiter pattern as the main scanner).
    Also receives and discards unsolicited inbound queries (ping, get_peers etc.)
    to stay friendly to DHT peers — they may include us in routing tables.
    """

    def __init__(self):
        self.transport  = None
        self._waiters: dict[bytes, asyncio.Queue] = {}
        self.own_node_id: bytes = random.randbytes(20)

        # Stats
        self.queries_sent    = 0
        self.responses_recv  = 0
        self.samples_recv    = 0
        self.nodes_recv      = 0

    def connection_made(self, transport):
        self.transport = transport

    def _send(self, msg: bytes, addr: tuple):
        try:
            self.transport.sendto(msg, addr)
        except Exception:
            pass

    def datagram_received(self, data: bytes, addr):
        try:
            msg = bdecode(data)
        except Exception:
            return

        y   = msg.get(b"y", b"")
        tid = msg.get(b"t", b"")

        if y == b"r":
            q = self._waiters.get(tid)
            if q:
                q.put_nowait(msg)
                self.responses_recv += 1
            return

        # Inbound query — ACK minimally so we stay friendly
        if y == b"q":
            q_type = msg.get(b"q", b"")
            if q_type == b"ping":
                self._send(_bencode({
                    b"t": tid, b"y": b"r",
                    b"r": {b"id": self.own_node_id},
                }), addr)

    def error_received(self, exc):
        pass

    def send_sample_infohashes(self, addr: tuple,
                                target: bytes) -> tuple[bytes, asyncio.Queue]:
        """Send a BEP-51 sample_infohashes query.

        target: random 20-byte node_id used to select which slice of the
                keyspace we want samples from. Rotate through all 256 first-byte
                values to cover the full DHT keyspace over time.
        """
        tid = random.randbytes(4)
        msg = _bencode({
            b"t": tid, b"y": b"q", b"q": b"sample_infohashes",
            b"a": {
                b"id":     self.own_node_id,
                b"target": target,
            },
        })
        q = asyncio.Queue()
        self._waiters[tid] = q
        self._send(msg, addr)
        self.queries_sent += 1
        return tid, q

    def cancel(self, tid: bytes):
        self._waiters.pop(tid, None)

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def load_known_hashes() -> set[str]:
    """Load all hashes already in hashes_v2.db — skip these when crawling."""
    if not DB_PATH.exists():
        return set()
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT hash FROM hashes").fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


_STRIP_QUALITY = re.compile(
    r"[\._\s](?:1080p|2160p|4K|720p|480p|BluRay|Blu-Ray|BDRip|BRRip|WEB-DL|WEBRip"
    r"|HDCAM|CAM|HEVC|x264|x265|H\.264|H\.265|AVC|DTS|AAC|AC3|DD5|AMZN|NF|HULU"
    r"|mkv|mp4|avi|srt|rarbg|yify|yts|eztv|successfulcrab).*$",
    re.IGNORECASE,
)
_STRIP_EPISODE = re.compile(r"[\._\s][Ss]\d{1,2}[Ee]\d{1,2}.*$")
_CLEAN_SEPS    = re.compile(r"[\._]+")


def _normalise_title(raw: str) -> str:
    """
    Strip quality/episode markers from a torrent name to get a clean title.
    'Bones.S12E03.The.New.Tricks.1080p.WEB-DL' → 'bones'
    'The Boys S04E08 1080p' → 'the boys'
    """
    t = raw
    t = _STRIP_EPISODE.sub("", t)    # strip episode info first (SxxExx…)
    t = _STRIP_QUALITY.sub("", t)    # strip quality markers
    t = _CLEAN_SEPS.sub(" ", t)      # dots/underscores → spaces
    # Strip group tags: [GroupName] or -GroupName at end
    t = re.sub(r"\s*[\[\(][^\]\)]*[\]\)]\s*$", "", t)
    t = re.sub(r"\s+-\w+$", "", t)
    # Strip year in parens
    t = re.sub(r"\s*\(\d{4}\)\s*", "", t)
    return t.strip().lower()


def _match_catalog_title(conn: sqlite3.Connection, raw_title: str) -> tuple[str, str] | None:
    """
    Try to match a raw torrent title to a catalog entry in the `titles` table.
    Returns (ip_id, canonical_title) if matched, None otherwise.

    Strategy:
      1. Normalise the torrent name (strip quality/episode tags)
      2. Try exact LOWER() match against titles table
      3. Try prefix match (torrent title starts with catalog title)
    """
    norm = _normalise_title(raw_title)
    if not norm or len(norm) < 3:
        return None

    # Exact match
    row = conn.execute(
        "SELECT ip_id, title FROM titles WHERE LOWER(title) = ? LIMIT 1",
        (norm,)
    ).fetchone()
    if row:
        return row[0], row[1]

    # Prefix match — catalog title must be at least 4 chars to avoid false positives
    words = norm.split()
    for n_words in range(min(len(words), 5), 1, -1):
        prefix = " ".join(words[:n_words])
        if len(prefix) < 4:
            continue
        row = conn.execute(
            "SELECT ip_id, title FROM titles "
            "WHERE LOWER(title) = ? OR LOWER(title) LIKE ? LIMIT 1",
            (prefix, prefix + " %"),
        ).fetchone()
        if row:
            return row[0], row[1]

    return None


def insert_discovered_hashes(new_hashes: list[dict], today: str,
                             filter_media: bool = False) -> int:
    """
    Insert newly discovered hashes directly into hashes_v2.db.

    With filter_media=False (default):
      Inserts all hashes with placeholder metadata (title="BEP-51 Discovery",
      category="Unknown"). Requires a separate enrichment pass (collect.py) to resolve.

    With filter_media=True:
      Only inserts hashes resolved to Movies/Series/Anime with real titles.
      Tries to match each resolved title against the `titles` catalog table:
        - If matched: inserts with the real ip_id from the catalog
        - If unmatched: inserts with a synthetic bep51-xxxx ip_id (still useful
          for the peer counter even without a catalog match)
      Adult content and unresolved hashes are always skipped.

    Returns number of rows inserted.
    """
    if not DB_PATH.exists() or not new_hashes:
        return 0
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        inserted = 0
        catalog_matched = 0
        for h in new_hashes:
            hash_hex = h["hash"]
            if filter_media:
                title    = h.get("name", "BEP-51 Discovery")
                category = h.get("category", "Unknown")
                seeders  = h.get("seeders", 0)
                # Try catalog match for a real ip_id
                catalog = _match_catalog_title(conn, title)
                if catalog:
                    ip_id = catalog[0]
                    title = catalog[1]  # use canonical catalog title
                    catalog_matched += 1
                else:
                    ip_id = f"bep51-{hash_hex[:12]}"
            else:
                title    = "BEP-51 Discovery"
                category = "Unknown"
                seeders  = 0
                ip_id    = f"bep51-{hash_hex[:12]}"
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO hashes
                        (hash, ip_id, title, category, source, first_seen, last_seen, seeders)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    hash_hex, ip_id, title, category,
                    "bep51", today, today, seeders,
                ))
                if conn.execute("SELECT changes()").fetchone()[0] > 0:
                    inserted += 1
            except sqlite3.Error:
                pass
        conn.commit()
        conn.close()
        if filter_media and catalog_matched:
            print(f"  [bep51]  Catalog matched: {catalog_matched:,}/{len(new_hashes):,} "
                  f"hashes linked to real ip_ids", flush=True)
        return inserted
    except Exception as e:
        print(f"  [bep51] DB insert failed: {e}")
        return 0

# ---------------------------------------------------------------------------
# Crawl coroutine
# ---------------------------------------------------------------------------

async def sample_one(addr: tuple, target: bytes,
                     transport: BEP51Transport,
                     timeout: float,
                     rate_limited: dict) -> dict | None:
    """
    Send one sample_infohashes query and return the parsed response dict.

    Returns None on timeout or error.
    Respects per-node rate limits: if a node responded with interval=N,
    we skip it for N seconds (rate_limited dict: addr → next_allowed_time).
    """
    # Rate limit check
    now = time.monotonic()
    if rate_limited.get(addr, 0) > now:
        return None

    tid, q = transport.send_sample_infohashes(addr, target)
    try:
        msg = await asyncio.wait_for(q.get(), timeout=timeout)
        r   = msg.get(b"r", {})

        # Record rate limit for this node (use interval if given, else 10s default)
        interval = r.get(b"interval", 10)
        if isinstance(interval, int) and interval > 0:
            rate_limited[addr] = time.monotonic() + interval

        return r
    except asyncio.TimeoutError:
        return None
    finally:
        transport.cancel(tid)


async def run_crawler(
    duration_secs: int,
    concurrency: int,
    num_sockets: int,
    query_timeout: float,
    known_hashes: set[str],
    min_num: int,
) -> tuple[list[dict], dict]:
    """
    Crawl the DHT using BEP-51 sample_infohashes for `duration_secs` seconds.

    Keyspace coverage strategy:
    - Rotate through all 256 first-byte values for the target node_id.
    - Each socket has a unique own_node_id → probes different DHT slices.
    - Use the node pool from the main scanner for a warm start.

    Returns:
        new_hashes  — list of {"hash": hex, "num": int, "seen_at": iso}
        stats       — dict of counters for the summary
    """
    loop       = asyncio.get_running_loop()
    deadline   = time.monotonic() + duration_secs

    # Create N UDP sockets
    transports_protos: list[tuple] = []
    for _ in range(num_sockets):
        t, p = await loop.create_datagram_endpoint(
            BEP51Transport,
            local_addr=("0.0.0.0", 0),
            family=socket.AF_INET,
        )
        transports_protos.append((t, p))
    protocols = [p for _, p in transports_protos]

    # Seed node queue from bootstrap + saved pool
    bootstrap  = _resolve_bootstrap()
    pool_nodes = load_node_pool()
    node_queue: deque[tuple] = deque()

    # Mix pool nodes and bootstrap, capped so we don't start with too much
    seed = list(set(pool_nodes + bootstrap))
    random.shuffle(seed)
    node_queue.extend(seed[:5000])

    # Add bootstrap if queue is empty
    if not node_queue:
        for ip, port in bootstrap:
            node_queue.append((ip, port))

    print(f"  Node queue seeded : {len(node_queue):,} nodes "
          f"({len(bootstrap)} bootstrap + {min(len(pool_nodes), 5000)} pool)")

    # Shared state
    discovered:    set[str]      = set()    # hashes seen this session
    new_hashes:    list[dict]    = []       # hashes NOT in known_hashes
    seen_nodes:    set[tuple]    = set()    # avoid re-querying same node too soon
    rate_limited:  dict          = {}       # addr → next_allowed_monotonic

    # Keyspace rotation: cycle through all 256 first-byte values so we sample
    # hashes from every corner of the DHT, not just what's near our node_id.
    current_prefix = [0]

    # Use a mutable container so _next_target can reassign without nonlocal
    prefix_state = [iter(range(256))]

    def _next_target() -> bytes:
        """Generate a random target node_id with rotating first byte."""
        try:
            current_prefix[0] = next(prefix_state[0])
        except StopIteration:
            # Exhausted all 256 prefixes — restart
            prefix_state[0] = iter(range(256))
            current_prefix[0] = next(prefix_state[0])
        return bytes([current_prefix[0]]) + random.randbytes(19)

    semaphore = asyncio.Semaphore(concurrency)
    stats     = {"queries": 0, "responses": 0, "nodes_added": 0,
                 "samples_total": 0, "new_found": 0}

    async def process_one(addr: tuple, proto: BEP51Transport):
        target = _next_target()
        async with semaphore:
            resp = await sample_one(addr, target, proto, query_timeout, rate_limited)

        if resp is None:
            return

        stats["responses"] += 1
        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # Extract samples (concatenated 20-byte info_hashes)
        raw_samples = resp.get(b"samples", b"")
        num_on_node = resp.get(b"num", 0)   # total hashes this node stores

        if isinstance(raw_samples, bytes) and len(raw_samples) >= 20:
            n_samples = len(raw_samples) // 20
            stats["samples_total"] += n_samples

            for i in range(n_samples):
                ih_bytes = raw_samples[i*20 : i*20+20]
                ih_hex   = ih_bytes.hex()

                if ih_hex in discovered:
                    continue
                discovered.add(ih_hex)

                if ih_hex not in known_hashes:
                    # Filter by min_num: only keep hashes from busy nodes
                    if min_num > 0 and isinstance(num_on_node, int) and num_on_node < min_num:
                        continue
                    new_hashes.append({
                        "hash":       ih_hex,
                        "num":        num_on_node if isinstance(num_on_node, int) else 0,
                        "seen_at":    now_iso,
                        "first_seen": str(date.today()),
                    })
                    stats["new_found"] += 1

        # Add routing nodes to queue for further crawling
        raw_nodes = resp.get(b"nodes", b"")
        for node_id, ip, port in _compact_to_nodes(raw_nodes):
            addr_tuple = (ip, port)
            if addr_tuple not in seen_nodes:
                node_queue.append(addr_tuple)
                stats["nodes_added"] += 1

    # ── Main crawl loop ──────────────────────────────────────────────────
    tasks:     list[asyncio.Task] = []
    last_print = time.monotonic()

    while time.monotonic() < deadline:
        # Drain completed tasks
        tasks = [t for t in tasks if not t.done()]

        # Refill from queue
        while len(tasks) < concurrency * 2 and node_queue and time.monotonic() < deadline:
            addr = node_queue.popleft()
            if addr in seen_nodes:
                continue
            seen_nodes.add(addr)
            # Round-robin across sockets
            proto = protocols[len(tasks) % num_sockets]
            stats["queries"] += 1
            t = asyncio.create_task(process_one(addr, proto))
            tasks.append(t)

        if not tasks and not node_queue:
            # Nothing left — re-seed from bootstrap
            for ip, port in bootstrap:
                node_queue.append((ip, port))
            seen_nodes.clear()   # allow re-querying after full drain
            await asyncio.sleep(1.0)
            continue

        if tasks:
            done, _ = await asyncio.wait(tasks, timeout=1.0,
                                         return_when=asyncio.FIRST_COMPLETED)
            tasks = [t for t in tasks if not t.done()]

        # Progress print every 30s
        elapsed = time.monotonic() - (deadline - duration_secs)
        if time.monotonic() - last_print >= 30:
            last_print = time.monotonic()
            remaining  = max(0, deadline - time.monotonic())
            rate       = stats["new_found"] / max(elapsed, 1) * 60
            print(
                f"  [{elapsed/duration_secs*100:4.0f}%  {remaining/60:.0f}m left]  "
                f"queries={stats['queries']:,}  resp={stats['responses']:,}  "
                f"samples={stats['samples_total']:,}  "
                f"new={stats['new_found']:,}  ({rate:.0f}/min)  "
                f"queue={len(node_queue):,}",
                flush=True,
            )

    # Cancel remaining tasks
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    # Close all sockets
    for t, _ in transports_protos:
        t.close()

    # Aggregate socket stats
    for p in protocols:
        stats["queries"]   = max(stats["queries"], p.queries_sent)

    return new_hashes, stats

# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

FIELDNAMES = ["hash", "num", "seen_at", "first_seen"]


def write_csv(new_hashes: list[dict], path: Path) -> None:
    """Append new hashes to today's discovered CSV."""
    is_new = not path.exists()
    with open(path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if is_new:
            writer.writeheader()
        writer.writerows(new_hashes)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="BEP-51 DHT crawler — discover new info_hashes"
    )
    parser.add_argument("--duration",    type=int,   default=30,
                        help="Crawl duration in minutes (default: 30)")
    parser.add_argument("--concurrency", type=int,   default=200,
                        help="Max concurrent queries per socket (default: 200)")
    parser.add_argument("--sockets",     type=int,   default=2,
                        help="UDP sockets (default: 2)")
    parser.add_argument("--timeout",     type=float, default=3.0,
                        help="Per-query timeout in seconds (default: 3.0)")
    parser.add_argument("--min-num",     type=int,   default=0,
                        help="Only store hashes from nodes with >= N total hashes "
                             "(0 = keep all, default: 0)")
    parser.add_argument("--insert",      action="store_true", default=False,
                        help="Insert new hashes into hashes_v2.db "
                             "(category=Unknown, source=bep51). "
                             "Default: CSV only.")
    parser.add_argument("--filter-media", action="store_true", default=False,
                        dest="filter_media",
                        help="Before inserting, resolve each hash via torrent cache sites "
                             "(itorrents.org / torrage.info) and only keep Movies/Series/Anime. "
                             "Eliminates junk — no separate enrichment pass needed. "
                             "Implies --insert. Uses 40 parallel HTTP workers.")
    parser.add_argument("--resolve-workers", type=int, default=40,
                        dest="resolve_workers",
                        help="HTTP worker threads for --filter-media resolution (default: 40)")
    parser.add_argument("--bep09",       action="store_true", default=False,
                        help="Enable BEP-09 direct peer metadata fetch as second-pass resolver "
                             "(30-50%% hit rate vs 0.1%% for cache-only; slower). "
                             "Requires --filter-media.")
    parser.add_argument("--no-csv",      action="store_true", default=False,
                        help="Skip CSV output (useful with --insert only)")
    args = parser.parse_args()

    # --filter-media implies --insert
    if args.filter_media:
        args.insert = True

    today      = str(date.today())
    csv_path   = DISCOVERED_DIR / f"{today}.csv"
    duration_s = args.duration * 60

    print(f"\n{'='*60}")
    print(f"  BEP-51 DHT Crawler  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"  Duration          : {args.duration} min")
    print(f"  Concurrency       : {args.concurrency}")
    print(f"  Sockets           : {args.sockets}")
    print(f"  Query timeout     : {args.timeout}s")
    print(f"  Min-num filter    : {args.min_num if args.min_num > 0 else 'off'}")
    print(f"  DB insert         : {'yes (media-filtered)' if args.filter_media else 'yes' if args.insert else 'no (CSV only)'}")
    if args.filter_media:
        bep09_note = f" + BEP-09 ({_BEP09_BATCH_CAP:,} hashes)" if args.bep09 else ""
        print(f"  Filter media      : ON — resolve workers={args.resolve_workers}{bep09_note}")
    print(f"  Output CSV        : {csv_path}")
    print()

    # Load known hashes so we don't re-discover what we already track
    known = load_known_hashes()
    print(f"  Known hashes      : {len(known):,} (loaded from DB — will skip these)")

    # Also skip hashes already discovered today (idempotent re-runs)
    already_today: set[str] = set()
    if csv_path.exists():
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                already_today.add(row["hash"])
        known |= already_today
        print(f"  Already today     : {len(already_today):,} (from today's CSV — will skip)")
    print()

    t0 = time.monotonic()

    new_hashes, stats = asyncio.run(run_crawler(
        duration_secs = duration_s,
        concurrency   = args.concurrency,
        num_sockets   = max(1, args.sockets),
        query_timeout = args.timeout,
        known_hashes  = known,
        min_num       = args.min_num,
    ))

    elapsed = time.monotonic() - t0

    # ── Output ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("  BEP-51 Crawl Summary")
    print(f"{'='*60}")
    print(f"  Duration          : {elapsed/60:.1f} min")
    print(f"  Queries sent      : {stats['queries']:,}")
    print(f"  Responses recv    : {stats['responses']:,}  "
          f"({stats['responses']/max(stats['queries'],1)*100:.0f}% hit rate)")
    print(f"  Samples received  : {stats['samples_total']:,}")
    print(f"  New hashes found  : {stats['new_found']:,}  "
          f"({stats['new_found']/max(elapsed/60,0.01):.0f}/min)")
    print(f"  Nodes discovered  : {stats['nodes_added']:,}")

    if new_hashes:
        if not args.no_csv:
            write_csv(new_hashes, csv_path)
            print(f"\n  CSV               : {csv_path} "
                  f"(+{len(new_hashes):,} rows, {csv_path.stat().st_size//1024} KB total)")

        if args.filter_media:
            # Resolve hashes — Pass 1: torrent cache, Pass 2: BEP-09 (if enabled)
            mode = "torrent cache" + (" + BEP-09 peer fetch" if args.bep09 else "")
            print(f"\n  Resolving {len(new_hashes):,} hashes via {mode} "
                  f"({args.resolve_workers} workers)…")
            media_hashes = resolve_media_batch(
                new_hashes,
                max_workers=args.resolve_workers,
                enable_bep09=args.bep09,
            )
            print(f"  Media resolved    : {len(media_hashes):,}/{len(new_hashes):,} "
                  f"({len(media_hashes)/max(len(new_hashes),1)*100:.1f}% hit rate)")
            # Count by category for the summary
            by_cat: dict[str, int] = {}
            for h in media_hashes:
                by_cat[h.get("category", "Unknown")] = by_cat.get(h.get("category", "Unknown"), 0) + 1
            for cat, cnt in sorted(by_cat.items(), key=lambda x: -x[1]):
                print(f"    {cat:<10}: {cnt:,}")
            n_inserted = insert_discovered_hashes(media_hashes, today, filter_media=True)
            print(f"  DB inserted       : {n_inserted:,} media hashes into hashes_v2.db")
            n_skipped = len(new_hashes) - len(media_hashes)
            print(f"  DB skipped        : {n_skipped:,} junk/unresolved hashes (not inserted)")
        elif args.insert:
            n_inserted = insert_discovered_hashes(new_hashes, today)
            print(f"  DB inserted       : {n_inserted:,} new hashes into hashes_v2.db")
            if n_inserted < len(new_hashes):
                dupes = len(new_hashes) - n_inserted
                print(f"  DB skipped        : {dupes:,} already in hashes table")

        # Top 10 sample of discovered hashes
        print("\n  Sample of discovered hashes:")
        for h in new_hashes[:10]:
            print(f"    {h['hash']}  (node had {h['num']:,} total)")
        if len(new_hashes) > 10:
            print(f"    ... and {len(new_hashes)-10:,} more")
    else:
        print("\n  No new hashes discovered this run.")
        if stats["queries"] < 10:
            print("  (Too few queries — check network / bootstrap DNS)")

    print()


if __name__ == "__main__":
    main()
