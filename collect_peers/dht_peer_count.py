#!/usr/bin/env python3
"""
dht_peer_count.py — DHT peer counter with country breakdown
------------------------------------------------------------
Queries the BitTorrent DHT network for each hash in hashes_v2.db,
geolocates peer IPs via GeoLite2, and writes one row per (hash, country).

Output: data/peer_counts/YYYY-MM-DD.csv

Columns:
    date, hash, ip_id, title, category, seeders, country, peer_count

Usage:
    python dht_peer_count.py                     # single pass, daily CSV
    python dht_peer_count.py --loop              # repeat all day until midnight
    python dht_peer_count.py --loop --loop-delay 120  # 2-min cooldown between passes
    python dht_peer_count.py --limit 500         # first N hashes
    python dht_peer_count.py --category Series   # one category
    python dht_peer_count.py --concurrency 150   # concurrent DHT queries
    python dht_peer_count.py --timeout 7         # seconds per hash (default 7)
    python dht_peer_count.py --workers 1         # worker processes (default 1)
"""

import os
import argparse
import asyncio
import concurrent.futures
import csv
import functools
import hashlib
import heapq
import hmac
import json
import math
import multiprocessing as mp
import random
import signal
import socket
import struct
import sqlite3
import threading
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

import geoip2.database
import dht_single_writer as _sw

# Bounded thread pool for GeoIP lookups.
# 4 threads is plenty — the mmdb read is fast and the in-memory cache
# covers most IPs after the first few hundred hashes.
_GEO_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4,
                                                       thread_name_prefix="geoip")

# ---------------------------------------------------------------------------
# Global shared node pool — numpy structured array
# ---------------------------------------------------------------------------
# All concurrent hash queries contribute newly discovered DHT nodes here.
# Each query seeds from BOOTSTRAP + the 300 XOR-closest nodes from this pool.
#
# Storage layout (Step 2 of infra improvements):
#   _POOL_ARR: numpy structured array, 31 bytes/entry × 200K cap = 5.9MB
#   _POOL_IDX: dict[bytes, int]  nid(20B) → row index for O(1) lookup = ~61MB
#
# vs old Python dict approach: ~200MB for pool+verified+last_seen combined
# Saves ~130MB per process vs pre-Step-1 baseline.
#
# _POOL_DTYPE fields:
#   nid       20B  node_id bytes
#   ip_int    4B   IPv4 packed as big-endian uint32
#   port      2B   port uint16
#   verified  1B   1=verified (responded recently), 0=unverified
#   last_seen 4B   unix timestamp uint32 (valid until 2106)
#   responses 2B   lifetime good responses (capped at 65535)  ← Step 3
#   timeouts  2B   lifetime timeouts       (capped at 65535)  ← Step 3

import numpy as np

_POOL_DTYPE = np.dtype([
    ('nid',       'u1', (20,)),
    ('ip_int',    np.uint32),
    ('port',      np.uint16),
    ('verified',  np.uint8),
    ('last_seen', np.uint32),
    ('responses', np.uint16),   # Step 3: quality tracking
    ('timeouts',  np.uint16),   # Step 3: quality tracking
])

_NODE_POOL_MAX  = 200_000
_NODE_POOL_SEED = 300
_NODE_POOL_UNVERIFIED_CAP = int(_NODE_POOL_MAX * 0.20)
_NODE_POOL_MAX_AGE = 6 * 3600   # verified nodes older than 6h are demoted on load
# Step 3: eviction thresholds
_POOL_EVICT_MIN_TIMEOUTS = 5    # evict if timeouts ≥ this AND responses == 0
NODE_POOL_PATH  = Path(__file__).parent / "data" / "node_pool.json"

_POOL_ARR = np.zeros(_NODE_POOL_MAX, dtype=_POOL_DTYPE)  # pre-allocated, 6.5MB (zeros)
_POOL_N   = 0                                              # active entry count
_POOL_IDX: dict[bytes, int] = {}                           # nid_bytes → row index
_POOL_VERIFIED_CNT: int = 0                                # count of verified rows

# IPv6 pool — kept as a compact dict (smaller, ~30K nodes)
# Same tuple layout: nid → (ip: str, port: int, verified: bool, last_seen: float)
_POOL_VERIFIED_CNT_V6: int = 0

# ---------------------------------------------------------------------------
# IPv6 DHT node pool (TODO-5 / BEP-32)
# ---------------------------------------------------------------------------
# Separate from IPv4 pool — IPv6 nodes can only be queried from an AF_INET6
# socket. Keeping them separate avoids trying to reach IPv6 addrs on IPv4.
# Same Kademlia XOR distance metric applies (20-byte node_id is protocol-agnostic).

_NODE_POOL_V6: dict[bytes, tuple] = {}
# nid → (ip: str, port: int, verified: bool, last_seen: float)
NODE_POOL_V6_PATH = Path(__file__).parent / "data" / "node_pool_v6.json"

# ---------------------------------------------------------------------------
# IP ↔ integer helpers (used by numpy pool)
# ---------------------------------------------------------------------------

def _ip4_to_int(ip: str) -> int:
    """Pack an IPv4 string to a big-endian uint32. Returns 0 on failure."""
    try:
        return struct.unpack('>I', socket.inet_aton(ip))[0]
    except OSError:
        return 0

def _int_to_ip4(n: int) -> str:
    """Unpack a big-endian uint32 to an IPv4 string."""
    return socket.inet_ntoa(struct.pack('>I', n))

# ---------------------------------------------------------------------------
# Hash → closest-node cache
# ---------------------------------------------------------------------------
# For each info_hash we've successfully queried, stores the 5 leaf DHT nodes
# (closest by XOR) that responded in the final round.  On the next pass those
# nodes go to the *front* of targets so we hit them in round 1 instead of
# doing all 8 iterative hops.  Memory: ~1.6 MB for 3 K active hashes, capped
# at _HASH_NODE_CACHE_MAX entries so it can never grow unbounded.
#
# Only hashes that had at least one peer are cached — zero-peer hashes rarely
# have useful leaf nodes so the walk result would not help future queries.
#
# Cache is tier-specific: node_pool_active.json → hash_node_cache_active.json.
# Separate caches prevent cross-contamination of routing knowledge.
_HASH_NODE_CACHE: dict[str, list] = {}   # info_hash_hex → [(nid_hex, ip, port), ...]
_HASH_NODE_CACHE_MAX = 50_000            # cap — oldest entries evicted at limit
HASH_CACHE_PATH: Path = Path()           # set in main() alongside NODE_POOL_PATH

# ---------------------------------------------------------------------------
# Async node pool save queue — background thread so saves never block the scan
# ---------------------------------------------------------------------------
# The scan loop puts (path, data) onto the queue and continues immediately.
# The worker thread drains it and writes to disk. If a new save arrives before
# the previous one finishes, the queue holds both; worker processes in order.
# On shutdown the sentinel None flushes and exits the worker thread cleanly.

import queue as _queue
_SAVE_QUEUE: _queue.Queue = _queue.Queue()

def _save_worker() -> None:
    while True:
        item = _SAVE_QUEUE.get()
        if item is None:
            _SAVE_QUEUE.task_done()
            break
        path, tmp_path, data = item
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp_path, "w") as f:
                json.dump(data, f)
            tmp_path.replace(path)
        except Exception as e:
            print(f"  [save_worker] FAILED writing {path.name}: {e}", flush=True)
        finally:
            _SAVE_QUEUE.task_done()

_save_thread = threading.Thread(target=_save_worker, daemon=True, name="node-pool-saver")
_save_thread.start()

# ---------------------------------------------------------------------------
# announce_peer listener — passive peer detection (BEP-5)
# ---------------------------------------------------------------------------

# HMAC secret for stateless token generation. Tokens expire after ~1 hour.
# (Note: each DHTTransport instance uses its own self.own_node_id — see TODO-4)
_TOKEN_SECRET: bytes = random.randbytes(16)

# All tracked info_hashes (binary) — populated at start of each pass.
# O(1) lookup when validating incoming announce_peer messages.
_KNOWN_HASHES: set[bytes] = set()


def _make_token(ip: str) -> bytes:
    """4-byte HMAC token tied to an IP and the current hour."""
    hour = int(time.time()) // 3600
    return hmac.new(_TOKEN_SECRET, f"{ip}:{hour}".encode(), hashlib.sha1).digest()[:4]


def _verify_token(ip: str, token: bytes) -> bool:
    """Accept tokens from current or previous hour (covers ~2h window)."""
    hour = int(time.time()) // 3600
    for h in (hour, hour - 1):
        expected = hmac.new(
            _TOKEN_SECRET, f"{ip}:{h}".encode(), hashlib.sha1
        ).digest()[:4]
        if hmac.compare_digest(expected, token):
            return True
    return False


def _load_known_hashes() -> int:
    """Load all tracked info_hashes into _KNOWN_HASHES for O(1) announce lookup.
    Called at the start of each pass so newly collected hashes are included.
    Returns the count loaded."""
    global _KNOWN_HASHES
    # timeout/busy_timeout so this start-of-pass reader waits for a lock instead
    # of throwing instantly under DB contention (e.g. a heavy concurrent export
    # reading hashes_v2.db) — mirrors the §36 writer guard. A bare connect here
    # was the last residual crash vector.
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    rows = conn.execute("SELECT hash FROM hashes").fetchall()
    conn.close()
    _KNOWN_HASHES = {bytes.fromhex(r[0]) for r in rows}
    return len(_KNOWN_HASHES)


def _pool_swap_remove(idx: int) -> None:
    """O(1) removal from numpy pool: swap slot idx with the last slot, shrink N."""
    global _POOL_N, _POOL_VERIFIED_CNT
    last = _POOL_N - 1
    nid_remove = bytes(_POOL_ARR['nid'][idx])
    if _POOL_ARR['verified'][idx]:
        _POOL_VERIFIED_CNT -= 1
    del _POOL_IDX[nid_remove]
    if idx != last:
        # Move last entry into the freed slot and update its index
        _POOL_ARR[idx] = _POOL_ARR[last]
        moved_nid = bytes(_POOL_ARR['nid'][idx])
        _POOL_IDX[moved_nid] = idx
    _POOL_N -= 1


def _evict_bad_nodes() -> int:
    """Remove nodes that timed out repeatedly without ever responding (Step 3).

    A node is "bad" if it has accumulated _POOL_EVICT_MIN_TIMEOUTS consecutive
    timeouts and zero successful responses.  These nodes are either dead, behind
    a firewall, or actively ignoring us — wasting UDP budget every pass.

    Must only be called from the main asyncio thread (between passes), never
    from a thread-pool executor — no lock needed since asyncio is single-threaded.

    Returns number of nodes evicted.
    """
    if _POOL_N == 0:
        return 0
    n = _POOL_N
    bad_mask = (
        (_POOL_ARR['timeouts'][:n]  >= _POOL_EVICT_MIN_TIMEOUTS) &
        (_POOL_ARR['responses'][:n] == 0)
    )
    bad_indices = list(np.where(bad_mask)[0])
    if not bad_indices:
        return 0
    # Swap-remove in reverse order so earlier indices aren't invalidated
    # by the swap of a later index.
    for idx in sorted(bad_indices, reverse=True):
        _pool_swap_remove(idx)
    return len(bad_indices)


def _pool_add(nodes: list[tuple],
              verified_ids: set[bytes] | None = None,
              v6: bool = False) -> None:
    """
    Add (node_id, ip, port) tuples to the pool; mark any verified_ids as live.
    On eviction, unverified nodes are removed first — verified nodes are kept
    as long as possible since they are known-live and most useful for XOR routing.

    IPv4 (v6=False): numpy structured array (_POOL_ARR / _POOL_IDX) — 31 bytes/entry
    IPv6 (v6=True):  dict-based (_NODE_POOL_V6) — smaller pool, kept simple
    """
    global _POOL_N, _POOL_VERIFIED_CNT, _POOL_VERIFIED_CNT_V6

    if not nodes and not verified_ids:
        return

    if v6:
        # ── IPv6: dict-based (unchanged from Step 1) ───────────────────────
        pool = _NODE_POOL_V6
        vcnt = _POOL_VERIFIED_CNT_V6
        _unverified_cap = int(_NODE_POOL_MAX * 0.20)
        for node_id, ip, port in nodes:
            existing = pool.get(node_id)
            if existing is None:
                if (len(pool) - vcnt) >= _unverified_cap:
                    continue
                pool[node_id] = (ip, port, False, 0.0)
            else:
                pool[node_id] = (ip, port, existing[2], existing[3])
        if verified_ids:
            now = time.time()
            for v in verified_ids:
                existing = pool.get(v)
                if existing is not None:
                    if not existing[2]:
                        vcnt += 1
                    pool[v] = (existing[0], existing[1], True, now)
        _POOL_VERIFIED_CNT_V6 = vcnt
        if len(pool) > _NODE_POOL_MAX:
            excess = len(pool) - _NODE_POOL_MAX
            unverified = [k for k, e in pool.items() if not e[2]]
            to_evict = random.sample(unverified, min(excess, len(unverified)))
            if len(to_evict) < excess:
                vkeys = [k for k, e in pool.items() if e[2]]
                to_evict += random.sample(vkeys, min(excess - len(to_evict), len(vkeys)))
            ev = 0
            for k in to_evict:
                if pool[k][2]: ev += 1
                del pool[k]
            _POOL_VERIFIED_CNT_V6 -= ev
        return

    # ── IPv4: numpy array ────────────────────────────────────────────────────
    _unverified_cap = int(_NODE_POOL_MAX * 0.20)
    now_ts = int(time.time())

    for node_id, ip, port in nodes:
        ip_int = _ip4_to_int(ip)
        if ip_int == 0:
            continue   # skip non-IPv4
        if node_id in _POOL_IDX:
            # Update ip/port in place; preserve verified & last_seen
            idx = _POOL_IDX[node_id]
            _POOL_ARR['ip_int'][idx] = ip_int
            _POOL_ARR['port'][idx]   = port
        else:
            # New node — reject if unverified cap reached
            unverified_count = _POOL_N - _POOL_VERIFIED_CNT
            if unverified_count >= _unverified_cap:
                continue
            if _POOL_N >= _NODE_POOL_MAX:
                # Evict one random unverified before inserting
                unv_indices = np.where(_POOL_ARR['verified'][:_POOL_N] == 0)[0]
                if len(unv_indices):
                    _pool_swap_remove(int(random.choice(unv_indices)))
                else:
                    continue   # all verified, skip new unverified node
            idx = _POOL_N
            _POOL_ARR['nid'][idx]       = np.frombuffer(node_id, dtype='u1')
            _POOL_ARR['ip_int'][idx]    = ip_int
            _POOL_ARR['port'][idx]      = port
            _POOL_ARR['verified'][idx]  = 0
            _POOL_ARR['last_seen'][idx] = 0
            _POOL_ARR['responses'][idx] = 0   # always zero-init Step 3 fields
            _POOL_ARR['timeouts'][idx]  = 0   # (slot may have been used previously)
            _POOL_IDX[node_id] = idx
            _POOL_N += 1

    if verified_ids:
        for v in verified_ids:
            if v in _POOL_IDX:
                idx = _POOL_IDX[v]
                if not _POOL_ARR['verified'][idx]:
                    _POOL_ARR['verified'][idx]  = 1
                    _POOL_VERIFIED_CNT += 1
                _POOL_ARR['last_seen'][idx] = now_ts

    # Bulk eviction if pool is still over cap (shouldn't happen often with per-insert check)
    if _POOL_N > _NODE_POOL_MAX:
        excess = _POOL_N - _NODE_POOL_MAX
        unv_indices = list(np.where(_POOL_ARR['verified'][:_POOL_N] == 0)[0])
        to_evict_idx = random.sample(unv_indices, min(excess, len(unv_indices)))
        if len(to_evict_idx) < excess:
            v_indices = list(np.where(_POOL_ARR['verified'][:_POOL_N] == 1)[0])
            to_evict_idx += random.sample(v_indices, min(excess - len(to_evict_idx), len(v_indices)))
        # Sort descending so swap-remove doesn't invalidate earlier indices
        for idx in sorted(to_evict_idx, reverse=True):
            _pool_swap_remove(idx)


def load_node_pool() -> None:
    """Load persisted node pool from disk (survives restarts).

    Pool entry: nid → (ip, port, verified: bool, last_seen: float)
    Nodes whose last_seen is older than _NODE_POOL_MAX_AGE are demoted to
    unverified (verified=False) — prevents warm-but-stale death spiral.
    """
    global _POOL_N, _POOL_VERIFIED_CNT
    try:
        path = NODE_POOL_PATH
        if not path.exists() or path.stat().st_size == 0:
            tmp = path.with_suffix(".tmp")
            if tmp.exists() and tmp.stat().st_size > 0:
                print("  Node pool         : main file missing/empty — recovering from .tmp")
                tmp.replace(path)
        if NODE_POOL_PATH.exists() and NODE_POOL_PATH.stat().st_size > 0:
            with open(NODE_POOL_PATH, "r") as f:
                loaded = json.load(f)
            # Format v4: [node_id_hex, ip, port, verified_flag, last_seen_ts]  ← current
            # Format v3: [node_id_hex, ip, port, verified_flag]                ← no timestamp
            # Format v2: [node_id_hex, ip, port]                               ← no verified
            now     = time.time()
            demoted = 0
            n_loaded = 0
            for entry in loaded:
                if len(entry) < 3 or n_loaded >= _NODE_POOL_MAX:
                    break
                nid = bytes.fromhex(entry[0])
                if nid in _POOL_IDX:
                    continue   # skip duplicate
                ip_int = _ip4_to_int(entry[1])
                if ip_int == 0:
                    continue   # skip invalid/IPv6 addresses
                port = int(entry[2])
                is_verified = bool(entry[3]) if len(entry) >= 4 else False
                ts = float(entry[4]) if len(entry) >= 5 else 0.0
                if is_verified and ts > 0 and (now - ts) < _NODE_POOL_MAX_AGE:
                    ver, last_seen = 1, int(ts)
                else:
                    ver, last_seen = 0, 0
                    if is_verified:
                        demoted += 1
                _POOL_ARR['nid'][n_loaded]       = np.frombuffer(nid, dtype='u1')
                _POOL_ARR['ip_int'][n_loaded]    = ip_int
                _POOL_ARR['port'][n_loaded]      = port
                _POOL_ARR['verified'][n_loaded]  = ver
                _POOL_ARR['last_seen'][n_loaded] = last_seen
                _POOL_ARR['responses'][n_loaded] = 0   # reset Step 3 counters on load
                _POOL_ARR['timeouts'][n_loaded]  = 0
                _POOL_IDX[nid] = n_loaded
                if ver:
                    _POOL_VERIFIED_CNT += 1
                n_loaded += 1
            _POOL_N = n_loaded
            demoted_note = f"  [{demoted:,} age-demoted]" if demoted else ""
            unverified = _POOL_N - _POOL_VERIFIED_CNT
            print(f"  Node pool         : {_POOL_N:,} nodes loaded  "
                  f"({_POOL_VERIFIED_CNT:,} verified  /  {unverified:,} unverified{demoted_note})")
        else:
            print("  Node pool         : empty (no saved pool yet)")
    except Exception as e:
        print(f"  Node pool         : failed to load ({e}) — starting empty")


def save_node_pool() -> None:
    """Queue an async save of the node pool — returns immediately, write happens in background.

    Format v4: [node_id_hex, ip, port, verified_flag, last_seen_unix_ts]
    Atomic write via .tmp → os.replace, handled by the _save_worker thread.

    If unverified nodes exceed 30% of the pool, only verified nodes are persisted —
    so a bloated bootstrap never gets written to disk (runtime pool still has them;
    they age out naturally).
    """
    n = _POOL_N
    if n == 0:
        return
    unverified_count = n - _POOL_VERIFIED_CNT
    if unverified_count / n > 0.30:
        # Persist only verified entries — skip the unverified bulk
        v_indices = np.where(_POOL_ARR['verified'][:n] == 1)[0]
        data = [
            [bytes(_POOL_ARR['nid'][i]).hex(),
             _int_to_ip4(int(_POOL_ARR['ip_int'][i])),
             int(_POOL_ARR['port'][i]),
             1,
             int(_POOL_ARR['last_seen'][i])]
            for i in v_indices
        ]
        print(f"  Auto-flushed      : {unverified_count:,} unverified nodes from save "
              f"(ratio was {unverified_count/n:.0%})", flush=True)
    else:
        data = [
            [bytes(_POOL_ARR['nid'][i]).hex(),
             _int_to_ip4(int(_POOL_ARR['ip_int'][i])),
             int(_POOL_ARR['port'][i]),
             int(_POOL_ARR['verified'][i]),
             int(_POOL_ARR['last_seen'][i])]
            for i in range(n)
        ]
    _SAVE_QUEUE.put((NODE_POOL_PATH, NODE_POOL_PATH.with_suffix(".tmp"), data))
    print(f"  Node pool queued  : {n:,} nodes  "
          f"({_POOL_VERIFIED_CNT:,} verified)", flush=True)
    save_hash_node_cache()    # always save alongside node pool (uses same async queue)


def load_node_pool_v6() -> None:
    """Load persisted IPv6 node pool from disk (same layout as IPv4)."""
    global _NODE_POOL_V6, _POOL_VERIFIED_CNT_V6
    try:
        if not NODE_POOL_V6_PATH.exists() or NODE_POOL_V6_PATH.stat().st_size == 0:
            tmp = NODE_POOL_V6_PATH.with_suffix(".tmp")
            if tmp.exists() and tmp.stat().st_size > 0:
                tmp.replace(NODE_POOL_V6_PATH)
        if NODE_POOL_V6_PATH.exists() and NODE_POOL_V6_PATH.stat().st_size > 0:
            with open(NODE_POOL_V6_PATH) as f:
                loaded = json.load(f)
            pool    = {}
            vcnt    = 0
            now     = time.time()
            demoted = 0
            for n in loaded:
                if len(n) >= 3:
                    nid = bytes.fromhex(n[0])
                    if len(n) >= 4 and n[3]:
                        ts = float(n[4]) if len(n) >= 5 else 0.0
                        if ts > 0 and (now - ts) < _NODE_POOL_MAX_AGE:
                            pool[nid] = (n[1], n[2], True, ts)
                            vcnt += 1
                        else:
                            pool[nid] = (n[1], n[2], False, 0.0)
                            demoted += 1
                    else:
                        pool[nid] = (n[1], n[2], False, 0.0)
            _NODE_POOL_V6         = pool
            _POOL_VERIFIED_CNT_V6 = vcnt
            demoted_note = f"  [{demoted:,} age-demoted]" if demoted else ""
            print(f"  Node pool (v6)    : {len(pool):,} nodes loaded  "
                  f"({vcnt:,} verified{demoted_note})")
        else:
            print("  Node pool (v6)    : empty (no saved pool yet)")
    except Exception as e:
        print(f"  Node pool (v6)    : failed to load ({e}) — starting empty")


def save_node_pool_v6() -> None:
    """Queue an async save of the IPv6 node pool — returns immediately."""
    if not _NODE_POOL_V6:
        return
    data = [
        [node_id.hex(), e[0], e[1], int(e[2]), e[3]]
        for node_id, e in _NODE_POOL_V6.items()
    ]
    _SAVE_QUEUE.put((NODE_POOL_V6_PATH, NODE_POOL_V6_PATH.with_suffix(".tmp"), data))


def load_hash_node_cache() -> None:
    """Load per-hash closest-node cache from disk into _HASH_NODE_CACHE."""
    global _HASH_NODE_CACHE
    if not HASH_CACHE_PATH or not HASH_CACHE_PATH.exists():
        return
    try:
        with open(HASH_CACHE_PATH) as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            _HASH_NODE_CACHE = loaded
            print(f"  Hash node cache   : {len(_HASH_NODE_CACHE):,} entries loaded  "
                  f"← {HASH_CACHE_PATH.name}", flush=True)
    except Exception as e:
        print(f"  Hash node cache   : failed to load ({e}) — starting empty", flush=True)


def save_hash_node_cache() -> None:
    """Queue an async save of the hash→node cache alongside the node pool save."""
    if not HASH_CACHE_PATH or not _HASH_NODE_CACHE:
        return
    # Evict oldest entries if over cap — dict preserves insertion order in Python 3.7+
    data = dict(_HASH_NODE_CACHE)
    if len(data) > _HASH_NODE_CACHE_MAX:
        excess = len(data) - _HASH_NODE_CACHE_MAX
        for key in list(data.keys())[:excess]:
            del data[key]
    _SAVE_QUEUE.put((HASH_CACHE_PATH, HASH_CACHE_PATH.with_suffix(".tmp"), data))
    print(f"  Hash node cache   : {len(data):,} entries queued", flush=True)


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DB_PATH    = Path(os.environ.get("DHT_DB_PATH", str(Path(__file__).parent / "data" / "hashes_v2.db")))
# Early-stop for the Kademlia walk (env-gated; default 0 = OFF = original 8-round walk).
_EARLYSTOP           = int(os.environ.get("DHT_EARLYSTOP", "0"))
_EARLYSTOP_MINROUNDS = int(os.environ.get("DHT_EARLYSTOP_MINROUNDS", "2"))
_EARLYSTOP_NEWPEERS  = int(os.environ.get("DHT_EARLYSTOP_NEWPEERS", "1"))
# Fixed per-round get_peers timeout in ms (env-gated; 0 = OFF = original timeout/rounds).
# RTT p95 ~280ms; 96% of replies <=300ms, so ~400ms captures nearly all AND removes the
# wasteful 2.5s/round adaptive inflation on popular hashes. See docs/research/dht_timeout_tuning.md
_ROUND_MS = float(os.environ.get("DHT_ROUND_MS", "0"))
OUTPUT_DIR = Path(__file__).parent / "data" / "peer_counts"
GEODB_PATH = Path(__file__).parent / "data" / "GeoLite2-Country.mmdb"

# ---------------------------------------------------------------------------
# Minimal bencode  (stdlib only)
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
# GeoIP
# ---------------------------------------------------------------------------

_geo_reader: geoip2.database.Reader | None = None


def _get_geo_reader() -> geoip2.database.Reader:
    global _geo_reader
    if _geo_reader is None:
        if not GEODB_PATH.exists():
            raise FileNotFoundError(
                f"GeoLite2 DB not found at {GEODB_PATH}.\n"
                "Download from: https://github.com/P3TERX/GeoLite.mmdb"
            )
        _geo_reader = geoip2.database.Reader(str(GEODB_PATH))
    return _geo_reader


@functools.lru_cache(maxsize=200_000)
def ip_to_country(ip: str) -> str:
    """Return ISO 2-letter country code, or 'XX' if unknown.

    Results are cached via lru_cache (up to 200K entries, ~10MB RAM).
    The mmdb binary search is fast (~1µs); the OS keeps the 60MB file
    in page cache so repeated reads of the same CIDR blocks are near-instant.
    No pre-load needed — cache warms up in the first pass.
    """
    try:
        resp = _get_geo_reader().country(ip)
        return resp.country.iso_code or "XX"
    except Exception:
        return "XX"

# ---------------------------------------------------------------------------
# DHT bootstrap nodes
# ---------------------------------------------------------------------------
# Only verified official hostnames — sources are known and maintained.
# Hardcoded IPs were removed: unknown ownership, risk of going stale or
# being reassigned to unrelated hosts.
_BOOTSTRAP_HOSTS = [
    ("router.bittorrent.com",  6881),   # BitTorrent Inc, US
    ("router.utorrent.com",    6881),   # uTorrent (BitTorrent Inc), US
    ("dht.libtorrent.org",     6881),   # libtorrent, EU
    ("dht.transmissionbt.com", 6881),   # Transmission, EU
]

def _resolve_bootstrap() -> list[tuple]:
    resolved = []
    for host, port in _BOOTSTRAP_HOSTS:
        try:
            ip = socket.gethostbyname(host)
            resolved.append((ip, port))
            print(f"  Bootstrap: {host} → {ip}:{port}")
        except socket.gaierror as e:
            print(f"  Bootstrap DNS failed: {host} ({e})")
    return resolved


def _resolve_bootstrap_v6() -> list[tuple]:
    """Resolve bootstrap hostnames to IPv6 addresses (BEP-32).
    Returns empty list if IPv6 DNS is unavailable on this host.
    """
    resolved = []
    for host, port in _BOOTSTRAP_HOSTS:
        try:
            infos = socket.getaddrinfo(host, port, socket.AF_INET6,
                                       socket.SOCK_DGRAM)
            if infos:
                ip = infos[0][4][0]
                resolved.append((ip, port))
                print(f"  Bootstrap (v6): {host} → [{ip}]:{port}")
        except socket.gaierror:
            pass   # IPv6 DNS unavailable — silent, not a fatal error
    return resolved


BOOTSTRAP    = _resolve_bootstrap()
BOOTSTRAP_V6 = _resolve_bootstrap_v6()   # may be empty on IPv4-only hosts


def _xor_dist(node_id: bytes, ih_int: int) -> int:
    """Kademlia XOR distance: node_id (bytes) vs info_hash (pre-converted int)."""
    return int.from_bytes(node_id, "big") ^ ih_int


def _estimate_bloom(bf: bytearray) -> int:
    """
    Estimate peer/seeder count from a BEP-33 bloom filter (256 bytes = 2048 bits).
    Formula from BEP-33 spec:
        n ≈ log(c / m) / (k × log(1 − 1/m))
    where c = zero bits in merged filter, m = 2048, k = 2.
    Returns 0 for empty filter; capped at 50,000 for overflowed filters.
    """
    m = 2048  # 256 bytes × 8 bits
    set_bits = sum(bin(b).count("1") for b in bf)
    c = m - set_bits  # zero bits
    if c == 0:
        return 50_000  # filter is full — severely overflowed, cap estimate
    if c == m:
        return 0       # empty filter — no peers announced
    try:
        return max(0, int(math.log(c / m) / (2 * math.log(1 - 1 / m))))
    except (ValueError, ZeroDivisionError):
        return 0


def _compute_bep33(bfsd: bytes, bfpe: bytes) -> dict:
    """Compute BEP-33 seeder/leecher estimates from raw bloom filter bytes."""
    return {
        "seeders":  _estimate_bloom(bytearray(bfsd)),
        "leechers": _estimate_bloom(bytearray(bfpe)),
    }


def _merge_socket_results(
    results: list[tuple[dict, dict, bytes, bytes]]
) -> tuple[dict, dict, dict]:
    """
    Merge (by_country, ip_country, bfsd_bytes, bfpe_bytes) tuples from N
    parallel DHT walks (one per socket in TODO-4 multi-socket mode).

    - ip_country: union of all IPs across sockets (dedup — same IP always
                  maps to the same country, so last-write-wins is correct)
    - by_country: recount from merged ip_country (avoids double-counting
                  when the same IP appears in multiple sockets' results)
    - bep33:      OR-merge raw bloom bytes first, then estimate once —
                  bloom OR is the correct union operation per BEP-33 spec
    """
    merged_ip_country: dict[str, str] = {}
    bfsd_merged = bytearray(256)
    bfpe_merged = bytearray(256)

    for _, ip_country, bfsd, bfpe in results:
        merged_ip_country.update(ip_country)
        for i in range(256):
            bfsd_merged[i] |= bfsd[i]
            bfpe_merged[i] |= bfpe[i]

    merged_by_country: dict[str, int] = defaultdict(int)
    for country in merged_ip_country.values():
        merged_by_country[country] += 1

    return (
        dict(merged_by_country),
        merged_ip_country,
        _compute_bep33(bytes(bfsd_merged), bytes(bfpe_merged)),
    )


def _compact_to_peers(raw) -> set[str]:
    """Compact IPv4 peer list (6 bytes: 4 IP + 2 port) → set of IP strings."""
    peers = set()
    if isinstance(raw, list):
        for item in raw:
            peers |= _compact_to_peers(item)
        return peers
    if not isinstance(raw, bytes):
        return peers
    for i in range(0, len(raw) - 5, 6):
        port = struct.unpack_from("!H", raw, i + 4)[0]
        if port > 0:
            peers.add(socket.inet_ntoa(raw[i:i+4]))
    return peers


def _compact_to_peers_v6(raw) -> set[str]:
    """Compact IPv6 peer list (18 bytes: 16 IP + 2 port) → set of IP strings.
    Returned in `values6` by nodes when we send want=[n4, n6] (BEP-32).
    """
    peers = set()
    if isinstance(raw, list):
        for item in raw:
            peers |= _compact_to_peers_v6(item)
        return peers
    if not isinstance(raw, bytes):
        return peers
    for i in range(0, len(raw) - 17, 18):
        port = struct.unpack_from("!H", raw, i + 16)[0]
        if port > 0:
            try:
                peers.add(socket.inet_ntop(socket.AF_INET6, raw[i:i+16]))
            except Exception:
                pass
    return peers


def _compact_to_nodes_full(data: bytes) -> list[tuple]:
    """Compact IPv4 node list (26 bytes each) → list of (node_id, ip, port)."""
    nodes = []
    if not isinstance(data, bytes):
        return nodes
    for i in range(0, len(data) - 25, 26):
        port = struct.unpack_from("!H", data, i + 24)[0]
        if port > 0:
            nodes.append((data[i:i+20], socket.inet_ntoa(data[i+20:i+24]), port))
    return nodes


def _compact_to_nodes_full_v6(data: bytes) -> list[tuple]:
    """Compact IPv6 node list (38 bytes each: 20 node_id + 16 IP + 2 port).
    Returned in `nodes6` by nodes supporting BEP-32.
    """
    nodes = []
    if not isinstance(data, bytes):
        return nodes
    for i in range(0, len(data) - 37, 38):
        port = struct.unpack_from("!H", data, i + 36)[0]
        if port > 0:
            try:
                ip = socket.inet_ntop(socket.AF_INET6, data[i+20:i+36])
                nodes.append((data[i:i+20], ip, port))
            except Exception:
                pass
    return nodes


# ---------------------------------------------------------------------------
# Shared DHT transport
# ---------------------------------------------------------------------------

class DHTTransport(asyncio.DatagramProtocol):
    """UDP socket for one DHT node identity.

    Outbound: routes get_peers responses to waiting coroutines by tid.
    Inbound:  handles announce_peer, get_peers, ping, find_node queries
              so we stay in routing tables and receive passive peer detections.

    Each socket has its own `own_node_id` so multiple sockets can occupy
    different slices of the DHT keyspace simultaneously (TODO-4).
    """

    def __init__(self, is_ipv6: bool = False):
        self.transport   = None
        self._waiters:    dict[bytes, asyncio.Queue] = {}
        self.announce_queue: asyncio.Queue | None    = None  # set by run() after creation
        self._announce_hits = 0                              # passive announces this pass
        # Each socket instance gets a unique node_id so it occupies its own
        # position in the DHT keyspace and builds independent routing contacts.
        self.own_node_id: bytes = random.randbytes(20)
        # Protocol flag — determines which node pool and bootstrap list to use.
        self.is_ipv6: bool = is_ipv6

    def connection_made(self, transport):
        self.transport = transport

    def _send(self, msg: bytes, addr: tuple) -> None:
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

        # ── Outbound response: route to waiting coroutine ──────────────────
        if y == b"r":
            q = self._waiters.get(tid)
            if q:
                q.put_nowait(msg)
            return

        # ── Inbound query: handle DHT server-side messages ─────────────────
        if y != b"q":
            return

        q_type    = msg.get(b"q", b"")
        a         = msg.get(b"a", {})
        sender_id = a.get(b"id", b"")

        # Every node that contacts us is implicitly live — add to pool.
        if len(sender_id) == 20:
            _pool_add([(sender_id, addr[0], addr[1])], verified_ids={sender_id})

        if q_type == b"ping":
            self._send(_bencode({
                b"t": tid, b"y": b"r",
                b"r": {b"id": self.own_node_id},
            }), addr)

        elif q_type == b"find_node":
            self._send(_bencode({
                b"t": tid, b"y": b"r",
                b"r": {b"id": self.own_node_id, b"nodes": b""},
            }), addr)

        elif q_type == b"get_peers":
            # Issue a token so the querier can announce_peer to us later.
            # We have no peers to share, so nodes=b"" is fine.
            token = _make_token(addr[0])
            self._send(_bencode({
                b"t": tid, b"y": b"r",
                b"r": {b"id": self.own_node_id, b"token": token, b"nodes": b""},
            }), addr)

        elif q_type == b"announce_peer":
            info_hash    = a.get(b"info_hash", b"")
            token        = a.get(b"token", b"")
            peer_ip      = addr[0]

            # Always ACK — BEP-5 requires a response regardless of acceptance.
            self._send(_bencode({
                b"t": tid, b"y": b"r",
                b"r": {b"id": self.own_node_id},
            }), addr)

            # Record only if: hash is in our watchlist AND token is valid.
            if (info_hash in _KNOWN_HASHES
                    and _verify_token(peer_ip, token)
                    and self.announce_queue is not None):
                try:
                    self.announce_queue.put_nowait((info_hash.hex(), peer_ip))
                    self._announce_hits += 1
                except asyncio.QueueFull:
                    pass  # queue full — drop quietly, don't block event loop

    def error_received(self, exc):
        pass

    def send_get_peers(self, addr: tuple, info_hash: bytes,
                       node_id: bytes) -> tuple[bytes, asyncio.Queue]:
        tid = random.randbytes(4)
        # scrape=1  — requests BEP-33 bloom filters (BFsd/BFpe).
        # want=[n4, n6] — requests BOTH IPv4 and IPv6 node/peer lists (BEP-32).
        # Nodes that don't support either key simply ignore them — zero cost.
        msg = _bencode({
            b"t": tid, b"y": b"q", b"q": b"get_peers",
            b"a": {
                b"id":        node_id,
                b"info_hash": info_hash,
                b"scrape":    1,
                b"want":      [b"n4", b"n6"],
            },
        })
        q = asyncio.Queue()
        self._waiters[tid] = q
        try:
            self.transport.sendto(msg, addr)
        except Exception:
            pass
        return tid, q

    def cancel(self, tid: bytes):
        self._waiters.pop(tid, None)

# ---------------------------------------------------------------------------
# Peer discovery — returns dict of {country_code: peer_count}
# ---------------------------------------------------------------------------

async def get_peers_by_country(
    info_hash_hex: str,
    dht: DHTTransport,
    timeout: float = 10.0,
    rounds: int = 8,
) -> tuple[dict, dict, bytes, bytes]:
    """
    Run one Kademlia get_peers walk for a single DHT socket.

    Returns:
        (by_country, ip_country, bfsd_bytes, bfpe_bytes)

        by_country  — {country_code: peer_count} e.g. {"US": 12, "IN": 5}
        ip_country  — {peer_ip: country_code}
        bfsd_bytes  — 256-byte raw bloom filter for seeders  (BEP-33)
        bfpe_bytes  — 256-byte raw bloom filter for leechers (BEP-33)

    Callers merge results from N sockets with _merge_socket_results(), which
    OR-merges the raw bloom bytes and deduplicates IPs before computing bep33.
    """
    info_hash     = bytes.fromhex(info_hash_hex)
    ih_int        = int.from_bytes(info_hash, "big")   # pre-compute for XOR ops
    node_id       = random.randbytes(20)               # per-hash fallback sender id
    all_peer_ips: set[str]   = set()
    # BEP-33 bloom filters: OR-merged across all responding nodes.
    # 256 bytes = 2048 bits; two filters: seeds (BFsd) and peers/leechers (BFpe).
    bfsd_merged = bytearray(256)
    bfpe_merged = bytearray(256)

    # Select the correct pool and bootstrap list based on socket protocol.
    is_v6      = dht.is_ipv6
    _bootstrap = BOOTSTRAP_V6 if is_v6 else BOOTSTRAP

    # XOR-select the _NODE_POOL_SEED closest pool nodes by Kademlia distance.
    # Nodes closest in keyspace are most likely to hold routing info for this hash.
    #
    # IPv4: vectorized numpy XOR on first 8 bytes of nid (O(N) C-speed).
    #   np.argpartition gives top-k in O(N) vs heapq O(N log k) — and avoids
    #   Python per-item overhead entirely.  For N=200K, k=300: ~5ms → <0.5ms.
    #
    # IPv6: dict-based pool is small; keep heapq path.
    #
    # IMPORTANT: run in executor — the selection loop (even vectorized) must not
    # block the event loop while 150 coroutines are live.
    if is_v6:
        _pool_has_data = bool(_NODE_POOL_V6)
        _snap_v6       = dict(_NODE_POOL_V6) if _pool_has_data else {}
        _snap_n        = 0
    else:
        # Snapshot only the count — use global _POOL_ARR / _POOL_IDX by reference.
        # No copy needed: _xor_select() is read-only on the array; the asyncio
        # event loop is single-threaded and only writes at yield points.  The
        # worst-case race (swap-remove during a numpy read) yields a stale IP
        # address for one XOR candidate — harmless (query fails, timeout credited).
        # Eliminates: 200 concurrent copies × 7MB = 1.4GB  +  200 × 27MB = 5.4GB
        # that the old `.copy()` / `dict()` approach created at concurrency=200.
        _snap_n        = _POOL_N
        _pool_has_data = _snap_n > 0
        _snap_v6       = {}

    if _pool_has_data:
        def _xor_select() -> list[tuple]:
            # ── Step 1: prepend cached leaf nodes for this hash ──────────────
            cached_targets: list[tuple] = []
            if info_hash_hex in _HASH_NODE_CACHE:
                for nid_hex, ip, port in _HASH_NODE_CACHE[info_hash_hex]:
                    nid = bytes.fromhex(nid_hex)
                    cached_targets.append((nid, ip, port))
            cached_ids = {c[0] for c in cached_targets}

            # ── Step 2: XOR-select remaining slots ──────────────────────────
            remaining = _NODE_POOL_SEED - len(cached_targets)
            if remaining <= 0:
                return cached_targets

            if is_v6:
                # Dict path — IPv6 pool is small
                verified_pool = {k: e for k, e in _snap_v6.items()
                                 if e[2] and k not in cached_ids}
                if len(verified_pool) >= remaining:
                    source = verified_pool
                else:
                    source = {k: e for k, e in _snap_v6.items()
                              if k not in cached_ids}
                closest = heapq.nsmallest(
                    remaining, source.items(),
                    key=lambda kv: _xor_dist(kv[0], ih_int),
                )
                rest = [(nid, e[0], e[1]) for nid, e in closest]
                return cached_targets + rest

            # Numpy path — IPv4: read global _POOL_ARR directly (no copy)
            n   = _snap_n   # row count captured at closure-creation time
            arr = _POOL_ARR  # global array — read-only in this function

            # Build boolean mask for entries already in cached_ids
            cached_mask = np.zeros(n, dtype=bool)
            for cid in cached_ids:
                idx_c = _POOL_IDX.get(cid)   # global dict — read-only lookup
                if idx_c is not None and idx_c < n:
                    cached_mask[idx_c] = True

            # XOR on top 8 bytes of nid (64 bits) — sufficient precision for k-closest.
            # ih_int is 160-bit; top 64 bits = ih_int >> 96.
            nid_prefix = np.ascontiguousarray(
                arr['nid'][:n, :8]
            ).view(np.dtype('>u8')).reshape(n)
            ih_prefix = np.uint64(ih_int >> 96)
            xor = nid_prefix ^ ih_prefix

            # Step 3: exclude nodes that have only timed out (bad nodes).
            # bad_mask is True for nodes that haven't responded despite multiple tries.
            bad_mask  = ((arr['timeouts'][:n]  >= _POOL_EVICT_MIN_TIMEOUTS) &
                         (arr['responses'][:n] == 0))
            # Prefer verified and not-bad; fall back progressively.
            _MAX_U64  = np.uint64(0xFFFF_FFFF_FFFF_FFFF)
            v_mask    = (arr['verified'][:n] == 1) & ~cached_mask & ~bad_mask
            v_count   = int(v_mask.sum())
            if v_count >= remaining:
                xor_sel = np.where(v_mask, xor, _MAX_U64)
            else:
                # Fall back to all non-cached non-bad, then include bad if still short
                ok_mask = ~cached_mask & ~bad_mask
                ok_count = int(ok_mask.sum())
                if ok_count >= remaining:
                    xor_sel = np.where(ok_mask, xor, _MAX_U64)
                else:
                    # Last resort: include bad nodes too (better than empty)
                    xor_sel = np.where(~cached_mask, xor, _MAX_U64)

            # argpartition: O(N) — indices of the `remaining` smallest XOR values.
            k = min(remaining, n) - 1
            top_idx = (np.argpartition(xor_sel, k)[:remaining]
                       if k < n - 1 else np.arange(n))

            rest = [
                (bytes(arr['nid'][i]),
                 _int_to_ip4(int(arr['ip_int'][i])),
                 int(arr['port'][i]))
                for i in top_idx
                if not cached_mask[i]
            ][:remaining]
            return cached_targets + rest

        _loop = asyncio.get_running_loop()
        pool_targets = await _loop.run_in_executor(None, _xor_select)
    else:
        pool_targets = []

    # targets: list of (target_node_id: bytes|None, ip: str, port: int)
    # Bootstrap nodes have no known node_id — use None, fall back to random sender.
    targets = [(None, ip, port) for ip, port in _bootstrap] + pool_targets
    round_timeout = (_ROUND_MS / 1000.0) if _ROUND_MS > 0 else (timeout / rounds)

    # queried/seen track (ip, port) only — node_id is irrelevant for dedup.
    queried: set[tuple] = set()
    # Accumulate all responding node_ids across rounds — used to update the
    # hash→node cache after the walk completes.
    all_verified_ids: set[bytes] = set()

    async def query_one(target: tuple) -> dict | None:
        target_node_id, ip, port = target
        addr = (ip, port)
        # ── Sybil / Neighbour node-ID spoofing (TODO-3) ──────────────────
        # Forge our sender node_id to be XOR-close to the target's node_id.
        # The target sees us as its "nearest neighbour" and stores us in its
        # k-bucket permanently → future announce_peer messages flow to us
        # for every hash in our keyspace, even between active passes.
        #
        # Spoofed id = first 15 bytes of target's id + 5 random bytes.
        # This places us firmly in the target's own Kademlia neighbourhood
        # while remaining unique (avoids exact-ID collisions).
        #
        # Nodes that don't know their own id (bootstrap, None) fall back to
        # the per-hash random node_id — still valid, just not spoofed.
        if target_node_id and len(target_node_id) == 20:
            sender_id = target_node_id[:15] + random.randbytes(5)
        else:
            sender_id = node_id  # fallback: per-hash random id
        # ─────────────────────────────────────────────────────────────────
        tid, q = dht.send_get_peers(addr, info_hash, sender_id)
        try:
            resp = await asyncio.wait_for(q.get(), timeout=round_timeout)
            # ── Step 3: credit the target node on success ─────────────────
            if target_node_id and not is_v6 and target_node_id in _POOL_IDX:
                idx = _POOL_IDX[target_node_id]
                if idx < _POOL_N:
                    cnt = int(_POOL_ARR['responses'][idx])
                    if cnt < 65535:
                        _POOL_ARR['responses'][idx] = cnt + 1
            return resp
        except asyncio.TimeoutError:
            # ── Step 3: penalise the target node on timeout ───────────────
            if target_node_id and not is_v6 and target_node_id in _POOL_IDX:
                idx = _POOL_IDX[target_node_id]
                if idx < _POOL_N:
                    cnt = int(_POOL_ARR['timeouts'][idx])
                    if cnt < 65535:
                        _POOL_ARR['timeouts'][idx] = cnt + 1
            return None
        finally:
            dht.cancel(tid)

    rounds_used = 0
    for _round in range(rounds):
        if not targets:
            break
        rounds_used = _round + 1
        _peers_before = len(all_peer_ips)

        seen  = set()
        batch = []
        for t in targets:
            _, ip, port = t
            addr = (ip, port)
            if addr not in queried and addr not in seen:
                seen.add(addr); batch.append(t)
            if len(batch) >= 64:
                break

        targets = []
        if not batch:
            break

        for t in batch:
            _, ip, port = t
            queried.add((ip, port))

        responses = await asyncio.gather(*[query_one(t) for t in batch])

        new_node_data:    list[tuple] = []
        new_node_data_v6: list[tuple] = []
        verified_this_round: set[bytes] = set()
        for resp in responses:
            if resp is None:
                continue
            r = resp.get(b"r", {})
            # IPv4 peers (values) + IPv6 peers (values6, BEP-32)
            # Both are geolocated the same way — MaxMind handles IPv6 natively.
            all_peer_ips |= _compact_to_peers(r.get(b"values", []))
            all_peer_ips |= _compact_to_peers_v6(r.get(b"values6", []))
            # IPv4 routing nodes for next round
            new_node_data.extend(_compact_to_nodes_full(r.get(b"nodes", b"")))
            # IPv6 routing nodes — stored in V6 pool, usable by IPv6 socket only
            new_node_data_v6.extend(_compact_to_nodes_full_v6(r.get(b"nodes6", b"")))
            # Mark the responding node as verified — it just proved it's live.
            # r.id is the responding node's own node_id (always present in responses).
            responder_id = r.get(b"id", b"")
            if len(responder_id) == 20:
                verified_this_round.add(responder_id)
                all_verified_ids.add(responder_id)   # accumulate across all rounds
            # BEP-33: OR-merge bloom filters from every responding node.
            # Nodes that don't support BEP-33 return neither key — safe to skip.
            bfsd = r.get(b"BFsd", b"")
            bfpe = r.get(b"BFpe", b"")
            if len(bfsd) == 256:
                for i, byte in enumerate(bfsd):
                    bfsd_merged[i] |= byte
            if len(bfpe) == 256:
                for i, byte in enumerate(bfpe):
                    bfpe_merged[i] |= byte

        # Always persist IPv6 nodes we discover into the V6 pool regardless of
        # which socket made the query — IPv4 responses include nodes6 too.
        if new_node_data_v6:
            _pool_add(new_node_data_v6, verified_ids=None, v6=True)

        if new_node_data:
            # XOR-sort discovered nodes so next round queries the closest ones first.
            # This is the iterative Kademlia walk — each round gets nearer to the hash.
            new_node_data.sort(key=lambda n: _xor_dist(n[0], ih_int))
            # Keep (node_id, ip, port) tuples — node_id used for Sybil spoofing
            # in the next round. Previously discarded here with [(ip,port) for ...].
            if is_v6:
                # IPv6 socket: use V6 nodes for next round targets
                targets.extend(new_node_data)
                _pool_add(new_node_data, verified_ids=verified_this_round, v6=True)
            else:
                targets.extend(new_node_data)
                _pool_add(new_node_data, verified_ids=verified_this_round, v6=False)
        elif verified_this_round:
            # Responses with no new nodes still carry verified node_ids —
            # mark them as verified directly in the pool entry.
            _pool_add([], verified_ids=verified_this_round, v6=is_v6)

        # Early-stop (env-gated, default OFF): once a round adds no NEW peers,
        # deeper Kademlia jumps mostly find more nodes, not peers -- CPU burn
        # for a ~8.8%-unique signal. Stop after MINROUNDS once new < threshold.
        if (_EARLYSTOP and rounds_used >= _EARLYSTOP_MINROUNDS and
                (len(all_peer_ips) - _peers_before) < _EARLYSTOP_NEWPEERS):
            break

    # Raw bloom filter bytes — returned for proper OR-merging when multiple
    # sockets run in parallel (TODO-4). Callers use _merge_socket_results()
    # to union them across sockets and call _compute_bep33() once at the end.
    bfsd_bytes = bytes(bfsd_merged)
    bfpe_bytes = bytes(bfpe_merged)

    # ── Update hash→node cache ───────────────────────────────────────────────
    # Only cache hashes that had peers — zero-peer results don't have useful
    # leaf nodes worth returning to next pass.
    # Pick the 5 verified responders XOR-closest to this hash; they are the
    # "responsible" nodes in the Kademlia keyspace and most likely to have
    # fresh peer lists on the next query.
    if all_peer_ips and all_verified_ids:
        # XOR-closest verified responders — used as leaf-node hints next pass.
        closest_verified = heapq.nsmallest(
            5, all_verified_ids,
            key=lambda nid: _xor_dist(nid, ih_int),
        )
        cached_entries = []
        for nid in closest_verified:
            if nid in _POOL_IDX:
                idx = _POOL_IDX[nid]
                if idx < _POOL_N:
                    cached_entries.append((
                        nid.hex(),
                        _int_to_ip4(int(_POOL_ARR['ip_int'][idx])),
                        int(_POOL_ARR['port'][idx]),
                    ))
            elif nid in _NODE_POOL_V6:
                e = _NODE_POOL_V6[nid]
                cached_entries.append((nid.hex(), e[0], e[1]))
        if cached_entries:
            _HASH_NODE_CACHE[info_hash_hex] = cached_entries

    # Skip GeoIP executor for zero-peer hashes — no point geolocating nothing.
    # Still return raw bloom bytes so the caller can OR-merge across sockets.
    if not all_peer_ips:
        return {}, {}, bfsd_bytes, bfpe_bytes

    # Geolocate in the bounded GeoIP thread pool (max 4 threads).
    # Using the default pool (32 threads) with 150 concurrent hashes causes all
    # threads to hammer the mmdb reader simultaneously, pegging the CPU.
    def _geolocate_all(ips: set) -> dict[str, str]:
        return {ip: ip_to_country(ip) for ip in ips}

    loop       = asyncio.get_running_loop()
    ip_country = await loop.run_in_executor(_GEO_EXECUTOR, _geolocate_all, all_peer_ips)

    by_country: dict[str, int] = defaultdict(int)
    for country in ip_country.values():
        by_country[country] += 1

    return dict(by_country), ip_country, bfsd_bytes, bfpe_bytes


async def _drain_announces(queue: asyncio.Queue, today: str,
                           announce_buffer: dict) -> int:
    """Background task: drain the announce_peer queue, geolocate, write to DB.

    Also buffers every hit into announce_buffer so run() can flush announce-only
    hashes to the CSV at pass end without a round-trip to SQLite.

    announce_buffer format:
        { hash_hex: { country: set[peer_ip] } }

    Runs for the lifetime of a pass. Each item is (hash_hex, peer_ip).
    Returns total count processed when cancelled at pass end.
    """
    loop  = asyncio.get_running_loop()
    total = 0
    while True:
        try:
            hash_hex, peer_ip = await asyncio.wait_for(queue.get(), timeout=2.0)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break

        # Geolocate in the shared GeoIP executor (same pool as active scanning)
        country = await loop.run_in_executor(
            _GEO_EXECUTOR, ip_to_country, peer_ip
        )
        # Write to SQLite (persistent, survives restarts)
        await loop.run_in_executor(
            None, _upsert_peers_threadsafe, hash_hex, {peer_ip: country}, today
        )
        # Buffer in-memory so run() can flush to CSV at pass end
        announce_buffer.setdefault(hash_hex, {}).setdefault(country, set()).add(peer_ip)
        total += 1

    return total


# ---------------------------------------------------------------------------
# Peers table  (unique IPs per hash, with geolocation + seen dates)
# ---------------------------------------------------------------------------

def init_peers_table(db: sqlite3.Connection):
    db.execute("""
        CREATE TABLE IF NOT EXISTS peers (
            hash       TEXT NOT NULL,
            ip         TEXT NOT NULL,
            country    TEXT,
            first_seen TEXT,
            last_seen  TEXT,
            PRIMARY KEY (hash, ip)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS idx_peers_hash    ON peers(hash)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_peers_country ON peers(country)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_peers_seen    ON peers(last_seen)")
    # Composite index for load_hashes() ordering CTE:
    # WHERE ip != '_queried_' + GROUP BY hash + MAX(last_seen)
    db.execute("CREATE INDEX IF NOT EXISTS idx_peers_real_seen ON peers(ip, hash, last_seen)")
    db.commit()


def upsert_peers(db: sqlite3.Connection, hash_val: str,
                 ip_country: dict[str, str], today: str):
    """
    Insert new IPs or update last_seen for existing ones.
    If ip_country is empty (zero peers found), write a sentinel row
    ip='_queried_' so the hash is marked as done today for resume purposes.
    """
    if ip_country:
        for ip, country in ip_country.items():
            db.execute("""
                INSERT INTO peers (hash, ip, country, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(hash, ip) DO UPDATE SET
                    country   = excluded.country,
                    last_seen = excluded.last_seen
            """, (hash_val, ip, country, today, today))
    else:
        # Sentinel: marks hash as queried today even with no peers found
        db.execute("""
            INSERT INTO peers (hash, ip, country, first_seen, last_seen)
            VALUES (?, '_queried_', 'XX', ?, ?)
            ON CONFLICT(hash, ip) DO UPDATE SET
                last_seen = excluded.last_seen
        """, (hash_val, today, today))


# Thread-local storage: one persistent SQLite connection per thread.
# Opening a new connection per write has measurable overhead (file open, header
# read, lock acquisition). The Python docs recommend threading.local() for
# concurrent SQLite access — connections must never be shared across threads.
_db_local = threading.local()


def _get_thread_db() -> sqlite3.Connection:
    """Return this thread's persistent SQLite connection, creating it if needed."""
    if not hasattr(_db_local, "conn"):
        # timeout=60: with up to 8 worker processes (4 active + 4 dormant) all
        # writing to this one DB, a single SQLite writer lock can be contended
        # for a while. 60s busy-wait absorbs that. synchronous=NORMAL is safe
        # under WAL (no corruption risk, only the last txn is at risk on power
        # loss) and shortens each write's lock window — fewer lock conflicts.
        conn = sqlite3.connect(DB_PATH, timeout=60)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA journal_size_limit=536870912")  # cap WAL at 512MB (was -1 → ballooned to 13GB)
        _db_local.conn = conn
    return _db_local.conn


def _upsert_peers_threadsafe(hash_val: str, ip_country: dict[str, str], today: str):
    """
    Thread-safe variant: uses a thread-local persistent connection.
    Called from run_in_executor — each executor thread reuses its own connection
    instead of opening/closing one per hash (reduces connect overhead ~150×).
    WAL mode allows concurrent readers while a write is in progress.

    Resilience: with 8 worker processes contending for the single SQLite writer
    lock, a write can still raise "database is locked" even after the 60s
    busy-wait (e.g. while the WAL is large / a checkpoint is in flight). That must
    NOT crash the whole worker — a crash aborts the entire pass and trips
    Restart=always churn. Instead we retry with backoff; on persistent failure we
    log and drop this single upsert. The write is idempotent (ON CONFLICT) and the
    peer is re-discovered next pass, so dropping one is self-healing.
    """
    if _sw.ENABLED:
        try:
            _sw.ensure_writer(str(DB_PATH))
            _sw.enqueue(hash_val, ip_country, today)
            return
        except Exception as _ipc_e:
            if not getattr(_sw, '_warned', False):
                print(f'  WARN: single-writer IPC failed ({_ipc_e!r}); direct-write fallback', flush=True)
                _sw._warned = True
    conn = _get_thread_db()
    for attempt in range(6):
        try:
            upsert_peers(conn, hash_val, ip_country, today)
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower():
                raise
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt == 5:
                print(f"  WARN: upsert dropped for {hash_val[:12]}… after "
                      f"6 locked retries (self-heals next pass): {e}")
                return
            time.sleep(0.5 * (attempt + 1))  # 0.5,1,1.5,2,2.5s backoff


# ---------------------------------------------------------------------------
# Load hashes from DB
# ---------------------------------------------------------------------------

def load_hashes(category: str | None = None,
                limit: int | None = None,
                skip_dead_days: int = 0,
                active_only_days: int = 0,
                active_min_peers: int = 1,
                dormant_only_days: int = 0,
                new_only: bool = False) -> list[dict]:
    """
    Load hashes from DB, ordered by most recent DHT peer activity first.

    Ordering strategy (priority):
      1. Hashes with real peer IPs seen most recently come first.
         Uses MAX(peers.last_seen WHERE ip != '_queried_') per hash.
      2. Hashes never found in DHT (no real peer rows) go last,
         ordered by source-reported seeders DESC as a tiebreaker.

    This ensures each pass hits live/popular content immediately instead
    of grinding through tens of thousands of dead hashes first.

    skip_dead_days > 0: exclude hashes that returned 0 DHT peers every day
    for the last N days (only the sentinel row exists, no real IPs).
    These are genuinely dead torrents — no point querying them every pass.

    active_only_days > 0: ONLY include hashes that had at least one real peer
    recorded within the last N days. Use for fast frequent passes that skip
    dormant content entirely (e.g. --active-only 3 --loop).

    dormant_only_days > 0: ONLY include hashes that had NO real peers in the
    last N days (but not yet pruned by skip_dead_days). Use for a slow daily
    background scan of dormant content (e.g. --dormant-only 3 run once/day).
    Mutually exclusive with active_only_days.
    """
    # timeout/busy_timeout so this worklist reader waits for a lock instead of
    # throwing under DB contention (see §36 / _load_known_hashes).
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")

    where_parts = []
    params: list = []

    if category:
        where_parts.append("h.category = ?")
        params.append(category)

    if skip_dead_days > 0:
        # Exclude hashes where:
        #   • a sentinel row (_queried_) exists within the last N days
        #   • AND no real IP has ever been recorded
        where_parts.append("""
            h.hash NOT IN (
                SELECT p.hash FROM peers p
                WHERE p.ip     = '_queried_'
                  AND p.last_seen >= date('now', ?)
                  AND p.hash NOT IN (
                      SELECT DISTINCT hash FROM peers WHERE ip != '_queried_'
                  )
            )
        """)
        params.append(f"-{skip_dead_days} days")

    if active_only_days > 0:
        # ONLY hashes that had at least N distinct real peers in the last N days.
        # active_min_peers > 1 filters out announce_peer noise (single-IP flashes
        # that qualify a hash as "active" but don't represent real active swarms).
        # New/unscanned hashes are handled by a separate "--new-only" job
        # (see crontab) so the active loop stays fast (~5K hashes, not 2M).
        where_parts.append("""
            h.hash IN (
                SELECT hash FROM peers
                WHERE ip != '_queried_'
                  AND last_seen >= date('now', ?)
                GROUP BY hash
                HAVING COUNT(DISTINCT ip) >= ?
                    OR MIN(first_seen) >= date('now')
            )
        """)
        params.append(f"-{active_only_days} days")
        params.append(active_min_peers)
    elif dormant_only_days > 0:
        # ONLY hashes with NO real peers in the last N days AND have been
        # scanned at least once (sentinel exists). Excludes brand-new hashes
        # since those are handled by the active loop above.
        # Drives a slow background scan — lets active hashes run at full speed.
        where_parts.append("""
            h.hash NOT IN (
                SELECT DISTINCT hash FROM peers
                WHERE ip != '_queried_'
                  AND last_seen >= date('now', ?)
            )
            AND
            h.hash IN (
                SELECT DISTINCT hash FROM peers
            )
        """)
        params.append(f"-{dormant_only_days} days")

    if new_only:
        # ONLY hashes that have NEVER been scanned at all (not in peers table).
        # Use for a periodic cron job to onboard freshly-added trending hashes.
        # Mutually exclusive with active_only_days / dormant_only_days.
        where_parts.append("""
            h.hash NOT IN (
                SELECT DISTINCT hash FROM peers
            )
        """)

    # Skip BEP-51 discovered hashes that haven't been enriched yet.
    # collect.py's TMDB/MAL enrichment (and bep51 --filter-media) resolves their
    # title/category; until then they are title="BEP-51 Discovery" /
    # category="Unknown" and have no metadata useful for analysis. Scanning 800K+
    # unenriched hashes would make
    # every pass 10x longer. Once enriched (category != "Unknown") they
    # are picked up automatically on the next pass.
    where_parts.append("""
        NOT (h.source = 'bep51' AND h.category = 'Unknown')
    """)

    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    # CTE: compute per-hash most-recent real peer seen date.
    # idx_peers_hash makes the GROUP BY efficient.
    # NULLS LAST pushes never-found hashes to the end.
    sql = f"""
        WITH last_active AS (
            SELECT hash, MAX(last_seen) AS max_seen
            FROM peers
            WHERE ip != '_queried_'
            GROUP BY hash
        )
        SELECT h.hash, h.ip_id, h.title, h.category, h.seeders
        FROM hashes h
        LEFT JOIN last_active la ON la.hash = h.hash
        {where}
        ORDER BY la.max_seen DESC NULLS LAST,
                 h.seeders    DESC
    """
    if limit:
        sql += f" LIMIT {limit}"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [
        {"hash": r[0], "ip_id": r[1], "title": r[2] or "",
         "category": r[3], "seeders": r[4] or 0}
        for r in rows
    ]


def count_hashes(db: sqlite3.Connection,
                 category: str | None = None,
                 limit: int | None = None) -> int:
    """
    Fast COUNT(*) of hashes (no skip_dead_days filter).
    Used to report how many dead hashes were skipped without fetching them all.
    """
    where_parts: list[str] = []
    params: list = []
    if category:
        where_parts.append("category = ?")
        params.append(category)
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    sql   = f"SELECT COUNT(*) FROM hashes {where}"
    if limit:
        # COUNT(*) with LIMIT still needs a subquery to honour the row cap
        sql = f"SELECT COUNT(*) FROM (SELECT 1 FROM hashes {where} LIMIT {limit})"
    return db.execute(sql, params).fetchone()[0]

# ---------------------------------------------------------------------------
# CSV output — one row per (hash, country)
# ---------------------------------------------------------------------------

FIELDNAMES = ["date", "run_time", "hash", "ip_id", "title", "category",
              "seeders", "country", "peer_count",
              "bep33_seeders", "bep33_leechers"]


def get_csv_path() -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    return OUTPUT_DIR / f"{date.today()}.csv"


def append_csv(rows: list[dict], csv_path: Path, write_header: bool):
    """Direct synchronous CSV write — used as fallback and by the writer thread."""
    with open(csv_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Step 5: Async CSV write queue
# ---------------------------------------------------------------------------
# Decouples CSV I/O from the asyncio event loop and from other processes.
# The discovery loop calls csv_queue_put() — non-blocking, returns instantly.
# A single daemon thread owns the file handle and drains the queue in batches.
#
# Multi-process safety: each process writes to its own per-process CSV
# (e.g. 2026-05-28_w0.csv, 2026-05-28_w1.csv) — no lock contention.
# merge_and_upload.py merges them. When WORKER_ID is None (single-process
# mode), the original shared path is used unchanged for full compatibility.
# ---------------------------------------------------------------------------

_CSV_QUEUE: "_queue.Queue[tuple[list[dict], Path, bool] | None]" = _queue.Queue(maxsize=20_000)
_CSV_WRITER_THREAD: threading.Thread | None = None


def _csv_writer_loop() -> None:
    """Background daemon thread: drains _CSV_QUEUE and writes to disk in batches."""
    while True:
        item = _CSV_QUEUE.get()
        if item is None:          # sentinel → shutdown
            _CSV_QUEUE.task_done()
            break
        rows, csv_path, write_header = item
        try:
            append_csv(rows, csv_path, write_header)
        except Exception as e:
            print(f"  [csv-writer] ERROR writing to {csv_path.name}: {e}", flush=True)
        finally:
            _CSV_QUEUE.task_done()


def _start_csv_writer() -> None:
    global _CSV_WRITER_THREAD
    if _CSV_WRITER_THREAD is None or not _CSV_WRITER_THREAD.is_alive():
        _CSV_WRITER_THREAD = threading.Thread(
            target=_csv_writer_loop,
            name="csv-writer",
            daemon=True,
        )
        _CSV_WRITER_THREAD.start()


_start_csv_writer()   # start once at import time


def csv_queue_put(rows: list[dict], csv_path: Path, write_header: bool) -> None:
    """
    Non-blocking enqueue of a CSV batch.
    Falls back to direct synchronous write if the queue is unexpectedly full
    (queue holds 20K batches of 200 rows = 4M rows buffer — should never fill).
    """
    if not rows:
        return
    try:
        _CSV_QUEUE.put_nowait((rows, csv_path, write_header))
    except _queue.Full:
        # Safety fallback — write directly so no rows are lost
        print("  [csv-writer] queue full — writing directly (check for stalls)", flush=True)
        append_csv(rows, csv_path, write_header)


def csv_queue_flush() -> None:
    """Block until all queued CSV writes have been committed to disk.
    Call before reporting pass stats or exiting so counts are accurate."""
    _CSV_QUEUE.join()


def get_worker_csv_path(base_csv_path: Path, worker_id: int | None) -> Path:
    """
    Return a per-worker CSV path when running multiple processes.
    worker_id=None  → original path unchanged (single-process, backward compat)
    worker_id=0     → 2026-05-28_w0.csv
    worker_id=1     → 2026-05-28_w1.csv
    merge_and_upload.py merges all _wN.csv files before upload.
    """
    if worker_id is None:
        return base_csv_path
    stem = base_csv_path.stem          # e.g. "2026-05-28"
    suffix = base_csv_path.suffix      # ".csv"
    return base_csv_path.with_name(f"{stem}_w{worker_id}{suffix}")


# ---------------------------------------------------------------------------
# Main async runner
# ---------------------------------------------------------------------------

async def run(hashes: list[dict], concurrency: int, timeout: float,
              flush_every: int, csv_path: Path, today: str,
              run_time: str, write_hdr: bool,
              num_sockets: int = 1, ipv6: bool = False,
              loop_mode: bool = True):

    loop = asyncio.get_running_loop()

    # Install SIGTERM handler here — we're inside the running loop so
    # loop.add_signal_handler() is safe. This avoids the manual
    # new_event_loop() pattern which leaks stale thread pools across passes.
    def _handle_sigterm():
        global _shutdown_requested
        if _shutdown_requested:
            return
        _shutdown_requested = True
        print("\n  SIGTERM received — saving node pool and exiting cleanly …",
              flush=True)
        if loop_mode:  # only active loop persists pool; dormant must not overwrite it
            save_node_pool()
            save_node_pool_v6()
        csv_queue_flush()   # drain any buffered CSV rows before tasks are cancelled
        # Cancel all running tasks cleanly instead of loop.stop().
        # loop.stop() causes RuntimeError in pending awaits → exit code 1
        # → triggers OnFailure crash alert for what is actually a clean shutdown.
        # task.cancel() raises CancelledError which propagates through run()'s
        # finally block (sockets closed, announce task cancelled) and exits with 0.
        for task in asyncio.all_tasks(loop):
            task.cancel()

    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    except (NotImplementedError, RuntimeError):
        pass  # Windows or already-closed loop

    # ── Create N IPv4 UDP sockets (TODO-4: multiple DHT node identities) ──
    # Each socket has its own `own_node_id` (assigned in DHTTransport.__init__)
    # so it occupies a different position in the DHT keyspace. Different nodes
    # store each socket in their k-buckets → more announce_peer coverage.
    # All sockets share one announce_queue so _drain_announces handles all hits.
    num_sockets = max(1, num_sockets)
    transports_dhts: list[tuple] = []
    for _ in range(num_sockets):
        t, d = await loop.create_datagram_endpoint(
            DHTTransport,
            local_addr=("0.0.0.0", 0),
            family=socket.AF_INET,
        )
        transports_dhts.append((t, d))

    # ── Optional IPv6 socket (TODO-5 / BEP-32) ───────────────────────────
    # One IPv6 socket added to the dhts pool when --ipv6 is set.
    # Queries the same hashes via the IPv6 DHT keyspace in parallel.
    # Peers behind IPv6 (no NAT, common in EU/APAC) are invisible to IPv4.
    # Falls back silently if the host has no IPv6 connectivity.
    if ipv6 and BOOTSTRAP_V6:
        try:
            t6, d6 = await loop.create_datagram_endpoint(
                lambda: DHTTransport(is_ipv6=True),
                local_addr=("::", 0),
                family=socket.AF_INET6,
            )
            transports_dhts.append((t6, d6))
            print(f"  IPv6 socket       : enabled  "
                  f"({len(BOOTSTRAP_V6)} bootstrap nodes)")
        except Exception as e:
            print(f"  IPv6 socket       : unavailable ({e}) — IPv4 only")
    elif ipv6:
        print("  IPv6 socket       : skipped (no IPv6 bootstrap nodes resolved)")

    dhts: list[DHTTransport] = [d for _, d in transports_dhts]

    # ── announce_peer listener setup ──────────────────────────────────────
    # Load all tracked hashes into memory so datagram_received can do O(1)
    # announce_peer lookups. Reload each pass to include newly collected hashes.
    n_known        = _load_known_hashes()
    announce_queue = asyncio.Queue(maxsize=10_000)
    for d in dhts:
        d.announce_queue = announce_queue   # all sockets feed the same queue
    # announce_buffer: hash_hex → country → set[ip]
    # Filled by _drain_announces; flushed to CSV at pass end for announce-only hashes.
    announce_buffer: dict[str, dict[str, set]] = {}
    announce_task = asyncio.create_task(
        _drain_announces(announce_queue, today, announce_buffer),
        name="drain_announces",
    )
    socket_ids = "  ".join(d.own_node_id.hex()[:8] for d in dhts)
    print(f"  announce_peer     : listening  ({n_known:,} hashes in watchlist)")
    if num_sockets > 1:
        print(f"  DHT sockets       : {num_sockets}  node_ids: {socket_ids}")
    # ──────────────────────────────────────────────────────────────────────

    semaphore = asyncio.Semaphore(concurrency)
    total     = len(hashes)

    print(f"  Hashes to process : {total:,}")
    print(f"  Concurrency       : {concurrency}")
    print(f"  Timeout per hash  : {timeout}s base  ({int(timeout/8*1000)}ms per round at 8 rounds)")
    print(f"  Flush every       : {flush_every} completions")
    print(f"  Output            : {csv_path}")
    print()

    # BEP-33 aggregate counters — accumulated across all hashes in this pass.
    bep33_total_seeders  = 0
    bep33_total_leechers = 0
    bep33_hashes_with_data = 0   # hashes where at least one node returned a filter

    async def process_one(row: dict) -> list[dict]:
        """Returns list of CSV rows — one per country found.

        With num_sockets > 1 (TODO-4): runs all N sockets in parallel for
        this hash, then merges results — unions IPs, OR-merges bloom filters.
        """
        nonlocal bep33_total_seeders, bep33_total_leechers, bep33_hashes_with_data
        # Adaptive timeout: popular hashes warrant longer discovery windows.
        # Truly dead hashes (seeders=0) get a shorter window to avoid stalling.
        seeders = row.get("seeders", 0) or 0
        if seeders >= 500:
            adaptive_timeout = max(timeout, 20.0)   # very popular — 20s
        elif seeders >= 100:
            adaptive_timeout = max(timeout, 15.0)   # popular — 15s
        elif seeders == 0:
            adaptive_timeout = min(timeout, 6.0)    # likely dead — 6s
        else:
            adaptive_timeout = timeout              # normal

        async with semaphore:
            # Run all N sockets simultaneously for this hash.
            # Each socket has its own own_node_id → queries different DHT nodes
            # → more keyspace coverage and more announce_peer sources.
            socket_results = await asyncio.gather(*[
                get_peers_by_country(row["hash"], d, timeout=adaptive_timeout)
                for d in dhts
            ])

        by_country, ip_country, bep33 = _merge_socket_results(socket_results)

        # Accumulate BEP-33 stats (non-zero means at least one node returned a filter).
        if bep33["seeders"] > 0 or bep33["leechers"] > 0:
            bep33_hashes_with_data  += 1
            bep33_total_seeders     += bep33["seeders"]
            bep33_total_leechers    += bep33["leechers"]

        # Thread-safe DB write via thread-local persistent connection.
        # _upsert_peers_threadsafe uses threading.local() — one connection
        # per executor thread, reused across hashes (not opened per hash).
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, _upsert_peers_threadsafe, row["hash"], ip_country, today
        )

        if not by_country:
            # No real peer IPs found — but if BEP-33 has data, still emit one row
            # so bloom-filter estimates are preserved in the CSV.
            if bep33["seeders"] > 0 or bep33["leechers"] > 0:
                return [{
                    "date": today, "run_time": run_time,
                    "hash": row["hash"], "ip_id": row["ip_id"],
                    "title": row["title"], "category": row["category"],
                    "seeders": row["seeders"], "country": "XX",
                    "peer_count": 0,
                    "bep33_seeders":  bep33["seeders"],
                    "bep33_leechers": bep33["leechers"],
                }]
            return []   # truly zero — skip from CSV entirely

        # BEP-33 estimates are per-hash (not per-country), so write them on the
        # first-country row only to avoid double-counting on aggregation.
        rows_out = []
        for i, (country, count) in enumerate(sorted(by_country.items())):
            rows_out.append({
                "date": today, "run_time": run_time,
                "hash": row["hash"], "ip_id": row["ip_id"],
                "title": row["title"], "category": row["category"],
                "seeders": row["seeders"], "country": country,
                "peer_count": count,
                "bep33_seeders":  bep33["seeders"] if i == 0 else 0,
                "bep33_leechers": bep33["leechers"] if i == 0 else 0,
            })
        return rows_out

    # Process in chunks to limit pending tasks in memory at any one time.
    # Within each chunk as_completed() keeps the semaphore fully saturated.
    # chunk_size >> concurrency so the semaphore never idles at chunk edges.
    chunk_size  = concurrency * 10   # e.g. 150*10 = 1500 tasks live at once

    pending_rows:           list[dict] = []
    total_hashes_with_peers            = 0
    completed                          = 0
    last_report_peers                  = 0
    # Track which hashes the active scan wrote to CSV (even if peer_count=0 via BEP-33).
    # Announce-only hashes (not here) will be flushed separately at pass end.
    active_scan_hashes: set[str]       = set()
    # Quick lookup: hash_hex → row metadata (ip_id, title, category, seeders).
    # Used when building CSV rows for announce-only hashes.
    hash_meta: dict[str, dict]         = {h["hash"]: h for h in hashes}

    try:
        for chunk_start in range(0, total, chunk_size):
            chunk = hashes[chunk_start : chunk_start + chunk_size]
            tasks = [asyncio.create_task(process_one(r)) for r in chunk]

            for fut in asyncio.as_completed(tasks):
                rows = await fut
                completed += 1

                if rows:
                    pending_rows.extend(rows)
                    total_hashes_with_peers += 1
                    active_scan_hashes.add(rows[0]["hash"])

                # Flush CSV every flush_every completions (DB already committed per-task)
                if completed % flush_every == 0 or completed == total:
                    if pending_rows:
                        csv_queue_put(pending_rows, csv_path, write_hdr)
                        write_hdr    = False
                        pending_rows = []

                    new_peers = total_hashes_with_peers - last_report_peers
                    last_report_peers = total_hashes_with_peers
                    pct = completed / total * 100
                    print(
                        f"  [{completed:>6}/{total}  {pct:5.1f}%]  "
                        f"with_peers={total_hashes_with_peers}  "
                        f"+{new_peers} this flush",
                        flush=True,
                    )

                # Periodic node pool checkpoint every 5,000 hashes.
                # Ensures a warm pool survives any mid-pass crash or SIGKILL
                # (SIGTERM handler also saves, but SIGKILL can't be caught).
                # Only the looping active scan persists the pool — one-shot
                # dormant runs must not overwrite the active scan's warm pool.
                if completed % 5_000 == 0 and completed > 0 and loop_mode:
                    save_node_pool()
                    save_node_pool_v6()

    finally:
        # Stop the announce drain task, then close all sockets.
        # Cancel first so it can flush any remaining queue items before we close.
        announce_task.cancel()
        try:
            await announce_task
        except asyncio.CancelledError:
            pass
        for t, _ in transports_dhts:
            t.close()   # release all N UDP sockets, even on exception

        # Flush any CSV rows not yet written (e.g. mid-flush SIGTERM).
        if pending_rows:
            csv_queue_put(pending_rows, csv_path, write_hdr)
            write_hdr    = False
            pending_rows = []

        # ── Flush announce-only hits to CSV ───────────────────────────────
        # Moved into finally so this runs on both normal completion AND SIGTERM.
        # Previously this was after the try/finally and was silently skipped on
        # SIGTERM — announce-only hashes would be missing from the daily Parquet.
        announce_only_hashes = {h for h in announce_buffer if h not in active_scan_hashes}
        if announce_only_hashes:
            announce_csv_rows: list[dict] = []
            for hash_hex in announce_only_hashes:
                meta = hash_meta.get(hash_hex)
                if meta is None:
                    # Hash skipped by dead-day filter or arrived post-load.
                    # Caught by merge_and_upload.py's SQLite fallback — skip here.
                    continue
                country_ips = announce_buffer[hash_hex]
                for i, (country, ips) in enumerate(sorted(country_ips.items())):
                    announce_csv_rows.append({
                        "date":           today,
                        "run_time":       "announce",
                        "hash":           hash_hex,
                        "ip_id":          meta["ip_id"],
                        "title":          meta["title"],
                        "category":       meta["category"],
                        "seeders":        meta["seeders"],
                        "country":        country,
                        "peer_count":     len(ips),
                        "bep33_seeders":  0,
                        "bep33_leechers": 0,
                    })
            if announce_csv_rows:
                csv_queue_put(announce_csv_rows, csv_path, write_hdr)
                write_hdr = False
                n_flushed = len({r["hash"] for r in announce_csv_rows})
                print(f"  announce_peer CSV : flushed {n_flushed:,} announce-only hashes "
                      f"({len(announce_csv_rows):,} rows) to CSV")

        # Wait for all queued CSV writes to land before printing pass stats.
        csv_queue_flush()

    # Log BEP-33 aggregate stats for this pass.
    # Sum announce hits across all N sockets — each records its own count.
    announce_hits = sum(d._announce_hits for d in dhts)
    print(f"  announce_peer     : {announce_hits:,} passive hits received"
          + (f"  ({num_sockets} sockets)" if num_sockets > 1 else ""))
    print(f"  BEP-33 scrape     : {bep33_hashes_with_data:,} hashes had filter data  "
          f"(est. {bep33_total_seeders:,} seeders  /  {bep33_total_leechers:,} leechers)")

    # ── Step 3: evict bad nodes after the pass, then log pool health ─────────
    evicted = _evict_bad_nodes()
    if _POOL_N > 0:
        n = _POOL_N
        total_req  = int(_POOL_ARR['responses'][:n].sum()) + int(_POOL_ARR['timeouts'][:n].sum())
        total_resp = int(_POOL_ARR['responses'][:n].sum())
        resp_rate  = total_resp / total_req if total_req > 0 else 0.0
        print(f"  Node pool         : {n:,} total  "
              f"({_POOL_VERIFIED_CNT:,} verified  /  {n - _POOL_VERIFIED_CNT:,} unverified)  "
              f"[evicted {evicted:,} dead  |  response rate {resp_rate:.0%}]")
    else:
        print("  Node pool         : 0 total")

    return total_hashes_with_peers

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def _build_already_done(csv_path: Path, db: sqlite3.Connection, today: str) -> set[str]:
    """Hashes already queried today (from CSV + peers table sentinel rows)."""
    done: set[str] = set()
    if csv_path.exists():
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                done.add(row["hash"])
    peer_done = {
        r[0] for r in db.execute(
            "SELECT DISTINCT hash FROM peers WHERE last_seen = ?", (today,)
        ).fetchall()
    }
    done |= peer_done
    return done


def _print_pass_summary(pass_num: int, with_peers: int,
                         elapsed: float, loop: bool):
    """Brief per-pass summary line."""
    mins, secs = divmod(int(elapsed), 60)
    hrs,  mins = divmod(mins, 60)
    elapsed_str = f"{hrs}h{mins:02d}m{secs:02d}s" if hrs else f"{mins}m{secs:02d}s"
    label = f"Pass {pass_num}" if loop else "Run"
    print(f"\n  {label} done in {elapsed_str}  —  {with_peers:,} hashes had peers")


def _print_final_summary(csv_path: Path):
    """Full summary of today's CSV (all passes combined)."""
    if not csv_path.exists():
        return

    print(f"\n{'='*60}")
    print("Final summary — today's CSV")
    print(f"{'='*60}")

    with open(csv_path) as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("  No rows written yet.")
        return

    uniq_hashes: set[str]       = {r["hash"] for r in rows}
    country_totals: dict[str, int] = defaultdict(int)
    cat_totals:     dict[str, int] = defaultdict(int)
    title_totals:   dict[str, int] = defaultdict(int)

    for r in rows:
        pc = int(r["peer_count"])
        if pc > 0:
            country_totals[r["country"]] += pc
            cat_totals[r["category"]]    += pc
            title_totals[r["title"]]     += pc

    print(f"  Unique hashes in CSV : {len(uniq_hashes):,}")
    print(f"  Total CSV rows       : {len(rows):,}")
    print(f"  Countries seen       : {len(country_totals)}")
    print()

    if country_totals:
        max_cnt = max(country_totals.values())
        print("  Top 10 countries by peer count:")
        for country, cnt in sorted(country_totals.items(), key=lambda x: -x[1])[:10]:
            bar = "█" * min(cnt // max(max_cnt // 20, 1), 20)
            print(f"    {country}  {cnt:>6}  {bar}")
        print()

    print("  By category:")
    for cat, cnt in sorted(cat_totals.items(), key=lambda x: -x[1]):
        print(f"    {cat:<8}  {cnt:>6} peers")

    if title_totals:
        print()
        print("  Top 10 titles by total peers:")
        for title, cnt in sorted(title_totals.items(), key=lambda x: -x[1])[:10]:
            print(f"    {cnt:>6}  {title[:55]}")

    print(f"\n  CSV: {csv_path}")


# ---------------------------------------------------------------------------
# Multiprocessing support — each worker runs its own asyncio event loop
# ---------------------------------------------------------------------------

def _save_worker_pool(worker_id: int) -> None:
    """Save this worker's node pool to a per-worker temp file (atomic write)."""
    path = NODE_POOL_PATH.with_name(f"node_pool_w{worker_id}.json")
    tmp  = path.with_suffix(".tmp")
    try:
        n    = _POOL_N
        data = [
            [bytes(_POOL_ARR['nid'][i]).hex(),
             _int_to_ip4(int(_POOL_ARR['ip_int'][i])),
             int(_POOL_ARR['port'][i])]
            for i in range(n)
        ]
        with open(tmp, "w") as f:
            json.dump(data, f)
        tmp.replace(path)   # atomic on POSIX
    except Exception as e:
        print(f"  [w{worker_id}] Node pool save failed: {e}")


def _merge_worker_pools(num_workers: int) -> None:
    """Union all per-worker node pool files into the main node_pool.json."""
    merged: dict[bytes, tuple] = {}
    if NODE_POOL_PATH.exists():
        try:
            with open(NODE_POOL_PATH) as f:
                for n in json.load(f):
                    if len(n) == 3:
                        merged[bytes.fromhex(n[0])] = (n[1], n[2])
        except Exception:
            pass
    for i in range(num_workers):
        path = NODE_POOL_PATH.with_name(f"node_pool_w{i}.json")
        if path.exists():
            try:
                with open(path) as f:
                    for n in json.load(f):
                        if len(n) == 3:
                            merged[bytes.fromhex(n[0])] = (n[1], n[2])
                path.unlink()
            except Exception:
                pass
    if len(merged) > _NODE_POOL_MAX:
        for k in random.sample(list(merged.keys()), len(merged) - _NODE_POOL_MAX):
            del merged[k]
    try:
        NODE_POOL_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = NODE_POOL_PATH.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump([[k.hex(), v[0], v[1]] for k, v in merged.items()], f)
        tmp.replace(NODE_POOL_PATH)   # atomic on POSIX
        print(f"  Node pool merged  : {len(merged):,} nodes saved")
    except Exception as e:
        print(f"  Node pool merge failed: {e}")


def _merge_worker_csvs(num_workers: int, csv_path: Path, write_hdr: bool) -> None:
    """Concatenate per-worker temp CSVs into the final daily CSV."""
    with open(csv_path, "a", newline="") as out_f:
        writer = csv.DictWriter(out_f, fieldnames=FIELDNAMES)
        if write_hdr:
            writer.writeheader()
        for i in range(num_workers):
            temp = csv_path.with_name(f".{csv_path.stem}_w{i}.csv")
            if temp.exists():
                with open(temp, newline="") as in_f:
                    for row in csv.DictReader(in_f):
                        writer.writerow(row)
                temp.unlink()


def _run_worker(worker_id: int, hashes_chunk: list[dict],
                concurrency: int, timeout: float, flush_every: int,
                csv_path_str: str, today: str, run_time: str,
                num_sockets: int = 1, ipv6: bool = False) -> tuple[int, int]:
    """
    Subprocess entry point. Runs a full asyncio pass over hashes_chunk.
    Writes to a hidden temp CSV; main process merges them all afterward.
    Returns (worker_id, with_peers_count).
    """
    global NODE_POOL_PATH, _POOL_N, _POOL_VERIFIED_CNT

    csv_path = Path(csv_path_str)
    temp_csv = csv_path.with_name(f".{csv_path.stem}_w{worker_id}.csv")

    # ── Step 4: pool isolation ───────────────────────────────────────────────
    # Each worker loads and maintains its OWN node pool file so that over
    # successive passes the pools diverge — worker 0 specialises in the
    # routing nodes near its hash slice, worker 1 in its own slice.
    #
    # First-ever run: no per-worker file exists → fall back to the parent's
    # fork-copied pool (already in memory from the main process load_node_pool).
    # Subsequent runs: load from the worker's own file to restore the diverged pool.
    worker_pool_path = NODE_POOL_PATH.with_name(f"node_pool_w{worker_id}.json")
    if worker_pool_path.exists() and worker_pool_path.stat().st_size > 0:
        # Override path so load_node_pool reads the worker-specific file,
        # then reset the pool globals so it starts fresh from that file.
        NODE_POOL_PATH   = worker_pool_path
        _POOL_N          = 0
        _POOL_VERIFIED_CNT = 0
        _POOL_IDX.clear()
        load_node_pool()
        print(f"  [w{worker_id}] Pool (isolated)  : {_POOL_N:,} nodes loaded from {worker_pool_path.name}")
    else:
        # First run — parent's pool is already in memory via fork; just report it.
        print(f"  [w{worker_id}] Pool (seed)       : {_POOL_N:,} nodes (parent pool, no worker file yet)")
    if ipv6:
        load_node_pool_v6()

    print(f"  [w{worker_id}] Starting — {len(hashes_chunk):,} hashes, "
          f"concurrency={concurrency}  sockets={num_sockets}"
          f"{'  ipv6=on' if ipv6 else ''}", flush=True)

    with_peers = asyncio.run(run(
        hashes      = hashes_chunk,
        concurrency = concurrency,
        timeout     = timeout,
        flush_every = flush_every,
        csv_path    = temp_csv,
        today       = today,
        run_time    = run_time,
        write_hdr   = True,   # each temp CSV gets its own header
        num_sockets = num_sockets,
        ipv6        = ipv6,
    ))

    _save_worker_pool(worker_id)
    print(f"  [w{worker_id}] Done — with_peers={with_peers}", flush=True)
    return worker_id, with_peers


# ---------------------------------------------------------------------------
# Graceful shutdown — SIGTERM flag (checked in main loop)
# ---------------------------------------------------------------------------
# Handler is installed inside run() where the event loop is already running.
# loop.add_signal_handler() is asyncio-safe; signal.signal() is NOT (re-entrant).

_shutdown_requested = False   # set by SIGTERM handler; checked in the main loop


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flush-every", type=int,   default=200,
                        help="Flush CSV + commit DB every N completions (default 200)")
    parser.add_argument("--batch-size",  type=int,   default=None,
                        help="Alias for --flush-every (kept for backward compat)")
    parser.add_argument("--concurrency", type=int,   default=5)
    parser.add_argument("--timeout",     type=float, default=10.0)
    parser.add_argument("--limit",       type=int,   default=None)
    parser.add_argument("--category",    type=str,   default=None,
                        choices=["Movies", "Series", "Anime"])
    parser.add_argument("--loop",           action="store_true",
                        help="Repeat passes all day; exits automatically at midnight")
    parser.add_argument("--loop-delay",    type=int,   default=60,
                        help="Seconds to wait between passes in --loop mode (default 60)")
    parser.add_argument("--skip-dead-days", type=int,  default=3,
                        help="Skip hashes with 0 DHT peers for last N days (default 3, 0=disabled)")
    parser.add_argument("--active-only", type=int, default=0, dest="active_only_days",
                        metavar="DAYS",
                        help="Only scan hashes that had ≥N real DHT peers in the last N days. "
                             "Use with --loop for fast frequent passes over live content. "
                             "Mutually exclusive with --dormant-only. (default: 0=disabled)")
    parser.add_argument("--active-min-peers", type=int, default=3, dest="active_min_peers",
                        metavar="N",
                        help="Minimum unique peer IPs required for --active-only filter. "
                             "Filters out announce_peer noise (single-flash hashes). "
                             "(default: 3)")
    parser.add_argument("--dormant-only", type=int, default=0, dest="dormant_only_days",
                        metavar="DAYS",
                        help="Only scan hashes with NO real DHT peers in the last N days. "
                             "Use for a slow daily background pass over dormant content. "
                             "Mutually exclusive with --active-only. (default: 0=disabled)")
    parser.add_argument("--new-only", action="store_true", default=False, dest="new_only",
                        help="Only scan hashes that have NEVER been scanned (not in peers table). "
                             "Use for a periodic job to onboard freshly-added hashes. "
                             "Mutually exclusive with --active-only and --dormant-only.")
    parser.add_argument("--workers", type=int, default=1,
                        help="Worker processes (default: 1). "
                             "Each runs its own asyncio loop + UDP socket.")
    parser.add_argument("--snake-stride", type=int, default=100, dest="snake_stride",
                        help="Step 4: rotate hash list by this many positions each pass "
                             "(default 100, set 0 to disable). Ensures different hashes get "
                             "queried at different pool-warmup states across passes.")
    parser.add_argument("--slice", type=str, default=None, dest="hash_slice",
                        help="Step 6: multi-process hash sharding — format N/M. "
                             "This process handles hashes[N::M] (every Mth hash starting at N). "
                             "Example: --slice 0/3  --slice 1/3  --slice 2/3 for 3 workers. "
                             "Worker CSV is written to YYYY-MM-DD_wN.csv and merged by "
                             "merge_and_upload.py. Omit for single-process mode (original behaviour).")
    parser.add_argument("--worker-id", type=int, default=None, dest="worker_id",
                        help="Numeric worker ID used to name the per-worker CSV and node pool. "
                             "Set automatically from --slice (N in N/M) if not given explicitly.")
    parser.add_argument("--sockets", type=int, default=1,
                        help="UDP sockets per worker (default: 1). "
                             "Each socket has its own DHT node_id, covering a "
                             "different keyspace slice to receive more announce_peer hits. "
                             "2–4 recommended; beyond 4 shows diminishing returns.")
    parser.add_argument("--ipv6", action="store_true", default=False,
                        help="Enable IPv6 DHT socket (BEP-32). Adds one AF_INET6 socket "
                             "per worker alongside the IPv4 sockets. Discovers peers "
                             "behind IPv6 (no NAT, common in EU/APAC) invisible to IPv4. "
                             "Requires the host to have IPv6 connectivity.")
    args = parser.parse_args()

    # --batch-size is the old name; --flush-every takes precedence
    if args.batch_size is not None and args.flush_every == 200:
        args.flush_every = args.batch_size

    # ── Step 6: parse --slice N/M ─────────────────────────────────────────────
    _slice_index: int | None = None
    _slice_total: int | None = None
    if args.hash_slice:
        try:
            _n, _m = args.hash_slice.split("/")
            _slice_index = int(_n)
            _slice_total = int(_m)
            assert 0 <= _slice_index < _slice_total, \
                f"--slice {args.hash_slice}: N must be 0 ≤ N < M"
        except (ValueError, AssertionError) as _e:
            print(f"  ERROR: invalid --slice '{args.hash_slice}' — expected format N/M (e.g. 0/3). {_e}")
            raise SystemExit(1)
        # Derive worker_id from slice index if not given explicitly
        if args.worker_id is None:
            args.worker_id = _slice_index

    num_workers = args.workers

    db = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")   # allow concurrent readers + one writer
    db.execute("PRAGMA busy_timeout=15000") # wait up to 15s before failing on lock
    db.execute("PRAGMA journal_size_limit=536870912")  # cap WAL at 512MB: truncate on checkpoint (was -1 → 13GB bloat)
    init_peers_table(db)
    # GeoIP warms automatically via lru_cache on first lookup — no pre-load needed
    # Select tier-specific node pool so each scan mode builds its own DHT
    # knowledge base independently — no cross-contamination between tiers.
    global NODE_POOL_PATH, NODE_POOL_V6_PATH
    _data_dir = Path(__file__).parent / "data"
    if args.active_only_days:
        NODE_POOL_PATH   = _data_dir / "node_pool_active.json"
        NODE_POOL_V6_PATH = _data_dir / "node_pool_active_v6.json"
    elif args.dormant_only_days:
        NODE_POOL_PATH   = _data_dir / "node_pool_dormant.json"
        NODE_POOL_V6_PATH = _data_dir / "node_pool_dormant_v6.json"
    elif args.new_only:
        NODE_POOL_PATH   = _data_dir / "node_pool_new.json"
        NODE_POOL_V6_PATH = _data_dir / "node_pool_new_v6.json"
    # else: default node_pool.json (single-pass / unfiltered runs)
    # Derive tier-specific hash→node cache path from pool path:
    #   node_pool_active.json  → hash_node_cache_active.json
    #   node_pool_dormant.json → hash_node_cache_dormant.json
    #   node_pool.json         → hash_node_cache.json
    global HASH_CACHE_PATH
    HASH_CACHE_PATH = NODE_POOL_PATH.with_name(
        NODE_POOL_PATH.name.replace("node_pool", "hash_node_cache")
    )
    print(f"  Node pool file    : {NODE_POOL_PATH.name}")
    print(f"  Hash cache file   : {HASH_CACHE_PATH.name}")

    # Seed empty tier pools from the main verified pool so they don't bootstrap cold.
    # A fresh tier pool hitting only bootstrap nodes fills up with unverified junk fast.
    _main_pool = _data_dir / "node_pool.json"
    if (not NODE_POOL_PATH.exists() or NODE_POOL_PATH.stat().st_size == 0) \
            and NODE_POOL_PATH != _main_pool and _main_pool.exists():
        try:
            with open(_main_pool) as _f:
                _all = json.load(_f)
            _seed = [n for n in _all if len(n) > 3 and n[3] == 1][:50_000]
            if _seed:
                NODE_POOL_PATH.parent.mkdir(parents=True, exist_ok=True)
                with open(NODE_POOL_PATH, "w") as _f:
                    json.dump(_seed, _f)
                print(f"  Seeded pool       : {len(_seed):,} verified nodes from node_pool.json",
                      flush=True)
        except Exception as _e:
            print(f"  Seed pool         : failed ({_e}) — starting empty", flush=True)

    load_node_pool()          # pre-warm shared DHT node pool from disk
    load_hash_node_cache()    # pre-warm hash→closest-node cache
    if args.ipv6:
        load_node_pool_v6()   # pre-warm IPv6 node pool if IPv6 mode is on

    run_date = date.today()   # detect midnight rollover
    pass_num = 0

    try:
        while True:
            if _shutdown_requested:
                break   # SIGTERM already saved pool; exit cleanly

            pass_num += 1
            today    = str(date.today())
            _base_csv = get_csv_path()
            csv_path  = get_worker_csv_path(_base_csv, args.worker_id)

            # Stop when the calendar date rolls past the day we started
            if date.today() != run_date:
                print(f"\n  Date changed to {today}. Exiting — tomorrow's cron will start fresh.")
                break

            print(f"\n{'='*60}")
            if args.loop:
                print(f"  Pass {pass_num}  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("  DHT Peer Counter (country breakdown) — hash_trackerv2")
            print(f"{'='*60}")
            print(f"  CSV: {csv_path}")
            if _slice_index is not None:
                print(f"  Slice             : {_slice_index}/{_slice_total}  (worker {args.worker_id})")

            # Single query for the active hash list; use COUNT(*) for total —
            # avoids fetching the full unfiltered list just to measure its length.
            hashes_all   = load_hashes(category=args.category, limit=args.limit,
                                       skip_dead_days=args.skip_dead_days,
                                       active_only_days=args.active_only_days,
                                       active_min_peers=args.active_min_peers,
                                       dormant_only_days=args.dormant_only_days,
                                       new_only=args.new_only)
            total_in_db  = count_hashes(db, category=args.category, limit=args.limit)
            dead_skipped = total_in_db - len(hashes_all)

            # Print scan mode so logs make it obvious which tier is running
            if args.active_only_days:
                print(f"  Scan mode         : ACTIVE-ONLY (≥{args.active_min_peers} peers in last {args.active_only_days}d)")
            elif args.dormant_only_days:
                print(f"  Scan mode         : DORMANT-ONLY (no peers in last {args.dormant_only_days}d)")
            elif args.new_only:
                print("  Scan mode         : NEW-ONLY (hashes never scanned before)")
            if dead_skipped:
                print(f"  Skipping {dead_skipped:,} dead hashes (0 peers for {args.skip_dead_days}+ days)"
                      f"  →  {len(hashes_all):,} remaining")

            if not hashes_all:
                # A loop service must NOT exit just because one pass found nothing
                # (e.g. the dormant tier momentarily empties after a DB remap). An
                # exit-0 here trips Restart=always→StartLimitBurst and the unit dies
                # PERMANENTLY (start-limit-hit) even once work reappears. So in loop
                # mode, wait and re-check; only a one-shot run exits.
                if args.loop:
                    print(f"  No hashes found. Waiting {args.loop_delay}s …")
                    time.sleep(args.loop_delay)
                    continue
                print("  No hashes found.")
                break

            # ── Step 4: snake offset — rotate hash list each pass ────────────
            # Pass 1: hashes[0:], Pass 2: hashes[100:]+hashes[:100], etc.
            # Ensures different hashes get queried at different pool-warmup states
            # (pool is cold at pass start, warm mid-pass — snake spreads this evenly).
            if args.snake_stride > 0 and len(hashes_all) > 1:
                offset = ((pass_num - 1) * args.snake_stride) % len(hashes_all)
                if offset:
                    hashes_all = hashes_all[offset:] + hashes_all[:offset]
                    print(f"  Snake offset      : {offset:,} (pass {pass_num}, stride {args.snake_stride})")

            # ── Step 6: hash slice — each process owns hashes[N::M] ──────────
            # Splits the hash list so N parallel processes cover non-overlapping
            # subsets.  --slice 0/3 → indices 0,3,6,9…  --slice 1/3 → 1,4,7,10…
            # Applied AFTER snake rotation so the interleaving pattern is stable
            # and each worker still benefits from snake-driven pool-warmup diversity.
            if _slice_index is not None and _slice_total is not None and _slice_total > 1:
                hashes_all = hashes_all[_slice_index::_slice_total]
                print(f"  Hash slice        : {_slice_index}/{_slice_total}  "
                      f"→  {len(hashes_all):,} hashes this process")

            # Pass 1 in loop mode: resume from today's state so we survive crashes.
            # One-shot runs (no --loop, e.g. dormant cron): always scan all — each
            # scheduled run is independent and should not skip a prior run's results.
            # Pass 2+: re-query ALL hashes — new peers may have joined since last pass.
            if pass_num == 1 and args.loop:
                already_done = _build_already_done(csv_path, db, today)
                hashes = [h for h in hashes_all if h["hash"] not in already_done]
                if already_done:
                    print(f"  Resuming — skipping {len(already_done):,} / {len(hashes_all):,} already done today")
            else:
                hashes = hashes_all   # full re-query; catches new peers joining
                if pass_num > 1:
                    print(f"  Full re-query of {len(hashes):,} hashes")

            if not hashes:
                if args.loop:
                    print(f"  Nothing left to query. Waiting {args.loop_delay}s …")
                    time.sleep(args.loop_delay)
                    continue
                break

            write_hdr  = not csv_path.exists()
            run_time   = datetime.now().strftime("%H:%M")
            t0         = time.monotonic()

            if num_workers <= 1:
                # ── Single-process mode (original behaviour) ──────────────
                # asyncio.run() manages the event loop cleanly — proper executor
                # shutdown between passes prevents thread pool accumulation.
                # SIGTERM handler is installed inside run() on the running loop.
                try:
                    with_peers = asyncio.run(run(
                        hashes      = hashes,
                        concurrency = args.concurrency,
                        timeout     = args.timeout,
                        flush_every = args.flush_every,
                        csv_path    = csv_path,
                        today       = today,
                        run_time    = run_time,
                        write_hdr   = write_hdr,
                        num_sockets = args.sockets,
                        ipv6        = args.ipv6,
                        loop_mode   = args.loop,
                    ))
                except asyncio.CancelledError:
                    # Clean SIGTERM shutdown — task.cancel() was called by handler.
                    # Exit the pass loop; main() finally block closes the DB.
                    print("  SIGTERM — pass cancelled cleanly, exiting.", flush=True)
                    break
                if args.loop:
                    save_node_pool()
                    save_node_pool_v6()

            else:
                # ── Multiprocessing mode ──────────────────────────────────
                # Round-robin split so each worker gets an even mix of
                # popular/dead hashes (list is sorted by seeders DESC).
                chunks = [hashes[i::num_workers] for i in range(num_workers)]
                chunks = [c for c in chunks if c]   # drop empty slices
                actual_workers = len(chunks)

                print(f"  Workers           : {actual_workers} processes "
                      f"× concurrency {args.concurrency} "
                      f"× {args.sockets} sockets"
                      f"= {actual_workers * args.concurrency} total concurrent queries")

                ctx = mp.get_context("fork")   # Linux default; avoids re-import overhead
                with ctx.Pool(actual_workers) as pool:
                    results = pool.starmap(_run_worker, [
                        (i, chunks[i], args.concurrency, args.timeout,
                         args.flush_every, str(csv_path), today, run_time,
                         args.sockets, args.ipv6)
                        for i in range(actual_workers)
                    ])

                with_peers = sum(wp for _, wp in results)
                _merge_worker_csvs(actual_workers, csv_path, write_hdr)
                # Step 4: workers maintain isolated pools (node_pool_w{id}.json).
                # _merge_worker_pools writes a merged node_pool.json as a backup
                # and to seed new workers on first run — workers do NOT load from it.
                _merge_worker_pools(actual_workers)

            elapsed = time.monotonic() - t0
            _print_pass_summary(pass_num, with_peers, elapsed, args.loop)

            if not args.loop:
                break

            # Cap WAL growth. Long-lived reader connections across the 8 worker
            # processes hold back auto-checkpoint, so the WAL can balloon (seen at
            # 686MB on a 4.2GB DB) — once large, every write traverses it and the
            # busy-timeout gets exceeded → crash-loop. A TRUNCATE checkpoint
            # between passes resets the WAL to whatever the current readers allow.
            # Wrapped: a busy/failed checkpoint must never break the loop.
            try:
                cp = db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if cp and cp[0] == 1:
                    print(f"  WAL checkpoint busy (readers active): {cp}")
            except Exception as _cp_e:
                print(f"  WAL checkpoint skipped: {_cp_e}")

            print(f"  Next pass in {args.loop_delay}s …  (Ctrl-C to stop)")
            time.sleep(args.loop_delay)

    except KeyboardInterrupt:
        print("\n\n  Interrupted by user.")

    finally:
        db.close()
        _print_final_summary(get_csv_path())


if __name__ == "__main__":
    main()
