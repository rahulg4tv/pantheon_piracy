#!/usr/bin/env python3
"""
tracker_harvest.py — BitTorrent tracker-announce peer harvester.

WHY THIS EXISTS
---------------
The DHT (BEP-5 get_peers) only ever returns the peers that announced to the
~8 nodes XOR-closest to an infohash — a structural *sample* of the swarm, not
the population. Measured ceiling on this deployment: the all-time DHT distinct-IP
union over 16 days roughly equals NBCU's *single-day* IP_COUNT for the same
title. To reach NBCU-scale per-country IP counts we must talk to the torrent's
trackers directly.

This module announces (BEP-15 UDP + HTTP/compact) to a fixed pool of large
public trackers, pages the swarm over several rounds, and returns the distinct
peer IPs. Output is designed to be unioned into the existing `peers` table
alongside DHT-sourced IPs (same (hash, ip, country) shape).

It performs an *announce* (not just a scrape): scrape returns counts only, while
announce returns the actual peer IP:port list we need for GeoIP bucketing. We
announce with numwant only and never transfer data (event=started once, then
plain refreshes), so we read the swarm without seeding/leeching content.

Standalone: `python3 tracker_harvest.py <infohash_hex> [<infohash_hex> ...]`
Importable: `harvest_infohash(ih_hex, rounds=...) -> set[(ip, port)]`
"""
from __future__ import annotations
import socket
import struct
import os
import random
import time
import ipaddress
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Public trackers that accept announces for any infohash ──────────────────
# UDP (BEP-15) — the workhorses; HTTP fallbacks listed separately.
UDP_TRACKERS = [
    ("tracker.opentrackr.org", 1337),
    ("open.stealth.si", 80),
    ("tracker.openbittorrent.com", 6969),
    ("exodus.desync.com", 6969),
    ("tracker.torrent.eu.org", 451),
    ("open.demonii.com", 1337),
    ("tracker.dler.org", 6969),
    ("explodie.org", 6969),
    ("tracker.0x7c0.com", 6969),
    ("opentracker.io", 6969),
    ("tracker.tiny-vps.com", 6969),
    ("tracker.bittor.pw", 1337),
]
HTTP_TRACKERS = [
    "https://tracker.tamersunion.org:443/announce",
    "http://tracker.openbittorrent.com:80/announce",
    "https://tracker.gbitt.info:443/announce",
]

_PROTOCOL_ID = 0x41727101980  # BEP-15 magic connection constant
_ANNOUNCE_PORT = 6881         # the port we claim to listen on (we don't)
_UDP_TIMEOUT = 2.5
_NUMWANT = 200                # trackers cap responses; we page via rounds


def _rand_peer_id() -> bytes:
    # Azureus-style prefix + random; fresh per round so the tracker hands us a
    # different random slice of the swarm each time → more unique IPs.
    return b"-HT0001-" + bytes(random.getrandbits(8) for _ in range(12))


def _parse_compact_peers(blob: bytes) -> set[tuple[str, int]]:
    out = set()
    for i in range(0, len(blob) - 5, 6):
        ip = socket.inet_ntoa(blob[i:i + 4])
        port = struct.unpack(">H", blob[i + 4:i + 6])[0]
        out.add((ip, port))
    return out


def _udp_announce(host: str, port: int, info_hash: bytes,
                  rounds: int) -> set[tuple[str, int]]:
    peers: set[tuple[str, int]] = set()
    try:
        addr = (socket.gethostbyname(host), port)
    except OSError:
        return peers
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(_UDP_TIMEOUT)
    try:
        for _ in range(rounds):
            # ── connect handshake ──
            txn = random.getrandbits(32)
            req = struct.pack(">QII", _PROTOCOL_ID, 0, txn)
            try:
                sock.sendto(req, addr)
                resp = sock.recv(16)
            except OSError:
                break
            if len(resp) < 16:
                break
            action, rtxn = struct.unpack(">II", resp[:8])
            if action != 0 or rtxn != txn:
                break
            conn_id = struct.unpack(">Q", resp[8:16])[0]

            # ── announce ──
            txn = random.getrandbits(32)
            req = struct.pack(
                ">QII20s20sQQQIIIiH",
                conn_id, 1, txn,
                info_hash, _rand_peer_id(),
                0,                       # downloaded
                random.getrandbits(40),  # left (nonzero → we look like a leecher)
                0,                       # uploaded
                0,                       # event 0=none
                0,                       # IP 0=tracker uses source
                random.getrandbits(32),  # key (fresh → fresh swarm slice)
                _NUMWANT,
                _ANNOUNCE_PORT,
            )
            try:
                sock.sendto(req, addr)
                resp = sock.recv(4096)
            except OSError:
                break
            if len(resp) < 20:
                continue
            action, rtxn = struct.unpack(">II", resp[:8])
            if action != 1 or rtxn != txn:
                continue
            before = len(peers)
            peers |= _parse_compact_peers(resp[20:])
            # If a round yields no new IPs, the swarm slice is exhausted here.
            if len(peers) == before:
                break
    finally:
        sock.close()
    return peers


def _http_announce(url: str, info_hash: bytes,
                   rounds: int) -> set[tuple[str, int]]:
    peers: set[tuple[str, int]] = set()
    for _ in range(rounds):
        q = {
            "info_hash": info_hash,
            "peer_id": _rand_peer_id(),
            "port": _ANNOUNCE_PORT,
            "uploaded": 0,
            "downloaded": 0,
            "left": random.getrandbits(40),
            "compact": 1,
            "numwant": _NUMWANT,
            "event": "started",
        }
        full = url + "?" + urllib.parse.urlencode(q)
        try:
            with urllib.request.urlopen(full, timeout=_UDP_TIMEOUT) as r:
                body = r.read()
        except Exception:
            break
        blob = _extract_bencode_peers(body)
        if not blob:
            break
        before = len(peers)
        peers |= _parse_compact_peers(blob)
        if len(peers) == before:
            break
    return peers


def _extract_bencode_peers(body: bytes) -> bytes:
    # Minimal: find the compact "5:peers<len>:<bytes>" field without a full
    # bencode parser. Good enough for compact=1 tracker responses.
    key = b"5:peers"
    idx = body.find(key)
    if idx < 0:
        return b""
    j = idx + len(key)
    colon = body.find(b":", j)
    if colon < 0:
        return b""
    try:
        length = int(body[j:colon])
    except ValueError:
        return b""
    start = colon + 1
    return body[start:start + length]


def harvest_infohash(ih_hex: str, rounds: int = 4) -> set[tuple[str, int]]:
    """Announce to every tracker (in parallel) and union all peer IP:ports."""
    info_hash = bytes.fromhex(ih_hex)
    peers: set[tuple[str, int]] = set()
    with ThreadPoolExecutor(max_workers=len(UDP_TRACKERS) + len(HTTP_TRACKERS)) as ex:
        futs = [ex.submit(_udp_announce, h, p, info_hash, rounds)
                for h, p in UDP_TRACKERS]
        futs += [ex.submit(_http_announce, u, info_hash, rounds)
                 for u in HTTP_TRACKERS]
        for f in as_completed(futs):
            try:
                peers |= f.result()
            except Exception:
                pass
    return peers


def _public_only(peers: set[tuple[str, int]]) -> set[tuple[str, int]]:
    out = set()
    for ip, port in peers:
        try:
            a = ipaddress.ip_address(ip)
        except ValueError:
            continue
        if a.is_global and not a.is_multicast:
            out.add((ip, port))
    return out


if __name__ == "__main__":
    import sys
    hashes = sys.argv[1:]
    if not hashes:
        print("usage: tracker_harvest.py <infohash_hex> [...]")
        raise SystemExit(1)
    rounds = int(os.environ.get("ROUNDS", "4"))
    for ih in hashes:
        t0 = time.time()
        peers = _public_only(harvest_infohash(ih, rounds=rounds))
        ips = {ip for ip, _ in peers}
        print(f"{ih}  distinct_ips={len(ips):>6,}  peers={len(peers):>6,}  "
              f"{time.time() - t0:5.1f}s")
