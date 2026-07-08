#!/usr/bin/env python3
"""dmca_site_discovery.py — auto-discover new Channel-2 streaming sites from the
Google Transparency takedown-domain list.

Cross-references the top Google-takedown domains (/data/transparency/top_domains.tsv,
format: "<takedown_count>\\t<domain>") against the existing stream_sites registry, and
surfaces high-volume VIDEO-piracy domains we are not yet scraping. Other piracy verticals
(books, adult, music, software, torrent-indexers, pure file-hosts) are filtered out — this
discovery feed is for the web-STREAMING channel only.

Read-only by default. With --apply, inserts the top candidates into stream_sites as
status='candidate' so the existing liveness sweep validates them before the collector
scrapes them (no blind scraping of junk)."""
from __future__ import annotations
import sqlite3, re, argparse, datetime

TSV = "/data/transparency/top_domains.tsv"
DB  = "/data/db/stream_demand.db"

# NOT the web-streaming channel — other verticals / channels we don't want here.
# (webtoon/manhwa = comics, adult-cam, books, music, software, torrents, file-hosts)
EXCLUDE = re.compile(
    r"(porn|xxx|xnxx|xvideos|xhamster|\bsex|hentai|nude|escort|onlyfans|"
    r"cam(whore|girl|stream|s)|tnaflix|chaturbat|stripchat|\bfap|\bjav\b|javhd|"
    r"pornhub|redtube|youjizz|spankbang|brazzers|"
    r"webtoon|manhwa|manhua|\bmanga|\btoon|comic|"
    r"annas-archive|libgen|epub|ebook|\bbook|scribd|audiobook|kindle|"
    r"mp3|\bsong|music|flac|\baudio|spotif|"
    r"crack|warez|keygen|nulled|getintopc|softonic|\bsoft\b|"
    r"rarbg|rutor|1337x|thepiratebay|\btpb\b|nyaa|torrent|magnet|\bnzb|usenet|"
    r"rapidgator|nitroflare|mediafire|mega\.nz|dropbox|ddownload|1fichier|"
    r"krakenfiles|uploadrar|sendspace|file(factory|store|fox))", re.I)

# positive web-streaming signals (boost ranking confidence)
INCLUDE = re.compile(
    r"(film|movie|watch|stream|flix|cinema|tvshow|series|episode|"
    r"anime|drama|kdrama|putlocker|soap2?day|123movie|fmovies|"
    r"gomovies|primewire|yesmovies|dramacool|kissasian|kissanime|gogoanime|"
    r"megashare|solarmovie|vumoo|sflix|hurawatch|aniwave|9anime|hianime|"
    r"pelis|cuevana|repelis|voir|serie|pelicula|filme|dizi|izle|hdtoday|"
    r"ridomovies|lookmovie|braflix|myflixer|goku|m4ufree)", re.I)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="insert top candidates as status=candidate")
    ap.add_argument("--top", type=int, default=80)
    a = ap.parse_args()

    con = sqlite3.connect(DB)
    have = {d.lower() for (d,) in con.execute("SELECT domain FROM stream_sites")}

    scanned = excluded = known = no_signal = 0
    cands = []
    for line in open(TSV, encoding="utf-8", errors="replace"):
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 2:
            continue
        try:
            cnt = int(parts[0])
        except ValueError:
            continue
        dom = parts[1].strip().lower()
        scanned += 1
        if dom in have:
            known += 1; continue
        if EXCLUDE.search(dom):
            excluded += 1; continue
        if not INCLUDE.search(dom):   # REQUIRE a positive video-streaming signal (high precision
            no_signal += 1; continue  # — the no-signal bucket is books/file-hosts/adult mirrors)
        cands.append((cnt, dom))

    cands.sort(key=lambda x: x[0], reverse=True)   # by takedown volume
    top = cands[:a.top]
    print("registry=%d | scanned=%d | excluded(non-video)=%d | no-video-signal=%d | already-known=%d | video-candidates=%d"
          % (len(have), scanned, excluded, no_signal, known, len(cands)))
    print("TOP %d streaming-candidate domains  (takedowns | domain):" % a.top)
    for cnt, dom in top:
        print("  %12d  %s" % (cnt, dom))

    if a.apply:
        now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
        before = con.total_changes
        con.executemany(
            "INSERT OR IGNORE INTO stream_sites(domain,kind,rank_signal,status,first_seen) "
            "VALUES(?,?,?,?,?)",
            [(dom, "streaming", cnt, "candidate", now) for cnt, dom in top])
        con.commit()
        print("INSERTED %d new candidate sites (status=candidate) — liveness sweep will validate, "
              "then the collector scrapes the live ones." % (con.total_changes - before))
    con.close()


if __name__ == "__main__":
    main()
