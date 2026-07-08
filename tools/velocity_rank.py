#!/usr/bin/env python3
"""velocity_rank.py — daily "fastest-rising / new-release" demand ranking.

READ-ONLY analysis companion to export_nbcu.py. It ranks titles by day-over-day
growth in distinct peer IPs (today's daily feed CSV vs yesterday's), PER category,
and flags brand-new releases (titles whose hashes first appeared within
--new-days). It writes its OWN daily deliverable:

    /data/daily/velocity/<date>.csv
    -> s3://YOUR_S3_BUCKET/daily/velocity/<date>.csv   (uploaded by the
                                                                   nightly wrapper)

It does NOT touch the demand feed. Same IP_ID / IMDB_ID / ANIME_ID keys as
export_nbcu.py so the two can be joined.

Why a separate file: the demand feed answers "who has the most downloaders right
now"; this answers "what's spiking fastest". A binge drop (e.g. Spider-Noir)
ranks at the top here while a franchise tentpole (The Boys) still leads total
demand — two different, complementary questions.

Schema:
  RANK, TITLE, IP_ID, IMDB_ID, ANIME_ID, CATEGORY,
  IP_TODAY, IP_PREV, DELTA, PCT_GROWTH, IS_NEW_RELEASE
RANK is within CATEGORY, ordered by DELTA (absolute IP gain) descending.
PCT_GROWTH is blank for titles absent yesterday (new entrants — they sort high by
DELTA anyway). IS_NEW_RELEASE=1 when the title's earliest hash first_seen is
within --new-days.

Usage:
  /home/ec2-user/venv/bin/python3 velocity_rank.py            # today vs yesterday
  velocity_rank.py --date 2026-05-31 --prev 2026-05-30 --out /tmp/v.csv
"""
from __future__ import annotations
import argparse, csv, os, sqlite3, datetime, collections

DAILY_DIR = "/data/daily"
DB = "file:/data/db/hashes_v2.db?mode=ro"

FIELDS = ["RANK", "TITLE", "IP_ID", "IMDB_ID", "ANIME_ID", "CATEGORY",
          "IP_TODAY", "IP_PREV", "DELTA", "PCT_GROWTH", "IS_NEW_RELEASE"]


def _load(path: str) -> dict[str, dict]:
    """ip_id -> {TITLE, IMDB_ID, ANIME_ID, CATEGORY, ip} (IP summed over countries)."""
    agg: dict[str, dict] = {}
    if not os.path.exists(path):
        return agg
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            # Unmapped titles (not in the Pantheon catalog) carry a blank IP_ID;
            # fall back to TITLE so they don't all collapse into one "" group.
            k = r["IP_ID"] or r["TITLE"]
            ipc = int(r["IP_COUNT"])
            d = agg.get(k)
            if d is None:
                agg[k] = {"TITLE": r["TITLE"], "IMDB_ID": r.get("IMDB_ID", ""),
                          "ANIME_ID": r.get("ANIME_ID", ""),
                          "CATEGORY": r["CATEGORY"], "ip": ipc}
            else:
                d["ip"] += ipc
    return agg


def _first_seen(ip_ids: set[str]) -> dict[str, str]:
    """ip_id -> min(first_seen) from hashes, only for the ids we need."""
    out: dict[str, str] = {}
    if not ip_ids:
        return out
    c = sqlite3.connect(DB, uri=True, timeout=60)
    c.execute("PRAGMA busy_timeout=60000")
    ids = list(ip_ids)
    CHUNK = 900  # stay under SQLite's variable limit
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        ph = ",".join("?" * len(chunk))
        q = f"SELECT ip_id, MIN(first_seen) FROM hashes WHERE ip_id IN ({ph}) GROUP BY ip_id"
        for ip_id, fs in c.execute(q, chunk):
            out[ip_id] = fs
    c.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d"))
    ap.add_argument("--prev", default=None, help="comparison day (default: date-1)")
    ap.add_argument("--daily-dir", default=DAILY_DIR)
    ap.add_argument("--out", default=None)
    ap.add_argument("--new-days", type=int, default=14,
                    help="flag IS_NEW_RELEASE if earliest hash first_seen within N days")
    ap.add_argument("--floor", type=int, default=50, help="min IP_TODAY to include")
    a = ap.parse_args()

    date = a.date
    prev = a.prev or (datetime.date.fromisoformat(date) - datetime.timedelta(days=1)).isoformat()
    out = a.out or f"{a.daily_dir}/velocity/{date}.csv"

    today = _load(f"{a.daily_dir}/{date}.csv")
    yest = _load(f"{a.daily_dir}/{prev}.csv")
    if not today:
        raise SystemExit(f"no daily feed for {date} at {a.daily_dir}/{date}.csv")

    new_cut = (datetime.date.fromisoformat(date) - datetime.timedelta(days=a.new_days)).isoformat()
    fs = _first_seen(set(today) | set(yest))

    rows = []
    for k, d in today.items():
        if d["ip"] < a.floor:
            continue
        prev_ip = yest.get(k, {}).get("ip", 0)
        delta = d["ip"] - prev_ip
        pct = round(delta / prev_ip * 100, 1) if prev_ip > 0 else ""
        f0 = fs.get(k)
        is_new = 1 if (f0 and f0 >= new_cut) else 0
        rows.append({"TITLE": d["TITLE"], "IP_ID": k, "IMDB_ID": d["IMDB_ID"],
                     "ANIME_ID": d["ANIME_ID"], "CATEGORY": d["CATEGORY"],
                     "IP_TODAY": d["ip"], "IP_PREV": prev_ip, "DELTA": delta,
                     "PCT_GROWTH": pct, "IS_NEW_RELEASE": is_new})

    by_cat: dict[str, list] = collections.defaultdict(list)
    for r in rows:
        by_cat[r["CATEGORY"]].append(r)

    out_rows = []
    for cat, rs in by_cat.items():
        rs.sort(key=lambda x: -x["DELTA"])
        for i, r in enumerate(rs, 1):
            r["RANK"] = i
            out_rows.append(r)
    out_rows.sort(key=lambda r: (r["CATEGORY"], r["RANK"]))

    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in out_rows:
            w.writerow({k: r[k] for k in FIELDS})
    print(f"wrote {len(out_rows):,} rows -> {out}  (date={date} vs prev={prev})")

    for cat in sorted(by_cat):
        print(f"\nTop 10 rising — {cat}:")
        for r in sorted(by_cat[cat], key=lambda x: -x["DELTA"])[:10]:
            tag = "  [NEW]" if r["IS_NEW_RELEASE"] else ""
            pct = f"{r['PCT_GROWTH']}%" if r["PCT_GROWTH"] != "" else "new"
            print(f"  +{r['DELTA']:>8,}  ({r['IP_PREV']:>8,} -> {r['IP_TODAY']:>8,}, {pct:>7})  "
                  f"{r['TITLE'][:34]}{tag}")


if __name__ == "__main__":
    main()
