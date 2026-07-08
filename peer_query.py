#!/usr/bin/env python3
"""Ad-hoc peer-count top-N query with canonical ip_id collapsing.

Raw per-day peer rows (one per hash×country) fragment a single show across
multiple catalog ip_ids (e.g. Spider-Noir = series-tt30460310 + series-Q123956515),
so a naive groupby undercounts it. This applies the SAME canonical map the nightly
merge job uses (merge_and_upload.build_canonical_map / canonicalize) before
aggregating, so ad-hoc top lists match the merged dashboard numbers.

Reads the compacted Parquet partition for a date if present, else falls back to the
raw per-worker CSVs. Pass --no-canon to see the raw (fragmented) numbers for comparison.

Examples:
    python3 peer_query.py --date 2026-05-29 --country US --top 25
    python3 peer_query.py --metric peer_count --top 15
    python3 peer_query.py --ip-id series-tt30460310 --date 2026-05-29   # one show's breakdown
"""
import argparse
import glob
import os
import sys
from datetime import date as date_cls

import pandas as pd

from merge_and_upload import load_titles_catalog, build_canonical_map

PARQUET_DEFAULT = "/data/peer_counts_parquet"
CSV_DEFAULT = "/data/peer_counts"

# per-hash×country these are MAX'd across the day's passes (same as merge dedup);
# the rest carry along with the surviving row.
PASS_MAX_COLS = ["peer_count", "bep33_seeders", "bep33_leechers"]


def _load_day(day: str, parquet_dir: str, csv_dir: str) -> pd.DataFrame:
    part = os.path.join(parquet_dir, f"date={day}", "peer_counts.parquet")
    if os.path.exists(part):
        print(f"[query] reading Parquet {part}", file=sys.stderr)
        return pd.read_parquet(part)

    files = sorted(glob.glob(os.path.join(csv_dir, f"{day}_w*.csv")))
    single = os.path.join(csv_dir, f"{day}.csv")
    if os.path.exists(single):
        files.append(single)
    if not files:
        sys.exit(f"No Parquet partition or CSVs for {day}")
    print(f"[query] reading {len(files)} CSV(s) (no Parquet for {day})", file=sys.stderr)
    return pd.concat((pd.read_csv(f) for f in files), ignore_index=True)


def _dedup_passes(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse multiple same-day passes: MAX per hash×country (matches merge job)."""
    keys = ["hash", "ip_id", "title", "category", "country"]
    have_max = [c for c in PASS_MAX_COLS if c in df.columns]
    return df.groupby(keys, as_index=False)[have_max].max()


def main() -> None:
    ap = argparse.ArgumentParser(description="Ad-hoc canonical peer-count top-N")
    ap.add_argument("--date", help="YYYY-MM-DD (default: today UTC)")
    ap.add_argument("--parquet-dir", default=PARQUET_DEFAULT)
    ap.add_argument("--csv-dir", default=CSV_DEFAULT)
    ap.add_argument("--country", help="filter to one country code (e.g. US)")
    ap.add_argument("--category", help="filter to one category (e.g. Series)")
    ap.add_argument("--ip-id", help="show per-country breakdown for one ip_id (post-canon)")
    ap.add_argument("--metric", default="bep33_leechers",
                    choices=["bep33_leechers", "peer_count"],
                    help="ranking metric (default: bep33_leechers = active downloaders)")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--no-canon", action="store_true",
                    help="skip canonical remap (show raw fragmented numbers)")
    args = ap.parse_args()

    day = args.date or str(date_cls.today())
    df = _load_day(day, args.parquet_dir, args.csv_dir)
    if args.metric not in df.columns:
        sys.exit(f"metric '{args.metric}' not in columns: {list(df.columns)}")

    df = _dedup_passes(df)

    if not args.no_canon:
        # Pass the day's peer rows so orphan ip_ids that exist only in the peer
        # stream (e.g. a stale legacy series-Q… still emitted by a long-running
        # DHT worker) fold into the canonical map — otherwise they split a show's
        # count (e.g. "The Pitt" = series-tt31938062 + orphan series-Q131431817).
        alias_map, canon_title = build_canonical_map(load_titles_catalog(), extra_df=df)
        before = df["ip_id"].nunique()
        df["ip_id"] = df["ip_id"].map(lambda x: alias_map.get(x, x))
        df["title"] = df["ip_id"].map(canon_title).fillna(df["title"])
        print(f"[query] canonicalized ip_ids: {before:,} -> {df['ip_id'].nunique():,}",
              file=sys.stderr)

    if args.category:
        df = df[df["category"].str.lower() == args.category.lower()]

    if args.ip_id:
        sub = df[df["ip_id"] == args.ip_id]
        if sub.empty:
            sys.exit(f"no rows for ip_id {args.ip_id} on {day} (after canon)")
        title = sub["title"].iloc[0]
        per_country = (sub.groupby("country", as_index=False)[args.metric].sum()
                       .sort_values(args.metric, ascending=False))
        total = int(sub[args.metric].sum())
        print(f"\n{args.ip_id}  |  {title}  |  {args.metric}={total:,} (all countries)")
        for _, r in per_country.head(args.top).iterrows():
            print(f"  {r['country']:<4} {int(r[args.metric]):>10,}")
        return

    if args.country:
        df = df[df["country"] == args.country]

    grp = (df.groupby(["ip_id", "title", "category"], as_index=False)[args.metric].sum()
           .sort_values(args.metric, ascending=False))

    scope = f"country={args.country}" if args.country else "all countries"
    print(f"\nTop {args.top} by {args.metric} ({day}, {scope}"
          f"{', '+args.category if args.category else ''}"
          f"{'' if args.no_canon else ', canonical'}):")
    print(f"{'#':>3}  {args.metric:>10}  {'category':<8}  ip_id / title")
    for i, (_, r) in enumerate(grp.head(args.top).iterrows(), 1):
        print(f"{i:>3}  {int(r[args.metric]):>10,}  {r['category']:<8}  "
              f"{r['ip_id']}  {r['title']}")


if __name__ == "__main__":
    main()
