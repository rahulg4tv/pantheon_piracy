#!/usr/bin/env python3
"""Compact daily peer_counts CSV scratch files into partitioned Parquet.

Raw scanner output (one row per (hash, country)):
    /data/peer_counts/YYYY-MM-DD_w<N>.csv
Columnar serving layer (partitioned by date):
    /data/peer_counts_parquet/date=YYYY-MM-DD/peer_counts.parquet

This is a faithful columnar copy — raw ip_id is preserved, NO canonicalization
(that stays a query/merge concern). Query with DuckDB / pyarrow for fast,
column-projected, predicate-pushdown aggregations instead of full CSV scans.

The DHT scanner exits when the UTC date rolls, so a day's CSV set is complete
once that day ends; the nightly cron compacts the PREVIOUS (completed) day.
"""
import argparse
import glob
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

SRC_DEFAULT = "/data/peer_counts"
OUT_DEFAULT = "/data/peer_counts_parquet"

SCHEMA = pa.schema([
    ("date",           pa.string()),
    ("run_time",       pa.string()),
    ("hash",           pa.string()),
    ("ip_id",          pa.string()),
    ("title",          pa.string()),
    ("category",       pa.string()),
    ("seeders",        pa.int32()),
    ("country",        pa.string()),
    ("peer_count",     pa.int32()),
    ("bep33_seeders",  pa.int32()),
    ("bep33_leechers", pa.int32()),
])
INT_COLS = {"seeders", "peer_count", "bep33_seeders", "bep33_leechers"}

# The `date` column is redundant with the `date=YYYY-MM-DD` partition path and
# collides when reading the tree as a Hive-partitioned dataset, so we drop it
# from the file body — readers reconstruct it from the partition key.
WRITE_SCHEMA = pa.schema([f for f in SCHEMA if f.name != "date"])


def _align(table: pa.Table) -> pa.Table:
    """Coerce an arbitrary CSV-read table to SCHEMA (missing cols filled, ints nulls→0)."""
    cols = {}
    for name in SCHEMA.names:
        if name in table.column_names:
            col = table[name]
            if name in INT_COLS:
                col = pc.fill_null(col.cast(pa.int32(), safe=False), 0)
            else:
                col = col.cast(pa.string())
        else:
            fill = 0 if name in INT_COLS else None
            col = pa.array([fill] * table.num_rows, type=SCHEMA.field(name).type)
        cols[name] = col
    return pa.table(cols, schema=SCHEMA)


def compact(day: str, src: str, out: str, compression: str, delete_csv: bool) -> int:
    files = sorted(glob.glob(os.path.join(src, f"{day}_w*.csv")))
    single = os.path.join(src, f"{day}.csv")
    if os.path.exists(single):
        files.append(single)
    if not files:
        print(f"No CSV files for {day} in {src}", file=sys.stderr)
        return 1

    part_dir = Path(out) / f"date={day}"
    part_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = part_dir / ".peer_counts.parquet.tmp"
    final_path = part_dir / "peer_counts.parquet"

    convert_opts = pacsv.ConvertOptions(
        column_types={c: pa.int32() for c in INT_COLS},
        null_values=["", "NA", "null", "None"],
        strings_can_be_null=True,
    )
    read_opts = pacsv.ReadOptions(block_size=64 << 20)

    total_rows = 0
    writer = None
    try:
        for f in files:
            table = pacsv.read_csv(f, read_options=read_opts, convert_options=convert_opts)
            t2 = _align(table).select(WRITE_SCHEMA.names)
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, WRITE_SCHEMA, compression=compression)
            writer.write_table(t2)
            total_rows += t2.num_rows
            print(f"  + {os.path.basename(f)}: {t2.num_rows:,} rows")
    except Exception:
        if writer is not None:
            writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    finally:
        if writer is not None:
            writer.close()

    os.replace(tmp_path, final_path)  # atomic publish

    csv_bytes = sum(os.path.getsize(f) for f in files)
    pq_bytes = os.path.getsize(final_path)
    ratio = csv_bytes / max(pq_bytes, 1)
    print(f"\nDONE {day}: {total_rows:,} rows from {len(files)} CSV(s)")
    print(f"  CSV {csv_bytes/1e6:.1f} MB -> Parquet {pq_bytes/1e6:.1f} MB  ({ratio:.1f}x smaller, {compression})")
    print(f"  -> {final_path}")

    if delete_csv:
        for f in files:
            os.remove(f)
        print(f"  Deleted {len(files)} source CSV(s)")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Compact peer_counts CSV → partitioned Parquet")
    ap.add_argument("--date", help="YYYY-MM-DD to compact (default: yesterday UTC)")
    ap.add_argument("--src", default=SRC_DEFAULT)
    ap.add_argument("--out", default=OUT_DEFAULT)
    ap.add_argument("--compression", default="zstd", choices=["zstd", "snappy", "gzip"])
    ap.add_argument("--delete-csv", action="store_true",
                    help="delete source CSV(s) after a successful write (default: keep)")
    args = ap.parse_args()
    day = args.date or str(date.today() - timedelta(days=1))
    sys.exit(compact(day, args.src, args.out, args.compression, args.delete_csv))


if __name__ == "__main__":
    main()
