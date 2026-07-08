#!/usr/bin/env python3
"""
prune_dead_hashes.py — Remove hashes that have had no peers for N+ days.

Safe to run while dht_peer_count.py is running (SQLite WAL mode).
Always run --dry-run first to see what will be deleted.

Usage:
    python prune_dead_hashes.py --dry-run                    # preview, default 7 days
    python prune_dead_hashes.py --days 7                     # prune hashes dead 7+ days
    python prune_dead_hashes.py --days 14                    # more conservative
    python prune_dead_hashes.py --days 7 --category Anime    # prune one category only
    python prune_dead_hashes.py --days 7 --min-age-days 14   # spare hashes added < 14 days ago

A hash is pruned if ALL of the following are true:
  1. No real peer IP seen in the last --days days (only _queried_ sentinels)
  2. Has been queried at least once (has a _queried_ row → confirms it's been tried)
  3. Was added to the DB at least --min-age-days ago (default: 14)
     — protects recently-scraped hashes that haven't had enough passes to prove themselves

Hashes added recently (< --min-age-days) are ALWAYS skipped, regardless of peer history.
"""

import argparse
import datetime
import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "hashes_v2.db"
BATCH_SIZE = 500


def get_stats(conn: sqlite3.Connection, days: int, category: str | None,
              min_age_days: int = 14) -> tuple[int, int, list, int]:
    cutoff      = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    min_age_cut = (datetime.date.today() - datetime.timedelta(days=min_age_days)).isoformat()

    cat_filter = "AND h.category = ?" if category else ""
    params_dead  = [cutoff, min_age_cut] + ([category] if category else [])
    params_never = [min_age_cut] + ([category] if category else [])

    # Hashes dead for N+ days AND old enough to be prunable
    dead_sql = f"""
        SELECT h.hash, h.title, h.category, h.ip_id, h.first_seen
        FROM hashes h
        WHERE NOT EXISTS (
            SELECT 1 FROM peers p
            WHERE p.hash = h.hash
              AND p.ip != '_queried_'
              AND p.last_seen >= ?
        )
        AND EXISTS (
            SELECT 1 FROM peers p2
            WHERE p2.hash = h.hash
              AND p2.ip = '_queried_'
        )
        AND (h.first_seen IS NULL OR h.first_seen <= ?)
        {cat_filter}
        ORDER BY h.category, h.title
    """

    # Hashes never found at all (AND old enough)
    never_sql = f"""
        SELECT COUNT(*) FROM hashes h
        WHERE NOT EXISTS (
            SELECT 1 FROM peers p WHERE p.hash = h.hash AND p.ip != '_queried_'
        )
        AND EXISTS (
            SELECT 1 FROM peers p2 WHERE p2.hash = h.hash AND p2.ip = '_queried_'
        )
        AND (h.first_seen IS NULL OR h.first_seen <= ?)
        {cat_filter}
    """

    # How many hashes are too new to prune (protected by min_age_days)
    protected_sql = f"""
        SELECT COUNT(*) FROM hashes h
        WHERE h.first_seen > ?
        {cat_filter}
    """
    params_protected = [min_age_cut] + ([category] if category else [])

    dead_rows     = conn.execute(dead_sql,  params_dead).fetchall()
    never_count   = conn.execute(never_sql, params_never).fetchone()[0]
    protected     = conn.execute(protected_sql, params_protected).fetchone()[0]
    total         = conn.execute(
        "SELECT COUNT(*) FROM hashes" + (" WHERE category=?" if category else ""),
        ([category] if category else [])
    ).fetchone()[0]

    return total, never_count, dead_rows, protected


def prune(conn: sqlite3.Connection, dead_rows: list, dry_run: bool) -> tuple[int, int]:
    if dry_run:
        return 0, 0

    dead_hashes = [row[0] for row in dead_rows]
    deleted_hashes = 0
    deleted_peers = 0

    for i in range(0, len(dead_hashes), BATCH_SIZE):
        chunk = dead_hashes[i : i + BATCH_SIZE]
        placeholders = ",".join("?" * len(chunk))
        deleted_peers += conn.execute(
            f"DELETE FROM peers WHERE hash IN ({placeholders})", chunk
        ).rowcount
        deleted_hashes += conn.execute(
            f"DELETE FROM hashes WHERE hash IN ({placeholders})", chunk
        ).rowcount
        if i % 5000 == 0 and i > 0:
            conn.commit()
            print(f"  ... {i}/{len(dead_hashes)} pruned", flush=True)

    conn.commit()
    return deleted_hashes, deleted_peers


def main():
    parser = argparse.ArgumentParser(description="Prune dead hashes from DHT tracker DB")
    parser.add_argument("--days", type=int, default=7,
                        help="Prune hashes with no peers for this many days (default: 7)")
    parser.add_argument("--min-age-days", type=int, default=14,
                        help="Protect hashes added within this many days (default: 14). "
                             "Prevents pruning recently-scraped content that hasn't been "
                             "queried enough times to prove it's truly dead.")
    parser.add_argument("--category", type=str, default=None,
                        help="Limit to one category: Movies, Series, Anime")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview only — no changes made")
    parser.add_argument("--vacuum", action="store_true",
                        help="Run VACUUM after pruning to reclaim disk space (slow)")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt (required for cron/non-interactive use)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Pruning hashes dead for {args.days}+ days"
          + (f" in category '{args.category}'" if args.category else ""))
    print(f"Cutoff date (dead) : {(datetime.date.today() - datetime.timedelta(days=args.days)).isoformat()}")
    print(f"Min-age protection : hashes added within {args.min_age_days} days are NEVER pruned")
    print(f"Protected since    : {(datetime.date.today() - datetime.timedelta(days=args.min_age_days)).isoformat()}")
    print()

    total, never_count, dead_rows, protected = get_stats(
        conn, args.days, args.category, args.min_age_days
    )

    print(f"Total hashes in DB          : {total:,}")
    print(f"Protected (too new)         : {protected:,}  ← will not be pruned")
    print(f"Dead {args.days}+ days (prunable)  : {len(dead_rows):,} ({len(dead_rows)/total*100:.1f}%)")
    print(f"  of which never found a peer: {never_count:,}")
    print(f"  of which had peers before  : {len(dead_rows) - never_count:,}")
    print(f"Remaining after prune        : {total - len(dead_rows):,}")
    print()

    # Category breakdown of what will be pruned
    from collections import Counter
    cat_counts = Counter(row[2] for row in dead_rows)
    print("Prunable by category:")
    for cat, cnt in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {cat:<10} {cnt:>7,}")
    print()

    # Sample of what will be deleted
    print("Sample hashes to prune (first 15):")
    for h, title, cat, ip_id in dead_rows[:15]:
        label = title or ip_id or h[:16]
        print(f"  [{cat}] {label}")
    print()

    if args.dry_run:
        print("Dry run — no changes made.")
        print(f"Re-run without --dry-run to prune {len(dead_rows):,} hashes "
              f"({protected:,} protected by --min-age-days {args.min_age_days}).")
        conn.close()
        return

    if len(dead_rows) == 0:
        print("Nothing to prune.")
        conn.close()
        return

    if args.yes:
        print(f"--yes flag set — skipping confirmation, pruning {len(dead_rows):,} hashes.")
    else:
        try:
            confirm = input(f"Prune {len(dead_rows):,} hashes? [y/N] ").strip().lower()
        except EOFError:
            # stdin is /dev/null (e.g. cron) — require --yes for non-interactive runs
            print("ERROR: stdin is not a terminal. Use --yes to confirm pruning in cron/scripts.")
            conn.close()
            sys.exit(1)
        if confirm != "y":
            print("Aborted.")
            conn.close()
            return

    print("Pruning...", flush=True)
    deleted_hashes, deleted_peers = prune(conn, dead_rows, dry_run=False)
    print(f"Deleted {deleted_hashes:,} hashes and {deleted_peers:,} peer rows.")

    if args.vacuum:
        print("Running VACUUM (reclaiming disk space)...", flush=True)
        conn.execute("VACUUM")
        print("VACUUM done.")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
