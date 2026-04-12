"""Prune old data from the database to manage disk usage.

Keeps all posts (they're small) but prunes engagements and follows
older than --keep-days. Follows are only needed for delta computation,
and old engagements for posts that will never reach virality are noise.

Usage:
    python prune.py [--keep-days 14] [--vacuum] [--db bluesky.db]
"""

import argparse
from pathlib import Path

import db


def main():
    parser = argparse.ArgumentParser(description="Prune old data")
    parser.add_argument("--keep-days", type=int, default=14,
                        help="Keep data from the last N days (default: 14)")
    parser.add_argument("--vacuum", action="store_true",
                        help="Run VACUUM to reclaim disk space (slow, locks DB)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be deleted without deleting")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn = db.get_db(db_path)

    # Calculate cutoff in microseconds
    cutoff_sql = f"(strftime('%s','now') - {args.keep_days * 86400}) * 1000000"

    # Count what would be pruned
    eng_count = conn.execute(
        f"SELECT COUNT(*) FROM engagements WHERE time_us < ({cutoff_sql})"
    ).fetchone()[0]
    follow_count = conn.execute(
        f"SELECT COUNT(*) FROM follows WHERE time_us < ({cutoff_sql})"
    ).fetchone()[0]
    stats_count = conn.execute(
        f"SELECT COUNT(*) FROM collector_stats WHERE ts < datetime('now', '-{args.keep_days} days')"
    ).fetchone()[0]

    print(f"Cutoff: {args.keep_days} days ago")
    print(f"  Engagements to prune: {eng_count:,}")
    print(f"  Follows to prune:     {follow_count:,}")
    print(f"  Stats rows to prune:  {stats_count:,}")

    if args.dry_run:
        print("\nDry run — nothing deleted.")
        conn.close()
        return

    if eng_count + follow_count + stats_count == 0:
        print("\nNothing to prune.")
        conn.close()
        return

    print("\nPruning...")
    conn.execute(f"DELETE FROM engagements WHERE time_us < ({cutoff_sql})")
    conn.execute(f"DELETE FROM follows WHERE time_us < ({cutoff_sql})")
    conn.execute(f"DELETE FROM collector_stats WHERE ts < datetime('now', '-{args.keep_days} days')")
    conn.commit()
    print("  Deleted.")

    db_size = db_path.stat().st_size / (1024 * 1024)
    print(f"  DB size: {db_size:.0f} MB")

    if args.vacuum:
        print("  Running VACUUM (this may take a moment)...")
        conn.execute("VACUUM")
        db_size_after = db_path.stat().st_size / (1024 * 1024)
        print(f"  DB size after VACUUM: {db_size_after:.0f} MB")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
