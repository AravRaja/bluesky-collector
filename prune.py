# Prune engagement rows outside the per-post cascade window.
#
# For each post: keep individual engagement rows from creation to +2 hours (cascade data).
# Engagement arriving after the 2-hour window gets counted into post_stats totals, then deleted.
# Posts themselves are never deleted. Cascade rows are never deleted.
#
# Usage:
#     python prune.py [--keep-hours 2] [--follow-keep-days 7] [--vacuum] [--db bluesky.db]

import argparse
import time
from pathlib import Path

import db

CASCADE_WINDOW_US = 2 * 3600 * 1_000_000  # 2 hours in microseconds


def main():
    parser = argparse.ArgumentParser(description="Prune old engagement data")
    parser.add_argument("--keep-hours", type=int, default=2,
                        help="Cascade window per post in hours (default: 2)")
    parser.add_argument("--follow-keep-days", type=int, default=7,
                        help="Keep follows for N days (default: 7)")
    parser.add_argument("--vacuum", action="store_true",
                        help="Run VACUUM after pruning (slow, reclaims disk)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    window_us = args.keep_hours * 3600 * 1_000_000
    follow_cutoff_us = int((time.time() - args.follow_keep_days * 86400) * 1_000_000)

    db_path = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn = db.get_db(db_path)

    # Only process posts old enough that their cascade window has closed
    min_post_age_us = int(time.time() * 1_000_000) - window_us

    print(f"Cascade window: {args.keep_hours}h per post")
    print(f"Follow retention: {args.follow_keep_days} days")

    # Step 1: Find engagement rows outside their post's cascade window.
    # These are rows where: engagement.time_us > post.time_us + window
    # Only consider posts whose cascade window has fully closed (older than keep-hours)
    print("\nCounting engagement rows outside cascade window...")
    late_count = conn.execute(
        """SELECT COUNT(*) FROM engagements e
           JOIN posts p ON e.subject_uri = p.uri
           WHERE p.time_us < ?
             AND e.time_us > p.time_us + ?""",
        (min_post_age_us, window_us),
    ).fetchone()[0]

    follow_count = conn.execute(
        "SELECT COUNT(*) FROM follows WHERE time_us < ?", (follow_cutoff_us,)
    ).fetchone()[0]

    print(f"  Late engagement rows to prune: {late_count:,}")
    print(f"  Old follows to prune:          {follow_count:,}")

    if args.dry_run:
        print("\nDry run — nothing changed.")
        conn.close()
        return

    if late_count + follow_count == 0:
        print("\nNothing to prune.")
        conn.close()
        return

    # Step 2: For posts with late engagement, recount ALL engagement (cascade + late)
    # and write the total into post_stats. This is the single source of truth.
    print("\nAggregating totals into post_stats...")
    conn.execute(
        """INSERT OR REPLACE INTO post_stats (uri, likes, reposts, updated_at)
           SELECT p.uri,
             COALESCE((SELECT COUNT(*) FROM engagements WHERE type='like' AND subject_uri=p.uri), 0),
             COALESCE((SELECT COUNT(*) FROM engagements WHERE type='repost' AND subject_uri=p.uri), 0),
             strftime('%Y-%m-%dT%H:%M:%S','now')
           FROM posts p
           WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
             AND p.time_us < ?""",
        (min_post_age_us,),
    )
    conn.commit()
    stats_updated = conn.execute("SELECT changes()").fetchone()[0]
    print(f"  Updated {stats_updated:,} post_stats rows")

    # Step 3: Delete late engagement rows (outside cascade window)
    print("Deleting late engagement rows...")
    conn.execute(
        """DELETE FROM engagements WHERE rowid IN (
            SELECT e.rowid FROM engagements e
            JOIN posts p ON e.subject_uri = p.uri
            WHERE p.time_us < ?
              AND e.time_us > p.time_us + ?
        )""",
        (min_post_age_us, window_us),
    )
    conn.commit()
    deleted_eng = conn.execute("SELECT changes()").fetchone()[0]
    print(f"  Deleted {deleted_eng:,} engagement rows")

    # Step 4: Delete old follows
    if follow_count > 0:
        print("Deleting old follows...")
        conn.execute("DELETE FROM follows WHERE time_us < ?", (follow_cutoff_us,))
        conn.commit()
        deleted_fol = conn.execute("SELECT changes()").fetchone()[0]
        print(f"  Deleted {deleted_fol:,} follow rows")

    # Step 5: Prune old collector_stats (keep 30 days)
    conn.execute("DELETE FROM collector_stats WHERE ts < datetime('now', '-30 days')")
    conn.commit()

    db_size = db_path.resolve().stat().st_size / (1024 * 1024)
    print(f"\nDB size: {db_size:.0f} MB")

    if args.vacuum:
        print("Running VACUUM (this will take a while)...")
        conn.execute("VACUUM")
        db_size_after = db_path.resolve().stat().st_size / (1024 * 1024)
        print(f"DB size after VACUUM: {db_size_after:.0f} MB")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
