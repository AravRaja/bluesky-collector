# Prune engagement rows outside the per-post cascade window.
#
# Each post keeps detailed engagement rows for its first 2 hours (cascade data).
# Late engagement (after 2h) gets counted into post_stats incrementally, then deleted.
# post_stats also snapshots reposts at 3h, 4h, 6h, 12h, 24h for multi-horizon ML labels.
#
# Usage:
#     python prune.py [--keep-hours 2] [--follow-keep-days 7] [--vacuum] [--db bluesky.db]

import argparse
import time
from pathlib import Path

import db

HOUR_US = 3600 * 1_000_000
SNAPSHOT_HOURS = [3, 4, 6, 12, 24]


def main():
    parser = argparse.ArgumentParser(description="Prune old engagement data")
    parser.add_argument("--keep-hours", type=int, default=2)
    parser.add_argument("--follow-keep-days", type=int, default=7)
    parser.add_argument("--vacuum", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    window_us = args.keep_hours * HOUR_US
    now_us = int(time.time() * 1_000_000)
    min_post_age_us = now_us - window_us
    follow_cutoff_us = now_us - (args.follow_keep_days * 86400 * 1_000_000)

    db_path = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn = db.get_db(db_path)

    print(f"Cascade window: {args.keep_hours}h | Follow retention: {args.follow_keep_days}d")

    # Count late engagement rows (outside cascade window)
    late_count = conn.execute(
        """SELECT COUNT(*) FROM engagements e
           JOIN posts p ON e.subject_uri = p.uri
           WHERE p.time_us < ? AND e.time_us > p.time_us + ?""",
        (min_post_age_us, window_us),
    ).fetchone()[0]

    follow_count = conn.execute(
        "SELECT COUNT(*) FROM follows WHERE time_us < ?", (follow_cutoff_us,)
    ).fetchone()[0]

    print(f"  Late engagement to prune: {late_count:,}")
    print(f"  Old follows to prune:     {follow_count:,}")

    if args.dry_run:
        print("\nDry run — nothing changed.")
        conn.close()
        return

    # ---------------------------------------------------------------
    # Step 1: Initial aggregation for posts not yet in post_stats.
    # Counts only cascade engagement (within 2h window).
    # INSERT OR IGNORE = only runs for posts not already tracked.
    # ---------------------------------------------------------------
    print("\nStep 1: Initial aggregation for new posts...")
    conn.execute(
        """INSERT OR IGNORE INTO post_stats (uri, likes, reposts, replies, quotes)
           SELECT p.uri,
             COALESCE((SELECT COUNT(*) FROM engagements
                       WHERE type='like' AND subject_uri=p.uri
                       AND time_us <= p.time_us + ?), 0),
             COALESCE((SELECT COUNT(*) FROM engagements
                       WHERE type='repost' AND subject_uri=p.uri
                       AND time_us <= p.time_us + ?), 0),
             COALESCE((SELECT COUNT(*) FROM posts
                       WHERE reply_parent=p.uri), 0),
             COALESCE((SELECT COUNT(*) FROM posts
                       WHERE quote_of=p.uri), 0)
           FROM posts p
           WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
             AND p.time_us < ?""",
        (window_us, window_us, min_post_age_us),
    )
    conn.commit()
    new_posts = conn.execute("SELECT changes()").fetchone()[0]
    print(f"  New post_stats rows: {new_posts:,}")

    # ---------------------------------------------------------------
    # Step 2: Count late engagement about to be deleted.
    # ADD to existing post_stats (never overwrite).
    # ---------------------------------------------------------------
    print("Step 2: Incrementing post_stats with late engagement...")
    conn.execute(
        """INSERT INTO post_stats (uri, likes, reposts, replies, quotes)
           SELECT e.subject_uri,
               SUM(CASE WHEN e.type='like' THEN 1 ELSE 0 END),
               SUM(CASE WHEN e.type='repost' THEN 1 ELSE 0 END),
               0, 0
           FROM engagements e
           JOIN posts p ON e.subject_uri = p.uri
           WHERE p.time_us < ?
             AND e.time_us > p.time_us + ?
           GROUP BY e.subject_uri
           ON CONFLICT(uri) DO UPDATE SET
               likes = post_stats.likes + excluded.likes,
               reposts = post_stats.reposts + excluded.reposts,
               updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')""",
        (min_post_age_us, window_us),
    )
    conn.commit()

    # Refresh reply/quote counts (these are posts, never deleted, so recount is safe)
    print("  Refreshing reply/quote counts...")
    conn.execute(
        """UPDATE post_stats SET
             replies = COALESCE((SELECT COUNT(*) FROM posts WHERE reply_parent=post_stats.uri), 0),
             quotes = COALESCE((SELECT COUNT(*) FROM posts WHERE quote_of=post_stats.uri), 0),
             updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')
           WHERE uri IN (SELECT uri FROM posts WHERE time_us < ? AND reply_parent IS NULL AND quote_of IS NULL)""",
        (min_post_age_us,),
    )
    conn.commit()

    # ---------------------------------------------------------------
    # Step 3: Snapshot reposts at time horizons (3h, 4h, 6h, 12h, 24h).
    # Each bucket is written once when the post crosses that age.
    # ---------------------------------------------------------------
    print("Step 3: Time-bucketed snapshots...")
    for h in SNAPSHOT_HOURS:
        col = f"reposts_{h}h"
        age_cutoff_us = now_us - (h * HOUR_US)
        # Posts that are at least h hours old but don't have this snapshot yet
        conn.execute(
            f"""UPDATE post_stats SET {col} = reposts, updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')
                WHERE {col} IS NULL
                  AND uri IN (
                    SELECT uri FROM posts
                    WHERE time_us < ? AND reply_parent IS NULL AND quote_of IS NULL
                  )""",
            (age_cutoff_us,),
        )
    conn.commit()
    print("  Snapshots updated")

    # ---------------------------------------------------------------
    # Step 4: Delete late engagement rows (outside cascade window)
    # ---------------------------------------------------------------
    print("Step 4: Deleting late engagement rows...")
    conn.execute(
        """DELETE FROM engagements WHERE rowid IN (
            SELECT e.rowid FROM engagements e
            JOIN posts p ON e.subject_uri = p.uri
            WHERE p.time_us < ? AND e.time_us > p.time_us + ?
        )""",
        (min_post_age_us, window_us),
    )
    conn.commit()
    deleted = conn.execute("SELECT changes()").fetchone()[0]
    print(f"  Deleted {deleted:,} engagement rows")

    # ---------------------------------------------------------------
    # Step 5: Delete old follows + old collector_stats
    # ---------------------------------------------------------------
    if follow_count > 0:
        conn.execute("DELETE FROM follows WHERE time_us < ?", (follow_cutoff_us,))
        conn.commit()
        print(f"  Deleted {follow_count:,} follow rows")

    conn.execute("DELETE FROM collector_stats WHERE ts < datetime('now', '-30 days')")
    conn.commit()

    db_size = db_path.resolve().stat().st_size / (1024 * 1024)
    print(f"\nDB size: {db_size:.0f} MB")

    if args.vacuum:
        print("Running VACUUM...")
        conn.execute("VACUUM")
        db_size_after = db_path.resolve().stat().st_size / (1024 * 1024)
        print(f"DB size after VACUUM: {db_size_after:.0f} MB")

    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
