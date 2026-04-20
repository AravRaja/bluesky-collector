# Prune engagement rows outside the per-post cascade window — incremental version.
#
# Each step processes rows in small batches (BATCH_SIZE) with a short sleep
# between commits so the collector can grab the write lock and never starves.
#
# Each post keeps detailed engagement rows for its first 2 hours (cascade data).
# Late engagement (after 2h) gets counted into post_stats incrementally, then deleted.
# post_stats also snapshots reposts at 3h, 4h, 6h, 12h, 24h for multi-horizon ML labels.
#
# Usage:
#     python prune.py [--keep-hours 2] [--follow-keep-days 7] [--db bluesky.db]

import argparse
import time
from pathlib import Path

import db

HOUR_US = 3600 * 1_000_000
SNAPSHOT_HOURS = [3, 4, 6, 12, 24]
BATCH_SIZE = 500          # rows per commit — stays under SQLite's 999 param limit
SLEEP_SEC = 0.1           # yield between batches so collector can acquire the lock


def step1_initial_aggregation(conn, min_post_age_us, window_us):
    """Create post_stats rows for root posts older than the cascade window
    that aren't tracked yet. Counts engagement inside the 2h cascade window."""
    total = 0
    while True:
        cur = conn.execute(
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
                 AND p.time_us < ?
                 AND NOT EXISTS (SELECT 1 FROM post_stats ps WHERE ps.uri = p.uri)
               LIMIT ?""",
            (window_us, window_us, min_post_age_us, BATCH_SIZE),
        )
        conn.commit()
        n = cur.rowcount
        if n == 0:
            break
        total += n
        if total // 10000 != (total - n) // 10000:
            print(f"  step1: {total:,} new post_stats rows...")
        time.sleep(SLEEP_SEC)
    return total


def step2_late_engagement(conn, min_post_age_us, window_us):
    """Aggregate late (post-cascade) engagement into post_stats, then delete those rows."""
    total = 0
    while True:
        rows = conn.execute(
            """SELECT e.rowid, e.subject_uri, e.type FROM engagements e
               JOIN posts p ON e.subject_uri = p.uri
               WHERE p.time_us < ? AND e.time_us > p.time_us + ?
               LIMIT ?""",
            (min_post_age_us, window_us, BATCH_SIZE),
        ).fetchall()
        if not rows:
            break

        counts = {}
        rowids = []
        for rowid, uri, etype in rows:
            counts[(uri, etype)] = counts.get((uri, etype), 0) + 1
            rowids.append(rowid)

        like_rows = [(uri, n) for (uri, t), n in counts.items() if t == "like"]
        repost_rows = [(uri, n) for (uri, t), n in counts.items() if t == "repost"]

        if like_rows:
            conn.executemany(
                """INSERT INTO post_stats (uri, likes, reposts, replies, quotes)
                   VALUES (?, ?, 0, 0, 0)
                   ON CONFLICT(uri) DO UPDATE SET
                     likes = likes + excluded.likes,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')""",
                like_rows,
            )
        if repost_rows:
            conn.executemany(
                """INSERT INTO post_stats (uri, likes, reposts, replies, quotes)
                   VALUES (?, 0, ?, 0, 0)
                   ON CONFLICT(uri) DO UPDATE SET
                     reposts = reposts + excluded.reposts,
                     updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')""",
                repost_rows,
            )

        placeholders = ",".join("?" * len(rowids))
        conn.execute(
            f"DELETE FROM engagements WHERE rowid IN ({placeholders})", rowids
        )
        conn.commit()
        total += len(rowids)
        if total // 10000 != (total - len(rowids)) // 10000:
            print(f"  step2: {total:,} late engagement rows processed...")
        time.sleep(SLEEP_SEC)
    return total


def step3_refresh_reply_quote(conn, min_post_age_us):
    """Refresh reply/quote counts for tracked root posts.
    Posts are never deleted, so recount is always safe."""
    rowid_max = conn.execute(
        "SELECT COALESCE(MAX(rowid), 0) FROM post_stats"
    ).fetchone()[0]
    if rowid_max == 0:
        return 0
    total = 0
    current = 1
    while current <= rowid_max:
        cur = conn.execute(
            """UPDATE post_stats SET
                 replies = COALESCE((SELECT COUNT(*) FROM posts WHERE reply_parent=post_stats.uri), 0),
                 quotes  = COALESCE((SELECT COUNT(*) FROM posts WHERE quote_of=post_stats.uri), 0),
                 updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')
               WHERE rowid BETWEEN ? AND ?
                 AND uri IN (SELECT uri FROM posts
                             WHERE time_us < ? AND reply_parent IS NULL AND quote_of IS NULL)""",
            (current, current + BATCH_SIZE - 1, min_post_age_us),
        )
        conn.commit()
        total += cur.rowcount
        current += BATCH_SIZE
        time.sleep(SLEEP_SEC)
    return total


def step4_snapshots(conn, now_us):
    """Snapshot reposts at each horizon (3h/4h/6h/12h/24h). Each cell is written once."""
    rowid_max = conn.execute(
        "SELECT COALESCE(MAX(rowid), 0) FROM post_stats"
    ).fetchone()[0]
    if rowid_max == 0:
        return 0
    total = 0
    for h in SNAPSHOT_HOURS:
        col = f"reposts_{h}h"
        age_cutoff_us = now_us - (h * HOUR_US)
        current = 1
        while current <= rowid_max:
            cur = conn.execute(
                f"""UPDATE post_stats SET {col} = reposts,
                      updated_at = strftime('%Y-%m-%dT%H:%M:%S','now')
                    WHERE rowid BETWEEN ? AND ?
                      AND {col} IS NULL
                      AND uri IN (SELECT uri FROM posts
                                  WHERE time_us < ?
                                    AND reply_parent IS NULL AND quote_of IS NULL)""",
                (current, current + BATCH_SIZE - 1, age_cutoff_us),
            )
            conn.commit()
            total += cur.rowcount
            current += BATCH_SIZE
            time.sleep(SLEEP_SEC)
    return total


def step5_delete_follows(conn, follow_cutoff_us):
    """Delete follows older than cutoff."""
    total = 0
    while True:
        cur = conn.execute(
            """DELETE FROM follows WHERE rowid IN (
                 SELECT rowid FROM follows WHERE time_us < ? LIMIT ?
               )""",
            (follow_cutoff_us, BATCH_SIZE),
        )
        conn.commit()
        n = cur.rowcount
        if n == 0:
            break
        total += n
        time.sleep(SLEEP_SEC)
    return total


def main():
    parser = argparse.ArgumentParser(description="Prune old engagement data (incremental)")
    parser.add_argument("--keep-hours", type=int, default=2)
    parser.add_argument("--follow-keep-days", type=int, default=7)
    parser.add_argument("--db", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    window_us = args.keep_hours * HOUR_US
    now_us = int(time.time() * 1_000_000)
    min_post_age_us = now_us - window_us
    follow_cutoff_us = now_us - (args.follow_keep_days * 86400 * 1_000_000)

    db_path = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn = db.get_db(db_path)

    print(f"Cascade window: {args.keep_hours}h | Follow retention: {args.follow_keep_days}d")
    print(f"Batch size: {BATCH_SIZE} | Sleep between batches: {SLEEP_SEC}s")

    late_count = conn.execute(
        """SELECT COUNT(*) FROM engagements e
           JOIN posts p ON e.subject_uri = p.uri
           WHERE p.time_us < ? AND e.time_us > p.time_us + ?""",
        (min_post_age_us, window_us),
    ).fetchone()[0]
    follow_count = conn.execute(
        "SELECT COUNT(*) FROM follows WHERE time_us < ?", (follow_cutoff_us,)
    ).fetchone()[0]
    print(f"  Late engagement to process: {late_count:,}")
    print(f"  Old follows to delete:      {follow_count:,}")

    if args.dry_run:
        print("\nDry run — nothing changed.")
        conn.close()
        return

    t0 = time.time()

    print("\nStep 1: Initial aggregation...")
    n1 = step1_initial_aggregation(conn, min_post_age_us, window_us)
    print(f"  {n1:,} new post_stats rows ({time.time()-t0:.1f}s)")

    t = time.time()
    print("Step 2: Late engagement aggregation + delete...")
    n2 = step2_late_engagement(conn, min_post_age_us, window_us)
    print(f"  {n2:,} late engagement rows ({time.time()-t:.1f}s)")

    t = time.time()
    print("Step 3: Refresh reply/quote counts...")
    n3 = step3_refresh_reply_quote(conn, min_post_age_us)
    print(f"  {n3:,} post_stats rows updated ({time.time()-t:.1f}s)")

    t = time.time()
    print("Step 4: Time-bucketed snapshots...")
    n4 = step4_snapshots(conn, now_us)
    print(f"  {n4:,} snapshot cells updated ({time.time()-t:.1f}s)")

    if follow_count > 0:
        t = time.time()
        print("Step 5: Delete old follows...")
        n5 = step5_delete_follows(conn, follow_cutoff_us)
        print(f"  {n5:,} follow rows deleted ({time.time()-t:.1f}s)")

    conn.execute("DELETE FROM collector_stats WHERE ts < datetime('now', '-30 days')")
    conn.commit()

    db_size = db_path.resolve().stat().st_size / (1024 * 1024)
    print(f"\nDB size: {db_size:.0f} MB | Total time: {time.time()-t0:.1f}s")
    conn.close()


if __name__ == "__main__":
    main()
