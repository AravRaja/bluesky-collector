# Computes dashboard stats and writes a JSON file atomically.
# Called from the collector process every 60 seconds.
# Opens a short-lived read connection — never holds a lock.

import json
import os
import time
from pathlib import Path

import db

STATS_FILE = Path("/data/dashboard_stats.json")
CASCADE_WINDOW_US = 2 * 3600 * 1_000_000  # 2 hours in microseconds


def _engagement_totals(conn, uri):
    # post_stats has the full total if it exists; otherwise count live rows
    row = conn.execute("SELECT likes, reposts FROM post_stats WHERE uri = ?", (uri,)).fetchone()
    if row:
        return row[0], row[1]
    likes = conn.execute(
        "SELECT COUNT(*) FROM engagements WHERE type='like' AND subject_uri=?", (uri,)
    ).fetchone()[0]
    reposts = conn.execute(
        "SELECT COUNT(*) FROM engagements WHERE type='repost' AND subject_uri=?", (uri,)
    ).fetchone()[0]
    return likes, reposts


def write_stats(db_path):
    conn = db.get_reader(db_path)
    try:

        # totals from collector_stats (tiny table, instant)
        row = conn.execute(
            "SELECT MIN(ts), MAX(ts), COALESCE(SUM(posts),0), COALESCE(SUM(likes),0), "
            "COALESCE(SUM(reposts),0), COALESCE(SUM(follows),0) FROM collector_stats"
        ).fetchone()
        first_ts, latest_ts = row[0], row[1]
        total_posts, total_likes, total_reposts, total_follows = row[2], row[3], row[4], row[5]

        # events per second from latest stats row
        last = conn.execute(
            "SELECT posts, likes, reposts, follows FROM collector_stats ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        eps = round(sum(last) / 60, 1) if last else 0

        # cursor
        cursor = None
        cursor_file = Path(db_path).parent / ".cursor"
        if cursor_file.exists():
            text = cursor_file.read_text().strip()
            if text:
                cursor = int(text)

        # rate data (last 6h)
        rate_rows = conn.execute(
            "SELECT ts, posts, likes, reposts, follows FROM collector_stats "
            "WHERE ts >= datetime('now', '-6 hours') ORDER BY ts"
        ).fetchall()
        rate = [{"ts": r[0], "posts": r[1], "likes": r[2], "reposts": r[3], "follows": r[4]}
                for r in rate_rows]

        # db size
        real_path = Path(db_path).resolve()
        db_size_mb = round(real_path.stat().st_size / (1024 * 1024), 1) if real_path.exists() else 0

        # root posts count (uses idx_posts_root partial index)
        root_posts = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE reply_parent IS NULL AND quote_of IS NULL"
        ).fetchone()[0]

        # repost tiers from post_stats (fast, indexed)
        tiers = {}
        for t in [1, 5, 10, 25, 50, 100, 250, 500]:
            tiers[str(t)] = conn.execute(
                "SELECT COUNT(*) FROM post_stats WHERE reposts >= ?", (t,)
            ).fetchone()[0]

        # also count recent posts (no post_stats yet) that might qualify
        # these are posts < 2h old where engagement is still in live rows
        now_us = int(time.time() * 1_000_000)
        recent_viral = conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT e.subject_uri, COUNT(*) as rp
                FROM engagements e
                JOIN posts p ON e.subject_uri = p.uri
                WHERE e.type='repost'
                  AND p.reply_parent IS NULL AND p.quote_of IS NULL
                  AND p.time_us > ?
                GROUP BY e.subject_uri
                HAVING rp >= 100
            )""", (now_us - CASCADE_WINDOW_US,)
        ).fetchone()[0]

        # top 5 most reposted (from post_stats)
        top_rows = conn.execute(
            """SELECT ps.uri, p.text, p.did, p.time_us, ps.reposts
               FROM post_stats ps
               JOIN posts p ON ps.uri = p.uri
               WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
               ORDER BY ps.reposts DESC LIMIT 5"""
        ).fetchall()
        top_posts = [
            {"uri": r[0], "text": (r[1] or "")[:150], "did": r[2], "time_us": r[3], "reposts": r[4]}
            for r in top_rows
        ]

        # hourly throughput (last 24h, from collector_stats)
        hourly = conn.execute(
            """SELECT strftime('%Y-%m-%d %H:00', ts) as hour,
                 SUM(posts), SUM(likes), SUM(reposts), SUM(follows)
               FROM collector_stats WHERE ts >= datetime('now', '-24 hours')
               GROUP BY hour ORDER BY hour"""
        ).fetchall()
        hourly_data = [
            {"hour": r[0], "posts": r[1], "likes": r[2], "reposts": r[3], "follows": r[4]}
            for r in hourly
        ]

        # viral count
        viral_from_stats = conn.execute(
            "SELECT COUNT(*) FROM post_stats WHERE reposts >= 100"
        ).fetchone()[0]
        viral_count = viral_from_stats + recent_viral

        # viral posts list (top 50)
        viral_posts_rows = conn.execute(
            """SELECT ps.uri, p.text, p.did, p.time_us, p.created_at,
                      p.has_embed, p.embed_type, ps.likes, ps.reposts
               FROM post_stats ps
               JOIN posts p ON ps.uri = p.uri
               WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
                 AND ps.reposts >= 100
               ORDER BY ps.reposts DESC LIMIT 50"""
        ).fetchall()
        viral_posts = [
            {"uri": r[0], "text": (r[1] or "")[:200], "did": r[2], "time_us": r[3],
             "created_at": r[4], "has_embed": bool(r[5]), "embed_type": r[6],
             "likes": r[7], "reposts": r[8]}
            for r in viral_posts_rows
        ]

        # recent 50 root posts with engagement counts
        recent_rows = conn.execute(
            """SELECT uri, did, text, time_us, created_at, has_embed, embed_type
               FROM posts
               WHERE reply_parent IS NULL AND quote_of IS NULL
               ORDER BY time_us DESC LIMIT 50"""
        ).fetchall()
        recent = []
        for r in recent_rows:
            uri = r[0]
            likes, reposts = _engagement_totals(conn, uri)
            recent.append({
                "uri": uri, "did": r[1], "text": (r[2] or "")[:200],
                "time_us": r[3], "created_at": r[4],
                "has_embed": bool(r[5]), "embed_type": r[6],
                "likes": likes, "reposts": reposts,
            })

        result = {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "status": {
                "counts": {
                    "posts": total_posts, "likes": total_likes,
                    "reposts": total_reposts, "follows": total_follows,
                },
                "cursor": cursor,
                "events_per_sec": eps,
            },
            "rate": rate,
            "health": {
                "collection_started": first_ts,
                "latest_stat": latest_ts,
                "db_size_mb": db_size_mb,
                "totals": {
                    "posts": total_posts, "root_posts": root_posts,
                    "likes": total_likes, "reposts": total_reposts,
                    "follows": total_follows,
                },
                "repost_tiers": tiers,
                "top_reposted": top_posts,
                "hourly_throughput": hourly_data,
            },
            "viral": {"total": root_posts, "viral": viral_count,
                      "pct": round(viral_count / root_posts * 100, 4) if root_posts else 0},
            "viral_posts": viral_posts,
            "recent": recent,
        }

        # atomic write
        tmp = STATS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(result))
        os.rename(str(tmp), str(STATS_FILE))

    finally:
        conn.close()
