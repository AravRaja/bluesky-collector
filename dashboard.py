"""Flask dashboard for monitoring the Bluesky collector.

Usage:
    python dashboard.py [--port 5000] [--db bluesky.db]
"""

import argparse
import os
from pathlib import Path

from flask import Flask, jsonify, render_template, request

import db

app = Flask(__name__)
DB_PATH = db.DEFAULT_DB_PATH


def get_conn():
    return db.get_db(DB_PATH)


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    conn = get_conn()
    try:
        counts = {}
        counts["posts"] = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        counts["likes"] = conn.execute(
            "SELECT COUNT(*) FROM engagements WHERE type='like'"
        ).fetchone()[0]
        counts["reposts"] = conn.execute(
            "SELECT COUNT(*) FROM engagements WHERE type='repost'"
        ).fetchone()[0]
        counts["follows"] = conn.execute("SELECT COUNT(*) FROM follows").fetchone()[0]

        latest_cursor = None
        cursor_file = Path(__file__).parent / ".cursor"
        if cursor_file.exists():
            text = cursor_file.read_text().strip()
            if text:
                latest_cursor = int(text)

        # Events per second from last 2 stats rows
        rows = conn.execute(
            "SELECT ts, posts, likes, reposts, follows FROM collector_stats ORDER BY ts DESC LIMIT 2"
        ).fetchall()
        events_per_sec = 0
        if len(rows) >= 2:
            total_recent = sum(rows[0][1:5])
            events_per_sec = round(total_recent / 60, 1)

        return jsonify({
            "counts": counts,
            "cursor": latest_cursor,
            "events_per_sec": events_per_sec,
        })
    finally:
        conn.close()


@app.route("/api/rate")
def api_rate():
    hours = request.args.get("hours", 6, type=int)
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT ts, posts, likes, reposts, follows
            FROM collector_stats
            WHERE ts >= datetime('now', ?)
            ORDER BY ts
            """,
            (f"-{hours} hours",),
        ).fetchall()
        data = [
            {"ts": r[0], "posts": r[1], "likes": r[2], "reposts": r[3], "follows": r[4]}
            for r in rows
        ]
        return jsonify(data)
    finally:
        conn.close()


@app.route("/api/top")
def api_top():
    n = request.args.get("n", 20, type=int)
    window_min = request.args.get("window", 60, type=int)
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT p.uri, p.did, p.text, p.time_us, p.created_at,
                   COALESCE(l.likes, 0) as likes,
                   COALESCE(r.reposts, 0) as reposts,
                   p.has_embed, p.embed_type
            FROM posts p
            LEFT JOIN (
                SELECT subject_uri, COUNT(*) as likes
                FROM engagements WHERE type='like'
                GROUP BY subject_uri
            ) l ON p.uri = l.subject_uri
            LEFT JOIN (
                SELECT subject_uri, COUNT(*) as reposts
                FROM engagements WHERE type='repost'
                GROUP BY subject_uri
            ) r ON p.uri = r.subject_uri
            WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
              AND p.time_us >= (
                  (strftime('%s','now') - ? * 60) * 1000000
              )
            ORDER BY (COALESCE(l.likes, 0) + COALESCE(r.reposts, 0)) DESC
            LIMIT ?
            """,
            (window_min, n),
        ).fetchall()
        data = [
            {
                "uri": r[0], "did": r[1],
                "text": (r[2] or "")[:200],
                "time_us": r[3], "created_at": r[4],
                "likes": r[5], "reposts": r[6],
                "has_embed": bool(r[7]), "embed_type": r[8],
            }
            for r in rows
        ]
        return jsonify(data)
    finally:
        conn.close()


@app.route("/api/viral")
def api_viral():
    threshold = request.args.get("threshold", 100, type=int)
    conn = get_conn()
    try:
        total = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE reply_parent IS NULL AND quote_of IS NULL"
        ).fetchone()[0]
        if total == 0:
            return jsonify({"total": 0, "viral": 0, "pct": 0})
        viral = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT p.uri, COALESCE(r.reposts, 0) as rp
                FROM posts p
                LEFT JOIN (
                    SELECT subject_uri, COUNT(*) as reposts
                    FROM engagements WHERE type='repost'
                    GROUP BY subject_uri
                ) r ON p.uri = r.subject_uri
                WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
                  AND rp >= ?
            )
            """,
            (threshold,),
        ).fetchone()[0]
        return jsonify({"total": total, "viral": viral, "pct": round(viral / total * 100, 4)})
    finally:
        conn.close()


@app.route("/api/health")
def api_health():
    """Collection health stats for morning check — should I keep collecting?"""
    conn = get_conn()
    try:
        # Collection duration
        first_ts = conn.execute("SELECT MIN(ts) FROM collector_stats").fetchone()[0]
        latest_ts = conn.execute("SELECT MAX(ts) FROM collector_stats").fetchone()[0]

        # DB file size
        db_path = Path(DB_PATH)
        db_size_mb = round(db_path.stat().st_size / (1024 * 1024), 1) if db_path.exists() else 0

        # Total counts
        total_posts = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        root_posts = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE reply_parent IS NULL AND quote_of IS NULL"
        ).fetchone()[0]
        total_likes = conn.execute(
            "SELECT COUNT(*) FROM engagements WHERE type='like'"
        ).fetchone()[0]
        total_reposts = conn.execute(
            "SELECT COUNT(*) FROM engagements WHERE type='repost'"
        ).fetchone()[0]
        total_follows = conn.execute("SELECT COUNT(*) FROM follows").fetchone()[0]
        unique_authors = conn.execute("SELECT COUNT(DISTINCT did) FROM posts").fetchone()[0]

        # Engagement distribution — what % of root posts have any engagement?
        posts_with_likes = conn.execute(
            """SELECT COUNT(DISTINCT e.subject_uri)
               FROM engagements e
               JOIN posts p ON e.subject_uri = p.uri
               WHERE e.type='like' AND p.reply_parent IS NULL AND p.quote_of IS NULL"""
        ).fetchone()[0]
        posts_with_reposts = conn.execute(
            """SELECT COUNT(DISTINCT e.subject_uri)
               FROM engagements e
               JOIN posts p ON e.subject_uri = p.uri
               WHERE e.type='repost' AND p.reply_parent IS NULL AND p.quote_of IS NULL"""
        ).fetchone()[0]

        # Repost tiers — how many posts hit various repost counts
        tiers = {}
        for tier in [1, 5, 10, 25, 50, 100, 250, 500]:
            count = conn.execute(
                """SELECT COUNT(*) FROM (
                    SELECT e.subject_uri, COUNT(*) as rp
                    FROM engagements e
                    JOIN posts p ON e.subject_uri = p.uri
                    WHERE e.type='repost' AND p.reply_parent IS NULL AND p.quote_of IS NULL
                    GROUP BY e.subject_uri
                    HAVING rp >= ?
                )""",
                (tier,),
            ).fetchone()[0]
            tiers[str(tier)] = count

        # Top 5 most reposted posts ever
        top_reposted = conn.execute(
            """SELECT p.uri, p.text, p.did, p.time_us, COUNT(*) as rp
               FROM engagements e
               JOIN posts p ON e.subject_uri = p.uri
               WHERE e.type='repost' AND p.reply_parent IS NULL AND p.quote_of IS NULL
               GROUP BY e.subject_uri
               ORDER BY rp DESC
               LIMIT 5"""
        ).fetchall()
        top_posts = [
            {"uri": r[0], "text": (r[1] or "")[:150], "did": r[2], "time_us": r[3], "reposts": r[4]}
            for r in top_reposted
        ]

        # Hourly throughput (last 24h)
        hourly = conn.execute(
            """SELECT
                 strftime('%Y-%m-%d %H:00', ts) as hour,
                 SUM(posts) as posts, SUM(likes) as likes,
                 SUM(reposts) as reposts, SUM(follows) as follows
               FROM collector_stats
               WHERE ts >= datetime('now', '-24 hours')
               GROUP BY hour ORDER BY hour"""
        ).fetchall()
        hourly_data = [
            {"hour": r[0], "posts": r[1], "likes": r[2], "reposts": r[3], "follows": r[4]}
            for r in hourly
        ]

        return jsonify({
            "collection_started": first_ts,
            "latest_stat": latest_ts,
            "db_size_mb": db_size_mb,
            "totals": {
                "posts": total_posts,
                "root_posts": root_posts,
                "likes": total_likes,
                "reposts": total_reposts,
                "follows": total_follows,
                "unique_authors": unique_authors,
            },
            "engagement_coverage": {
                "root_posts_with_likes": posts_with_likes,
                "root_posts_with_reposts": posts_with_reposts,
                "pct_with_likes": round(posts_with_likes / root_posts * 100, 1) if root_posts else 0,
                "pct_with_reposts": round(posts_with_reposts / root_posts * 100, 1) if root_posts else 0,
            },
            "repost_tiers": tiers,
            "top_reposted": top_posts,
            "hourly_throughput": hourly_data,
        })
    finally:
        conn.close()


@app.route("/api/recent")
def api_recent():
    n = request.args.get("n", 50, type=int)
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT p.uri, p.did, p.text, p.time_us, p.created_at,
                   COALESCE(l.likes, 0) as likes,
                   COALESCE(r.reposts, 0) as reposts,
                   p.has_embed, p.embed_type
            FROM posts p
            LEFT JOIN (
                SELECT subject_uri, COUNT(*) as likes
                FROM engagements WHERE type='like'
                GROUP BY subject_uri
            ) l ON p.uri = l.subject_uri
            LEFT JOIN (
                SELECT subject_uri, COUNT(*) as reposts
                FROM engagements WHERE type='repost'
                GROUP BY subject_uri
            ) r ON p.uri = r.subject_uri
            WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
            ORDER BY p.time_us DESC
            LIMIT ?
            """,
            (n,),
        ).fetchall()
        data = [
            {
                "uri": r[0], "did": r[1],
                "text": (r[2] or "")[:200],
                "time_us": r[3], "created_at": r[4],
                "likes": r[5], "reposts": r[6],
                "has_embed": bool(r[7]), "embed_type": r[8],
            }
            for r in rows
        ]
        return jsonify(data)
    finally:
        conn.close()


@app.route("/api/post/timeline")
def api_post_timeline():
    """Return engagement events bucketed into 1-min intervals for first 30 min."""
    uri = request.args.get("uri", "")
    if not uri:
        return jsonify({"error": "uri required"}), 400
    conn = get_conn()
    try:
        # Get post creation time
        post = conn.execute(
            "SELECT time_us FROM posts WHERE uri = ?", (uri,)
        ).fetchone()
        if not post:
            return jsonify({"error": "post not found"}), 404
        post_time_us = post[0]
        window_us = 30 * 60 * 1_000_000  # 30 minutes

        # Likes in first 30 min
        likes = conn.execute(
            """SELECT time_us FROM engagements
               WHERE subject_uri = ? AND type = 'like'
               AND time_us >= ? AND time_us <= ?""",
            (uri, post_time_us, post_time_us + window_us),
        ).fetchall()

        # Reposts in first 30 min
        reposts = conn.execute(
            """SELECT time_us FROM engagements
               WHERE subject_uri = ? AND type = 'repost'
               AND time_us >= ? AND time_us <= ?""",
            (uri, post_time_us, post_time_us + window_us),
        ).fetchall()

        # Replies in first 30 min
        replies = conn.execute(
            """SELECT time_us FROM posts
               WHERE (reply_parent = ? OR reply_root = ?)
               AND time_us >= ? AND time_us <= ?""",
            (uri, uri, post_time_us, post_time_us + window_us),
        ).fetchall()

        # Bucket into 1-min intervals (cumulative)
        buckets = []
        for minute in range(31):
            threshold = post_time_us + minute * 60 * 1_000_000
            buckets.append({
                "minute": minute,
                "likes": sum(1 for r in likes if r[0] <= threshold),
                "reposts": sum(1 for r in reposts if r[0] <= threshold),
                "replies": sum(1 for r in replies if r[0] <= threshold),
            })

        return jsonify({"post_time_us": post_time_us, "buckets": buckets})
    finally:
        conn.close()


@app.route("/api/viral/posts")
def api_viral_posts():
    """Return actual viral posts (100+ reposts) with engagement counts."""
    n = request.args.get("n", 50, type=int)
    conn = get_conn()
    try:
        rows = conn.execute(
            """SELECT p.uri, p.did, p.text, p.time_us, p.created_at,
                      p.has_embed, p.embed_type,
                      COALESCE(l.likes, 0) as likes,
                      COALESCE(r.reposts, 0) as reposts
               FROM posts p
               LEFT JOIN (
                   SELECT subject_uri, COUNT(*) as likes
                   FROM engagements WHERE type='like'
                   GROUP BY subject_uri
               ) l ON p.uri = l.subject_uri
               INNER JOIN (
                   SELECT subject_uri, COUNT(*) as reposts
                   FROM engagements WHERE type='repost'
                   GROUP BY subject_uri
                   HAVING reposts >= 100
               ) r ON p.uri = r.subject_uri
               WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
               ORDER BY r.reposts DESC
               LIMIT ?""",
            (n,),
        ).fetchall()
        data = [
            {
                "uri": r[0], "did": r[1], "text": (r[2] or "")[:200],
                "time_us": r[3], "created_at": r[4],
                "has_embed": bool(r[5]), "embed_type": r[6],
                "likes": r[7], "reposts": r[8],
            }
            for r in rows
        ]
        return jsonify(data)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Bluesky Collector Dashboard")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    global DB_PATH
    if args.db:
        DB_PATH = Path(args.db)

    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
