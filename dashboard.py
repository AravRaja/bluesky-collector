"""Flask dashboard for monitoring the Bluesky collector.

Usage:
    python dashboard.py [--port 5000] [--db bluesky.db]
"""

import argparse
import os
import threading
import time
from pathlib import Path

from flask import Flask, jsonify, render_template, request

import db

# In-memory cache for expensive queries
_cache: dict = {}
CACHE_TTL = 300  # 5 minutes


def _cache_get(key):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(key, data):
    _cache[key] = {"data": data, "ts": time.time()}


def _compute_health():
    """Run all health queries. Called from background thread and on-demand."""
    conn = db.get_db(DB_PATH)
    try:
        row = conn.execute(
            "SELECT MIN(ts), MAX(ts), COALESCE(SUM(posts),0), COALESCE(SUM(likes),0), "
            "COALESCE(SUM(reposts),0), COALESCE(SUM(follows),0) FROM collector_stats"
        ).fetchone()
        first_ts, latest_ts, total_posts, total_likes, total_reposts, total_follows = row

        db_path = Path(DB_PATH)
        real_path = db_path.resolve()
        db_size_mb = round(real_path.stat().st_size / (1024 * 1024), 1) if real_path.exists() else 0

        # Fast because of idx_posts_root partial index
        root_posts = conn.execute(
            "SELECT COUNT(*) FROM posts WHERE reply_parent IS NULL AND quote_of IS NULL"
        ).fetchone()[0]

        # Single pass: collect repost counts per root post, bucket in Python
        rows = conn.execute(
            """SELECT COUNT(*) as rp
               FROM engagements e
               WHERE e.type='repost'
                 AND EXISTS (
                   SELECT 1 FROM posts p
                   WHERE p.uri = e.subject_uri
                     AND p.reply_parent IS NULL AND p.quote_of IS NULL
                 )
               GROUP BY e.subject_uri"""
        ).fetchall()
        counts = [r[0] for r in rows]
        tiers = {str(t): sum(1 for c in counts if c >= t) for t in [1, 5, 10, 25, 50, 100, 250, 500]}

        top_reposted = conn.execute(
            """SELECT p.uri, p.text, p.did, p.time_us, COUNT(*) as rp
               FROM engagements e
               JOIN posts p ON e.subject_uri = p.uri
               WHERE e.type='repost' AND p.reply_parent IS NULL AND p.quote_of IS NULL
               GROUP BY e.subject_uri ORDER BY rp DESC LIMIT 5"""
        ).fetchall()
        top_posts = [
            {"uri": r[0], "text": (r[1] or "")[:150], "did": r[2], "time_us": r[3], "reposts": r[4]}
            for r in top_reposted
        ]

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

        result = {
            "collection_started": first_ts,
            "latest_stat": latest_ts,
            "db_size_mb": db_size_mb,
            "totals": {
                "posts": total_posts,
                "root_posts": root_posts,
                "likes": total_likes,
                "reposts": total_reposts,
                "follows": total_follows,
            },
            "repost_tiers": tiers,
            "top_reposted": top_posts,
            "hourly_throughput": hourly_data,
        }
        _cache_set("health", result)
        return result
    finally:
        conn.close()


def _health_warmer():
    """Background thread: compute health cache every CACHE_TTL seconds."""
    # Initial warm-up after a short delay so the service starts fast
    time.sleep(5)
    while True:
        try:
            _compute_health()
        except Exception as e:
            print(f"[health warmer] error: {e}")
        time.sleep(CACHE_TTL)

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
        # Fast: SUM collector_stats (tiny table) instead of COUNT(*) on millions of rows
        row = conn.execute(
            "SELECT COALESCE(SUM(posts),0), COALESCE(SUM(likes),0), COALESCE(SUM(reposts),0), COALESCE(SUM(follows),0) FROM collector_stats"
        ).fetchone()
        counts = {"posts": row[0], "likes": row[1], "reposts": row[2], "follows": row[3]}

        latest_cursor = None
        cursor_file = Path(__file__).parent / ".cursor"
        if cursor_file.exists():
            text = cursor_file.read_text().strip()
            if text:
                latest_cursor = int(text)

        # Events per second from last stats row
        last_row = conn.execute(
            "SELECT posts, likes, reposts, follows FROM collector_stats ORDER BY ts DESC LIMIT 1"
        ).fetchone()
        events_per_sec = 0
        if last_row:
            events_per_sec = round(sum(last_row) / 60, 1)

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
    cache_key = f"viral_{threshold}"
    cached = _cache_get(cache_key)
    if cached:
        return jsonify(cached)

    conn = get_conn()
    try:
        # Viral count: posts with >= threshold reposts (uses idx_eng_type_subject)
        viral = conn.execute(
            """SELECT COUNT(*) FROM (
                SELECT e.subject_uri
                FROM engagements e
                JOIN posts p ON e.subject_uri = p.uri
                WHERE e.type='repost' AND p.reply_parent IS NULL AND p.quote_of IS NULL
                GROUP BY e.subject_uri
                HAVING COUNT(*) >= ?
            )""",
            (threshold,),
        ).fetchone()[0]
        # Use fast collector_stats sum for total instead of COUNT(*) on posts
        total_posts = conn.execute(
            "SELECT COALESCE(SUM(posts),0) FROM collector_stats"
        ).fetchone()[0]
        result = {"total": total_posts, "viral": viral, "pct": round(viral / total_posts * 100, 4) if total_posts else 0}
        _cache_set(cache_key, result)
        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/health")
def api_health():
    """Serve from background cache — never blocks the request."""
    cached = _cache_get("health")
    if cached:
        return jsonify(cached)
    # Cache miss (first request after startup): compute inline, subsequent served from cache
    return jsonify(_compute_health())


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

    # Start background health cache warmer
    threading.Thread(target=_health_warmer, daemon=True).start()

    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
