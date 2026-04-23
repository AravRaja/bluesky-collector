"""Export SQLite data to layered parquet files.

Layer 1: root_posts.parquet — all viral posts + sampled non-viral
Layer 2: reposts.parquet, replies.parquet, quotes.parquet, likes.parquet
         (cascade events within --cascade-window-min of post creation)

Fast approach: find viral posts via repost index first, then sample non-viral.
Never scans all root posts — runs in ~1-2 minutes even on large DBs.

Usage:
    python export.py [--since 2024-04-01] [--until 2024-04-15]
                     [--min-age-hours 2] [--sample-ratio 10]
                     [--cascade-window-min 30]
                     [--out-dir ./export] [--db bluesky.db]
"""

import argparse
import hashlib
import secrets
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import db
from virality_threshold import is_viral as _is_viral_fn

VIRAL_REPOST_THRESHOLD = 100
SALT_PATH = Path.home() / ".export_salt"


def load_or_create_salt():
    if SALT_PATH.exists():
        return SALT_PATH.read_text().strip()
    salt = secrets.token_hex(16)
    SALT_PATH.write_text(salt)
    SALT_PATH.chmod(0o600)
    print(f"Generated DID salt, saved to {SALT_PATH}")
    return salt


def hash_did_series(series, salt):
    salt_b = salt.encode()
    def h(d):
        if not isinstance(d, str):
            return d
        return hashlib.sha256(salt_b + d.encode()).hexdigest()[:16]
    return series.map(h)


def ts_to_time_us(ts_str):
    """Convert ISO date string to unix microseconds."""
    dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _add_time_delta(df, time_col, time_map, uri_col="root_post_uri"):
    """Add time_delta_sec — seconds after post creation when each event arrived."""
    if df.empty:
        df = df.copy()
        df["time_delta_sec"] = pd.Series(dtype=float)
        return df
    df = df.copy()
    df["time_delta_sec"] = (df[time_col] - df[uri_col].map(time_map)) / 1e6
    return df


def export_data(conn, since_us, until_us, min_age_us, sample_ratio,
                cascade_window_min, out_dir, no_sample=False, no_viral_label=False):
    out_dir.mkdir(parents=True, exist_ok=True)
    window_sec = cascade_window_min * 60
    salt = load_or_create_salt()

    # Only export posts whose author has a cached profile row.
    profile_filter = "AND EXISTS (SELECT 1 FROM profiles pr WHERE pr.did = p.did)"

    # -------------------------------------------------------------------
    # Step 1: Viral posts — from post_stats (pruned) + live engagement (recent)
    # -------------------------------------------------------------------
    print("Finding viral posts...")
    viral_from_stats = pd.read_sql_query(
        f"""SELECT p.uri, p.did, p.time_us, p.created_at, p.text, p.langs,
                  p.has_embed, p.embed_type, ps.reposts as total_reposts
           FROM post_stats ps
           JOIN posts p ON ps.uri = p.uri
           WHERE ps.reposts >= ?
             AND p.reply_parent IS NULL AND p.quote_of IS NULL
             AND p.time_us >= ? AND p.time_us <= ? AND p.time_us <= ?
             {profile_filter}""",
        conn,
        params=(VIRAL_REPOST_THRESHOLD, since_us, until_us, min_age_us),
    )
    viral_from_live = pd.read_sql_query(
        f"""SELECT p.uri, p.did, p.time_us, p.created_at, p.text, p.langs,
                  p.has_embed, p.embed_type, COUNT(*) as total_reposts
           FROM engagements e
           JOIN posts p ON e.subject_uri = p.uri
           WHERE e.type = 'repost'
             AND p.reply_parent IS NULL AND p.quote_of IS NULL
             AND p.time_us >= ? AND p.time_us <= ? AND p.time_us <= ?
             AND p.uri NOT IN (SELECT uri FROM post_stats)
             {profile_filter}
           GROUP BY e.subject_uri
           HAVING COUNT(*) >= ?""",
        conn,
        params=(since_us, until_us, min_age_us, VIRAL_REPOST_THRESHOLD),
    )
    viral_posts = pd.concat([viral_from_stats, viral_from_live], ignore_index=True)
    viral_posts = viral_posts.drop_duplicates(subset=["uri"])
    print(f"  Found {len(viral_posts)} viral posts")

    # -------------------------------------------------------------------
    # Step 2: Non-viral posts (sampled by default; --no-sample pulls all)
    # -------------------------------------------------------------------
    viral_uris = viral_posts["uri"].tolist()
    exclude_clause = ""
    exclude_params = ()
    if viral_uris:
        ph = ",".join("?" * len(viral_uris))
        exclude_clause = f"AND p.uri NOT IN ({ph})"
        exclude_params = tuple(viral_uris)

    if no_sample:
        print("Fetching ALL non-viral posts with cached profiles (no sampling)...")
        non_viral_posts = pd.read_sql_query(
            f"""SELECT p.uri, p.did, p.time_us, p.created_at, p.text, p.langs,
                       p.has_embed, p.embed_type
                FROM posts p
                WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
                  AND p.time_us >= ? AND p.time_us <= ? AND p.time_us <= ?
                  {exclude_clause}
                  {profile_filter}""",
            conn,
            params=(since_us, until_us, min_age_us, *exclude_params),
        )
    else:
        n_sample = len(viral_posts) * sample_ratio
        print(f"Sampling {n_sample} non-viral posts (most recent)...")
        non_viral_posts = pd.read_sql_query(
            f"""SELECT p.uri, p.did, p.time_us, p.created_at, p.text, p.langs,
                       p.has_embed, p.embed_type
                FROM posts p
                WHERE p.reply_parent IS NULL AND p.quote_of IS NULL
                  AND p.time_us >= ? AND p.time_us <= ? AND p.time_us <= ?
                  {exclude_clause}
                  {profile_filter}
                ORDER BY p.time_us DESC
                LIMIT ?""",
            conn,
            params=(since_us, until_us, min_age_us, *exclude_params, n_sample),
        )

    non_viral_posts["total_reposts"] = 0
    if not no_viral_label:
        viral_posts["is_viral"] = True
        non_viral_posts["is_viral"] = False
    layer1 = pd.concat([viral_posts, non_viral_posts], ignore_index=True)
    print(f"  Layer 1: {len(viral_posts)} viral + {len(non_viral_posts)} non-viral = {len(layer1)} posts")

    # -------------------------------------------------------------------
    # Step 3: Engagement totals — post_stats for old posts, live count for new
    # -------------------------------------------------------------------
    print("Counting engagements for sampled posts...")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _export_uris (uri TEXT PRIMARY KEY)")
    conn.execute("DELETE FROM _export_uris")
    conn.executemany("INSERT OR IGNORE INTO _export_uris VALUES (?)",
                     [(u,) for u in layer1["uri"].tolist()])
    conn.commit()

    # post_stats totals + time-bucketed snapshots
    ps_df = pd.read_sql_query(
        """SELECT ps.uri, ps.likes, ps.reposts,
                  ps.reposts_3h, ps.reposts_4h, ps.reposts_6h,
                  ps.reposts_12h, ps.reposts_24h
           FROM post_stats ps JOIN _export_uris x ON ps.uri = x.uri""",
        conn,
    )
    ps_likes = dict(zip(ps_df["uri"], ps_df["likes"]))
    ps_reposts = dict(zip(ps_df["uri"], ps_df["reposts"]))
    ps_snapshots = {col: dict(zip(ps_df["uri"], ps_df[col]))
                    for col in ["reposts_3h", "reposts_4h", "reposts_6h",
                                "reposts_12h", "reposts_24h"]}

    # live engagement counts (cascade rows still in engagements table)
    eng_counts = pd.read_sql_query(
        """SELECT e.subject_uri, e.type, COUNT(*) as cnt
           FROM engagements e JOIN _export_uris x ON e.subject_uri = x.uri
           GROUP BY e.subject_uri, e.type""",
        conn,
    )
    live_likes = {}
    live_reposts = {}
    for _, row in eng_counts.iterrows():
        if row["type"] == "like":
            live_likes[row["subject_uri"]] = row["cnt"]
        elif row["type"] == "repost":
            live_reposts[row["subject_uri"]] = row["cnt"]

    # For old posts: post_stats has the total (includes cascade + late).
    # For new posts: only live counts exist. Use whichever is larger (post_stats is authoritative when it exists).
    def get_likes(uri):
        if uri in ps_likes:
            return ps_likes[uri]
        return live_likes.get(uri, 0)

    def get_reposts(uri):
        if uri in ps_reposts:
            return ps_reposts[uri]
        return live_reposts.get(uri, 0)

    reply_counts = pd.read_sql_query(
        """SELECT p.reply_parent as uri, COUNT(*) as cnt
           FROM posts p JOIN _export_uris x ON p.reply_parent = x.uri
           GROUP BY p.reply_parent""",
        conn,
    )
    replies_map = dict(zip(reply_counts["uri"], reply_counts["cnt"]))

    quote_counts = pd.read_sql_query(
        """SELECT p.quote_of as uri, COUNT(*) as cnt
           FROM posts p JOIN _export_uris x ON p.quote_of = x.uri
           GROUP BY p.quote_of""",
        conn,
    )
    quotes_map = dict(zip(quote_counts["uri"], quote_counts["cnt"]))

    layer1["total_likes"]   = layer1["uri"].map(get_likes).fillna(0).astype(int)
    layer1["total_reposts"] = layer1["uri"].map(get_reposts).fillna(0).astype(int)
    layer1["total_replies"] = layer1["uri"].map(replies_map).fillna(0).astype(int)
    layer1["total_quotes"]  = layer1["uri"].map(quotes_map).fillna(0).astype(int)

    # Time-bucketed repost snapshots (None if post hasn't reached that age yet)
    for col, snap in ps_snapshots.items():
        layer1[col] = layer1["uri"].map(snap)

    # -------------------------------------------------------------------
    # Step 4: Author profiles
    # -------------------------------------------------------------------
    print("Looking up author profiles...")
    profiles = pd.read_sql_query(
        """SELECT pr.did, pr.handle, pr.followers_count, pr.follows_count, pr.posts_count
           FROM profiles pr
           JOIN (SELECT DISTINCT did FROM posts p JOIN _export_uris x ON p.uri = x.uri) d
             ON pr.did = d.did""",
        conn,
    )
    pm = profiles.set_index("did").to_dict("index")
    layer1["author_handle"]      = layer1["did"].map(lambda d: pm.get(d, {}).get("handle"))
    layer1["author_followers"]    = layer1["did"].map(lambda d: pm.get(d, {}).get("followers_count", 0))
    layer1["author_follows"]      = layer1["did"].map(lambda d: pm.get(d, {}).get("follows_count", 0))
    layer1["author_posts_count"]  = layer1["did"].map(lambda d: pm.get(d, {}).get("posts_count", 0))

    # -------------------------------------------------------------------
    # Step 5: Export Layer 1
    # -------------------------------------------------------------------
    print("Exporting Layer 1: root_posts.parquet")
    layer1["did"] = hash_did_series(layer1["did"], salt)
    layer1 = layer1.drop(columns=["author_handle"], errors="ignore")
    layer1.to_parquet(out_dir / "root_posts.parquet", index=False)

    # -------------------------------------------------------------------
    # Step 6: Layer 2 cascade — temp table already has the right URIs
    # -------------------------------------------------------------------
    l1_time   = dict(zip(layer1["uri"], layer1["time_us"]))
    window_us = cascade_window_min * 60 * 1_000_000

    print(f"Exporting Layer 2: reposts.parquet  (first {cascade_window_min} min)")
    reposts_df = pd.read_sql_query(
        """SELECT e.subject_uri as root_post_uri, e.did as reposter_did,
                  e.time_us as repost_time_us, e.created_at,
                  p.followers_count as reposter_followers,
                  p.follows_count as reposter_follows
           FROM engagements e
           JOIN _export_uris x ON e.subject_uri = x.uri
           LEFT JOIN profiles p ON e.did = p.did
           WHERE e.type='repost'""",
        conn,
    )
    reposts_df = _add_time_delta(reposts_df, "repost_time_us", l1_time)
    reposts_df = reposts_df[reposts_df["time_delta_sec"].between(0, window_sec)]
    reposts_df["reposter_did"] = hash_did_series(reposts_df["reposter_did"], salt)
    reposts_df.to_parquet(out_dir / "reposts.parquet", index=False)

    print(f"Exporting Layer 2: likes.parquet    (first {cascade_window_min} min)")
    likes_df = pd.read_sql_query(
        """SELECT e.subject_uri as root_post_uri, e.did as liker_did,
                  e.time_us as like_time_us, e.created_at
           FROM engagements e
           JOIN _export_uris x ON e.subject_uri = x.uri
           WHERE e.type='like'""",
        conn,
    )
    likes_df = _add_time_delta(likes_df, "like_time_us", l1_time)
    likes_df = likes_df[likes_df["time_delta_sec"].between(0, window_sec)]
    likes_df["liker_did"] = hash_did_series(likes_df["liker_did"], salt)
    likes_df.to_parquet(out_dir / "likes.parquet", index=False)

    print(f"Exporting Layer 2: replies.parquet  (first {cascade_window_min} min)")
    replies_df = pd.read_sql_query(
        """SELECT p.reply_root as root_post_uri, p.uri as reply_uri,
                  p.did as replier_did, p.time_us as reply_time_us,
                  p.text as reply_text, p.has_embed as reply_has_embed,
                  p.created_at,
                  pr.followers_count as replier_followers,
                  pr.follows_count as replier_follows
           FROM posts p
           JOIN _export_uris x ON p.reply_root = x.uri
           LEFT JOIN profiles pr ON p.did = pr.did""",
        conn,
    )
    replies_df = _add_time_delta(replies_df, "reply_time_us", l1_time)
    replies_df = replies_df[replies_df["time_delta_sec"].between(0, window_sec)]
    replies_df["replier_did"] = hash_did_series(replies_df["replier_did"], salt)
    replies_df.to_parquet(out_dir / "replies.parquet", index=False)

    print(f"Exporting Layer 2: quotes.parquet   (first {cascade_window_min} min)")
    quotes_df = pd.read_sql_query(
        """SELECT p.quote_of as root_post_uri, p.uri as quote_uri,
                  p.did as quoter_did, p.time_us as quote_time_us,
                  p.text as quote_text, p.has_embed as quote_has_embed,
                  p.created_at,
                  pr.followers_count as quoter_followers,
                  pr.follows_count as quoter_follows
           FROM posts p
           JOIN _export_uris x ON p.quote_of = x.uri
           LEFT JOIN profiles pr ON p.did = pr.did""",
        conn,
    )
    quotes_df = _add_time_delta(quotes_df, "quote_time_us", l1_time)
    quotes_df = quotes_df[quotes_df["time_delta_sec"].between(0, window_sec)]
    quotes_df["quoter_did"] = hash_did_series(quotes_df["quoter_did"], salt)
    quotes_df.to_parquet(out_dir / "quotes.parquet", index=False)

    conn.execute("DROP TABLE IF EXISTS _export_uris")

    print(f"\nExport complete → {out_dir}/")
    print(f"  root_posts.parquet : {len(layer1):>6} rows")
    print(f"  reposts.parquet    : {len(reposts_df):>6} rows  (≤{cascade_window_min} min)")
    print(f"  likes.parquet      : {len(likes_df):>6} rows  (≤{cascade_window_min} min)")
    print(f"  replies.parquet    : {len(replies_df):>6} rows  (≤{cascade_window_min} min)")
    print(f"  quotes.parquet     : {len(quotes_df):>6} rows  (≤{cascade_window_min} min)")


def main():
    parser = argparse.ArgumentParser(description="Export SQLite to layered parquets")
    parser.add_argument("--since",             type=str, default="2024-01-01")
    parser.add_argument("--until",             type=str, default="2099-12-31")
    parser.add_argument("--min-age-hours",      type=int, default=2,
                        help="Only posts older than N hours (ensures cascade has time to accumulate)")
    parser.add_argument("--sample-ratio",       type=int, default=10,
                        help="Non-viral samples per viral post")
    parser.add_argument("--cascade-window-min", type=int, default=30,
                        help="Only cascade events within this many minutes of post creation")
    parser.add_argument("--out-dir",            type=str, default="./export")
    parser.add_argument("--db",                 type=str, default=None)
    parser.add_argument("--no-sample",          action="store_true",
                        help="Skip non-viral sampling — include every post with a cached profile")
    parser.add_argument("--no-viral-label",     action="store_true",
                        help="Drop the is_viral column (derivable from total_reposts)")
    args = parser.parse_args()

    db_path    = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn       = db.get_db(db_path)
    since_us   = ts_to_time_us(args.since)
    until_us   = ts_to_time_us(args.until)
    now_us     = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    min_age_us = now_us - (args.min_age_hours * 3600 * 1_000_000)

    export_data(conn, since_us, until_us, min_age_us,
                args.sample_ratio, args.cascade_window_min, Path(args.out_dir),
                no_sample=args.no_sample, no_viral_label=args.no_viral_label)
    conn.close()


if __name__ == "__main__":
    main()
