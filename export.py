"""Export SQLite data to layered parquet files.

Layer 1: root_posts.parquet — all viral posts + sampled non-viral
Layer 2: reposts.parquet, replies.parquet, quotes.parquet, likes.parquet
         (only for posts in Layer 1)

Usage:
    python export.py [--since 2024-04-01] [--until 2024-04-15]
                     [--min-age-hours 2] [--sample-ratio 10]
                     [--out-dir ./export] [--db bluesky.db]
"""

import argparse
import random
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import db
from virality_threshold import is_viral


def ts_to_time_us(ts_str):
    """Convert ISO date string to unix microseconds."""
    dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def export_data(conn, since_us, until_us, min_age_us, sample_ratio, out_dir):
    """Run the full layered export."""

    out_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------------------------------------------------
    # Step 1: Identify root posts in the date range
    # -----------------------------------------------------------------------
    print("Querying root posts...")
    root_posts = pd.read_sql_query(
        """
        SELECT * FROM posts
        WHERE reply_parent IS NULL AND quote_of IS NULL
          AND time_us >= ? AND time_us <= ?
          AND time_us <= ?
        """,
        conn,
        params=(since_us, until_us, min_age_us),
    )
    print(f"  Found {len(root_posts)} root posts")

    if root_posts.empty:
        print("No root posts found in range. Exiting.")
        return

    # -----------------------------------------------------------------------
    # Step 2: Count engagements per root post
    # -----------------------------------------------------------------------
    print("Counting engagements...")
    uris = root_posts["uri"].tolist()

    # Likes and reposts from engagements table
    placeholders = ",".join("?" * len(uris))
    eng_counts = pd.read_sql_query(
        f"""
        SELECT subject_uri, type, COUNT(*) as cnt
        FROM engagements
        WHERE subject_uri IN ({placeholders})
        GROUP BY subject_uri, type
        """,
        conn,
        params=uris,
    )

    likes_map = {}
    reposts_map = {}
    for _, row in eng_counts.iterrows():
        if row["type"] == "like":
            likes_map[row["subject_uri"]] = row["cnt"]
        elif row["type"] == "repost":
            reposts_map[row["subject_uri"]] = row["cnt"]

    # Replies from posts table
    reply_counts = pd.read_sql_query(
        f"""
        SELECT reply_parent as uri, COUNT(*) as cnt
        FROM posts
        WHERE reply_parent IN ({placeholders})
        GROUP BY reply_parent
        """,
        conn,
        params=uris,
    )
    replies_map = dict(zip(reply_counts["uri"], reply_counts["cnt"]))

    # Quotes from posts table
    quote_counts = pd.read_sql_query(
        f"""
        SELECT quote_of as uri, COUNT(*) as cnt
        FROM posts
        WHERE quote_of IN ({placeholders})
        GROUP BY quote_of
        """,
        conn,
        params=uris,
    )
    quotes_map = dict(zip(quote_counts["uri"], quote_counts["cnt"]))

    root_posts["total_likes"] = root_posts["uri"].map(likes_map).fillna(0).astype(int)
    root_posts["total_reposts"] = root_posts["uri"].map(reposts_map).fillna(0).astype(int)
    root_posts["total_replies"] = root_posts["uri"].map(replies_map).fillna(0).astype(int)
    root_posts["total_quotes"] = root_posts["uri"].map(quotes_map).fillna(0).astype(int)

    # -----------------------------------------------------------------------
    # Step 3: Look up author profiles
    # -----------------------------------------------------------------------
    print("Looking up author profiles...")
    author_dids = root_posts["did"].unique().tolist()
    ph2 = ",".join("?" * len(author_dids))
    profiles = pd.read_sql_query(
        f"SELECT did, handle, followers_count, follows_count, posts_count FROM profiles WHERE did IN ({ph2})",
        conn,
        params=author_dids,
    )
    profile_map = profiles.set_index("did").to_dict("index")

    root_posts["author_handle"] = root_posts["did"].map(
        lambda d: profile_map.get(d, {}).get("handle"))
    root_posts["author_followers"] = root_posts["did"].map(
        lambda d: profile_map.get(d, {}).get("followers_count", 0))
    root_posts["author_follows"] = root_posts["did"].map(
        lambda d: profile_map.get(d, {}).get("follows_count", 0))
    root_posts["author_posts_count"] = root_posts["did"].map(
        lambda d: profile_map.get(d, {}).get("posts_count", 0))

    # -----------------------------------------------------------------------
    # Step 4: Apply virality threshold
    # -----------------------------------------------------------------------
    print("Applying virality threshold...")
    root_posts["is_viral"] = root_posts.apply(
        lambda r: is_viral(
            r["total_likes"], r["total_reposts"], r["total_replies"],
            r["total_quotes"], r["author_followers"] or 0,
        ),
        axis=1,
    )

    viral = root_posts[root_posts["is_viral"]]
    non_viral = root_posts[~root_posts["is_viral"]]
    n_viral = len(viral)
    n_sample = min(len(non_viral), n_viral * sample_ratio)
    sampled_non_viral = non_viral.sample(n=n_sample, random_state=42) if n_sample > 0 else non_viral.head(0)

    layer1 = pd.concat([viral, sampled_non_viral], ignore_index=True)
    print(f"  Layer 1: {n_viral} viral + {n_sample} non-viral = {len(layer1)} posts")

    # -----------------------------------------------------------------------
    # Step 5: Export Layer 1
    # -----------------------------------------------------------------------
    print("Exporting Layer 1: root_posts.parquet")
    layer1.to_parquet(out_dir / "root_posts.parquet", index=False)

    # -----------------------------------------------------------------------
    # Step 6: Export Layer 2 — cascade parquets for Layer 1 posts only
    # -----------------------------------------------------------------------
    layer1_uris = layer1["uri"].tolist()
    layer1_time_map = dict(zip(layer1["uri"], layer1["time_us"]))
    ph_l1 = ",".join("?" * len(layer1_uris))

    # -- Reposts --
    print("Exporting Layer 2: reposts.parquet")
    reposts_df = pd.read_sql_query(
        f"""
        SELECT e.subject_uri as root_post_uri, e.did as reposter_did,
               e.time_us as repost_time_us, e.created_at,
               p.followers_count as reposter_followers,
               p.follows_count as reposter_follows
        FROM engagements e
        LEFT JOIN profiles p ON e.did = p.did
        WHERE e.type = 'repost' AND e.subject_uri IN ({ph_l1})
        """,
        conn,
        params=layer1_uris,
    )
    if not reposts_df.empty:
        reposts_df["time_delta_sec"] = reposts_df.apply(
            lambda r: (r["repost_time_us"] - layer1_time_map.get(r["root_post_uri"], 0)) / 1e6,
            axis=1,
        )
    reposts_df.to_parquet(out_dir / "reposts.parquet", index=False)

    # -- Likes --
    print("Exporting Layer 2: likes.parquet")
    likes_df = pd.read_sql_query(
        f"""
        SELECT subject_uri as root_post_uri, did as liker_did,
               time_us as like_time_us, created_at
        FROM engagements
        WHERE type = 'like' AND subject_uri IN ({ph_l1})
        """,
        conn,
        params=layer1_uris,
    )
    if not likes_df.empty:
        likes_df["time_delta_sec"] = likes_df.apply(
            lambda r: (r["like_time_us"] - layer1_time_map.get(r["root_post_uri"], 0)) / 1e6,
            axis=1,
        )
    likes_df.to_parquet(out_dir / "likes.parquet", index=False)

    # -- Replies --
    print("Exporting Layer 2: replies.parquet")
    replies_df = pd.read_sql_query(
        f"""
        SELECT p.reply_root as root_post_uri, p.uri as reply_uri,
               p.did as replier_did, p.time_us as reply_time_us,
               p.text as reply_text, p.has_embed as reply_has_embed,
               p.created_at,
               pr.followers_count as replier_followers,
               pr.follows_count as replier_follows
        FROM posts p
        LEFT JOIN profiles pr ON p.did = pr.did
        WHERE p.reply_root IN ({ph_l1})
        """,
        conn,
        params=layer1_uris,
    )
    if not replies_df.empty:
        replies_df["time_delta_sec"] = replies_df.apply(
            lambda r: (r["reply_time_us"] - layer1_time_map.get(r["root_post_uri"], 0)) / 1e6,
            axis=1,
        )
    replies_df.to_parquet(out_dir / "replies.parquet", index=False)

    # -- Quotes --
    print("Exporting Layer 2: quotes.parquet")
    quotes_df = pd.read_sql_query(
        f"""
        SELECT p.quote_of as root_post_uri, p.uri as quote_uri,
               p.did as quoter_did, p.time_us as quote_time_us,
               p.text as quote_text, p.has_embed as quote_has_embed,
               p.created_at,
               pr.followers_count as quoter_followers,
               pr.follows_count as quoter_follows
        FROM posts p
        LEFT JOIN profiles pr ON p.did = pr.did
        WHERE p.quote_of IN ({ph_l1})
        """,
        conn,
        params=layer1_uris,
    )
    if not quotes_df.empty:
        quotes_df["time_delta_sec"] = quotes_df.apply(
            lambda r: (r["quote_time_us"] - layer1_time_map.get(r["root_post_uri"], 0)) / 1e6,
            axis=1,
        )
    quotes_df.to_parquet(out_dir / "quotes.parquet", index=False)

    print(f"\nExport complete. Files in {out_dir}/")
    print(f"  root_posts.parquet : {len(layer1)} rows")
    print(f"  reposts.parquet    : {len(reposts_df)} rows")
    print(f"  likes.parquet      : {len(likes_df)} rows")
    print(f"  replies.parquet    : {len(replies_df)} rows")
    print(f"  quotes.parquet     : {len(quotes_df)} rows")


def main():
    parser = argparse.ArgumentParser(description="Export SQLite to layered parquets")
    parser.add_argument("--since", type=str, default="2024-01-01",
                        help="Start date (ISO format, default: 2024-01-01)")
    parser.add_argument("--until", type=str, default="2099-12-31",
                        help="End date (ISO format, default: far future)")
    parser.add_argument("--min-age-hours", type=int, default=2,
                        help="Only export posts older than N hours (default: 2)")
    parser.add_argument("--sample-ratio", type=int, default=10,
                        help="Non-viral samples per viral post (default: 10)")
    parser.add_argument("--out-dir", type=str, default="./export",
                        help="Output directory (default: ./export)")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to SQLite database")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn = db.get_db(db_path)

    since_us = ts_to_time_us(args.since)
    until_us = ts_to_time_us(args.until)
    now_us = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    min_age_us = now_us - (args.min_age_hours * 3600 * 1_000_000)

    out_dir = Path(args.out_dir)

    export_data(conn, since_us, until_us, min_age_us, args.sample_ratio, out_dir)
    conn.close()


if __name__ == "__main__":
    main()
