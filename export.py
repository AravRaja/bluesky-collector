"""Export SQLite data to layered parquet files.

Layer 1: root_posts.parquet — all viral posts + sampled non-viral
Layer 2: reposts.parquet, replies.parquet, quotes.parquet, likes.parquet
         (cascade events within --cascade-window-min of post creation)

Usage:
    python export.py [--since 2024-04-01] [--until 2024-04-15]
                     [--min-age-hours 2] [--sample-ratio 10]
                     [--cascade-window-min 30]
                     [--out-dir ./export] [--db bluesky.db]
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import db
from virality_threshold import is_viral

# Columns to drop from Layer 1 — Bluesky internals, not useful for ML
_DROP_COLS = ["labels", "cid", "reply_parent", "reply_root", "quote_of", "rkey"]


def ts_to_time_us(ts_str):
    """Convert ISO date string to unix microseconds."""
    dt = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def _add_time_delta(df, time_col, time_map, uri_col="root_post_uri"):
    """Add time_delta_sec column — seconds after post creation."""
    if df.empty:
        df["time_delta_sec"] = []
        return df
    df = df.copy()
    df["time_delta_sec"] = df.apply(
        lambda r: (r[time_col] - time_map.get(r[uri_col], 0)) / 1e6,
        axis=1,
    )
    return df


def export_data(conn, since_us, until_us, min_age_us, sample_ratio,
                cascade_window_min, out_dir):
    """Run the full layered export."""
    out_dir.mkdir(parents=True, exist_ok=True)
    window_sec = cascade_window_min * 60

    # -------------------------------------------------------------------
    # Step 1: Root posts in date range
    # -------------------------------------------------------------------
    print("Querying root posts...")
    root_posts = pd.read_sql_query(
        """
        SELECT uri, did, time_us, created_at, text, langs,
               has_embed, embed_type
        FROM posts
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

    # -------------------------------------------------------------------
    # Step 2: Engagement counts
    # -------------------------------------------------------------------
    print("Counting engagements...")
    uris = root_posts["uri"].tolist()
    ph = ",".join("?" * len(uris))

    eng_counts = pd.read_sql_query(
        f"SELECT subject_uri, type, COUNT(*) as cnt FROM engagements "
        f"WHERE subject_uri IN ({ph}) GROUP BY subject_uri, type",
        conn, params=uris,
    )
    likes_map = {}
    reposts_map = {}
    for _, row in eng_counts.iterrows():
        if row["type"] == "like":
            likes_map[row["subject_uri"]] = row["cnt"]
        elif row["type"] == "repost":
            reposts_map[row["subject_uri"]] = row["cnt"]

    reply_counts = pd.read_sql_query(
        f"SELECT reply_parent as uri, COUNT(*) as cnt FROM posts "
        f"WHERE reply_parent IN ({ph}) GROUP BY reply_parent",
        conn, params=uris,
    )
    replies_map = dict(zip(reply_counts["uri"], reply_counts["cnt"]))

    quote_counts = pd.read_sql_query(
        f"SELECT quote_of as uri, COUNT(*) as cnt FROM posts "
        f"WHERE quote_of IN ({ph}) GROUP BY quote_of",
        conn, params=uris,
    )
    quotes_map = dict(zip(quote_counts["uri"], quote_counts["cnt"]))

    root_posts["total_likes"]   = root_posts["uri"].map(likes_map).fillna(0).astype(int)
    root_posts["total_reposts"] = root_posts["uri"].map(reposts_map).fillna(0).astype(int)
    root_posts["total_replies"] = root_posts["uri"].map(replies_map).fillna(0).astype(int)
    root_posts["total_quotes"]  = root_posts["uri"].map(quotes_map).fillna(0).astype(int)

    # -------------------------------------------------------------------
    # Step 3: Author profiles
    # -------------------------------------------------------------------
    print("Looking up author profiles...")
    author_dids = root_posts["did"].unique().tolist()
    ph2 = ",".join("?" * len(author_dids))
    profiles = pd.read_sql_query(
        f"SELECT did, handle, followers_count, follows_count, posts_count "
        f"FROM profiles WHERE did IN ({ph2})",
        conn, params=author_dids,
    )
    pm = profiles.set_index("did").to_dict("index")
    root_posts["author_handle"]       = root_posts["did"].map(lambda d: pm.get(d, {}).get("handle"))
    root_posts["author_followers"]     = root_posts["did"].map(lambda d: pm.get(d, {}).get("followers_count", 0))
    root_posts["author_follows"]       = root_posts["did"].map(lambda d: pm.get(d, {}).get("follows_count", 0))
    root_posts["author_posts_count"]   = root_posts["did"].map(lambda d: pm.get(d, {}).get("posts_count", 0))

    # -------------------------------------------------------------------
    # Step 4: Virality label + sampling
    # -------------------------------------------------------------------
    print("Applying virality threshold...")
    root_posts["is_viral"] = root_posts.apply(
        lambda r: is_viral(
            r["total_likes"], r["total_reposts"], r["total_replies"],
            r["total_quotes"], r["author_followers"] or 0,
        ),
        axis=1,
    )

    viral     = root_posts[root_posts["is_viral"]]
    non_viral = root_posts[~root_posts["is_viral"]]
    n_sample  = min(len(non_viral), len(viral) * sample_ratio)
    sampled   = non_viral.sample(n=n_sample, random_state=42) if n_sample > 0 else non_viral.head(0)
    layer1    = pd.concat([viral, sampled], ignore_index=True)
    print(f"  Layer 1: {len(viral)} viral + {n_sample} non-viral = {len(layer1)} posts")

    # -------------------------------------------------------------------
    # Step 5: Export Layer 1
    # -------------------------------------------------------------------
    print("Exporting Layer 1: root_posts.parquet")
    layer1.to_parquet(out_dir / "root_posts.parquet", index=False)

    # -------------------------------------------------------------------
    # Step 6: Export Layer 2 — cascade within window
    # -------------------------------------------------------------------
    l1_uris    = layer1["uri"].tolist()
    l1_time    = dict(zip(layer1["uri"], layer1["time_us"]))
    ph_l1      = ",".join("?" * len(l1_uris))
    window_us  = cascade_window_min * 60 * 1_000_000

    # Reposts
    print(f"Exporting Layer 2: reposts.parquet  (first {cascade_window_min} min)")
    reposts_df = pd.read_sql_query(
        f"""SELECT e.subject_uri as root_post_uri, e.did as reposter_did,
                   e.time_us as repost_time_us, e.created_at,
                   p.followers_count as reposter_followers,
                   p.follows_count as reposter_follows
            FROM engagements e
            LEFT JOIN profiles p ON e.did = p.did
            WHERE e.type='repost' AND e.subject_uri IN ({ph_l1})""",
        conn, params=l1_uris,
    )
    reposts_df = _add_time_delta(reposts_df, "repost_time_us", l1_time)
    reposts_df = reposts_df[reposts_df["time_delta_sec"].between(0, window_sec)]
    reposts_df.to_parquet(out_dir / "reposts.parquet", index=False)

    # Likes
    print(f"Exporting Layer 2: likes.parquet    (first {cascade_window_min} min)")
    likes_df = pd.read_sql_query(
        f"""SELECT subject_uri as root_post_uri, did as liker_did,
                   time_us as like_time_us, created_at
            FROM engagements
            WHERE type='like' AND subject_uri IN ({ph_l1})""",
        conn, params=l1_uris,
    )
    likes_df = _add_time_delta(likes_df, "like_time_us", l1_time)
    likes_df = likes_df[likes_df["time_delta_sec"].between(0, window_sec)]
    likes_df.to_parquet(out_dir / "likes.parquet", index=False)

    # Replies
    print(f"Exporting Layer 2: replies.parquet  (first {cascade_window_min} min)")
    replies_df = pd.read_sql_query(
        f"""SELECT p.reply_root as root_post_uri, p.uri as reply_uri,
                   p.did as replier_did, p.time_us as reply_time_us,
                   p.text as reply_text, p.has_embed as reply_has_embed,
                   p.created_at,
                   pr.followers_count as replier_followers,
                   pr.follows_count as replier_follows
            FROM posts p
            LEFT JOIN profiles pr ON p.did = pr.did
            WHERE p.reply_root IN ({ph_l1})""",
        conn, params=l1_uris,
    )
    replies_df = _add_time_delta(replies_df, "reply_time_us", l1_time)
    replies_df = replies_df[replies_df["time_delta_sec"].between(0, window_sec)]
    replies_df.to_parquet(out_dir / "replies.parquet", index=False)

    # Quotes
    print(f"Exporting Layer 2: quotes.parquet   (first {cascade_window_min} min)")
    quotes_df = pd.read_sql_query(
        f"""SELECT p.quote_of as root_post_uri, p.uri as quote_uri,
                   p.did as quoter_did, p.time_us as quote_time_us,
                   p.text as quote_text, p.has_embed as quote_has_embed,
                   p.created_at,
                   pr.followers_count as quoter_followers,
                   pr.follows_count as quoter_follows
            FROM posts p
            LEFT JOIN profiles pr ON p.did = pr.did
            WHERE p.quote_of IN ({ph_l1})""",
        conn, params=l1_uris,
    )
    quotes_df = _add_time_delta(quotes_df, "quote_time_us", l1_time)
    quotes_df = quotes_df[quotes_df["time_delta_sec"].between(0, window_sec)]
    quotes_df.to_parquet(out_dir / "quotes.parquet", index=False)

    print(f"\nExport complete → {out_dir}/")
    print(f"  root_posts.parquet : {len(layer1):>6} rows")
    print(f"  reposts.parquet    : {len(reposts_df):>6} rows  (≤{cascade_window_min} min)")
    print(f"  likes.parquet      : {len(likes_df):>6} rows  (≤{cascade_window_min} min)")
    print(f"  replies.parquet    : {len(replies_df):>6} rows  (≤{cascade_window_min} min)")
    print(f"  quotes.parquet     : {len(quotes_df):>6} rows  (≤{cascade_window_min} min)")


def main():
    parser = argparse.ArgumentParser(description="Export SQLite to layered parquets")
    parser.add_argument("--since",               type=str, default="2024-01-01")
    parser.add_argument("--until",               type=str, default="2099-12-31")
    parser.add_argument("--min-age-hours",        type=int, default=2)
    parser.add_argument("--sample-ratio",         type=int, default=10)
    parser.add_argument("--cascade-window-min",   type=int, default=30,
                        help="Only export cascade events within this many minutes of post creation")
    parser.add_argument("--out-dir",              type=str, default="./export")
    parser.add_argument("--db",                   type=str, default=None)
    args = parser.parse_args()

    db_path   = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn      = db.get_db(db_path)
    since_us  = ts_to_time_us(args.since)
    until_us  = ts_to_time_us(args.until)
    now_us    = int(datetime.now(timezone.utc).timestamp() * 1_000_000)
    min_age_us = now_us - (args.min_age_hours * 3600 * 1_000_000)

    export_data(conn, since_us, until_us, min_age_us,
                args.sample_ratio, args.cascade_window_min, Path(args.out_dir))
    conn.close()


if __name__ == "__main__":
    main()
