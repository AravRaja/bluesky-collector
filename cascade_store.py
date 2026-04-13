"""
cascade_store.py — Load and query the exported Bluesky cascade parquet dataset.

Quick start:
    from cascade_store import CascadeStore

    cs = CascadeStore("./export")

    # Dataset summary
    print(cs.summary())

    # All viral posts
    viral = cs.viral_posts()

    # All cascade events for one post (within 30 min)
    cascade = cs.get_cascade(uri, window_min=30)
    cascade["reposts"]   # DataFrame — who reposted, when
    cascade["likes"]     # DataFrame — who liked, when
    cascade["replies"]   # DataFrame — reply text, author, when
    cascade["quotes"]    # DataFrame — quote text, author, when

    # Per-minute bucketed counts — ready for time-series ML features
    features = cs.time_features(uri, window_min=30)
    #    minute  reposts  likes  replies  quotes
    # 0       0        0      3        0       0
    # 1       1        2     12        1       0
    # ...

    # Batch: feature matrix for every post (one row per post)
    matrix = cs.feature_matrix(window_min=30)
"""

from pathlib import Path

import pandas as pd


class CascadeStore:
    """Wraps the 5 exported parquet files and provides query helpers."""

    def __init__(self, export_dir: str | Path):
        d = Path(export_dir)
        self.posts    = pd.read_parquet(d / "root_posts.parquet")
        self._reposts = pd.read_parquet(d / "reposts.parquet")
        self._likes   = pd.read_parquet(d / "likes.parquet")
        self._replies = pd.read_parquet(d / "replies.parquet")
        self._quotes  = pd.read_parquet(d / "quotes.parquet")

        # Index each cascade table by post URI for O(1) lookup
        self._ri = self._reposts.groupby("root_post_uri")
        self._li = self._likes.groupby("root_post_uri")
        self._rpi = self._replies.groupby("root_post_uri")
        self._qi = self._quotes.groupby("root_post_uri")

    # ------------------------------------------------------------------
    # Basic accessors
    # ------------------------------------------------------------------

    def viral_posts(self) -> pd.DataFrame:
        """Return only viral posts (is_viral == True)."""
        return self.posts[self.posts["is_viral"]].copy()

    def non_viral_posts(self) -> pd.DataFrame:
        """Return only non-viral posts."""
        return self.posts[~self.posts["is_viral"]].copy()

    def get_post(self, uri: str) -> pd.Series | None:
        """Return the root post row for a given URI, or None."""
        row = self.posts[self.posts["uri"] == uri]
        return row.iloc[0] if not row.empty else None

    # ------------------------------------------------------------------
    # Cascade lookup
    # ------------------------------------------------------------------

    def get_cascade(self, uri: str, window_min: int = 30) -> dict[str, pd.DataFrame]:
        """All cascade events for one post within window_min minutes.

        Returns:
            {
              "reposts": DataFrame,
              "likes":   DataFrame,
              "replies": DataFrame,
              "quotes":  DataFrame,
            }
        Each DataFrame is sorted by time_delta_sec ascending.
        window_min is applied on top of whatever window was used at export time.
        """
        window_sec = window_min * 60

        def _fetch(idx):
            try:
                df = idx.get_group(uri)
                return df[df["time_delta_sec"] <= window_sec].sort_values("time_delta_sec").copy()
            except KeyError:
                return pd.DataFrame()

        return {
            "reposts": _fetch(self._ri),
            "likes":   _fetch(self._li),
            "replies": _fetch(self._rpi),
            "quotes":  _fetch(self._qi),
        }

    # ------------------------------------------------------------------
    # Time-series features
    # ------------------------------------------------------------------

    def time_features(self, uri: str, window_min: int = 30,
                      bucket_min: int = 1) -> pd.DataFrame:
        """Per-minute NEW event counts for a single post.

        Returns a DataFrame with columns:
            minute, reposts, likes, replies, quotes, total

        Each row is how many NEW events arrived in that 1-minute bucket.
        Useful as a time-series input to an ML model.
        """
        cascade = self.get_cascade(uri, window_min)
        buckets = list(range(1, window_min + 1))

        def _bucket(df, time_col="time_delta_sec"):
            if df.empty:
                return [0] * len(buckets)
            counts = []
            for b in buckets:
                lo = (b - bucket_min) * 60
                hi = b * 60
                counts.append(int(((df[time_col] > lo) & (df[time_col] <= hi)).sum()))
            return counts

        r = _bucket(cascade["reposts"])
        l = _bucket(cascade["likes"])
        rp = _bucket(cascade["replies"])
        q = _bucket(cascade["quotes"])

        df = pd.DataFrame({
            "minute":  buckets,
            "reposts": r,
            "likes":   l,
            "replies": rp,
            "quotes":  q,
        })
        df["total"] = df["reposts"] + df["likes"] + df["replies"] + df["quotes"]
        return df

    def feature_matrix(self, window_min: int = 30,
                       bucket_min: int = 1) -> pd.DataFrame:
        """Build a flat feature matrix for every post.

        Each post becomes one row with:
          - uri, is_viral
          - total_reposts, total_likes, total_replies, total_quotes (all-time from Layer 1)
          - author_followers
          - repost_m1 … repost_m30, like_m1 … like_m30, reply_m1 … reply_m30  (per-minute counts)
          - time_to_first_repost_sec, time_to_first_like_sec  (velocity signals)

        This is the most ML-ready format — one row per post, all features flat.
        """
        rows = []
        for _, post in self.posts.iterrows():
            uri = post["uri"]
            tf = self.time_features(uri, window_min, bucket_min)

            def _first(idx, time_col):
                try:
                    df = idx.get_group(uri)
                    df = df[df["time_delta_sec"] >= 0]
                    return float(df["time_delta_sec"].min()) if not df.empty else None
                except KeyError:
                    return None

            row = {
                "uri":          uri,
                "is_viral":     post["is_viral"],
                "text_len":     len(post.get("text") or ""),
                "has_embed":    int(post.get("has_embed") or 0),
                "author_followers": post.get("author_followers") or 0,
                "total_reposts": post.get("total_reposts") or 0,
                "total_likes":   post.get("total_likes") or 0,
                "total_replies": post.get("total_replies") or 0,
                "total_quotes":  post.get("total_quotes") or 0,
                "time_to_first_repost_sec": _first(self._ri, "repost_time_us"),
                "time_to_first_like_sec":   _first(self._li, "like_time_us"),
            }

            for _, r in tf.iterrows():
                m = int(r["minute"])
                row[f"repost_m{m}"] = r["reposts"]
                row[f"like_m{m}"]   = r["likes"]
                row[f"reply_m{m}"]  = r["replies"]
                row[f"quote_m{m}"]  = r["quotes"]

            rows.append(row)

        return pd.DataFrame(rows)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self) -> dict:
        """Quick overview of the dataset."""
        viral_n = int(self.posts["is_viral"].sum())
        return {
            "total_posts":     len(self.posts),
            "viral_posts":     viral_n,
            "non_viral_posts": len(self.posts) - viral_n,
            "total_reposts":   len(self._reposts),
            "total_likes":     len(self._likes),
            "total_replies":   len(self._replies),
            "total_quotes":    len(self._quotes),
        }
