from pathlib import Path
import pandas as pd


class CascadeStore:

    def __init__(self, export_dir):
        d = Path(export_dir)
        self.posts = pd.read_parquet(d / "root_posts.parquet")
        self._reposts = pd.read_parquet(d / "reposts.parquet")
        self._likes = pd.read_parquet(d / "likes.parquet")
        self._replies = pd.read_parquet(d / "replies.parquet")
        self._quotes = pd.read_parquet(d / "quotes.parquet")

        self._ri = self._reposts.groupby("root_post_uri")
        self._li = self._likes.groupby("root_post_uri")
        self._rpi = self._replies.groupby("root_post_uri")
        self._qi = self._quotes.groupby("root_post_uri")

    def viral_posts(self):
        return self.posts[self.posts["is_viral"]].copy()

    def non_viral_posts(self):
        return self.posts[~self.posts["is_viral"]].copy()

    def get_post(self, uri):
        row = self.posts[self.posts["uri"] == uri]
        return row.iloc[0] if not row.empty else None

    # all reposts/likes/replies/quotes for a post within a time window
    def get_cascade(self, uri, window_min=30):
        sec = window_min * 60

        def grab(idx):
            try:
                df = idx.get_group(uri)
                return df[df["time_delta_sec"] <= sec].sort_values("time_delta_sec").copy()
            except KeyError:
                return pd.DataFrame()

        return {
            "reposts": grab(self._ri),
            "likes": grab(self._li),
            "replies": grab(self._rpi),
            "quotes": grab(self._qi),
        }

    # per-minute new event counts for one post
    def time_features(self, uri, window_min=30, bucket_min=1):
        cascade = self.get_cascade(uri, window_min)
        buckets = list(range(1, window_min + 1))

        def count(df):
            if df.empty:
                return [0] * len(buckets)
            t = df["time_delta_sec"]
            return [int(((t > (b - bucket_min) * 60) & (t <= b * 60)).sum()) for b in buckets]

        df = pd.DataFrame({
            "minute": buckets,
            "reposts": count(cascade["reposts"]),
            "likes": count(cascade["likes"]),
            "replies": count(cascade["replies"]),
            "quotes": count(cascade["quotes"]),
        })
        df["total"] = df[["reposts", "likes", "replies", "quotes"]].sum(axis=1)
        return df

    # one row per post with all raw signals expanded (pick and choose what u guys acc wanna use)
    def build_base_table(self, window_min=30, bucket_min=1):
        rows = []
        for _, post in self.posts.iterrows():
            uri = post["uri"]
            tf = self.time_features(uri, window_min, bucket_min)

            def first_event(idx):
                try:
                    df = idx.get_group(uri)
                    df = df[df["time_delta_sec"] >= 0]
                    return float(df["time_delta_sec"].min()) if not df.empty else None
                except KeyError:
                    return None

            row = {
                "uri": uri,
                "is_viral": post["is_viral"],
                "text_len": len(post.get("text") or ""),
                "has_embed": int(post.get("has_embed") or 0),
                "author_followers": post.get("author_followers") or 0,
                "total_reposts": post.get("total_reposts") or 0,
                "total_likes": post.get("total_likes") or 0,
                "total_replies": post.get("total_replies") or 0,
                "total_quotes": post.get("total_quotes") or 0,
                "time_to_first_repost_sec": first_event(self._ri),
                "time_to_first_like_sec": first_event(self._li),
            }
            for _, r in tf.iterrows():
                m = int(r["minute"])
                row[f"repost_m{m}"] = r["reposts"]
                row[f"like_m{m}"] = r["likes"]
                row[f"reply_m{m}"] = r["replies"]
                row[f"quote_m{m}"] = r["quotes"]

            rows.append(row)

        return pd.DataFrame(rows)
   #run this to know what kind of data you are dealing with
    def summary(self):
        v = int(self.posts["is_viral"].sum())
        return {
            "total_posts": len(self.posts),
            "viral": v,
            "non_viral": len(self.posts) - v,
            "reposts": len(self._reposts),
            "likes": len(self._likes),
            "replies": len(self._replies),
            "quotes": len(self._quotes),
        }
