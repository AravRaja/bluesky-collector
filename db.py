"""Schema and DB helpers for the Bluesky collector."""

import sqlite3
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).parent / "bluesky.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS posts (
    uri            TEXT PRIMARY KEY,
    did            TEXT NOT NULL,
    rkey           TEXT NOT NULL,
    time_us        INTEGER NOT NULL,
    created_at     TEXT,
    text           TEXT,
    langs          TEXT,
    has_embed      INTEGER,
    embed_type     TEXT,
    labels         TEXT,
    cid            TEXT,
    reply_parent   TEXT,
    reply_root     TEXT,
    quote_of       TEXT
);

CREATE TABLE IF NOT EXISTS engagements (
    did            TEXT NOT NULL,
    type           TEXT NOT NULL,
    subject_uri    TEXT NOT NULL,
    time_us        INTEGER NOT NULL,
    created_at     TEXT,
    PRIMARY KEY (type, did, subject_uri)
);

CREATE TABLE IF NOT EXISTS follows (
    did            TEXT NOT NULL,
    subject_did    TEXT NOT NULL,
    time_us        INTEGER NOT NULL,
    created_at     TEXT,
    PRIMARY KEY (did, subject_did)
);

CREATE TABLE IF NOT EXISTS profiles (
    did              TEXT PRIMARY KEY,
    handle           TEXT,
    followers_count  INTEGER,
    follows_count    INTEGER,
    posts_count      INTEGER,
    fetched_at       TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now'))
);

CREATE TABLE IF NOT EXISTS collector_stats (
    ts             TEXT PRIMARY KEY DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now')),
    posts          INTEGER,
    likes          INTEGER,
    reposts        INTEGER,
    follows        INTEGER
);

CREATE INDEX IF NOT EXISTS idx_eng_subject ON engagements(subject_uri);
CREATE INDEX IF NOT EXISTS idx_eng_time    ON engagements(time_us);
CREATE INDEX IF NOT EXISTS idx_eng_type_subject ON engagements(type, subject_uri);
CREATE INDEX IF NOT EXISTS idx_posts_time  ON posts(time_us);
CREATE INDEX IF NOT EXISTS idx_posts_reply ON posts(reply_root);
CREATE INDEX IF NOT EXISTS idx_posts_reply_parent ON posts(reply_parent) WHERE reply_parent IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_posts_quote ON posts(quote_of) WHERE quote_of IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_posts_root ON posts(uri) WHERE reply_parent IS NULL AND quote_of IS NULL;
"""


def get_db(path=None):
    """Open a connection, set WAL + pragmas, create tables."""
    path = path or DEFAULT_DB_PATH
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA journal_size_limit=67108864")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")  # checkpoint every 1000 pages (~4MB)
    conn.executescript(SCHEMA_SQL)
    return conn


def insert_posts(conn, rows):
    """Batch insert posts. Skips duplicates via INSERT OR IGNORE."""
    if not rows:
        return
    conn.executemany(
        """INSERT OR IGNORE INTO posts
           (uri, did, rkey, time_us, created_at, text, langs,
            has_embed, embed_type, labels, cid, reply_parent, reply_root, quote_of)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                r["uri"], r["did"], r["rkey"], r["time_us"], r["created_at"],
                r["text"], r["langs"], r["has_embed"], r["embed_type"],
                r["labels"], r["cid"], r["reply_parent"], r["reply_root"],
                r["quote_of"],
            )
            for r in rows
        ],
    )
    conn.commit()


def insert_engagements(conn, rows):
    """Batch insert engagements. Skips duplicates."""
    if not rows:
        return
    conn.executemany(
        """INSERT OR IGNORE INTO engagements
           (did, type, subject_uri, time_us, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        [(r["did"], r["type"], r["subject_uri"], r["time_us"], r["created_at"]) for r in rows],
    )
    conn.commit()


def insert_follows(conn, rows):
    """Batch insert follows. Skips duplicates."""
    if not rows:
        return
    conn.executemany(
        """INSERT OR IGNORE INTO follows
           (did, subject_did, time_us, created_at)
           VALUES (?, ?, ?, ?)""",
        [(r["did"], r["subject_did"], r["time_us"], r["created_at"]) for r in rows],
    )
    conn.commit()


def upsert_profiles(conn, rows):
    """Insert or replace profile cache entries."""
    if not rows:
        return
    conn.executemany(
        """INSERT OR REPLACE INTO profiles
           (did, handle, followers_count, follows_count, posts_count, fetched_at)
           VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%S','now'))""",
        [
            (r["did"], r["handle"], r["followers_count"], r["follows_count"], r["posts_count"])
            for r in rows
        ],
    )
    conn.commit()


def record_stats(conn, posts, likes, reposts, follows):
    """Record a stats snapshot."""
    conn.execute(
        """INSERT INTO collector_stats (posts, likes, reposts, follows)
           VALUES (?, ?, ?, ?)""",
        (posts, likes, reposts, follows),
    )
    conn.commit()
