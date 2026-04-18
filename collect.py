"""Bluesky Jetstream WebSocket collector.

Connects to the Jetstream firehose, parses post/like/repost/follow events,
and batch-inserts them into SQLite. Supports cursor-based reconnection
and graceful shutdown via SIGINT/SIGTERM.

Usage:
    python collect.py
"""

import asyncio
import json
import signal
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlencode

import websockets

import db
import stats_writer

JETSTREAM_URL = "wss://jetstream2.us-east.bsky.network/subscribe"
WANTED_COLLECTIONS = [
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
]
DB_PATH = Path(__file__).parent / "bluesky.db"
CURSOR_FILE = Path(__file__).parent / ".cursor"
BATCH_SIZE = 500
BATCH_INTERVAL_SEC = 5
MAX_RECONNECT_DELAY_SEC = 60
STATS_INTERVAL_SEC = 60
STATS_WRITE_INTERVAL_SEC = 60
WAL_CHECKPOINT_INTERVAL_SEC = 300  # 5 minutes


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------

def build_ws_url(cursor=None):
    """Build Jetstream WebSocket URL with query params."""
    params = [("wantedCollections", c) for c in WANTED_COLLECTIONS]
    if cursor is not None:
        params.append(("cursor", cursor))
    return f"{JETSTREAM_URL}?{urlencode(params)}"


# ---------------------------------------------------------------------------
# URI helper
# ---------------------------------------------------------------------------

def make_uri(did, collection, rkey):
    """Construct an at:// URI."""
    return f"at://{did}/{collection}/{rkey}"


# ---------------------------------------------------------------------------
# Message parsers — each returns a dict or None (for deletes / irrelevant)
# ---------------------------------------------------------------------------

def parse_post(msg):
    """Parse a post commit event. Keeps all post types (root, reply, quote)."""
    commit = msg["commit"]
    if commit["operation"] == "delete":
        return None

    record = commit.get("record", {})
    did = msg["did"]
    rkey = commit["rkey"]
    uri = make_uri(did, "app.bsky.feed.post", rkey)

    # Reply detection
    reply = record.get("reply")
    reply_parent = None
    reply_root = None
    if reply and isinstance(reply, dict):
        parent = reply.get("parent")
        root = reply.get("root")
        reply_parent = parent.get("uri") if isinstance(parent, dict) else None
        reply_root = root.get("uri") if isinstance(root, dict) else None

    # Quote detection — embed with record ref
    quote_of = None
    embed = record.get("embed")
    if embed:
        embed_type = embed.get("$type", "")
        if "record" in embed_type:
            # app.bsky.embed.record or app.bsky.embed.recordWithMedia
            rec_ref = embed.get("record", {})
            quote_of = rec_ref.get("uri")

    # Embed type for the post
    has_embed = embed is not None
    post_embed_type = embed.get("$type") if embed else None

    # Labels
    labels_obj = record.get("labels")
    labels_list = []
    if labels_obj and isinstance(labels_obj, dict):
        for val in labels_obj.get("values", []):
            if isinstance(val, dict) and "val" in val:
                labels_list.append(val["val"])

    # Langs
    langs = record.get("langs")
    langs_str = json.dumps(langs) if langs else None

    labels_str = json.dumps(labels_list) if labels_list else None

    return {
        "uri": uri,
        "did": did,
        "rkey": rkey,
        "time_us": msg["time_us"],
        "created_at": record.get("createdAt"),
        "text": record.get("text"),
        "langs": langs_str,
        "has_embed": 1 if has_embed else 0,
        "embed_type": post_embed_type,
        "labels": labels_str,
        "cid": commit.get("cid"),
        "reply_parent": reply_parent,
        "reply_root": reply_root,
        "quote_of": quote_of,
    }


def parse_like(msg):
    """Parse a like commit event."""
    commit = msg["commit"]
    if commit["operation"] == "delete":
        return None
    record = commit.get("record", {})
    subject = record.get("subject", {})
    return {
        "did": msg["did"],
        "type": "like",
        "subject_uri": subject.get("uri"),
        "time_us": msg["time_us"],
        "created_at": record.get("createdAt"),
    }


def parse_repost(msg):
    """Parse a repost commit event."""
    commit = msg["commit"]
    if commit["operation"] == "delete":
        return None
    record = commit.get("record", {})
    subject = record.get("subject", {})
    return {
        "did": msg["did"],
        "type": "repost",
        "subject_uri": subject.get("uri"),
        "time_us": msg["time_us"],
        "created_at": record.get("createdAt"),
    }


def parse_follow(msg):
    """Parse a follow commit event."""
    commit = msg["commit"]
    if commit["operation"] == "delete":
        return None
    record = commit.get("record", {})
    return {
        "did": msg["did"],
        "subject_did": record.get("subject"),
        "time_us": msg["time_us"],
        "created_at": record.get("createdAt"),
    }


# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------

def save_cursor(time_us):
    """Atomically save cursor to disk."""
    tmp = CURSOR_FILE.with_suffix(".tmp")
    tmp.write_text(str(time_us))
    tmp.rename(CURSOR_FILE)


def load_cursor():
    """Load cursor from disk, or None if not found."""
    if CURSOR_FILE.exists():
        text = CURSOR_FILE.read_text().strip()
        if text:
            return int(text)
    return None


# ---------------------------------------------------------------------------
# Buffer flushing
# ---------------------------------------------------------------------------

def flush_buffers(conn, buffers, counters):
    """Write all buffers to SQLite and update counters."""
    if buffers["posts"]:
        db.insert_posts(conn, buffers["posts"])
        counters["posts"] += len(buffers["posts"])
    if buffers["engagements"]:
        db.insert_engagements(conn, buffers["engagements"])
        for e in buffers["engagements"]:
            counters[e["type"] + "s"] += 1
    if buffers["follows"]:
        db.insert_follows(conn, buffers["follows"])
        counters["follows"] += len(buffers["follows"])

    total_flushed = len(buffers["posts"]) + len(buffers["engagements"]) + len(buffers["follows"])
    buffers["posts"].clear()
    buffers["engagements"].clear()
    buffers["follows"].clear()
    return total_flushed


# ---------------------------------------------------------------------------
# Stats recorder
# ---------------------------------------------------------------------------

async def stats_recorder(conn, counters, stop_event):
    """Record stats snapshots to the DB every STATS_INTERVAL_SEC."""
    prev = {k: v for k, v in counters.items()}
    while not stop_event.is_set():
        try:
            await asyncio.sleep(STATS_INTERVAL_SEC)
        except asyncio.CancelledError:
            break
        try:
            delta_posts = counters["posts"] - prev["posts"]
            delta_likes = counters["likes"] - prev["likes"]
            delta_reposts = counters["reposts"] - prev["reposts"]
            delta_follows = counters["follows"] - prev["follows"]
            db.record_stats(conn, delta_posts, delta_likes, delta_reposts, delta_follows)
            prev = {k: v for k, v in counters.items()}
        except Exception as e:
            print(f"[stats_recorder] error: {e}")


async def dashboard_stats_writer(db_path, stop_event):
    """Write dashboard JSON stats file periodically."""
    await asyncio.sleep(10)  # let collector get going first
    while not stop_event.is_set():
        try:
            stats_writer.write_stats(db_path)
        except Exception as e:
            print(f"[stats_writer] error: {e}")
        try:
            await asyncio.sleep(STATS_WRITE_INTERVAL_SEC)
        except asyncio.CancelledError:
            break


async def wal_checkpointer(db_path, stop_event):
    """Periodically force a WAL checkpoint so the WAL stays small."""
    while not stop_event.is_set():
        try:
            await asyncio.sleep(WAL_CHECKPOINT_INTERVAL_SEC)
        except asyncio.CancelledError:
            break
        try:
            wal_conn = sqlite3.connect(str(db_path))
            wal_conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
            wal_conn.close()
        except Exception as e:
            print(f"[wal_checkpoint] error: {e}")


# ---------------------------------------------------------------------------
# Main consumer
# ---------------------------------------------------------------------------

async def consume_stream():
    """Connect to Jetstream, parse events, batch insert into SQLite."""
    conn = db.get_db(DB_PATH)
    last_time_us = load_cursor()
    reconnect_delay = 1
    start_time = time.time()

    buffers = {"posts": [], "engagements": [], "follows": []}
    counters = {"posts": 0, "likes": 0, "reposts": 0, "follows": 0}
    stop_event = asyncio.Event()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    stats_task = asyncio.create_task(stats_recorder(conn, counters, stop_event))
    dash_task = asyncio.create_task(dashboard_stats_writer(DB_PATH, stop_event))
    wal_task = asyncio.create_task(wal_checkpointer(DB_PATH, stop_event))

    PARSERS = {
        "app.bsky.feed.post": ("posts", parse_post),
        "app.bsky.feed.like": ("engagements", parse_like),
        "app.bsky.feed.repost": ("engagements", parse_repost),
        "app.bsky.graph.follow": ("follows", parse_follow),
    }

    last_flush_time = time.time()
    total_events = 0

    while not stop_event.is_set():
        url = build_ws_url(last_time_us)
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=10,
                                          close_timeout=5) as ws:
                reconnect_delay = 1
                print(f"Connected to Jetstream (cursor={last_time_us})")

                async for raw_msg in ws:
                    if stop_event.is_set():
                        break

                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    if msg.get("kind") != "commit":
                        continue

                    commit = msg.get("commit", {})
                    collection = commit.get("collection")
                    last_time_us = msg.get("time_us", last_time_us)

                    entry = PARSERS.get(collection)
                    if entry is None:
                        continue

                    buf_key, parser = entry
                    parsed = parser(msg)
                    if parsed is not None:
                        buffers[buf_key].append(parsed)
                        total_events += 1

                    # Flush on batch size or interval
                    buf_len = len(buffers["posts"]) + len(buffers["engagements"]) + len(buffers["follows"])
                    now = time.time()
                    if buf_len >= BATCH_SIZE or (now - last_flush_time) >= BATCH_INTERVAL_SEC:
                        try:
                            flushed = flush_buffers(conn, buffers, counters)
                        except Exception as e:
                            print(f"[flush] error: {e}, retrying next cycle")
                            continue
                        if last_time_us is not None:
                            save_cursor(last_time_us)
                        elapsed = now - start_time
                        rate = total_events / elapsed if elapsed > 0 else 0
                        print(
                            f"[{time.strftime('%H:%M:%S')}] "
                            f"Flushed {flushed} | "
                            f"Total: {counters['posts']}p {counters['likes']}l "
                            f"{counters['reposts']}r {counters['follows']}f | "
                            f"{rate:.0f} evt/s"
                        )
                        last_flush_time = now

        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            if stop_event.is_set():
                break
            print(f"Disconnected ({e}), reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY_SEC)

    # Graceful shutdown
    for task in (stats_task, dash_task, wal_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    flush_buffers(conn, buffers, counters)
    if last_time_us is not None:
        save_cursor(last_time_us)

    elapsed = time.time() - start_time
    print(
        f"\nShutdown complete. Cursor saved.\n"
        f"Total: {counters['posts']}p {counters['likes']}l "
        f"{counters['reposts']}r {counters['follows']}f "
        f"in {elapsed / 3600:.1f}h"
    )
    conn.close()


def main():
    asyncio.run(consume_stream())


if __name__ == "__main__":
    main()
