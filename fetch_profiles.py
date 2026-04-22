"""Fetch Bluesky user profiles and cache them in the profiles table.

Uses app.bsky.actor.getProfiles (up to 25 DIDs per request).
Optionally authenticates for higher rate limits.

Usage:
    python fetch_profiles.py [--stale-days 7] [--db bluesky.db]

Environment variables (optional, for higher rate limits):
    BSKY_HANDLE       — Bluesky handle (e.g. user.bsky.social)
    BSKY_APP_PASSWORD — App password from Settings > App Passwords
"""

import argparse
import os
import time
from pathlib import Path

import requests

import db

API_BASE = "https://public.api.bsky.app/xrpc"
AUTH_API_BASE = "https://bsky.social/xrpc"
BATCH_SIZE = 25
RATE_LIMIT_DELAY = 0.2


def authenticate(handle, app_password):
    """Create an authenticated session, return access token."""
    resp = requests.post(
        f"{AUTH_API_BASE}/com.atproto.server.createSession",
        json={"identifier": handle, "password": app_password},
    )
    resp.raise_for_status()
    return resp.json()["accessJwt"]


def _fetch_single_batch(dids, token):
    base = AUTH_API_BASE if token else API_BASE
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    params = [("actors", did) for did in dids]
    resp = requests.get(
        f"{base}/app.bsky.actor.getProfiles",
        params=params,
        headers=headers,
    )
    return resp


def fetch_profiles_batch(dids, token=None):
    """Fetch profiles. On 400, split the batch to isolate bad DIDs and recover the good ones."""
    if not dids:
        return []
    resp = _fetch_single_batch(dids, token)

    if resp.status_code == 400 and len(dids) > 1:
        mid = len(dids) // 2
        time.sleep(0.02)
        left = fetch_profiles_batch(dids[:mid], token)
        time.sleep(0.02)
        right = fetch_profiles_batch(dids[mid:], token)
        return left + right

    if resp.status_code == 400:
        return []

    resp.raise_for_status()
    profiles = resp.json().get("profiles", [])
    return [
        {
            "did": p["did"],
            "handle": p.get("handle"),
            "followers_count": p.get("followersCount", 0),
            "follows_count": p.get("followsCount", 0),
            "posts_count": p.get("postsCount", 0),
        }
        for p in profiles
    ]


def get_stale_dids(conn, stale_days):
    """Find DIDs from posts/engagements that need a fresh profile."""
    cur = conn.execute(
        """
        SELECT DISTINCT d.did FROM (
            SELECT did FROM posts
            UNION
            SELECT did FROM engagements
        ) d
        LEFT JOIN profiles p ON d.did = p.did
        WHERE p.did IS NULL
           OR p.fetched_at < datetime('now', ?)
        """,
        (f"-{stale_days} days",),
    )
    return [row[0] for row in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description="Fetch Bluesky profiles")
    parser.add_argument("--stale-days", type=int, default=7,
                        help="Re-fetch profiles older than N days (default: 7)")
    parser.add_argument("--db", type=str, default=None,
                        help="Path to SQLite database")
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else db.DEFAULT_DB_PATH
    conn = db.get_db(db_path)

    # Optional auth
    token = None
    handle = os.environ.get("BSKY_HANDLE")
    app_password = os.environ.get("BSKY_APP_PASSWORD")
    if handle and app_password:
        print(f"Authenticating as {handle}...")
        token = authenticate(handle, app_password)
        print("Authenticated (higher rate limits)")
    else:
        print("No auth credentials — using unauthenticated rate limits")

    dids = get_stale_dids(conn, args.stale_days)
    total = len(dids)
    print(f"Found {total} DIDs needing profile fetch")

    if total == 0:
        conn.close()
        return

    fetched = 0
    errors = 0
    for i in range(0, total, BATCH_SIZE):
        batch = dids[i : i + BATCH_SIZE]
        try:
            profiles = fetch_profiles_batch(batch, token)
            db.upsert_profiles(conn, profiles)
            fetched += len(profiles)
        except requests.HTTPError as e:
            errors += 1
            if e.response is not None and e.response.status_code == 429:
                retry_after = int(e.response.headers.get("Retry-After", 30))
                print(f"Rate limited, waiting {retry_after}s...")
                time.sleep(retry_after)
                # Retry this batch
                try:
                    profiles = fetch_profiles_batch(batch, token)
                    db.upsert_profiles(conn, profiles)
                    fetched += len(profiles)
                except Exception:
                    pass
            else:
                print(f"Error fetching batch: {e}")

        if (i // BATCH_SIZE + 1) % 10 == 0 or i + BATCH_SIZE >= total:
            print(f"Progress: {fetched}/{total} fetched, {errors} errors")

        time.sleep(RATE_LIMIT_DELAY)

    print(f"Done. Fetched {fetched} profiles, {errors} errors.")
    conn.close()


if __name__ == "__main__":
    main()
