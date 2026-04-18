# bluesky-collector

## Deploying to the Oracle VM

**Always use git push + git pull. Never use SCP.**

```bash
# 1. Commit and push locally
git add <files>
git commit -m "..."
git push

# 2. Pull on VM and restart the affected service
ssh -i ~/.ssh/oracle-bluesky.key ubuntu@141.147.86.46 \
  'cd /home/ubuntu && git pull origin master && sudo systemctl restart dashboard'
# or restart collector:
#   sudo systemctl restart collector
# or both:
#   sudo systemctl restart collector dashboard
```

**After restarting collector, always clear pycache:**
```bash
ssh -i ~/.ssh/oracle-bluesky.key ubuntu@141.147.86.46 \
  'sudo systemctl stop collector && rm -rf /home/ubuntu/__pycache__ && sudo systemctl start collector'
```

## VM details

- IP: `141.147.86.46`
- SSH key: `~/.ssh/oracle-bluesky.key`
- User: `ubuntu`
- Repo on VM: `/home/ubuntu/` (git clone of this repo)
- DB: `/data/bluesky.db` (200GB iSCSI block volume at `/data`)
- Symlink: `/home/ubuntu/bluesky.db → /data/bluesky.db`
- Stats JSON: `/data/dashboard_stats.json` (written by collector every 60s)

## Services

```bash
sudo systemctl status collector dashboard
sudo systemctl restart collector
sudo systemctl restart dashboard
sudo journalctl -u collector -f      # live logs
sudo journalctl -u dashboard -n 50
sudo systemctl list-timers           # check prune timer
```

## Architecture

- `collect.py` — WebSocket collector (Jetstream firehose → SQLite), runs 24/7
  - Also runs `stats_writer.py` every 60s (writes dashboard JSON)
  - Also runs WAL checkpoint every 5 min
- `dashboard.py` — Flask dashboard on port 5000, **reads JSON only, zero DB connections**
- `db.py` — Schema + DB helpers (WAL mode, indexes)
- `stats_writer.py` — computes all dashboard stats, writes `/data/dashboard_stats.json` atomically
- `export.py` — SQLite → layered parquet export for ML pipeline
- `prune.py` — hourly via systemd timer: aggregates engagement into `post_stats`, deletes late rows
- `cascade_store.py` — helper for loading exported parquets (used by team)
- `virality_threshold.py` — user-owned `is_viral()` function (reposts >= 100)

## Database tables

- `posts` — every post (root, reply, quote). Never deleted.
- `engagements` — individual likes/reposts with timestamps. Cascade rows (first 2h per post) kept forever. Late rows pruned hourly.
- `post_stats` — aggregated totals per root post: likes, reposts, replies, quotes, plus time-bucketed repost snapshots (reposts_3h, 4h, 6h, 12h, 24h)
- `follows` — follow events. Pruned after 7 days.
- `profiles` — cached author profiles from Bluesky API.
- `collector_stats` — per-minute delta counts for rate charts.

## How pruning works

Runs hourly via `prune.timer`. For each root post older than 2 hours:
1. First time: count all cascade engagement, write to `post_stats` (INSERT OR IGNORE)
2. Every cycle: count late engagement about to be deleted, ADD to `post_stats` (incremental, never overwrite)
3. Snapshot reposts at 3h/4h/6h/12h/24h thresholds (written once per post)
4. Refresh reply/quote counts (posts are never deleted, safe to recount)
5. Delete engagement rows outside cascade window
6. Delete follows older than 7 days

## Key design notes

- **Dashboard has ZERO database connections** — reads `/data/dashboard_stats.json` only
- Collector writes stats JSON every 60s + forces WAL checkpoint every 5 min
- WAL bloat prevention: no long-lived readers, explicit PRAGMA wal_checkpoint(PASSIVE)
- `collector_stats` stores per-minute delta counts (posts/likes/reposts/follows)
- Virality threshold: `reposts >= 100`
- Firehose can send malformed data (e.g. `reply: true` instead of object) — parsers must handle gracefully

## Daily health check

Things to verify:
- `sudo systemctl status collector dashboard` — both active
- `ls -lh /data/bluesky.db*` — WAL should be under 100MB
- `stat /data/dashboard_stats.json` — modified within last 2 minutes
- `df -h /data` — disk usage reasonable
- `sudo journalctl -u collector -n 5 --no-pager` — no crash loops

## Export

```bash
# Run on VM:
source .venv/bin/activate
python export.py --since 2026-04-15 --min-age-hours 2 --sample-ratio 10 \
  --cascade-window-min 30 --out-dir /tmp/export --db /data/bluesky.db
```

Produces: root_posts.parquet (Layer 1) + reposts/likes/replies/quotes.parquet (Layer 2 cascade).
Team repo: `AravRaja/virality-analysis-datascience` branch `bluesky-cascade-data`, directory `datasets/bluesky_cascade/`.
