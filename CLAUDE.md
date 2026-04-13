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

## VM details

- IP: `141.147.86.46`
- SSH key: `~/.ssh/oracle-bluesky.key`
- User: `ubuntu`
- Repo on VM: `/home/ubuntu/` (git clone of this repo)
- DB: `/data/bluesky.db` (200GB iSCSI block volume at `/data`)
- Symlink: `/home/ubuntu/bluesky.db → /data/bluesky.db`

## Services

```bash
sudo systemctl status collector dashboard
sudo systemctl restart collector
sudo systemctl restart dashboard
sudo journalctl -u collector -f      # live logs
sudo journalctl -u dashboard -n 50
```

## Architecture

- `collect.py` — WebSocket collector (Jetstream firehose → SQLite), runs 24/7
- `dashboard.py` — Flask dashboard on port 5000
- `db.py` — Schema + DB helpers (WAL mode, indexes)
- `export.py` — SQLite → layered parquet export for ML pipeline
- `virality_threshold.py` — user-owned `is_viral()` function (reposts >= 100)

## Key design notes

- SQLite WAL mode with `wal_autocheckpoint=1000` — prevents WAL bloat
- `collector_stats` stores per-minute delta counts (posts/likes/reposts/follows)
- `/api/status` uses `SUM(collector_stats)` for fast counts (not `COUNT(*)` on main tables)
- `/api/health` is served from a background cache (5-min TTL) — never blocks
- Virality threshold: `reposts >= 100`
