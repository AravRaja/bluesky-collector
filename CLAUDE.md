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
- `/data` is mounted by UUID in `/etc/fstab` (was `/dev/sdb`, switched because SCSI names aren't stable across reboots on Oracle Cloud iSCSI)

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
- `fetch_profiles.py` — standalone script, populates `profiles` via Bluesky API. Not scheduled — run manually before export.
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

**Prune is incremental** — each step commits in batches of `BATCH_SIZE` (500) rows with a `SLEEP_SEC` (0.1s) yield between commits. The collector interleaves its ~50ms flushes in those gaps, so the write lock is never held long enough to starve it. Steps 1 and 2 iterate root posts in `time_us` order using a cursor, so each batch is bounded by an index scan — no O(n²) behavior on large DBs. Tune `BATCH_SIZE` and `SLEEP_SEC` at the top of `prune.py` if needed.

**Why this design** — the pre-incremental version held a single 20+ minute write transaction per run. Once prune started taking longer than the hourly trigger interval, back-to-back runs left the collector with zero write access for hours, the cursor fell 36h behind real-time, and we nearly hit the 72h Jetstream retention limit. Incremental commits fix this at the cost of prune itself running slightly longer wall-clock.

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
- **Cursor lag** — the firehose cursor should be within seconds of real time. If lag grows into hours, prune is probably starving the collector (see below). Compute with:
  ```bash
  python3 -c "import datetime as dt; c=int(open('/home/ubuntu/.cursor').read()); print(f'lag: {(dt.datetime.utcnow().timestamp()*1e6-c)/3.6e9:.2f}h')"
  ```

## Troubleshooting: collector falling behind

Symptom: dashboard shows old data, cursor lag > 1h, `journalctl -u collector` shows repeated `[flush] error: database is locked`.

Typical cause: prune is hogging the write lock. Check:
- `ps auxf | grep prune.py` — is a prune process running?
- `sudo journalctl -u prune.service --since "6 hours ago" | grep -E "Starting|Finished"` — are prune runs overlapping or finishing within the hour?

Fix: `sudo systemctl stop prune.timer && sudo kill <prune-pid>` to unblock the collector, then diagnose why prune is slow (usually the engagements or post_stats table grew past expectations).

**Second failure mode: collector becomes disk-bound on large DB.** Once the DB grows past ~30-40GB, per-insert index updates on iSCSI (~5-10ms round trip each) throttle the collector below real-time rate (~300 evt/s). Symptoms: collector in `D` state, ~170 evt/s sustained, lag grows even with prune stopped. Fixes, in increasing order of effort:

1. Shrink the DB by aggressive prune + VACUUM (cheap, maybe 2x speedup)
2. Move DB from `/data` (iSCSI) to `/home/ubuntu/` (local NVMe on `/dev/sda1`) — ~5-10x speedup for smaller DBs that fit in `sda1`'s free space
3. For one-off catch-up sprints: copy DB to `/dev/shm` (tmpfs / RAM) — ~100x speedup, rsync back to `/data` periodically. Risk: power loss = lose last sync window.

**Third failure mode: collector gets stuck in a degraded state.** Seen once — collector shows 0% CPU and frozen cursor, but systemd says it's active. `systemctl stop` hangs and requires SIGKILL. Restart fixes it. Watch for this: if cursor stops advancing for 5+ minutes and the process is in D state with 0% CPU, it's stuck — `sudo systemctl kill -s SIGKILL collector` and restart.

## Fetching profiles

`fetch_profiles.py` is not scheduled — run it manually before `export.py` so `author_followers` etc. aren't zero.

```bash
# Unauth (slow, ~12h for ~1M DIDs, public.api.bsky.app)
python fetch_profiles.py --db /data/bluesky.db

# Auth (recommended, ~2-3h, higher rate limit)
BSKY_HANDLE=your.handle.bsky.social BSKY_APP_PASSWORD=xxxx-xxxx-xxxx-xxxx \
  python fetch_profiles.py --db /data/bluesky.db
```

App password at https://bsky.app/settings/app-passwords. Unauth uses `public.api.bsky.app`; authed uses `bsky.social`. Auth lets us lower `RATE_LIMIT_DELAY` from 0.2s to 0.05s.

**Partial-batch failure handling:** `getProfiles` returns 400 if ANY DID in the 25-DID batch is invalid (deleted / malformed). The script binary-splits the batch on 400 to isolate bad DIDs and recover the rest. Expect `~5-10%` of DIDs to truly fail (deleted accounts) even after recovery.

**Targeted fetch** — for a big DB it's wasteful to fetch profiles for every DID in posts+engagements. `get_stale_dids()` in the script can be modified to only pull DIDs that will appear in the export (viral authors + sampled authors + cascade participants). That's ~500K DIDs instead of 2M+ for a mature DB.

## Export

```bash
# Run on VM:
source .venv/bin/activate
python export.py --since 2026-04-15 --min-age-hours 2 --sample-ratio 10 \
  --cascade-window-min 30 --out-dir /tmp/export --db /data/bluesky.db
```

Produces: root_posts.parquet (Layer 1) + reposts/likes/replies/quotes.parquet (Layer 2 cascade).
Team repo: `AravRaja/virality-analysis-datascience` branch `bluesky-cascade-data`, directory `datasets/bluesky_cascade/`.

**DID hashing (privacy pass):** `export.py` hashes all DID columns with a random salt before writing parquets. The salt is auto-generated at first run and saved to `/home/ubuntu/.export_salt` (0600). Keep the salt if you ever want to re-identify specific rows; losing it means the export is permanently anonymized. The `author_handle` column is dropped from `root_posts.parquet` — DIDs (hashed) are the only identifier Duncan gets.

**Sampling:** default is `--sample-ratio 10` → 10 non-viral per viral, picked by "most recent N". This has two issues: temporal bias (most-recent posts have incomplete observation windows) and no distributional balance. For better ML training, consider stratified sampling — all hard-negatives (50-99 reposts), plus random samples from 0, 1-9, 10-49 tiers. Not yet wired into `export.py`.

## One-off orchestration pipelines

Long multi-step sequences (e.g. stop collector → backup → prune → fetch_profiles → export → zip) are run as shell scripts under `nohup` with a status file so the laptop can be closed mid-run. Convention used in this repo:

- Script at `/home/ubuntu/<name>_pipeline.sh`
- Status line in `/tmp/final_status.txt` (one-line "[N/X] current phase")
- Verbose log in `/tmp/final.log`
- Phase structure: numbered steps, each with `set_status` + `log` + exit-on-error + timing
- Launched via `nohup ... > /tmp/<name>.nohup 2>&1 &` so SSH disconnect doesn't kill it

Before destructive operations, always `sqlite3 .backup /data/bluesky.db.pre-<op>-backup` first — online backup works even while other writers are active, typically 3-5 min for a 30GB DB.
