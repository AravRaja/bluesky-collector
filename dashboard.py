# Flask dashboard — reads pre-computed JSON from the collector, zero DB connections.

import argparse
import json
import time
from pathlib import Path

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)
STATS_FILE = Path("/data/dashboard_stats.json")

_file_cache = {"data": None, "mtime": 0}


def _read_stats():
    try:
        mt = STATS_FILE.stat().st_mtime
        if _file_cache["data"] and mt == _file_cache["mtime"]:
            return _file_cache["data"]
        data = json.loads(STATS_FILE.read_text())
        _file_cache["data"] = data
        _file_cache["mtime"] = mt
        return data
    except (FileNotFoundError, json.JSONDecodeError):
        return None


@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/status")
def api_status():
    d = _read_stats()
    if not d:
        return jsonify({"counts": {}, "cursor": None, "events_per_sec": 0})
    return jsonify(d["status"])


@app.route("/api/rate")
def api_rate():
    d = _read_stats()
    if not d:
        return jsonify([])
    hours = request.args.get("hours", 6, type=int)
    # rate data is pre-computed for last 6h; client can filter further
    return jsonify(d.get("rate", []))


@app.route("/api/health")
def api_health():
    d = _read_stats()
    if not d:
        return jsonify({})
    return jsonify(d["health"])


@app.route("/api/viral")
def api_viral():
    d = _read_stats()
    if not d:
        return jsonify({"total": 0, "viral": 0, "pct": 0})
    return jsonify(d["viral"])


@app.route("/api/viral/posts")
def api_viral_posts():
    d = _read_stats()
    if not d:
        return jsonify([])
    return jsonify(d.get("viral_posts", []))


@app.route("/api/recent")
def api_recent():
    d = _read_stats()
    if not d:
        return jsonify([])
    return jsonify(d.get("recent", []))


def main():
    parser = argparse.ArgumentParser(description="Bluesky Collector Dashboard")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--stats-file", type=str, default=None)
    args = parser.parse_args()

    global STATS_FILE
    if args.stats_file:
        STATS_FILE = Path(args.stats_file)

    app.run(host="0.0.0.0", port=args.port, debug=False)


if __name__ == "__main__":
    main()
