#!/usr/bin/env python3
"""Export the Redis replay buffer to a ratings-CSV file for offline retraining.

Usage:
    python replay_export.py [--output PATH] [--redis-host HOST] [--redis-port PORT]
                            [--key replay:recommendations] [--limit N]

Output CSV columns: userId, movieId, rating, timestamp
rating = min(reward * 5.0, 5.0)  — maps [0, 1] reward to MovieLens 0–5 scale
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Sequence

import redis

_DEFAULT_KEY = "replay:recommendations"
_DEFAULT_OUTPUT = Path(__file__).parents[2] / "sampledata" / "replay_training.csv"


def entries_to_rows(entries: list[dict]) -> list[dict]:
    rows = []
    for e in entries:
        rating = min(float(e.get("reward", 0.0)) * 5.0, 5.0)
        rows.append({
            "userId": str(e["userId"]),
            "movieId": str(e["itemId"]),
            "rating": f"{rating:.1f}",
            "timestamp": str(int(e.get("timestamp", 0))),
        })
    return rows


def write_csv(entries: list[dict], output: Path) -> None:
    rows = entries_to_rows(entries)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["userId", "movieId", "rating", "timestamp"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {output}")


def main(args: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT)
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")))
    parser.add_argument("--key", default=_DEFAULT_KEY)
    parser.add_argument("--limit", type=int, default=-1,
                        help="Max entries to export (-1 = all, default).")
    cfg = parser.parse_args(args)

    client = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=False)
    raw = client.lrange(cfg.key, 0, cfg.limit)
    if not raw:
        print(f"No entries found at {cfg.key}")
        return
    entries = [json.loads(b) for b in raw]
    write_csv(entries, cfg.output)


if __name__ == "__main__":
    main()
