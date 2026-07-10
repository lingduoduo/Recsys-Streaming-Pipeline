#!/usr/bin/env python3
"""Export the Redis replay buffer to a ratings-CSV file for offline retraining.

Usage:
    python replay_export.py [--output PATH] [--parquet PATH] [--redis-host HOST]
                            [--redis-port PORT] [--key replay:recommendations] [--limit N]

Output CSV columns: userId, movieId, rating, timestamp
rating = min(reward * 5.0, 5.0)  — maps [0, 1] reward to MovieLens 0–5 scale

--parquet additionally writes the raw replay experience tuples (state/action/reward/
next_state, i.e. every field on each entry) to a Parquet file for downstream training.
"""
from __future__ import annotations

import argparse
import csv
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
            "userId": str(e["user"]),
            "movieId": str(e["action"]),
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


def write_parquet(entries: list[dict], output: Path) -> None:
    """Write the raw replay experience tuples (all fields per entry) to Parquet."""
    try:
        import pandas as pd
    except ImportError as e:  # pragma: no cover - depends on optional dep
        raise SystemExit(
            "--parquet requires pandas + pyarrow (pip install pandas pyarrow)"
        ) from e
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(entries).to_parquet(output, index=False)
    print(f"Wrote {len(entries)} experience tuples to {output}")


def main(args: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT)
    parser.add_argument(
        "--parquet",
        type=Path,
        default=None,
        help="Optional Parquet path for the raw replay experience tuples (requires pandas+pyarrow).",
    )
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")))
    parser.add_argument("--key", default=_DEFAULT_KEY)
    parser.add_argument("--limit", type=int, default=-1,
                        help="Max entries to export (-1 = all, default).")
    cfg = parser.parse_args(args)

    import replay_buffer
    client = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=False)
    entries = replay_buffer.load_from_redis(client, cfg.key, cfg.limit)
    if not entries:
        print(f"No entries found at {cfg.key}")
        return
    write_csv(entries, cfg.output)
    if cfg.parquet is not None:
        write_parquet(entries, cfg.parquet)


if __name__ == "__main__":
    main()
