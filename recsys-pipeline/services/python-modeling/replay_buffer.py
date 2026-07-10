"""Shared loader for the RL replay buffer (Redis list or Parquet dump).

Reads the real Java ReplayEvent schema (user/action/reward/...). Single owner of the
Redis key and JSON decoding so replay_export.py and ope_eval_report.py agree.
"""
from __future__ import annotations

import json

DEFAULT_KEY = "replay:recommendations"


def _parse(item) -> dict:
    if isinstance(item, bytes):
        item = item.decode("utf-8")
    return json.loads(item)


def load_from_redis(client, key: str = DEFAULT_KEY, limit: int = -1) -> list[dict]:
    return [_parse(x) for x in client.lrange(key, 0, limit)]


def load_from_parquet(path) -> list[dict]:
    import pandas as pd
    return pd.read_parquet(path).to_dict(orient="records")
