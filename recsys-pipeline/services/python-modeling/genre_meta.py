"""Fetch movie metadata (genres + release year) from Redis movie:{id}:features.

Non-Spark, lazy redis import → importable under spark-submit Python. Used by the keyword
analysis report when the training_samples Parquet does not already carry a `genres` column.
"""
from __future__ import annotations


def fetch_movie_meta(host: str, port: int) -> list[dict]:
    """Return [{item_id, genres: [..], release_year: int|None}] from movie:*:features.

    Returns [] if Redis is unreachable (the report then has no genres to break down by).
    """
    try:
        import redis
        client = redis.Redis(host=host, port=port, decode_responses=True)
        rows = []
        for key in client.scan_iter(match="movie:*:features"):
            h = client.hgetall(key)
            if not h:
                continue
            item_id = key.split(":")[1]
            genres = [g for g in (h.get("genres") or "").split(",") if g]
            try:
                year = int(h["releaseYear"])
            except (KeyError, ValueError, TypeError):
                year = None
            rows.append({"item_id": item_id, "genres": genres, "release_year": year})
        return rows
    except Exception as e:  # noqa: BLE001
        print(f"[warn] could not read movie meta from Redis ({e})")
        return []
