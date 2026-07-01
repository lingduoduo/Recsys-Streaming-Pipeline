#!/usr/bin/env python3
"""Consolidated analysis dashboard — keyword / query / relevance / recall / ranking as one HTML page.

Standalone pandas/Python (no Spark). Recomputes the metrics from a run's training_samples Parquet
+ Redis and writes a single self-contained index.html. Recall/ranking reuse the pure functions in
recall_eval_report.py / ranking_eval_report.py; genres/categories via genre_meta / movie_categories.

    REDIS_HOST=localhost python services/python-modeling/analysis_dashboard_report.py --input <parquet>
"""
from __future__ import annotations

import argparse
import os


def query_of(genres: list[str]) -> str:
    return " ".join(genres) if genres else "unknown"


def load_samples(input_dir: str, host: str = "localhost", port: int = 6379):
    import pandas as pd
    df = pd.read_parquet(input_dir)
    if "label" not in df.columns:
        df["label"] = (df["clicked"] if "clicked" in df.columns else 0).astype(float)
    df["label"] = df["label"].astype(float)
    if "clicked" not in df.columns:
        df["clicked"] = (df["label"] >= 1).astype(int)
    df["clicked"] = df["clicked"].astype(int)
    if "genres" not in df.columns:
        from genre_meta import fetch_movie_meta
        meta = {m["item_id"]: m["genres"] for m in fetch_movie_meta(host, port)}
        df["genres"] = df["item_id"].astype(str).map(lambda i: meta.get(i, []))
    df["genres"] = df["genres"].apply(lambda g: list(g) if g is not None else [])
    return df
