#!/usr/bin/env python3
"""Offline next-item prediction: can this pipeline predict what a user engages with next?

Builds per-user timelines from the training_samples Parquet store, splits them at a
timestamp cutoff, and scores a small sequential transformer against three trivial
baselines through one shared metric implementation.

Offline only: nothing here touches Spark, Redis, the serving path, or ONNX.
"""
from __future__ import annotations

import pandas as pd

SEED = 42

#: Columns whose presence makes a row a positive engagement.
POSITIVE_MASK_COLUMNS = ("clicked", "ordered", "thumb")


def positive_mask(df: pd.DataFrame) -> pd.Series:
    """Rows the user engaged with: a click, an order, or a thumb up.

    A thumb down (-1) states dislike and an abandon states early exit. Neither is a
    positive, and neither is used as a negative — they are simply not prediction targets.
    """
    mask = pd.Series(False, index=df.index)
    if "clicked" in df.columns:
        mask |= df["clicked"].fillna(0).astype(int) == 1
    if "ordered" in df.columns:
        mask |= df["ordered"].fillna(0).astype(int) == 1
    if "thumb" in df.columns:
        mask |= df["thumb"].fillna(0).astype(int) == 1
    return mask


def load_positives(input_dir: str) -> pd.DataFrame:
    """Read the training-samples Parquet directory and keep only positive engagements."""
    df = pd.read_parquet(input_dir)
    positives = df[positive_mask(df)] if len(df) else df
    if positives.empty:
        raise ValueError(
            f"No positive engagements found at {input_dir}. "
            "Run a simulation first, e.g. scripts/run-movie-category-sim.sh"
        )
    return positives


def build_timelines(positives: pd.DataFrame) -> dict[str, list[tuple[int, str]]]:
    """Per-user (impression_ts, item_id) pairs in engagement order.

    Sorted by (impression_ts, position, item_id). The tie-break is load-bearing: every
    impression in one slate carries the same impression_ts, so sorting on the timestamp
    alone leaves a slate's positives in whatever order the Parquet happened to hold them
    and makes "the next item" differ between runs on identical input.
    """
    ordered = positives.sort_values(
        ["user_id", "impression_ts", "position", "item_id"], kind="mergesort")
    timelines: dict[str, list[tuple[int, str]]] = {}
    for user, item, ts in zip(ordered["user_id"], ordered["item_id"], ordered["impression_ts"]):
        timelines.setdefault(str(user), []).append((int(ts), str(item)))
    return timelines


def repeat_rate(timelines: dict[str, list[tuple[int, str]]]) -> float:
    """Share of events whose item that user already engaged with earlier.

    Reported so a reader can judge how much of any system's score is explained by
    re-engagement rather than discovery.
    """
    total = repeats = 0
    for events in timelines.values():
        seen: set[str] = set()
        for _, item in events:
            total += 1
            if item in seen:
                repeats += 1
            seen.add(item)
    return repeats / total if total else 0.0
