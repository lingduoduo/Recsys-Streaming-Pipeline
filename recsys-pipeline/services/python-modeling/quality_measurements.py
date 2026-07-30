"""Offline recommendation-quality calculators that do not affect ranking decisions."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from itertools import combinations
import math

import pandas as pd

from measurement_contract import available, unavailable


def dcg(labels: Sequence[float], k: int) -> float:
    """Return discounted cumulative gain for the first ``k`` graded labels."""
    return sum(
        (2.0 ** max(0.0, float(label)) - 1.0) / math.log2(rank + 2)
        for rank, label in enumerate(labels[:k])
    )


def ndcg(labels: Sequence[float], k: int) -> float | None:
    """Return normalized discounted cumulative gain, or ``None`` without gain."""
    ideal = dcg(sorted(labels, reverse=True), k)
    return None if ideal == 0 else dcg(labels, k) / ideal


def reciprocal_rank(labels: Sequence[float], k: int) -> float:
    """Return the reciprocal rank of the first positive label within ``k``."""
    return next((1.0 / (index + 1) for index, value in enumerate(labels[:k]) if value > 0), 0.0)


def jaccard_distance(left: Iterable[str], right: Iterable[str]) -> float | None:
    """Return Jaccard distance, or ``None`` when neither collection has genres."""
    union = set(left) | set(right)
    return None if not union else 1.0 - len(set(left) & set(right)) / len(union)


def compute_relevance(slates: pd.DataFrame, ks: Sequence[int] = (5, 10, 20)) -> dict[str, object]:
    """Calculate slate-averaged graded relevance measures at each requested cutoff."""
    slate_labels, observed_labels, item_count = _complete_slate_labels(slates)
    if not slate_labels:
        return unavailable("missing complete labeled slates")

    rows: list[dict[str, object]] = []
    for k in ks:
        if k <= 0:
            continue
        ndcg_values = [value for labels in slate_labels if (value := ndcg(labels, k)) is not None]
        mrr_values = [reciprocal_rank(labels, k) for labels in slate_labels]
        recall_values = [_recall_at_k(labels, k) for labels in slate_labels]
        rows.append({
            "k": int(k),
            "ndcg_at_k": _mean(ndcg_values),
            "mrr_at_k": _mean(mrr_values),
            "recall_at_k": _mean(recall_values),
            "hit_rate_at_k": _mean([1.0 if value > 0 else 0.0 for value in mrr_values]),
            "evaluated_slate_count": len(slate_labels),
            "ndcg_evaluated_slate_count": len(ndcg_values),
            "positive_label_count": sum(label > 0 for labels in slate_labels for label in labels),
            "label_coverage": _ratio(observed_labels, item_count),
        })
    if not rows:
        return unavailable("missing positive relevance cutoffs")
    return available(
        "Graded relevance across labeled slates",
        rows,
        len(slate_labels),
        _ratio(observed_labels, item_count) or 0.0,
    )


def compute_satisfaction(samples: pd.DataFrame) -> dict[str, object]:
    """Calculate engagement and observed-feedback measures with signal coverage."""
    total = len(samples)
    if total == 0:
        return unavailable("missing satisfaction samples")

    clicked = _numeric_column(samples, "clicked")
    ordered = _numeric_column(samples, "ordered")
    rewards = _numeric_column(samples, "reward")
    ratings = _numeric_column(samples, "rating")
    dwell = _numeric_column(samples, "dwell_millis")
    completion = _numeric_column(samples, "completion_rate")
    negative = _observed_column(samples, "negative_feedback_reason")
    row = {
        "ctr": _mean(clicked),
        "ctr_coverage": _ratio(len(clicked), total),
        "order_rate": _mean(ordered),
        "order_coverage": _ratio(len(ordered), total),
        "mean_reward": _mean(rewards),
        "reward_coverage": _ratio(len(rewards), total),
        "mean_rating": _mean(ratings),
        "rating_coverage": _ratio(len(ratings), total),
        "negative_feedback_rate": _ratio(len(negative), total) if negative else None,
        "negative_feedback_coverage": _ratio(len(negative), total),
        "mean_dwell_millis": _mean(dwell),
        "dwell_coverage": _ratio(len(dwell), total),
        "mean_completion_rate": _mean(completion),
        "completion_coverage": _ratio(len(completion), total),
    }
    coverage = _ratio(len(clicked), total) or 0.0
    return available("Observed user satisfaction", [row], total, coverage)


def compute_freshness(
    samples: pd.DataFrame,
    now: datetime,
    window_days: int = 30,
) -> dict[str, object]:
    """Measure fresh-item exposure using timestamps, then explicit boolean fallback."""
    if len(samples) == 0:
        return unavailable("missing freshness samples")
    if window_days < 0:
        return unavailable("freshness window must be non-negative")

    timestamped = _timestamp_observations(samples, now, window_days)
    if timestamped:
        return _freshness_result(timestamped, len(samples), "published_at")

    boolean_rows = _boolean_freshness_observations(samples)
    if boolean_rows:
        return _freshness_result(boolean_rows, len(samples), "boolean_new_release")
    return unavailable("missing published_at and new_release freshness signals")


def compute_diversity(
    slates: pd.DataFrame,
    long_tail_percentile: float = 0.80,
) -> dict[str, object]:
    """Aggregate genre diversity and popularity-tail exposure across ranked slates."""
    if not 0.0 < long_tail_percentile < 1.0:
        return unavailable("long-tail percentile must be between zero and one")

    slate_items = [_items(row) for _, row in slates.iterrows()]
    slate_items = [items for items in slate_items if items]
    if not slate_items:
        return unavailable("missing slate items")

    popularities = [
        popularity
        for items in slate_items
        for item in items
        if (popularity := _numeric_item_value(item, "popularity")) is not None
    ]
    cutoff = float(pd.Series(popularities).quantile(long_tail_percentile)) if popularities else None
    slate_rows = [_diversity_for_slate(items, cutoff) for items in slate_items]
    all_items = [item for items in slate_items for item in items]
    genre_coverage = _ratio(sum(bool(_genres(item)) for item in all_items), len(all_items))
    row = {
        "unique_genres_at_k": _mean([entry["unique_genres_at_k"] for entry in slate_rows]),
        "normalized_genre_entropy": _mean([entry["normalized_genre_entropy"] for entry in slate_rows]),
        "intra_list_genre_distance": _mean([entry["intra_list_genre_distance"] for entry in slate_rows]),
        "long_tail_exposure_share": _mean([entry["long_tail_exposure_share"] for entry in slate_rows]),
        "genre_coverage": genre_coverage,
        "popularity_coverage": _ratio(len(popularities), len(all_items)),
        "long_tail_popularity_cutoff": _round(cutoff),
    }
    if not any(value is not None for key, value in row.items() if key not in {"genre_coverage", "popularity_coverage"}):
        return unavailable("missing genre and popularity diversity signals")
    return available("Catalog diversity across slates", [row], len(slate_items), genre_coverage or 0.0)


def _complete_slate_labels(slates: pd.DataFrame) -> tuple[list[list[float]], int, int]:
    labels_by_slate: list[list[float]] = []
    observed_labels = 0
    item_count = 0
    for _, row in slates.iterrows():
        items = _items(row)
        item_count += len(items)
        labels = [_numeric_item_value(item, "label") for item in items]
        observed_labels += sum(label is not None for label in labels)
        if items and all(label is not None for label in labels):
            labels_by_slate.append([float(label) for label in labels])
    return labels_by_slate, observed_labels, item_count


def _recall_at_k(labels: Sequence[float], k: int) -> float | None:
    positives = sum(value > 0 for value in labels)
    return None if positives == 0 else sum(value > 0 for value in labels[:k]) / positives


def _freshness_result(
    observations: list[tuple[bool, float | None, float | None, float | None]],
    total: int,
    source: str,
) -> dict[str, object]:
    fresh = [entry for entry in observations if entry[0]]
    established = [entry for entry in observations if not entry[0]]
    ages = [entry[1] for entry in observations if entry[1] is not None]
    row = {
        "freshness_source": source,
        "fresh_share": _ratio(len(fresh), len(observations)),
        "freshness_coverage": _ratio(len(observations), total),
        "mean_content_age_days": _mean(ages),
        "median_content_age_days": _median(ages),
        "fresh_ctr": _mean([entry[2] for entry in fresh]),
        "established_ctr": _mean([entry[2] for entry in established]),
        "fresh_mean_reward": _mean([entry[3] for entry in fresh]),
        "established_mean_reward": _mean([entry[3] for entry in established]),
    }
    return available("Fresh-item exposure", [row], total, _ratio(len(observations), total) or 0.0)


def _timestamp_observations(
    samples: pd.DataFrame,
    now: datetime,
    window_days: int,
) -> list[tuple[bool, float | None, float | None, float | None]]:
    if "published_at" not in samples:
        return []
    now_timestamp = pd.Timestamp(now)
    observations: list[tuple[bool, float | None, float | None, float | None]] = []
    for _, sample in samples.iterrows():
        published = pd.to_datetime(sample.get("published_at"), utc=True, errors="coerce")
        if pd.isna(published):
            continue
        age_days = max(0.0, (now_timestamp - published).total_seconds() / 86_400)
        observations.append((
            age_days <= window_days,
            age_days,
            _numeric_value(sample.get("clicked")),
            _numeric_value(sample.get("reward")),
        ))
    return observations


def _boolean_freshness_observations(samples: pd.DataFrame) -> list[tuple[bool, None, float | None, float | None]]:
    if "new_release" not in samples:
        return []
    observations: list[tuple[bool, None, float | None, float | None]] = []
    for _, sample in samples.iterrows():
        value = sample.get("new_release")
        if not pd.notna(value):
            continue
        observations.append((bool(value), None, _numeric_value(sample.get("clicked")), _numeric_value(sample.get("reward"))))
    return observations


def _diversity_for_slate(items: list[Mapping[str, object]], cutoff: float | None) -> dict[str, float | None]:
    genre_sets = [_genres(item) for item in items]
    observed_genres = [genres for genres in genre_sets if genres]
    flattened = [genre for genres in observed_genres for genre in genres]
    unique_genres = set(flattened)
    distances = [
        distance
        for left, right in combinations(observed_genres, 2)
        if (distance := jaccard_distance(left, right)) is not None
    ]
    popularities = [_numeric_item_value(item, "popularity") for item in items]
    observed_popularities = [value for value in popularities if value is not None]
    return {
        "unique_genres_at_k": float(len(unique_genres)) if unique_genres else None,
        "normalized_genre_entropy": _normalized_entropy(flattened),
        "intra_list_genre_distance": _mean(distances),
        "long_tail_exposure_share": (
            _ratio(sum(value < cutoff for value in observed_popularities), len(observed_popularities))
            if cutoff is not None and observed_popularities else None
        ),
    }


def _genres(item: Mapping[str, object]) -> list[str]:
    values = item.get("genres")
    if not isinstance(values, (list, tuple, set)):
        return []
    return [str(value) for value in values if pd.notna(value) and str(value)]


def _items(row: pd.Series) -> list[Mapping[str, object]]:
    values = row.get("items")
    if not isinstance(values, (list, tuple)):
        return []
    return [item for item in values if isinstance(item, Mapping)]


def _numeric_column(samples: pd.DataFrame, name: str) -> list[float]:
    if name not in samples:
        return []
    return [value for value in (_numeric_value(item) for item in samples[name]) if value is not None]


def _observed_column(samples: pd.DataFrame, name: str) -> list[object]:
    if name not in samples:
        return []
    return [value for value in samples[name] if pd.notna(value)]


def _numeric_item_value(item: Mapping[str, object], name: str) -> float | None:
    return _numeric_value(item.get(name))


def _numeric_value(value: object) -> float | None:
    if not pd.notna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalized_entropy(genres: Sequence[str]) -> float | None:
    unique_genres = set(genres)
    if not unique_genres:
        return None
    if len(unique_genres) == 1:
        return 0.0
    total = len(genres)
    entropy = -sum(
        (genres.count(genre) / total) * math.log(genres.count(genre) / total)
        for genre in unique_genres
    )
    return entropy / math.log(len(unique_genres))


def _mean(values: Iterable[float | None]) -> float | None:
    observed = [float(value) for value in values if value is not None]
    return _round(sum(observed) / len(observed)) if observed else None


def _median(values: Iterable[float | None]) -> float | None:
    observed = sorted(float(value) for value in values if value is not None)
    if not observed:
        return None
    midpoint = len(observed) // 2
    median = observed[midpoint] if len(observed) % 2 else (observed[midpoint - 1] + observed[midpoint]) / 2
    return _round(median)


def _ratio(numerator: int, denominator: int) -> float | None:
    return _round(numerator / denominator) if denominator else None


def _round(value: float | None) -> float | None:
    return round(float(value), 4) if value is not None else None
