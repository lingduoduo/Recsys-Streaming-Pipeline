"""Offline recommendation-quality calculators that do not affect ranking decisions."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from itertools import combinations
import math
from numbers import Real

import pandas as pd

from measurement_contract import available, unavailable

# The sim stamps events with wall-clock time, so a run spans minutes; a calendar bucket would be
# one point. Equal-width buckets over the observed span work for a minute or a quarter.
SERIES_BUCKETS = 24


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
    slate_records, observed_labels, item_count = _complete_slate_labels(slates)
    if not slate_records:
        return unavailable("missing complete labeled slates")
    slate_labels = [labels for labels, _ in slate_records]

    rows: list[dict[str, object]] = []
    for k in ks:
        if k <= 0:
            continue
        ndcg_values = [value for labels in slate_labels if (value := ndcg(labels, k)) is not None]
        mrr_values = [reciprocal_rank(labels, k) for labels in slate_labels]
        recall_values, hit_rate_values, evaluated_user_count, fold_count = _leave_one_out_metrics(slate_records, k)
        rows.append({
            "k": int(k),
            "ndcg_at_k": _mean(ndcg_values),
            "mrr_at_k": _mean(mrr_values),
            "recall_at_k": _mean(recall_values),
            "hit_rate_at_k": _mean(hit_rate_values),
            "evaluated_slate_count": len(slate_labels),
            "ndcg_evaluated_slate_count": len(ndcg_values),
            "evaluated_user_count": evaluated_user_count,
            "leave_one_out_fold_count": fold_count,
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
    if not any((clicked, ordered, rewards, ratings, dwell, completion, negative)):
        return unavailable("missing observed satisfaction signals")
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
        "users": _distinct(samples, "user_id"),
        "items": _distinct(samples, "item_id"),
        "rated_samples": len(ratings),
    }
    coverage = _ratio(len(clicked), total) or 0.0
    result = available("Observed user satisfaction", [row], total, coverage)
    result["series"], result["series_bucket_seconds"] = _satisfaction_series(samples)
    result["rating_distribution"] = _rating_distribution(ratings)
    return result


def _time_buckets(stamps: pd.Series) -> tuple[pd.Series, list[float], float] | None:
    """Bucket index per stamp over the observed span, the bucket starts, and the width.

    Whole-second stamps (what the joiner publishes) get a whole-second width: a fractional one
    gives buckets 2 or 3 distinct seconds apiece, a sawtooth in every count from the width alone.
    The bucket count then falls to fit, at most SERIES_BUCKETS. None without a span.
    """
    if stamps.empty or stamps.max() <= stamps.min():
        return None
    start = float(stamps.min())
    span = float(stamps.max()) - start
    if (stamps % 1 == 0).all():
        width = float(math.ceil((span + 1) / SERIES_BUCKETS))
        buckets = math.ceil((span + 1) / width)
    else:
        width = span / SERIES_BUCKETS
        buckets = SERIES_BUCKETS
    # Float division can put the maximum at exactly `buckets`; it belongs to the last bucket.
    index = ((stamps - start) // width).clip(upper=buckets - 1).astype(int)
    return index, [start + i * width for i in range(buckets)], width


def _satisfaction_series(samples: pd.DataFrame) -> tuple[list[dict[str, object]], float | None]:
    """Per-bucket engagement over the observed impression_ts span (epoch seconds)."""
    if "impression_ts" not in samples:
        return [], None
    stamps = pd.to_numeric(samples["impression_ts"], errors="coerce")
    observed = stamps.notna().to_numpy()
    buckets = _time_buckets(stamps[observed])
    if buckets is None:
        return [], None
    index, starts, width = buckets
    timed, index = samples[observed], index.to_numpy()
    series = []
    for position, start in enumerate(starts):
        part = timed[index == position]
        ratings = _numeric_column(part, "rating")
        series.append({
            "bucket_start": round(start, 1),
            "impressions": len(part),
            "users": _distinct(part, "user_id"),
            "ctr": _mean(_numeric_column(part, "clicked")),
            "order_rate": _mean(_numeric_column(part, "ordered")),
            "mean_rating": _mean(ratings),
            "ratings": len(ratings),
        })
    return series, round(width, 1)


def _rating_distribution(ratings: list[float]) -> list[dict[str, object]]:
    """Half-point bins from the lowest observed through 4.5; a 5.0 falls in the 4.5 bin."""
    if not ratings:
        return []
    halves = Counter(min(math.floor(r * 2), 9) for r in ratings)
    return [{"rating": h / 2, "count": halves.get(h, 0)} for h in range(min(halves), 10)]


def compute_freshness(
    samples: pd.DataFrame,
    now: datetime,
    window_days: int = 30,
) -> dict[str, object]:
    """Measure fresh-item exposure using timestamps, then explicit boolean fallback.

    A timezone-naive ``now`` is interpreted as UTC.
    """
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
    catalog_size: int | None = None,
) -> dict[str, object]:
    """Aggregate genre diversity and popularity-tail exposure across ranked slates."""
    if not 0.0 < long_tail_percentile < 1.0:
        return unavailable("long-tail percentile must be between zero and one")

    slate_inputs = [
        (_slate_id(index, row), _items(row), _string_value(row.get("user_id")),
         _numeric_value(row.get("request_ts")))
        for index, row in slates.iterrows()
    ]
    slate_inputs = [entry for entry in slate_inputs if entry[1]]
    if not slate_inputs:
        return unavailable("missing slate items")

    all_items = [item for _, items, _, _ in slate_inputs for item in items]
    cutoff = _distinct_item_cutoff(all_items, long_tail_percentile)
    slate_rows = [
        {"scope": "slate", "slate_id": slate_id, **_diversity_for_slate(items, cutoff)}
        for slate_id, items, _, _ in slate_inputs
    ]
    genre_coverage = _ratio(sum(bool(_genres(item)) for item in all_items), len(all_items))
    aggregate = {
        "scope": "aggregate",
        "unique_genres_at_k": _mean([entry["unique_genres_at_k"] for entry in slate_rows]),
        "normalized_genre_entropy": _mean([entry["normalized_genre_entropy"] for entry in slate_rows]),
        "intra_list_genre_distance": _mean([entry["intra_list_genre_distance"] for entry in slate_rows]),
        "long_tail_exposure_share": _mean([entry["long_tail_exposure_share"] for entry in slate_rows]),
        "genre_coverage": genre_coverage,
        "popularity_coverage": _ratio(
            sum(_numeric_item_value(item, "popularity") is not None for item in all_items), len(all_items)),
        "long_tail_popularity_cutoff": _round(cutoff),
        **_catalog_spread(slate_inputs, catalog_size),
    }
    if not any(value is not None for key, value in aggregate.items() if key not in {"genre_coverage", "popularity_coverage", "scope"}):
        return unavailable("missing genre and popularity diversity signals")
    result = available(
        "Catalog diversity across slates",
        [aggregate, *slate_rows],
        len(slate_inputs),
        genre_coverage or 0.0,
    )
    result["distributions"] = {
        key: _unit_histogram([entry[key] for entry in slate_rows])
        for key in ("normalized_genre_entropy", "intra_list_genre_distance")
    }
    result["genre_exposure"] = _genre_exposure(all_items)
    return result


def _item_key(item: Mapping[str, object], position: int) -> str:
    """The item's id; an item without one counts as its own distinct item."""
    return _string_value(item.get("item_id")) or f"#{position}"


def _distinct_item_cutoff(items: list[Mapping[str, object]], percentile: float) -> float | None:
    """Popularity quantile over distinct served items, each counted once.

    Taken over exposures instead, the quantile puts ~percentile of exposures below it by
    construction, so the long-tail share could not move whatever the recommender did.
    """
    first_seen: dict[str, float] = {}
    for position, item in enumerate(items):
        popularity = _numeric_item_value(item, "popularity")
        if popularity is not None:
            first_seen.setdefault(_item_key(item, position), popularity)
    return float(pd.Series(list(first_seen.values())).quantile(percentile)) if first_seen else None


def _catalog_spread(slate_inputs: list[tuple], catalog_size: int | None) -> dict[str, object]:
    """Spread across the catalog and across each user's history -- what per-slate averages miss.

    A recommender showing every user the same five genre-diverse items scores perfectly per
    slate; Gini, coverage and repeat rate are what move.
    """
    exposures: Counter[str] = Counter()
    seen_by_user: dict[str, list[str]] = {}
    for _, items, user, _ in slate_inputs:
        ids = [item_id for item in items if (item_id := _string_value(item.get("item_id")))]
        exposures.update(ids)
        if user:
            seen_by_user.setdefault(user, []).extend(ids)
    counts = sorted(exposures.values())
    total, n = sum(counts), len(counts)
    distinct = [len(set(ids)) for ids in seen_by_user.values()]
    seen = sum(len(ids) for ids in seen_by_user.values())
    return {
        "items_served": n or None,
        "catalog_size": catalog_size,
        "catalog_coverage": _ratio(n, catalog_size) if n and catalog_size else None,
        "exposure_gini": (_round(sum((2 * rank - n - 1) * c for rank, c in enumerate(counts, 1)) / (n * total))
                          if n else None),
        "top_decile_exposure_share": _round(sum(counts[-max(1, n // 10):]) / total) if n else None,
        "median_items_per_user": _median(distinct),
        "user_repeat_rate": _round(1 - sum(distinct) / seen) if seen else None,
    }


def _unit_histogram(values: list[float | None]) -> list[dict[str, object]]:
    """Ten bins over [0, 1]; exactly 1.0 lands in the last; None is skipped."""
    counts = [0] * 10
    for value in values:
        if value is not None:
            # round() absorbs float error such as 0.7 * 10 == 7.000000000000001.
            counts[min(int(round(value * 10, 9)), 9)] += 1
    return [{"bin_start": index / 10, "count": count} for index, count in enumerate(counts)]


def _genre_exposure(items: list[Mapping[str, object]]) -> list[dict[str, object]]:
    """Each genre's share of exposures beside its share of the distinct items served."""
    exposed = Counter(genre for item in items for genre in _genres(item))
    served_items: dict[str, list[str]] = {}
    for position, item in enumerate(items):
        served_items.setdefault(_item_key(item, position), _genres(item))
    served = Counter(genre for genres in served_items.values() for genre in genres)
    exposed_total, served_total = sum(exposed.values()), sum(served.values())
    rows = [{"genre": genre,
             "exposure_share": _ratio(count, exposed_total),
             "served_share": _ratio(served[genre], served_total)}
            for genre, count in exposed.items()]
    return sorted(rows, key=lambda row: (-row["exposure_share"], row["genre"]))


def _complete_slate_labels(slates: pd.DataFrame) -> tuple[list[tuple[list[float], str | None]], int, int]:
    labels_by_slate: list[tuple[list[float], str | None]] = []
    observed_labels = 0
    item_count = 0
    for _, row in slates.iterrows():
        items = _items(row)
        item_count += len(items)
        labels = [_numeric_item_value(item, "label") for item in items]
        observed_labels += sum(label is not None for label in labels)
        if items and all(label is not None for label in labels):
            labels_by_slate.append(([float(label) for label in labels], _string_value(row.get("user_id"))))
    return labels_by_slate, observed_labels, item_count


def _leave_one_out_metrics(
    slate_records: Sequence[tuple[Sequence[float], str | None]],
    k: int,
) -> tuple[list[float], list[float], int, int]:
    """Aggregate every complete labeled user fold under the LOO protocol."""
    outcomes_by_user: dict[str, list[float]] = {}
    for labels, user_id in slate_records:
        if user_id is None:
            continue
        outcomes_by_user.setdefault(user_id, []).append(1.0 if any(value > 0 for value in labels[:k]) else 0.0)
    recall_values = [sum(outcomes) / len(outcomes) for outcomes in outcomes_by_user.values()]
    return (
        recall_values,
        [1.0 if any(outcomes) else 0.0 for outcomes in outcomes_by_user.values()],
        len(outcomes_by_user),
        sum(len(outcomes) for outcomes in outcomes_by_user.values()),
    )


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
        "fresh_ctr_coverage": _ratio(sum(entry[2] is not None for entry in fresh), len(fresh)),
        "established_ctr": _mean([entry[2] for entry in established]),
        "established_ctr_coverage": _ratio(sum(entry[2] is not None for entry in established), len(established)),
        "fresh_mean_reward": _mean([entry[3] for entry in fresh]),
        "fresh_reward_coverage": _ratio(sum(entry[3] is not None for entry in fresh), len(fresh)),
        "established_mean_reward": _mean([entry[3] for entry in established]),
        "established_reward_coverage": _ratio(sum(entry[3] is not None for entry in established), len(established)),
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
    if now_timestamp.tzinfo is None:
        now_timestamp = now_timestamp.tz_localize("UTC")
    else:
        now_timestamp = now_timestamp.tz_convert("UTC")
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
        fresh = _boolean_value(value)
        if fresh is None:
            continue
        observations.append((fresh, None, _numeric_value(sample.get("clicked")), _numeric_value(sample.get("reward"))))
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
        "genre_coverage": _ratio(len(observed_genres), len(items)),
        "popularity_coverage": _ratio(len(observed_popularities), len(items)),
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


def _slate_id(index: object, row: pd.Series) -> str:
    return _string_value(row.get("request_id")) or f"row-{index}"


def _numeric_column(samples: pd.DataFrame, name: str) -> list[float]:
    if name not in samples:
        return []
    return [value for value in (_numeric_value(item) for item in samples[name]) if value is not None]


def _observed_column(samples: pd.DataFrame, name: str) -> list[object]:
    if name not in samples:
        return []
    return [value for value in samples[name] if pd.notna(value)]


def _distinct(samples: pd.DataFrame, name: str) -> int | None:
    """Distinct observed values; None when the column is absent -- 0 users would be a claim."""
    return int(samples[name].dropna().nunique()) if name in samples else None


def _numeric_item_value(item: Mapping[str, object], name: str) -> float | None:
    return _numeric_value(item.get(name))


def _numeric_value(value: object) -> float | None:
    if not pd.notna(value):
        return None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _boolean_value(value: object) -> bool | None:
    """Parse booleans and documented 0/1 or true/false serializations."""
    if pd.api.types.is_bool(value):
        return bool(value)
    if isinstance(value, Real) and math.isfinite(float(value)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    return None


def _string_value(value: object) -> str | None:
    if not pd.notna(value):
        return None
    text = str(value).strip()
    return text or None


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
