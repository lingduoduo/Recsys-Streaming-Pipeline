"""Bounded offline fairness and safety measurement calculators.

These functions summarize recorded recommendation outcomes only.  They do not
participate in candidate filtering, scoring, or ranking decisions.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import math
from numbers import Real

import pandas as pd

from measurement_contract import available, safe_ratio, unavailable


DEFAULT_DIMENSIONS: tuple[str, ...] = (
    "age_band",
    "gender",
    "occupation",
    "geo",
    "platform",
    "country",
    "subscription",
)
"""The only demographic dimensions permitted in published fairness results."""

SAFETY_REASONS: tuple[str, ...] = (
    "expired",
    "muted_product_type",
    "muted_genre",
    "muted_keyword",
    "muted_title",
    "unknown",
)
"""The fixed allowlist of candidate-filter reasons for policy accounting."""

ALLOWED_RELEVANCE_LABELS: frozenset[float] = frozenset((0.0, 1.0, 2.0))
"""The pipeline's complete impression/click/order graded-relevance domain."""


def compute_fairness(
    samples: pd.DataFrame,
    min_support: int = 100,
    dimensions: Sequence[str] = DEFAULT_DIMENSIONS,
) -> dict[str, object]:
    """Measure outcome disparities across supported demographic groups.

    Groups with fewer than ``min_support`` recorded candidates are suppressed
    before metrics and disparities are calculated. Missing group values are
    represented by the fixed ``"unknown"`` group; entirely absent demographic
    columns make the calculation unavailable rather than implying zero.
    """
    if samples.empty:
        return unavailable("missing fairness samples")
    if isinstance(min_support, bool) or not isinstance(min_support, int) or min_support <= 0:
        return unavailable("min_support must be a positive integer")

    requested = tuple(dict.fromkeys(dimension for dimension in dimensions if dimension in DEFAULT_DIMENSIONS))
    present_dimensions = tuple(dimension for dimension in requested if dimension in samples.columns)
    if not present_dimensions:
        return unavailable("missing supported demographic dimensions")

    rows = [_fairness_dimension_row(samples, dimension, min_support) for dimension in present_dimensions]
    return available(
        "Outcome parity across supported demographic groups",
        rows,
        len(samples),
        safe_ratio(len(present_dimensions), len(requested)) or 0.0,
        warnings=_fairness_warnings(requested, present_dimensions),
    )


def compute_safety(
    samples: pd.DataFrame,
    policy_version: str = "catalog-filter-v1",
) -> dict[str, object]:
    """Account for bounded filter decisions and independently labeled exposure.

    An absent ``unsafe_label`` field keeps unsafe exposure unavailable. A
    present field with observed false values is a measured zero, not missing
    data. Unrecognized non-empty filter reasons are counted under ``unknown``.
    Envelope coverage is the mean of complete filter-log availability (one
    when the ``filter_reason`` column is present) and unsafe-label coverage.
    """
    if samples.empty:
        return unavailable("missing safety samples")
    has_filter_reasons = "filter_reason" in samples.columns
    has_unsafe_labels = "unsafe_label" in samples.columns
    if not has_filter_reasons and not has_unsafe_labels:
        return unavailable("missing filter_reason and unsafe_label safety signals")

    reason_counts: dict[str, int | None] = (
        {reason: 0 for reason in SAFETY_REASONS}
        if has_filter_reasons
        else {reason: None for reason in SAFETY_REASONS}
    )
    filter_decisions: int | None = 0 if has_filter_reasons else None
    if has_filter_reasons:
        for value in samples["filter_reason"]:
            reason = _safety_reason(value)
            if reason is None:
                continue
            filter_decisions = (filter_decisions or 0) + 1
            reason_counts[reason] = (reason_counts[reason] or 0) + 1

    labels = [_boolean_value(value) for value in samples["unsafe_label"]] if has_unsafe_labels else []
    observed_labels = [label for label in labels if label is not None]
    unsafe_exposed = sum(label is True for label in observed_labels)
    total = len(samples)
    row = {
        "policy_version": str(policy_version),
        "evaluated_candidates": total,
        "filter_decisions": filter_decisions,
        "filter_decision_rate": safe_ratio(filter_decisions, total) if filter_decisions is not None else None,
        "reason_counts": reason_counts,
        "unknown_share": safe_ratio(reason_counts["unknown"], total) if reason_counts["unknown"] is not None else None,
        "unsafe_exposure_rate": safe_ratio(unsafe_exposed, len(observed_labels)),
        "unsafe_label_coverage": safe_ratio(len(observed_labels), total),
    }
    filter_log_coverage = 1.0 if has_filter_reasons else 0.0
    unsafe_label_coverage = safe_ratio(len(observed_labels), total) or 0.0
    return available(
        "Candidate safety policy accounting",
        [row],
        total,
        (filter_log_coverage + unsafe_label_coverage) / 2,
    )


def _fairness_dimension_row(samples: pd.DataFrame, dimension: str, min_support: int) -> dict[str, object]:
    group_frames: dict[str, list[dict[str, object]]] = {}
    for _, sample in samples.iterrows():
        group_frames.setdefault(_group_value(sample.get(dimension)), []).append(sample.to_dict())

    eligible = {
        group: group_samples
        for group, group_samples in group_frames.items()
        if len(group_samples) >= min_support
    }
    overall = {
        "ctr": _mean(_binary_values(samples, "clicked")),
        "order_rate": _mean(_binary_values(samples, "ordered")),
        "mean_reward": _mean(_numeric_values(samples, "reward")),
        "ndcg": _group_ndcg(samples.to_dict("records")),
    }
    groups = [
        _fairness_group_row(group, group_samples, len(samples), overall)
        for group, group_samples in sorted(eligible.items())
    ]
    ctrs = [group["ctr"] for group in groups]
    order_rates = [group["order_rate"] for group in groups]
    rewards = [group["mean_reward"] for group in groups]
    ndcgs = [group["ndcg"] for group in groups]
    return {
        "dimension": dimension,
        "evaluated_candidates": len(samples),
        "overall_ctr": overall["ctr"],
        "overall_order_rate": overall["order_rate"],
        "overall_mean_reward": overall["mean_reward"],
        "overall_ndcg": overall["ndcg"],
        "groups": groups,
        "evaluated_group_count": len(groups),
        "suppressed_group_count": len(group_frames) - len(groups),
        "suppressed_candidate_count": sum(len(values) for values in group_frames.values() if len(values) < min_support),
        "ctr_max_min_gap": _max_min_gap(ctrs),
        "ctr_disparity_ratio": _disparity_ratio(ctrs),
        "order_rate_max_min_gap": _max_min_gap(order_rates),
        "order_rate_disparity_ratio": _disparity_ratio(order_rates),
        "mean_reward_max_min_gap": _max_min_gap(rewards),
        "mean_reward_disparity_ratio": _disparity_ratio(rewards),
        "ndcg_max_min_gap": _max_min_gap(ndcgs),
        "ndcg_disparity_ratio": _disparity_ratio(ndcgs),
    }


def _fairness_group_row(
    group: str,
    samples: Sequence[dict[str, object]],
    total: int,
    overall: Mapping[str, float | None],
) -> dict[str, object]:
    clicked = _binary_values(samples, "clicked")
    ordered = _binary_values(samples, "ordered")
    rewards = _numeric_values(samples, "reward")
    ndcg = _group_ndcg(samples)
    ctr = _mean(clicked)
    order_rate = _mean(ordered)
    mean_reward = _mean(rewards)
    return {
        "group": group,
        "support": len(samples),
        "exposure_share": safe_ratio(len(samples), total),
        "ctr": ctr,
        "ctr_coverage": safe_ratio(len(clicked), len(samples)),
        "order_rate": order_rate,
        "order_coverage": safe_ratio(len(ordered), len(samples)),
        "mean_reward": mean_reward,
        "reward_coverage": safe_ratio(len(rewards), len(samples)),
        "ndcg": ndcg,
        "ndcg_evaluated_slate_count": _evaluable_slate_count(samples),
        "ctr_absolute_gap_from_overall": _absolute_gap(ctr, overall["ctr"]),
        "order_rate_absolute_gap_from_overall": _absolute_gap(order_rate, overall["order_rate"]),
        "mean_reward_absolute_gap_from_overall": _absolute_gap(mean_reward, overall["mean_reward"]),
        "ndcg_absolute_gap_from_overall": _absolute_gap(ndcg, overall["ndcg"]),
    }


def _fairness_warnings(requested: Sequence[str], present: Sequence[str]) -> list[str]:
    missing = [dimension for dimension in requested if dimension not in present]
    return [f"missing demographic dimension: {dimension}" for dimension in missing]


def _group_ndcg(samples: Sequence[dict[str, object]]) -> float | None:
    """Return mean NDCG for completely labeled, explicitly positioned slates."""
    if not samples or any("request_id" not in sample or "position" not in sample or "label" not in sample for sample in samples):
        return None
    slates: dict[str, list[dict[str, object]]] = {}
    for sample in samples:
        request_id = _string_value(sample.get("request_id"))
        if request_id is None:
            continue
        slates.setdefault(request_id, []).append(sample)

    values: list[float] = []
    for slate in slates.values():
        ranked: list[tuple[float, float]] = []
        for sample in slate:
            position = _numeric_value(sample.get("position"))
            label = _relevance_label(sample.get("label"))
            if position is None or label is None:
                ranked = []
                break
            ranked.append((position, label))
        if not ranked:
            continue
        labels = [label for _, label in sorted(ranked, key=lambda value: value[0])]
        ideal = _dcg(sorted(labels, reverse=True))
        if ideal > 0:
            values.append(_dcg(labels) / ideal)
    return _mean(values)


def _evaluable_slate_count(samples: Sequence[dict[str, object]]) -> int:
    """Count slates that have ordered numeric labels and non-zero ideal gain."""
    if not samples or any("request_id" not in sample or "position" not in sample or "label" not in sample for sample in samples):
        return 0
    slates: dict[str, list[dict[str, object]]] = {}
    for sample in samples:
        request_id = _string_value(sample.get("request_id"))
        if request_id is not None:
            slates.setdefault(request_id, []).append(sample)
    count = 0
    for slate in slates.values():
        labels = [_relevance_label(sample.get("label")) for sample in slate]
        positions = [_numeric_value(sample.get("position")) for sample in slate]
        if all(value is not None for value in labels) and all(value is not None for value in positions):
            if _dcg(sorted((float(value) for value in labels), reverse=True)) > 0:
                count += 1
    return count


def _dcg(labels: Iterable[float]) -> float:
    return sum(
        (2.0 ** max(0.0, label) - 1.0) / math.log2(index + 2)
        for index, label in enumerate(labels)
    )


def _numeric_values(samples: pd.DataFrame | Sequence[dict[str, object]], column: str) -> list[float]:
    if isinstance(samples, pd.DataFrame):
        if column not in samples.columns:
            return []
        values = samples[column]
    else:
        values = (sample.get(column) for sample in samples)
    return [numeric for value in values if (numeric := _numeric_value(value)) is not None]


def _binary_values(samples: pd.DataFrame | Sequence[dict[str, object]], column: str) -> list[float]:
    if isinstance(samples, pd.DataFrame):
        if column not in samples.columns:
            return []
        values = samples[column]
    else:
        values = (sample.get(column) for sample in samples)
    return [float(parsed) for value in values if (parsed := _boolean_value(value)) is not None]


def _numeric_value(value: object) -> float | None:
    if _is_missing(value):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _relevance_label(value: object) -> float | None:
    if pd.api.types.is_bool(value) or not isinstance(value, Real):
        return None
    numeric = _numeric_value(value)
    if numeric not in ALLOWED_RELEVANCE_LABELS:
        return None
    return numeric


def _boolean_value(value: object) -> bool | None:
    if _is_missing(value):
        return None
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


def _safety_reason(value: object) -> str | None:
    text = _string_value(value)
    if text is None:
        return None
    return text if text in SAFETY_REASONS[:-1] else "unknown"


def _group_value(value: object) -> str:
    return _string_value(value) or "unknown"


def _string_value(value: object) -> str | None:
    if _is_missing(value) or not isinstance(value, (str, Real)):
        return None
    text = str(value).strip()
    return text or None


def _is_missing(value: object) -> bool:
    try:
        result = pd.isna(value)
    except (TypeError, ValueError):
        return False
    return bool(result) if isinstance(result, (bool, type(pd.NA))) else False


def _mean(values: Iterable[float]) -> float | None:
    observed = [float(value) for value in values]
    return safe_ratio(sum(observed), len(observed))


def _max_min_gap(values: Iterable[float | None]) -> float | None:
    observed = [float(value) for value in values if value is not None]
    return max(observed) - min(observed) if len(observed) >= 2 else None


def _disparity_ratio(values: Iterable[float | None]) -> float | None:
    observed = [float(value) for value in values if value is not None]
    return safe_ratio(min(observed), max(observed)) if len(observed) >= 2 else None


def _absolute_gap(value: float | None, overall: float | None) -> float | None:
    return abs(value - overall) if value is not None and overall is not None else None
