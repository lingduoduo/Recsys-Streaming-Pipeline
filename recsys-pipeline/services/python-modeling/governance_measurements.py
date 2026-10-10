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
    "device",
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

    outcomes = _parse_outcomes(samples)
    everyone = range(len(samples))
    overall = {
        "ctr": _mean(_observed(outcomes["clicked"], everyone)),
        "order_rate": _mean(_observed(outcomes["ordered"], everyone)),
        "mean_reward": _mean(_observed(outcomes["reward"], everyone)),
        "ndcg": _group_ndcg(outcomes, everyone)[0],
    }
    rows = [_fairness_dimension_row(samples, dimension, min_support, outcomes, overall)
            for dimension in present_dimensions]
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
    when filter reasons were observed) and unsafe-label coverage.

    A signal counts as present only when a value was actually observed. The
    pipeline materializes ``filter_reason`` and ``unsafe_label`` columns on
    every training sample whether or not anything was logged, so an
    entirely-null column is treated as absent rather than as a measured zero.
    """
    if samples.empty:
        return unavailable("missing safety samples")
    reasons = _observed_reasons(samples)
    observed_labels = _observed_labels(samples)
    has_filter_reasons = bool(reasons)
    has_unsafe_labels = bool(observed_labels)
    if not has_filter_reasons and not has_unsafe_labels:
        return unavailable("missing filter_reason and unsafe_label safety signals")

    reason_counts: dict[str, int | None] = (
        {reason: 0 for reason in SAFETY_REASONS}
        if has_filter_reasons
        else {reason: None for reason in SAFETY_REASONS}
    )
    for reason in reasons:
        reason_counts[reason] = (reason_counts[reason] or 0) + 1
    filter_decisions: int | None = len(reasons) if has_filter_reasons else None

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


def _fairness_dimension_row(
    samples: pd.DataFrame,
    dimension: str,
    min_support: int,
    outcomes: Mapping[str, list | None],
    overall: Mapping[str, float | None],
) -> dict[str, object]:
    group_frames: dict[str, list[int]] = {}
    for index, value in enumerate(samples[dimension]):
        group_frames.setdefault(_group_value(value), []).append(index)

    eligible = {
        group: group_samples
        for group, group_samples in group_frames.items()
        if len(group_samples) >= min_support
    }
    groups = [
        _fairness_group_row(group, group_samples, len(samples), overall, outcomes)
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
    samples: Sequence[int],
    total: int,
    overall: Mapping[str, float | None],
    outcomes: Mapping[str, list | None],
) -> dict[str, object]:
    clicked = _observed(outcomes["clicked"], samples)
    ordered = _observed(outcomes["ordered"], samples)
    rewards = _observed(outcomes["reward"], samples)
    ndcg, evaluated_slates = _group_ndcg(outcomes, samples)
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
        "ndcg_evaluated_slate_count": evaluated_slates,
        "ctr_absolute_gap_from_overall": _absolute_gap(ctr, overall["ctr"]),
        "order_rate_absolute_gap_from_overall": _absolute_gap(order_rate, overall["order_rate"]),
        "mean_reward_absolute_gap_from_overall": _absolute_gap(mean_reward, overall["mean_reward"]),
        "ndcg_absolute_gap_from_overall": _absolute_gap(ndcg, overall["ndcg"]),
    }


def _fairness_warnings(requested: Sequence[str], present: Sequence[str]) -> list[str]:
    missing = [dimension for dimension in requested if dimension not in present]
    return [f"missing demographic dimension: {dimension}" for dimension in missing]


def _parse_outcomes(samples: pd.DataFrame) -> dict[str, list | None]:
    """Parse each sample's outcomes once; dimensions and groups then read them by row index.

    Slate fields are None unless request_id, position and label are all columns: NDCG needs
    the three together.
    """
    def column(name, parse):
        if name not in samples.columns:
            return [None] * len(samples)
        return [parse(value) for value in samples[name]]

    has_slates = all(name in samples.columns for name in ("request_id", "position", "label"))
    return {
        "clicked": column("clicked", _boolean_value),
        "ordered": column("ordered", _boolean_value),
        "reward": column("reward", _numeric_value),
        "request_id": column("request_id", _string_value) if has_slates else None,
        "position": column("position", _numeric_value),
        "label": column("label", _relevance_label),
    }


def _observed(values: Sequence[object], rows: Iterable[int]) -> list[float]:
    return [float(values[index]) for index in rows if values[index] is not None]


def _group_ndcg(outcomes: Mapping[str, list | None], rows: Sequence[int]) -> tuple[float | None, int]:
    """Return mean NDCG for completely labeled, explicitly positioned slates, and their count."""
    request_ids, positions, labels = outcomes["request_id"], outcomes["position"], outcomes["label"]
    if request_ids is None or not rows:
        return None, 0
    slates: dict[str, list[int]] = {}
    for index in rows:
        if (request_id := request_ids[index]) is not None:
            slates.setdefault(request_id, []).append(index)

    values: list[float] = []
    for slate in slates.values():
        ranked = [(positions[index], labels[index]) for index in slate]
        if any(position is None or label is None for position, label in ranked):
            continue
        ordered = [label for _, label in sorted(ranked, key=lambda value: value[0])]
        ideal = _dcg(sorted(ordered, reverse=True))
        if ideal > 0:
            values.append(_dcg(ordered) / ideal)
    return _mean(values), len(values)


def _dcg(labels: Iterable[float]) -> float:
    return sum(
        (2.0 ** max(0.0, label) - 1.0) / math.log2(index + 2)
        for index, label in enumerate(labels)
    )


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


def _observed_reasons(samples: pd.DataFrame) -> list[str]:
    """Return the filter reasons actually recorded; a null-filled column records none."""
    if "filter_reason" not in samples.columns:
        return []
    return [reason for value in samples["filter_reason"] if (reason := _safety_reason(value)) is not None]


def _observed_labels(samples: pd.DataFrame) -> list[bool]:
    """Return the unsafe labels actually recorded; a null-filled column records none."""
    if "unsafe_label" not in samples.columns:
        return []
    return [label for value in samples["unsafe_label"] if (label := _boolean_value(value)) is not None]


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
