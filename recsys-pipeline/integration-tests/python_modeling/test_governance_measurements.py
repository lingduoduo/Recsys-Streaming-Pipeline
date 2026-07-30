"""Integration tests for offline fairness and safety measurement calculators."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

from governance_measurements import compute_fairness, compute_safety  # noqa: E402


def test_fairness_suppresses_small_groups_and_reports_gaps():
    """Groups below support are excluded before disparity calculations."""
    rows = (
        [{"gender": "a", "clicked": 1, "ordered": 0, "reward": 1.0, "label": 1.0}] * 2
        + [{"gender": "b", "clicked": 0, "ordered": 0, "reward": 0.0, "label": 0.0}] * 2
        + [{"gender": "tiny", "clicked": 1, "ordered": 1, "reward": 1.0, "label": 2.0}]
    )

    result = compute_fairness(pd.DataFrame(rows), min_support=2, dimensions=("gender",))

    groups = result["rows"][0]["groups"]
    assert [group["group"] for group in groups] == ["a", "b"]
    assert result["rows"][0]["suppressed_group_count"] == 1
    assert result["rows"][0]["ctr_max_min_gap"] == 1.0
    assert result["rows"][0]["ctr_disparity_ratio"] == 0.0


def test_safety_distinguishes_filtering_from_unsafe_labels():
    """Filtering decisions and post-filter unsafe labels have separate denominators."""
    samples = pd.DataFrame([
        {"filter_reason": "muted_genre", "unsafe_label": None},
        {"filter_reason": None, "unsafe_label": True},
        {"filter_reason": None, "unsafe_label": False},
    ])

    result = compute_safety(samples, policy_version="catalog-filter-v2")

    row = result["rows"][0]
    assert row["policy_version"] == "catalog-filter-v2"
    assert row["filter_decision_rate"] == 1 / 3
    assert row["unsafe_exposure_rate"] == 0.5
    assert row["unsafe_label_coverage"] == 2 / 3


def test_fairness_marks_absent_demographics_unavailable_and_never_emits_unbounded_dimensions():
    """Absent demographics are unavailable, while unsupported names cannot leak labels."""
    missing = compute_fairness(
        pd.DataFrame([{"clicked": 1, "ordered": 0, "reward": 1.0}]),
        dimensions=("gender",),
    )
    bounded = compute_fairness(
        pd.DataFrame([
            {"gender": "a", "internal_tier": "private", "clicked": 1},
            {"gender": "a", "internal_tier": "private", "clicked": 0},
        ]),
        min_support=2,
        dimensions=("gender", "internal_tier"),
    )

    assert missing["status"] == "unavailable"
    assert missing["rows"] == []
    assert [row["dimension"] for row in bounded["rows"]] == ["gender"]


def test_fairness_uses_position_to_score_group_ndcg():
    """Changing the displayed row order must not change an explicitly ranked slate."""
    result = compute_fairness(
        pd.DataFrame([
            {"gender": "a", "request_id": "r1", "position": 2, "label": 0.0},
            {"gender": "a", "request_id": "r1", "position": 0, "label": 2.0},
            {"gender": "a", "request_id": "r1", "position": 1, "label": 1.0},
        ]),
        min_support=3,
        dimensions=("gender",),
    )

    group = result["rows"][0]["groups"][0]
    assert group["ndcg"] == 1.0
    assert group["ndcg_evaluated_slate_count"] == 1


def test_safety_keeps_missing_filter_signals_unavailable_instead_of_zero():
    """Unsafe labels do not prove that no candidate was filtered by policy."""
    row = compute_safety(pd.DataFrame([{"unsafe_label": False}]))["rows"][0]

    assert row["filter_decision_rate"] is None
    assert row["unknown_share"] is None
    assert all(count is None for count in row["reason_counts"].values())


def test_safety_keeps_missing_unsafe_labels_unavailable_and_bounds_unknown_reasons():
    """Absent labels remain null and arbitrary filter values collapse to unknown."""
    row = compute_safety(pd.DataFrame([{"filter_reason": "new_unreviewed_reason"}]))["rows"][0]

    assert row["unsafe_exposure_rate"] is None
    assert row["unsafe_label_coverage"] == 0.0
    assert row["reason_counts"] == {
        "expired": 0,
        "muted_product_type": 0,
        "muted_genre": 0,
        "muted_keyword": 0,
        "muted_title": 0,
        "unknown": 1,
    }
    assert row["unknown_share"] == pytest.approx(1.0)
