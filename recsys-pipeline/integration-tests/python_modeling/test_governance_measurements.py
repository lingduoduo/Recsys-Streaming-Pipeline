"""Integration tests for offline fairness and safety measurement calculators."""

import sys
from pathlib import Path
import math

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


def test_fairness_groups_publish_absolute_gaps_from_the_overall_metrics():
    """Each eligible group must be directly comparable to its dimension overall."""
    result = compute_fairness(
        pd.DataFrame([
            {"gender": "a", "request_id": "a", "position": 0, "label": 2.0,
             "clicked": 1, "ordered": 0, "reward": 2.0},
            {"gender": "a", "request_id": "a", "position": 1, "label": 0.0,
             "clicked": 1, "ordered": 0, "reward": 2.0},
            {"gender": "b", "request_id": "b", "position": 0, "label": 0.0,
             "clicked": 0, "ordered": 1, "reward": 0.0},
            {"gender": "b", "request_id": "b", "position": 1, "label": 2.0,
             "clicked": 0, "ordered": 1, "reward": 0.0},
        ]),
        min_support=2,
        dimensions=("gender",),
    )

    groups = {group["group"]: group for group in result["rows"][0]["groups"]}
    assert groups["a"]["ctr_absolute_gap_from_overall"] == 0.5
    assert groups["a"]["order_rate_absolute_gap_from_overall"] == 0.5
    assert groups["a"]["mean_reward_absolute_gap_from_overall"] == 1.0
    assert groups["a"]["ndcg_absolute_gap_from_overall"] == pytest.approx(0.1845351)
    assert groups["b"]["ndcg_absolute_gap_from_overall"] == pytest.approx(0.1845351)


def test_fairness_group_gaps_stay_unavailable_when_the_overall_signal_is_missing():
    """A missing outcome cannot be represented as a zero gap from the overall."""
    result = compute_fairness(
        pd.DataFrame([
            {"gender": "a", "clicked": None},
            {"gender": "a", "clicked": None},
            {"gender": "b", "clicked": None},
            {"gender": "b", "clicked": None},
        ]),
        min_support=2,
        dimensions=("gender",),
    )

    assert all(
        group["ctr_absolute_gap_from_overall"] is None
        for group in result["rows"][0]["groups"]
    )


def test_fairness_does_not_claim_parity_from_one_measurable_group():
    """Disparity requires two groups with observed values after suppression."""
    single_group = compute_fairness(
        pd.DataFrame([{"gender": "a", "clicked": 1}]),
        min_support=1,
        dimensions=("gender",),
    )["rows"][0]
    one_observed_group = compute_fairness(
        pd.DataFrame([
            {"gender": "a", "clicked": 1},
            {"gender": "a", "clicked": 1},
            {"gender": "b", "clicked": None},
            {"gender": "b", "clicked": None},
        ]),
        min_support=2,
        dimensions=("gender",),
    )["rows"][0]

    for row in (single_group, one_observed_group):
        assert row["ctr_max_min_gap"] is None
        assert row["ctr_disparity_ratio"] is None


def test_fairness_rejects_extreme_labels_without_crashing_ndcg():
    """Out-of-domain finite relevance labels leave NDCG unavailable, not fatal."""
    result = compute_fairness(
        pd.DataFrame([{
            "gender": "a", "request_id": "r1", "position": 0, "label": 1024.0,
        }]),
        min_support=1,
        dimensions=("gender",),
    )

    row = result["rows"][0]
    assert row["overall_ndcg"] is None
    assert row["groups"][0]["ndcg"] is None


def test_fairness_accepts_only_boolean_or_zero_one_outcome_encodings():
    """Out-of-range and non-finite click/order telemetry is excluded from coverage."""
    nullable_boolean = compute_fairness(
        pd.DataFrame({
            "gender": ["a", "a", "a", "a"],
            "clicked": pd.array([True, False, pd.NA, True], dtype="boolean"),
            "ordered": pd.array([False, True, pd.NA, False], dtype="boolean"),
        }),
        min_support=4,
        dimensions=("gender",),
    )["rows"][0]["groups"][0]
    numeric = compute_fairness(
        pd.DataFrame({
            "gender": ["a"] * 6,
            "clicked": [0, 1, 2, -1, math.inf, math.nan],
            "ordered": [1, 0, -1, 2, -math.inf, math.nan],
        }),
        min_support=6,
        dimensions=("gender",),
    )["rows"][0]["groups"][0]

    assert nullable_boolean["ctr"] == pytest.approx(2 / 3)
    assert nullable_boolean["ctr_coverage"] == pytest.approx(3 / 4)
    assert nullable_boolean["order_rate"] == pytest.approx(1 / 3)
    assert nullable_boolean["order_coverage"] == pytest.approx(3 / 4)
    assert numeric["ctr"] == 0.5
    assert numeric["ctr_coverage"] == pytest.approx(2 / 6)
    assert numeric["order_rate"] == 0.5
    assert numeric["order_coverage"] == pytest.approx(2 / 6)


@pytest.mark.parametrize(
    ("samples", "expected_coverage"),
    [
        (pd.DataFrame([{"unsafe_label": None}, {"unsafe_label": None}]), 0.0),
        (pd.DataFrame([{"unsafe_label": True}, {"unsafe_label": None}]), 0.25),
        (pd.DataFrame([{"filter_reason": None}, {"filter_reason": None}]), 0.5),
        (pd.DataFrame([
            {"filter_reason": None, "unsafe_label": False},
            {"filter_reason": "expired", "unsafe_label": True},
        ]), 1.0),
    ],
)
def test_safety_coverage_uses_observed_label_fraction_and_filter_log_availability(samples, expected_coverage):
    """Envelope coverage is the mean of complete filter logging and label coverage."""
    assert compute_safety(samples)["coverage"] == pytest.approx(expected_coverage)
