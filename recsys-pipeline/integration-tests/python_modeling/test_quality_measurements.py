import sys
from datetime import datetime, timezone
from pathlib import Path
import math

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

from quality_measurements import (  # noqa: E402
    compute_diversity,
    compute_freshness,
    compute_relevance,
    compute_satisfaction,
)


def test_relevance_uses_graded_gain_and_rank():
    """A perfect graded ordering receives maximum NDCG and first-rank MRR."""
    slates = pd.DataFrame([{
        "request_id": "r1",
        "items": [
            {"label": 2.0},
            {"label": 0.0},
            {"label": 1.0},
        ],
    }])

    result = compute_relevance(slates, ks=(3,))

    assert result["status"] == "available"
    assert result["rows"][0]["ndcg_at_k"] == pytest.approx(0.9639)
    assert result["rows"][0]["mrr_at_k"] == 1.0


def test_satisfaction_reports_optional_signal_coverage():
    """Only observed optional signals contribute to their published measures."""
    samples = pd.DataFrame([
        {"clicked": 1, "ordered": 0, "reward": 1.0, "rating": 5.0,
         "negative_feedback_reason": None, "dwell_millis": 1000, "completion_rate": 0.8},
        {"clicked": 0, "ordered": 0, "reward": 0.0, "rating": None,
         "negative_feedback_reason": "not_interested", "dwell_millis": None,
         "completion_rate": None},
    ])

    row = compute_satisfaction(samples)["rows"][0]

    assert row["ctr"] == 0.5
    assert row["mean_rating"] == 5.0
    assert row["rating_coverage"] == 0.5
    assert row["negative_feedback_rate"] == 0.5


def test_freshness_uses_timestamp_and_labels_boolean_fallback():
    """Timestamps take precedence; boolean labels are explicitly identified."""
    now = datetime(2026, 7, 30, tzinfo=timezone.utc)
    timestamped = pd.DataFrame([
        {"published_at": "2026-07-20T00:00:00Z", "new_release": False, "clicked": 1, "reward": 1.0},
        {"published_at": "2026-05-01T00:00:00Z", "new_release": True, "clicked": 0, "reward": 0.0},
    ])

    result = compute_freshness(timestamped, now, window_days=30)

    assert result["rows"][0]["freshness_source"] == "published_at"
    assert result["rows"][0]["fresh_share"] == 0.5

    fallback = compute_freshness(
        pd.DataFrame([{"new_release": True, "clicked": 1, "reward": 1.0}]),
        now,
        window_days=30,
    )

    assert fallback["rows"][0]["freshness_source"] == "boolean_new_release"
    assert fallback["rows"][0]["mean_content_age_days"] is None


def test_diversity_reports_entropy_jaccard_and_long_tail():
    """Disjoint genres and a low-popularity item are measured at slate level."""
    slates = pd.DataFrame([{
        "items": [
            {"genres": ["drama"], "popularity": 100.0},
            {"genres": ["comedy"], "popularity": 5.0},
        ]
    }])

    row = compute_diversity(slates, long_tail_percentile=0.80)["rows"][0]

    assert row["unique_genres_at_k"] == 2.0
    assert row["normalized_genre_entropy"] == 1.0
    assert row["intra_list_genre_distance"] == 1.0
    assert row["long_tail_exposure_share"] == 0.5


def test_relevance_keeps_zero_gain_slates_nullable_without_losing_other_slates():
    """Zero-gain slates lack NDCG while evaluable slates still contribute."""
    zero_only = pd.DataFrame([{"items": [{"label": 0.0}, {"label": 0.0}]}])
    mixed = pd.DataFrame([
        {"items": [{"label": 0.0}, {"label": 0.0}]},
        {"items": [{"label": 2.0}, {"label": 1.0}]},
    ])

    assert compute_relevance(zero_only, ks=(2,))["rows"][0]["ndcg_at_k"] is None
    row = compute_relevance(mixed, ks=(2,))["rows"][0]
    assert row["ndcg_at_k"] == 1.0
    assert row["ndcg_evaluated_slate_count"] == 1


def test_satisfaction_keeps_missing_optional_signals_unavailable():
    """Absent optional fields remain null rather than being fabricated as zeros."""
    row = compute_satisfaction(pd.DataFrame([{"clicked": 1, "ordered": 0, "reward": 1.0}]))["rows"][0]

    assert row["mean_rating"] is None
    assert row["rating_coverage"] == 0.0
    assert row["negative_feedback_rate"] is None
    assert row["negative_feedback_coverage"] == 0.0
    assert row["mean_dwell_millis"] is None
    assert row["dwell_coverage"] == 0.0
    assert row["mean_completion_rate"] is None
    assert row["completion_coverage"] == 0.0


def test_freshness_is_unavailable_without_timestamp_or_boolean_signal():
    """Freshness cannot infer a content age without either supported source."""
    result = compute_freshness(
        pd.DataFrame([{"clicked": 1, "reward": 1.0}]),
        datetime(2026, 7, 30, tzinfo=timezone.utc),
    )

    assert result["status"] == "unavailable"
    assert result["rows"] == []


def test_diversity_excludes_empty_genre_sets_from_pairwise_distance():
    """An empty genre list reduces coverage instead of adding a maximal distance."""
    row = compute_diversity(pd.DataFrame([{
        "items": [
            {"genres": [], "popularity": 1.0},
            {"genres": ["drama"], "popularity": 2.0},
        ],
    }]))["rows"][0]

    assert row["genre_coverage"] == 0.5
    assert row["intra_list_genre_distance"] is None


def test_diversity_reports_no_intra_list_distance_for_one_item_slate():
    """A single item has no pair from which to calculate a distance."""
    row = compute_diversity(pd.DataFrame([{
        "items": [{"genres": ["drama"], "popularity": 1.0}],
    }]))["rows"][0]

    assert row["intra_list_genre_distance"] is None


def test_relevance_aggregates_leave_one_out_folds_by_user():
    """Recall and hit rate are user averages, not averages over individual folds."""
    slates = pd.DataFrame([
        {"user_id": "u1", "items": [{"label": 1.0}, {"label": 0.0}]},
        {"user_id": "u1", "items": [{"label": 0.0}, {"label": 1.0}]},
        {"user_id": "u1", "items": [{"label": 0.0}, {"label": 1.0}]},
        {"user_id": "u2", "items": [{"label": 0.0}, {"label": 1.0}]},
    ])

    row = compute_relevance(slates, ks=(1,))["rows"][0]

    assert row["recall_at_k"] == pytest.approx(0.1667)
    assert row["hit_rate_at_k"] == 0.5
    assert row["evaluated_user_count"] == 2


def test_non_finite_numeric_values_are_unobserved_in_columns_and_labels():
    """NaN and infinities do not leak into JSON-facing metric values or coverage."""
    satisfaction = compute_satisfaction(pd.DataFrame({"clicked": [math.nan, math.inf, -math.inf, 1.0]}))
    relevance = compute_relevance(pd.DataFrame([
        {"items": [{"label": math.inf}]},
        {"items": [{"label": 1.0}]},
    ]), ks=(1,))

    assert satisfaction["rows"][0]["ctr"] == 1.0
    assert satisfaction["rows"][0]["ctr_coverage"] == 0.25
    row = relevance["rows"][0]
    assert row["ndcg_at_k"] == 1.0
    assert row["label_coverage"] == 0.5


def test_freshness_interprets_naive_now_as_utc():
    """A naive analysis clock has an explicit UTC policy instead of raising."""
    result = compute_freshness(
        pd.DataFrame([{"published_at": "2026-07-20T00:00:00Z"}]),
        datetime(2026, 7, 30),
    )

    assert result["status"] == "available"
    assert result["rows"][0]["mean_content_age_days"] == 10.0


@pytest.mark.parametrize(
    ("value", "expected_share"),
    [("False", 0.0), ("True", 1.0), (0, 0.0), (1, 1.0)],
)
def test_freshness_parses_only_documented_boolean_encodings(value, expected_share):
    """Object-typed boolean freshness values are parsed explicitly, not by truthiness."""
    result = compute_freshness(
        pd.DataFrame([{"new_release": value}]),
        datetime(2026, 7, 30, tzinfo=timezone.utc),
    )

    assert result["status"] == "available"
    assert result["rows"][0]["fresh_share"] == expected_share


def test_freshness_rejects_invalid_boolean_encoding():
    """Unknown boolean-like text is unavailable rather than silently classified fresh."""
    result = compute_freshness(
        pd.DataFrame([{"new_release": "sometimes"}]),
        datetime(2026, 7, 30, tzinfo=timezone.utc),
    )

    assert result["status"] == "unavailable"


def test_satisfaction_is_unavailable_without_an_observed_supported_signal():
    """Unrelated rows cannot masquerade as an observed satisfaction measurement."""
    result = compute_satisfaction(pd.DataFrame([{"unrelated": "value"}]))

    assert result["status"] == "unavailable"


def test_freshness_publishes_outcome_coverage_for_each_cohort():
    """Fresh and established outcome means state the support within their own cohorts."""
    result = compute_freshness(
        pd.DataFrame([
            {"published_at": "2026-07-29T00:00:00Z", "clicked": 1.0, "reward": 2.0},
            {"published_at": "2026-07-28T00:00:00Z", "clicked": None, "reward": None},
            {"published_at": "2026-06-01T00:00:00Z", "clicked": 0.0, "reward": 0.0},
            {"published_at": "2026-06-02T00:00:00Z", "clicked": None, "reward": 5.0},
        ]),
        datetime(2026, 7, 30, tzinfo=timezone.utc),
    )

    row = result["rows"][0]
    assert row["fresh_ctr_coverage"] == 0.5
    assert row["fresh_reward_coverage"] == 0.5
    assert row["established_ctr_coverage"] == 0.5
    assert row["established_reward_coverage"] == 1.0


def test_diversity_keeps_per_slate_rows_alongside_aggregate():
    """Callers can inspect each slate as well as the aggregate measurement."""
    result = compute_diversity(pd.DataFrame([
        {"request_id": "r1", "items": [{"genres": ["drama"], "popularity": 1.0}]},
        {"request_id": "r2", "items": [
            {"genres": ["comedy"], "popularity": 2.0},
            {"genres": ["action"], "popularity": 3.0},
        ]},
    ]))

    assert result["rows"][0]["scope"] == "aggregate"
    slate_rows = {row["slate_id"]: row for row in result["rows"][1:]}
    assert slate_rows["r1"]["scope"] == "slate"
    assert slate_rows["r1"]["unique_genres_at_k"] == 1.0
    assert slate_rows["r2"]["unique_genres_at_k"] == 2.0
