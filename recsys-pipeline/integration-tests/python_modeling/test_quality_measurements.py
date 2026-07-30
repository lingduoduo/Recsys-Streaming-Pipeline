import sys
from datetime import datetime, timezone
from pathlib import Path

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
