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
    _time_buckets,
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


def test_leave_one_out_counts_complete_zero_folds_and_all_miss_users():
    """Complete misses count as zero outcomes while incomplete folds remain unavailable."""
    result = compute_relevance(pd.DataFrame([
        {"user_id": "u1", "items": [{"label": 1.0}, {"label": 0.0}]},
        {"user_id": "u1", "items": [{"label": 1.0}, {"label": None}]},
        {"user_id": "u1", "items": [{"label": 0.0}, {"label": 0.0}]},
        {"user_id": "u2", "items": [{"label": 0.0}, {"label": 0.0}]},
    ]), ks=(1,))

    row = result["rows"][0]
    assert row["recall_at_k"] == 0.25
    assert row["hit_rate_at_k"] == 0.5
    assert row["evaluated_user_count"] == 2
    assert row["leave_one_out_fold_count"] == 3


def test_freshness_accepts_homogeneous_and_nullable_pandas_boolean_columns():
    """Native pandas boolean scalars are valid fallbacks, while pd.NA stays missing."""
    now = datetime(2026, 7, 30, tzinfo=timezone.utc)
    homogeneous = compute_freshness(
        pd.DataFrame({"new_release": pd.Series([True, False], dtype=bool)}), now
    )
    nullable = compute_freshness(
        pd.DataFrame({"new_release": pd.Series([True, False, pd.NA], dtype="boolean")}), now
    )

    assert homogeneous["status"] == "available"
    assert homogeneous["rows"][0]["fresh_share"] == 0.5
    assert nullable["status"] == "available"
    assert nullable["rows"][0]["fresh_share"] == 0.5
    assert nullable["rows"][0]["freshness_coverage"] == pytest.approx(0.6667)


@pytest.mark.parametrize("value", ["yes", "1.0", 2, -1])
def test_freshness_rejects_arbitrary_truthy_boolean_encodings(value):
    """Only documented boolean encodings are accepted by the fallback parser."""
    result = compute_freshness(
        pd.DataFrame([{"new_release": value}]),
        datetime(2026, 7, 30, tzinfo=timezone.utc),
    )

    assert result["status"] == "unavailable"


def _timed_samples():
    # Four samples over a 24-second span of whole seconds.
    return pd.DataFrame([
        {"impression_ts": 100, "user_id": "u1", "item_id": "i1", "clicked": 1, "ordered": 1, "rating": 4.0},
        {"impression_ts": 100, "user_id": "u2", "item_id": "i2", "clicked": 0, "ordered": 0, "rating": None},
        {"impression_ts": 105, "user_id": "u1", "item_id": "i2", "clicked": 1, "ordered": 0, "rating": None},
        {"impression_ts": 124, "user_id": "u3", "item_id": "i3", "clicked": 0, "ordered": 1, "rating": 5.0},
    ])


def test_satisfaction_counts_the_population_behind_its_averages():
    row = compute_satisfaction(_timed_samples())["rows"][0]

    assert (row["users"], row["items"], row["rated_samples"]) == (3, 3, 2)


def test_satisfaction_population_is_none_not_zero_without_the_columns():
    row = compute_satisfaction(pd.DataFrame([{"clicked": 1}]))["rows"][0]

    assert row["users"] is None and row["items"] is None
    assert row["rated_samples"] == 0


def test_satisfaction_series_buckets_the_observed_span():
    # 25 whole seconds (100..124) -> 2 s buckets, 13 of them: ts 100 in 0, 105 in 2, 124 in 12.
    result = compute_satisfaction(_timed_samples())
    series = result["series"]

    assert len(series) == 13 and result["series_bucket_seconds"] == 2.0
    assert series[0] == {"bucket_start": 100.0, "impressions": 2, "users": 2, "ctr": 0.5,
                         "order_rate": 0.5, "mean_rating": 4.0, "ratings": 1}
    # A rated-free bucket has no mean rating -- not a mean of 0.
    assert series[2] == {"bucket_start": 104.0, "impressions": 1, "users": 1, "ctr": 1.0,
                         "order_rate": 0.0, "mean_rating": None, "ratings": 0}
    # Empty buckets stay, so the x-axis is evenly spaced.
    assert series[5] == {"bucket_start": 110.0, "impressions": 0, "users": 0, "ctr": None,
                         "order_rate": None, "mean_rating": None, "ratings": 0}
    # The maximum timestamp lands in the last bucket.
    assert series[12]["impressions"] == 1 and series[12]["mean_rating"] == 5.0


def test_satisfaction_series_skips_unparseable_timestamps_only():
    samples = pd.concat([_timed_samples(), pd.DataFrame([
        {"impression_ts": None, "user_id": "u9", "item_id": "i9", "clicked": 1, "ordered": 0},
        {"impression_ts": "not-a-time", "user_id": "u9", "item_id": "i9", "clicked": 1, "ordered": 0},
    ])], ignore_index=True)
    result = compute_satisfaction(samples)

    assert sum(b["impressions"] for b in result["series"]) == 4
    assert result["rows"][0]["users"] == 4  # the summary still counts every sample


@pytest.mark.parametrize("frame", [
    pd.DataFrame([{"clicked": 1}, {"clicked": 0}]),
    pd.DataFrame([{"clicked": 1, "impression_ts": 7}, {"clicked": 0, "impression_ts": 7}]),
])
def test_satisfaction_series_is_empty_without_a_span(frame):
    result = compute_satisfaction(frame)

    assert result["series"] == [] and result["series_bucket_seconds"] is None


def test_satisfaction_rating_distribution_keeps_empty_half_point_bins():
    samples = pd.DataFrame([{"clicked": 1, "rating": r} for r in (3.0, 3.2, 4.9, 5.0)])

    assert compute_satisfaction(samples)["rating_distribution"] == [
        {"rating": 3.0, "count": 2}, {"rating": 3.5, "count": 0},
        {"rating": 4.0, "count": 0}, {"rating": 4.5, "count": 2}]


def test_satisfaction_without_ratings_has_no_distribution():
    result = compute_satisfaction(pd.DataFrame([{"clicked": 1, "impression_ts": 1},
                                                {"clicked": 0, "impression_ts": 2}]))

    assert result["rating_distribution"] == []
    assert all(b["mean_rating"] is None for b in result["series"])


def test_satisfaction_series_whole_second_stamps_fill_buckets_evenly():
    # A steady stream: 10 impressions in each of 67 whole seconds, as the joiner publishes them.
    # A 66/24 = 2.75 s bucket would hold 2 or 3 of those seconds -- a sawtooth from width alone.
    samples = pd.DataFrame([{"impression_ts": 1000 + s, "clicked": 0} for s in range(67) for _ in range(10)])
    result = compute_satisfaction(samples)
    counts = [b["impressions"] for b in result["series"]]

    assert result["series_bucket_seconds"] == 3.0 and len(counts) == 23
    assert set(counts[:-1]) == {30} and counts[-1] == 10  # only the last bucket is partial


def test_satisfaction_series_fractional_stamps_keep_24_equal_buckets():
    samples = pd.DataFrame([{"impression_ts": 0.5 * i, "clicked": 0} for i in range(49)])
    result = compute_satisfaction(samples)

    assert len(result["series"]) == 24 and result["series_bucket_seconds"] == 1.0
    assert result["series"][-1]["impressions"] == 3  # 23.0, 23.5 and the maximum, 24.0


def test_time_buckets_use_whole_second_widths_and_need_a_span():
    index, starts, width = _time_buckets(pd.Series([0, 1, 65, 66]))

    assert width == 3.0 and len(starts) == 23
    assert list(index) == [0, 0, 21, 22]
    assert _time_buckets(pd.Series([7, 7])) is None
    assert _time_buckets(pd.Series([], dtype=float)) is None


def test_diversity_long_tail_cutoff_counts_each_item_once():
    """An exposure-weighted quantile puts ~percentile of exposures below it by construction."""
    items = ([{"item_id": "low", "popularity": 10.0}] * 8
             + [{"item_id": "mid", "popularity": 50.0}, {"item_id": "high", "popularity": 100.0}])
    row = compute_diversity(pd.DataFrame([{"request_id": "r1", "items": items}]))["rows"][0]

    # Over exposures the cutoff would be 18.0 and the share 0.8; over distinct items it is 80.0.
    assert row["long_tail_popularity_cutoff"] == 80.0
    assert row["long_tail_exposure_share"] == 0.9


_GENRES = {"a": ["drama"], "b": ["comedy"], "c": ["drama", "comedy"], "d": ["action"]}
_POPULARITY = {"a": 10.0, "b": 20.0, "c": 30.0, "d": 40.0}


def _catalog_slates():
    # Exposures a:6, b:2, c:1, d:1. u1 sees `a` twice; u3 sees nothing but `a`.
    def item(key):
        return {"item_id": key, "genres": _GENRES[key], "popularity": _POPULARITY[key]}
    return pd.DataFrame([
        {"request_id": "r1", "user_id": "u1", "request_ts": 10, "items": [item("a"), item("b"), item("c")]},
        {"request_id": "r2", "user_id": "u1", "request_ts": 11, "items": [item("a"), item("d")]},
        {"request_id": "r3", "user_id": "u2", "request_ts": 12, "items": [item("a"), item("b")]},
        {"request_id": "r4", "user_id": "u3", "request_ts": 13, "items": [item("a"), item("a"), item("a")]},
    ])


def test_diversity_measures_spread_across_the_catalog_and_each_user():
    row = compute_diversity(_catalog_slates(), catalog_size=8)["rows"][0]

    assert (row["items_served"], row["catalog_size"], row["catalog_coverage"]) == (4, 8, 0.5)
    # Sorted counts [1, 1, 2, 6]: Gini 16 / 40; the top decile is the single top item.
    assert (row["exposure_gini"], row["top_decile_exposure_share"]) == (0.4, 0.6)
    # Distinct per user 4, 2, 1 over exposures 5, 2, 3.
    assert (row["median_items_per_user"], row["user_repeat_rate"]) == (2.0, 0.3)


def test_diversity_spread_is_none_without_the_identities_it_needs():
    slates = _catalog_slates().drop(columns=["user_id"])
    row = compute_diversity(slates)["rows"][0]

    assert row["catalog_size"] is None and row["catalog_coverage"] is None
    assert row["median_items_per_user"] is None and row["user_repeat_rate"] is None
    assert row["items_served"] == 4  # item spread still measurable
