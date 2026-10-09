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
    # Counts over the 8-item catalog [0, 0, 0, 0, 1, 1, 2, 6]: Gini 56 / 80. Eight items have no
    # top decile (see test_diversity_top_decile_needs_ten_items).
    assert row["exposure_gini"] == 0.7
    # Distinct per user 4, 2, 1 over exposures 5, 2, 3.
    assert (row["median_items_per_user"], row["user_repeat_rate"]) == (2.0, 0.3)


def test_diversity_spread_is_none_without_the_identities_it_needs():
    slates = _catalog_slates().drop(columns=["user_id"])
    row = compute_diversity(slates)["rows"][0]

    assert row["catalog_size"] is None and row["catalog_coverage"] is None
    assert row["median_items_per_user"] is None and row["user_repeat_rate"] is None
    assert row["items_served"] == 4  # item spread still measurable


def test_diversity_publishes_per_slate_distributions():
    hist = compute_diversity(_catalog_slates())["distributions"]

    # Entropy: r1, r2, r3 are 1.0 (last bin); r4 is all-drama, 0.0.
    assert [b["count"] for b in hist["normalized_genre_entropy"]] == [1, 0, 0, 0, 0, 0, 0, 0, 0, 3]
    # Distance: r4 0.0, r1 0.6667, r2 and r3 1.0.
    assert [b["count"] for b in hist["intra_list_genre_distance"]] == [1, 0, 0, 0, 0, 0, 1, 0, 0, 2]
    assert hist["intra_list_genre_distance"][6]["bin_start"] == 0.6


def test_diversity_compares_genre_exposure_with_what_is_served():
    shares = compute_diversity(_catalog_slates())["genre_exposure"]

    # Exposures mention drama 7, comedy 3, action 1; distinct items drama 2, comedy 2, action 1.
    assert shares == [
        {"genre": "drama", "exposure_share": 0.6364, "served_share": 0.4},
        {"genre": "comedy", "exposure_share": 0.2727, "served_share": 0.4},
        {"genre": "action", "exposure_share": 0.0909, "served_share": 0.2},
    ]


def test_diversity_series_buckets_slates_by_request_time():
    result = compute_diversity(_catalog_slates())
    series = result["series"]

    # request_ts 10..13 -> four 1 s buckets, one slate each.
    assert result["series_bucket_seconds"] == 1.0 and [b["slates"] for b in series] == [1, 1, 1, 1]
    # Cutoff over distinct items is 34.0, so every item in r1 is in the tail.
    assert series[0] == {"bucket_start": 10.0, "slates": 1, "normalized_genre_entropy": 1.0,
                         "intra_list_genre_distance": 0.6667, "long_tail_exposure_share": 1.0}
    assert series[3]["normalized_genre_entropy"] == 0.0


def test_diversity_series_is_empty_without_request_times():
    result = compute_diversity(_catalog_slates().drop(columns=["request_ts"]))

    assert result["series"] == [] and result["series_bucket_seconds"] is None


def test_diversity_gini_counts_unserved_catalog_items():
    """Two items shown to everyone out of ten is maximally uneven, not perfectly even."""
    items = [{"item_id": "x", "genres": ["drama"], "popularity": 1.0},
             {"item_id": "y", "genres": ["comedy"], "popularity": 2.0}]
    slates = pd.DataFrame([{"request_id": f"r{i}", "user_id": f"u{i}", "items": items} for i in range(5)])
    row = compute_diversity(slates, catalog_size=10)["rows"][0]

    # Counts over the catalog [0 x 8, 5, 5]: Gini 80 / 100; over served items alone it read 0.0.
    assert (row["exposure_gini"], row["top_decile_exposure_share"]) == (0.8, 0.5)


def test_diversity_is_unavailable_without_genre_or_popularity_signals():
    slates = pd.DataFrame([{"request_id": "r1", "user_id": "u1", "items": [{"item_id": "a"}, {"item_id": "b"}]}])

    assert compute_diversity(slates)["warnings"] == ["missing genre and popularity diversity signals"]


def test_diversity_histograms_are_absent_not_zero_without_their_signal():
    slates = pd.DataFrame([{"request_id": "r1", "items": [{"item_id": "a", "popularity": 1.0},
                                                         {"item_id": "b", "popularity": 2.0}]}])
    hist = compute_diversity(slates)["distributions"]

    assert hist == {"normalized_genre_entropy": [], "intra_list_genre_distance": []}


def test_diversity_publishes_the_repeat_rate_uniform_serving_would_give():
    row = compute_diversity(_catalog_slates(), catalog_size=8)["rows"][0]

    # Users with 5, 2 and 3 exposures over 8 items expect 8 * (1 - (7/8)**E) distinct each.
    assert row["user_repeat_rate_uniform"] == 0.1588
    assert compute_diversity(_catalog_slates())["rows"][0]["user_repeat_rate_uniform"] is None


def test_series_ignore_infinite_timestamps():
    samples = pd.concat([_timed_samples(), pd.DataFrame([
        {"impression_ts": float("inf"), "user_id": "u9", "item_id": "i9", "clicked": 1, "ordered": 0}])],
        ignore_index=True)
    result = compute_satisfaction(samples)

    assert sum(b["impressions"] for b in result["series"]) == 4  # the inf row leaves the series only
    slates = _catalog_slates().astype({"request_ts": float})
    slates.loc[0, "request_ts"] = float("-inf")
    assert sum(b["slates"] for b in compute_diversity(slates)["series"]) == 3


def test_series_bucket_width_keeps_sub_second_precision():
    samples = pd.DataFrame([{"impression_ts": 0.25 * i, "clicked": 0} for i in range(5)])

    # A 1 s span of fractional stamps: 24 buckets of 1/24 s, not "0.0".
    assert compute_satisfaction(samples)["series_bucket_seconds"] == 0.042


def test_diversity_coverage_is_none_when_served_items_outnumber_the_catalog():
    row = compute_diversity(_catalog_slates(), catalog_size=3)["rows"][0]

    # Four served ids against a catalog of three: the populations disagree, so no ratio.
    assert row["items_served"] == 4 and row["catalog_coverage"] is None


def test_diversity_top_decile_needs_ten_items():
    assert compute_diversity(_catalog_slates(), catalog_size=8)["rows"][0]["top_decile_exposure_share"] is None


_T0 = int(pd.Timestamp("2026-07-30", tz="UTC").timestamp())


def _fresh_samples(ts=True):
    # Ages at exposure: m1 7 d (fresh, 0-7 d), m2 30 d (fresh, 8-30 d), m3 31 d, m4 575 d.
    rows = []
    for item, published, clicks in (("m1", "2026-07-23", (1, 1)), ("m2", "2026-06-30", (0, 1)),
                                    ("m3", "2026-06-29", (0, 0)), ("m4", "2025-01-01", (1, 0))):
        for clicked in clicks:
            rows.append({"item_id": item, "published_at": f"{published}T00:00:00Z", "clicked": clicked,
                         **({"impression_ts": _T0} if ts else {})})
    return pd.DataFrame(rows)


def test_freshness_ages_at_exposure_not_export():
    """Ageing against export time made a run exported 30 days later look entirely stale."""
    first = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))
    later = compute_freshness(_fresh_samples(), datetime(2026, 9, 1, tzinfo=timezone.utc))

    assert first["rows"] == later["rows"]
    row = first["rows"][0]
    assert (row["fresh_share"], row["median_content_age_days"], row["age_at_exposure_coverage"]) == (0.5, 30.5, 1.0)


def test_freshness_falls_back_to_export_time_without_impression_ts():
    now = datetime(2026, 7, 30, tzinfo=timezone.utc)
    assert compute_freshness(_fresh_samples(ts=False), now)["rows"][0]["age_at_exposure_coverage"] == 0.0
    mixed = _fresh_samples().astype({"impression_ts": float})
    mixed.loc[0, "impression_ts"] = float("nan")
    assert compute_freshness(mixed, now)["rows"][0]["age_at_exposure_coverage"] == 0.875


def test_freshness_ctr_gap_is_item_clustered():
    row = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))["rows"][0]

    # Fresh 3/4 over m1, m2; established 1/4 over m3, m4. Each cohort's clustered SE is
    # sqrt((0.5**2 + 0.5**2) / 4**2 * 2) = 0.25, so the gap's SE is 0.3536 and z = 0.5 / 0.3536.
    assert (row["fresh_ctr_diff"], row["fresh_ctr_diff_se"], row["fresh_ctr_diff_z"]) == (0.5, 0.3536, 1.41)
    no_ids = compute_freshness(_fresh_samples().drop(columns=["item_id"]), datetime(2026, 8, 1, tzinfo=timezone.utc))
    assert no_ids["rows"][0]["fresh_ctr_diff_z"] is None


def test_freshness_compares_exposure_with_supply():
    row = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))["rows"][0]

    # Two of four movies are fresh, and they take half the exposures: lift 1.0.
    assert (row["fresh_item_share"], row["fresh_exposure_lift"]) == (0.5, 1.0)


def test_freshness_age_bands_put_edges_inside_and_keep_empty_bands():
    bands = compute_freshness(_fresh_samples(), datetime(2026, 8, 1, tzinfo=timezone.utc))["age_bands"]

    assert [(b["band"], b["exposures"], b["items"]) for b in bands] == [
        ("0-7 d", 2, 1), ("8-30 d", 2, 1), ("31-90 d", 2, 1), ("91-365 d", 0, 0), ("> 1 y", 2, 1)]
    assert bands[3]["ctr"] is None and bands[0]["z"] is None  # empty band; one movie is untestable
    boolean_only = compute_freshness(pd.DataFrame([{"new_release": True}]), datetime(2026, 8, 1, tzinfo=timezone.utc))
    assert boolean_only["age_bands"] == []


def test_freshness_age_band_z_is_item_clustered_against_overall_ctr():
    rows = []
    for item, published, clicks in (("a", "2026-07-27", (1, 1)), ("b", "2026-07-26", (1, 0)),
                                    ("c", "2025-06-01", (0, 0)), ("d", "2025-03-01", (0, 1))):
        rows += [{"item_id": item, "published_at": f"{published}T00:00:00Z", "clicked": c, "impression_ts": _T0}
                 for c in clicks]
    result = compute_freshness(pd.DataFrame(rows), datetime(2026, 8, 1, tzinfo=timezone.utc))
    bands = {b["band"]: b for b in result["age_bands"]}

    # p0 = 4/8. Residuals about p0: a 2 - 1, b 1 - 1 -> se 0.3536, z (0.75 - 0.5) / se.
    assert bands["0-7 d"]["z"] == 0.71 and bands["> 1 y"]["z"] == -0.71


def test_freshness_parses_published_at_strings_in_mixed_formats():
    """Vectorised parsing guessed one format from the first row and dropped every other row."""
    samples = pd.DataFrame([
        {"published_at": "2026-07-23T00:00:00Z", "clicked": 1, "impression_ts": _T0},
        {"published_at": "2026-06-01", "clicked": 0, "impression_ts": _T0},
        {"published_at": "2026-07-01T00:00:00.5+02:00", "clicked": 0, "impression_ts": _T0},
    ])

    row = compute_freshness(samples, datetime(2026, 8, 1, tzinfo=timezone.utc))["rows"][0]
    assert row["freshness_coverage"] == 1.0


def test_freshness_treats_out_of_range_impression_ts_as_missing():
    """Milliseconds (or garbage) must not crash the export: age falls back to `now` for that row."""
    samples = _fresh_samples().astype({"impression_ts": float})
    samples.loc[0, "impression_ts"] = _T0 * 1000.0
    samples.loc[1, "impression_ts"] = 1e20

    row = compute_freshness(samples, datetime(2026, 7, 30, tzinfo=timezone.utc))["rows"][0]
    assert row["age_at_exposure_coverage"] == 0.75
