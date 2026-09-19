import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
sys.path.insert(0, str(Path(__file__).parents[2] / "frontend"))


def _df(pd):
    # 3 impressions: item_2 shown twice (1 click), item_1 shown once (clicked+ordered)
    return pd.DataFrame({
        "user_id":    ["u1", "u2", "u1"],
        "session_id": ["s1", "s2", "s1"],
        "item_id":    ["item_2", "item_2", "item_1"],
        "label":      [1.0, 0.0, 2.0],
        "genres":     [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })


def test_load_samples_normalizes_columns(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash

    parquet = tmp_path / "samples"
    _df(pd).to_parquet(parquet, index=False)

    out = dash.load_samples(str(parquet))
    assert list(out["clicked"]) == [1, 0, 1]          # derived from label >= 1
    assert out["genres"].apply(type).eq(list).all()
    assert dash.query_of(["Sci-Fi", "Action"]) == "Sci-Fi Action"
    assert dash.query_of([]) == "unknown"


def test_load_samples_rejects_empty_parquet_directory(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash

    samples = tmp_path / "samples"
    samples.mkdir()

    with pytest.raises(ValueError, match="No training samples found"):
        dash.load_samples(str(samples))


def test_load_samples_enriches_empty_genres_from_redis(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u2"],
        "session_id": ["s1", "s2"],
        "item_id": ["item_1", "item_2"],
        "label": [1.0, 0.0],
        "genres": [[], ["Drama"]],
    }).to_parquet(parquet, index=False)
    monkeypatch.setattr(genre_meta, "fetch_movie_meta", lambda host, port: [
        {"item_id": "item_1", "genres": ["Sci-Fi", "Action"]},
        {"item_id": "item_2", "genres": ["Comedy"]},
    ])

    out = dash.load_samples(str(parquet), "redis.test", 6380)

    assert out["genres"].tolist() == [["Sci-Fi", "Action"], ["Drama"]]


def test_load_slates_normalizes_items_and_fills_catalog_signals(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import feature_derivations as genre_meta
    import ranking_eval_report as ranking

    experiences = tmp_path / "experiences"
    pd.DataFrame([{
        "request_id": "r1", "user_id": "u1",
        "items": [
            {"position": 0, "item_id": "item_1", "label": 2.0},
            {"position": 1, "item_id": "item_2", "label": 0.0,
             "genres": ["Comedy"], "popularity": 5.0},
        ],
    }]).to_parquet(experiences, index=False)
    monkeypatch.setattr(genre_meta, "fetch_movie_meta",
                        lambda host, port: [{"item_id": "item_1", "genres": ["Drama"]}])
    monkeypatch.setattr(ranking, "fetch_popularity", lambda host, port: {"item_1": 100.0})

    items = dash.load_slates(str(experiences))["items"].iloc[0]
    # Slate items keep their own signals; the catalog only fills what the slate omits.
    assert [item["genres"] for item in items] == [["Drama"], ["Comedy"]]
    assert [item["popularity"] for item in items] == [100.0, 5.0]
    assert dash.load_slates(None) is None


def test_export_replaces_non_finite_values_with_null():
    pytest.importorskip("numpy")
    import export_dashboard_json as exporter

    safe = exporter._json_safe({"ctr": float("nan"), "latency": [float("inf"), 1.0], "n": 3})

    assert safe == {"ctr": None, "latency": [None, 1.0], "n": 3}
    json.dumps(safe, allow_nan=False)


def test_export_bounds_diversity_slate_rows_and_says_so():
    import export_dashboard_json as exporter

    rows = [{"scope": "aggregate"}] + [{"scope": "slate", "slate_id": f"r{i}"} for i in range(25)]
    bounded = exporter._bounded_slate_rows({"status": "available", "rows": rows, "warnings": []})

    assert len(bounded["rows"]) == exporter.SLATE_ROW_LIMIT + 1
    assert bounded["rows"][0]["scope"] == "aggregate"      # the full-support row is kept
    assert bounded["warnings"] == ["showing 10 of 25 slate rows; the aggregate covers all"]

    short = {"status": "available", "rows": rows[:3], "warnings": []}
    assert exporter._bounded_slate_rows(short) == short    # nothing dropped, nothing claimed


def test_compute_relevance_funnel_and_means(tmp_path):
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
    r = dash.compute_relevance(frame)
    assert r["funnel"] == {"impression": 3, "click": 2, "order": 1}
    assert r["ctr"] == round(2 / 3, 4) and r["cvr"] == round(1 / 3, 4)
    bq = {row["query"]: row["mean_score"] for _, row in r["by_query"].iterrows()}
    assert bq["Sci-Fi Action"] == 2.0            # single ordered impression
    assert bq["Drama"] == 0.5                     # labels 1.0 and 0.0


def test_compute_keyword_distribution_and_divergence():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
    r = dash.compute_keyword(frame)
    bk = {row["keyword"]: row for _, row in r["by_keyword"].iterrows()}
    assert bk["Drama"]["movie_impressions"] == 2     # two Drama impressions
    assert bk["Drama"]["query_clicks"] == 1          # one Drama click
    assert bk["Sci-Fi"]["query_clicks"] == 1         # item_1 clicked
    assert set(r["tops"]) == {"l1", "l2", "l3"}
    assert (r["tops"]["l2"]["rank"] >= 1).all()


def test_compute_query_top_and_length_buckets():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    })
    r = dash.compute_query(frame)
    tq = {row["query"]: row for _, row in r["top_queries"].iterrows()}
    assert tq["Drama"]["impressions"] == 2 and tq["Drama"]["ctr"] == 0.5
    assert tq["Sci-Fi Action"]["query_len"] == 13
    buckets = {row["bucket"]: row for _, row in r["by_length"].iterrows()}
    assert buckets["short (<=10)"]["impressions"] == 2   # "Drama" is 5 chars
    assert buckets["long (>10)"]["impressions"] == 1     # "Sci-Fi Action" is 13 chars


def test_ranking_uses_position_without_redis_signals():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    frame = pd.DataFrame({
        "user_id": ["u1", "u1"], "session_id": ["s1", "s1"],
        "item_id": ["item_1", "item_2"], "label": [1.0, 0.0],
        "clicked": [1, 0], "position": [0, 1],
        "genres": [["Drama"], ["Comedy"]],
    })
    # Recall still needs a Redis corpus, but ranking can evaluate position from Parquet.
    assert dash.compute_recall(frame, "localhost", 6399) is None
    ranking = dash.compute_ranking(frame, "localhost", 6399)
    rows = {row["signal"]: row for row in ranking["rows"]}
    assert rows["position"]["coverage"] == 1.0
    assert rows["position"]["n"] == 2
    assert rows["popularity"]["coverage"] == 0.0
    assert rows["embedding"]["coverage"] == 0.0





def test_compute_ope_returns_none_without_redis():
    import analysis_dashboard_report as dash
    # Nothing listening on 6399 -> load fails -> None (rendered as an N/A card).
    assert dash.compute_ope("localhost", 6399) is None


def test_compute_ope_evaluates_from_replay_events(monkeypatch):
    pytest.importorskip("numpy")
    import analysis_dashboard_report as dash
    import ope_support

    def _event(i):
        rel = (i % 10) / 10.0
        reward = 1.0 if rel >= 0.5 else 0.0
        return {"requestId": f"r{i}", "user": "u", "action": f"m{i}", "actionPosition": 0,
                "coldStart": False, "modelPredictions": {"relevance": rel},
                "reward": reward, "clicked": int(reward),
                "actionSpace": [{"item": f"m{i}", "coldStart": False, "impressions": i % 50,
                                 "clicks": int(reward), "modelPredictions": {"relevance": rel}}]}

    events = [_event(i) for i in range(120)]
    monkeypatch.setattr(ope_support, "load_from_redis", lambda *a, **k: events)
    r = dash.compute_ope("localhost", 6399, bootstrap_samples=20)
    policies = {row["policy"] for row in r["rows"]}
    assert "logging" in policies and "model:relevance" in policies
    assert "est AUC" in r["headline"] and r["calibration"]["n_test"] > 0
    # Every row carries bootstrap interval fields the section renderer reads.
    assert all("value_ci_low" in row and "lift_ci_high" in row for row in r["rows"])




def test_demographics_are_hoisted_from_user_features_within_the_allowlist():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "clicked": [1, 0],
        # dict shape (JSON input) and key/value-pair shape (pyarrow map) both occur
        "user_features": [
            {"gender": "female", "subscription": "premium", "email": "a@b.c"},
            [("gender", "male"), ("subscription", "free"), ("email", "d@e.f")],
        ],
    })

    hoisted = dash._with_demographic_columns(samples)

    assert list(hoisted["gender"]) == ["female", "male"]
    assert list(hoisted["subscription"]) == ["premium", "free"]
    assert "email" not in hoisted.columns          # outside DEFAULT_DIMENSIONS: never published
    assert "gender" not in samples.columns         # the input frame is not mutated


def test_demographic_hoisting_is_a_no_op_without_user_features():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    samples = pd.DataFrame({"user_id": ["u1"], "clicked": [1]})
    assert dash._with_demographic_columns(samples) is samples


def test_keyword_clicks_use_the_same_predicate_as_query_clicks():
    """An order logged without `clicked` still counts as a click everywhere.

    Keyword CTR and query CTR appear on the same page, so they must count the
    same events; `clicked` is an upstream column that may not track `label`.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    frame = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "item_id": ["item_1", "item_2"],
        "label": [2.0, 0.0],
        "clicked": [0, 0],
        "genres": [["Drama"], ["Drama"]],
    })

    keyword_clicks = int(dash.compute_keyword(frame)["by_keyword"]["query_clicks"].sum())
    query_clicks = int(dash.compute_query(frame)["top_queries"]["clicks"].sum())

    assert keyword_clicks == query_clicks == 1


def test_compute_relevance_publishes_clicks_orders_and_rates():
    """by_query and by_genre carry the rates the engagement tables display."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    result = dash.compute_relevance(_df(pd))

    by_query = {row["query"]: row for _, row in result["by_query"].iterrows()}
    drama = by_query["Drama"]
    assert (drama["impressions"], drama["clicks"], drama["orders"]) == (2, 1, 0)
    assert (drama["ctr"], drama["cvr"]) == (0.5, 0.0)

    scifi = by_query["Sci-Fi Action"]
    assert (scifi["impressions"], scifi["clicks"], scifi["orders"]) == (1, 1, 1)
    assert (scifi["ctr"], scifi["cvr"]) == (1.0, 1.0)

    by_genre = {row["genre"]: row for _, row in result["by_genre"].iterrows()}
    assert (by_genre["Drama"]["clicks"], by_genre["Drama"]["orders"]) == (1, 0)
    assert (by_genre["Action"]["clicks"], by_genre["Action"]["orders"]) == (1, 1)


def test_compute_keyword_publishes_relevance_and_rates():
    """The heatmap colours by mean_score and the Top-K selector sorts by it."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    result = dash.compute_keyword(_df(pd))
    rows = {row["keyword"]: row for _, row in result["by_keyword"].iterrows()}

    drama = rows["Drama"]
    assert (drama["movie_impressions"], drama["query_clicks"], drama["query_orders"]) == (2, 1, 0)
    assert drama["mean_score"] == 0.5
    assert (drama["ctr"], drama["cvr"]) == (0.5, 0.0)

    for row in result["by_keyword"].to_dict(orient="records"):
        assert row["mean_score"] is not None

    for level in ("l1", "l2", "l3"):
        assert "ctr" in result["tops"][level].columns


def test_compute_query_publishes_average_length_and_bucket_query_counts():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    result = dash.compute_query(_df(pd))

    # "Drama" (5 chars) twice, "Sci-Fi Action" (13 chars) once.
    assert result["average_query_length"] == round((5 + 5 + 13) / 3, 2)

    buckets = {row["bucket"]: row for _, row in result["by_length"].iterrows()}
    assert buckets["short (<=10)"]["queries"] == 1
    assert buckets["long (>10)"]["queries"] == 1


def test_cvr_is_orders_per_impression_not_per_click():
    """The whole dashboard reads cvr as orders/impressions; the two differ here.

    4 impressions, 2 clicks, 1 order: orders/impressions is 0.25, orders/clicks
    would be 0.5. Every other fixture in this file is degenerate for that
    distinction, so this is the test that pins it.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    frame = pd.DataFrame({
        "user_id": ["u1", "u2", "u3", "u4"],
        "item_id": ["i1", "i2", "i3", "i4"],
        "label": [2.0, 1.0, 0.0, 0.0],
        "genres": [["Horror"], ["Horror"], ["Horror"], ["Horror"]],
    })

    engagement = dash.compute_relevance(frame)
    row = engagement["by_query"].iloc[0]
    assert (row["impressions"], row["clicks"], row["orders"]) == (4, 2, 1)
    assert row["ctr"] == 0.5
    assert row["cvr"] == 0.25

    keyword = dash.compute_keyword(frame)["by_keyword"].iloc[0]
    assert (keyword["movie_impressions"], keyword["query_clicks"], keyword["query_orders"]) == (4, 2, 1)
    assert keyword["ctr"] == 0.5
    assert keyword["cvr"] == 0.25


def test_keyword_shown_but_never_clicked_yields_zero_not_nan():
    """The single-groupby dist() must reproduce the old fillna(0) behaviour.

    A keyword with impressions and no clicks previously came back absent from the
    clicks groupby and was filled with 0. It must still be a real 0, and never NaN.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    frame = pd.DataFrame({
        "user_id": ["u1", "u2"],
        "item_id": ["i1", "i2"],
        "label": [1.0, 0.0],
        "genres": [["Drama"], ["Horror"]],
    })

    rows = {r["keyword"]: r for _, r in dash.compute_keyword(frame)["by_keyword"].iterrows()}
    horror = rows["Horror"]
    assert horror["movie_impressions"] == 1
    assert horror["query_clicks"] == 0
    assert horror["ctr"] == 0.0
    assert horror["cvr"] == 0.0
    assert horror["query_share"] == 0.0
    assert horror["mean_score"] == 0.0
    for field in ("query_clicks", "ctr", "cvr", "query_share", "divergence"):
        assert not pd.isna(horror[field]), f"{field} is NaN"


def test_compute_ranking_reports_a_null_positive_rate_when_nothing_was_scored():
    """A signal with no scored rows has no positive rate — not a rate of zero."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    frame = _df(pd).assign(position=[0, 1, 0])
    # Port 1 refuses connections, so popularity and embeddings come back empty.
    result = dash.compute_ranking(frame, "localhost", 1)

    rows = {row["signal"]: row for row in result["rows"]}
    assert rows["popularity"]["n"] == 0
    assert rows["popularity"]["positive_rate"] is None

    # `position` is derived from the frame, so it is always scorable.
    assert rows["position"]["positive_rate"] == round(2 / 3, 4)


def test_compute_keyword_grid_crosses_category_with_keyword():
    """The heatmap's axes. `keyword` is a genre; `category` is that genre's family.

    A row is exploded across its genres, so a film tagged Action and Comedy contributes
    to both keyword columns under its own category -- the same convention top_keywords
    uses, so the heatmap and the taxonomy tables agree.
    """
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    df = pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "session_id": ["s1", "s2", "s3"],
        "item_id": ["i1", "i2", "i3"],
        "label": [1.0, 0.0, 1.0],
        "genres": [["Action", "Comedy"], ["Action"], ["Documentary"]],
    })
    grid = dash.compute_keyword(df)["grid"]
    rows = {(r["category"], r["keyword"]): r for _, r in grid.iterrows()}

    # Action is the primary genre of rows 1 and 2, so their family is Action&Adventure.
    assert ("Action&Adventure", "Action") in rows
    assert ("Action&Adventure", "Comedy") in rows, "the exploded second genre must appear"
    assert ("Other", "Documentary") in rows, "Documentary's family is Other"

    action = rows[("Action&Adventure", "Action")]
    assert action["movie_impressions"] == 2 and action["query_clicks"] == 1
    assert action["ctr"] == 0.5


def test_compute_keyword_grid_is_not_rank_capped():
    """tops caps at ten per family for its tables; the grid must not, or the heatmap
    would show a truncated row and look like a measurement."""
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    genres = ["Action", "Adventure", "War", "Western", "Comedy", "Children",
              "Crime", "Thriller", "Mystery", "Film-Noir", "Horror", "Drama"]
    # Every row's primary genre is Action, so all twelve keywords land in one family.
    df = pd.DataFrame({
        "user_id": [f"u{i}" for i in range(len(genres))],
        "session_id": [f"s{i}" for i in range(len(genres))],
        "item_id": [f"i{i}" for i in range(len(genres))],
        "label": [1.0] * len(genres),
        "genres": [["Action", g] if g != "Action" else ["Action"] for g in genres],
    })
    grid = dash.compute_keyword(df)["grid"]
    in_family = grid[grid["category"] == "Action&Adventure"]
    assert len(in_family) > 10, f"expected more than ten keywords, got {len(in_family)}"
