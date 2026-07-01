import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))


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
