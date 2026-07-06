import os
import subprocess
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


def test_load_samples_enriches_empty_genres_from_redis(tmp_path, monkeypatch):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import analysis_dashboard_report as dash
    import genre_meta

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


def test_renderers_emit_svg_and_tables():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash
    bar = dash.svg_bar(["A", "B"], [1.0, 3.0], title="t")
    assert "<svg" in bar and "<rect" in bar and "<title>" in bar
    line = dash.svg_line([5, 10], {"bm25": [0.1, 0.2]}, title="recall")
    assert "<svg" in line and "<polyline" in line
    tbl = dash.html_table(pd.DataFrame({"k": [1], "v": [2]}))
    assert "<table" in tbl and "<th>k</th>" in tbl and "<td>2</td>" in tbl
    page = dash.render_html("Dashboard", [dash.section("S", "head", "body"),
                                          dash.na_card("Recall", "no corpus")])
    assert "<html" in page and "Dashboard" in page and "no corpus" in page


def test_render_html_uses_modern_product_analytics_structure():
    pd = pytest.importorskip("pandas")
    import analysis_dashboard_report as dash

    table = dash.html_table(pd.DataFrame([{"metric": "ctr", "value": 0.42}]))
    page = dash.render_html("Analysis Dashboard", [
        dash.section("Engagement", "CTR 42%", table),
        dash.na_card("Ranking", "no embeddings"),
    ])

    for marker in ('<meta name="viewport"', 'class="page-shell"',
                   'class="hero"', 'class="report-card"',
                   'class="insight"', 'class="table-shell"',
                   'class="report-card status-card"'):
        assert marker in page


def test_render_html_embeds_responsive_visual_system():
    import analysis_dashboard_report as dash

    page = dash.render_html("Dashboard", [dash.section("S", "H", "B")])
    bar = dash.svg_bar(["click"], [12], title="Funnel")
    line = dash.svg_line([5, 10], {"hybrid": [0.2, 0.4]}, title="Recall")

    assert "--canvas:#f5f7fb" in page
    assert "--indigo:#4f46e5" in page
    assert "@media (max-width:700px)" in page
    assert "prefers-reduced-motion:reduce" in page
    assert 'class="chart"' in bar and 'rx="6"' in bar
    assert 'class="chart"' in line and "#4f46e5" in line


def test_main_writes_recall_na_and_position_ranking_without_redis(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u2", "u1"], "session_id": ["s1", "s2", "s1"],
        "item_id": ["item_2", "item_2", "item_1"], "label": [1.0, 0.0, 2.0],
        "clicked": [1, 0, 1], "genres": [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    }).to_parquet(parquet, index=False)

    out = tmp_path / "report-dashboard"
    script = Path(__file__).parents[2] / "services/python-modeling/analysis_dashboard_report.py"
    subprocess.run([sys.executable, str(script), "--input", str(parquet), "--outdir", str(out)],
                   check=True, capture_output=True, timeout=120,
                   env={**os.environ, "REDIS_PORT": "6399"})

    page = (out / "index.html").read_text()
    assert "Engagement funnel" in page and "Keyword gap" in page and "Query intent" in page
    assert "N/A — no movie:*:features in Redis" in page
    assert "<h2>Ranking</h2>" in page
    assert "<td>position</td>" in page
