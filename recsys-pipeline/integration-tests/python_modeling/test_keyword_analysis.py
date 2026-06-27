import csv
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import movie_categories as mc  # noqa: E402


def test_secondary_genre():
    assert mc.secondary_genre(["Sci-Fi", "Action"]) == "Action"
    assert mc.secondary_genre(["Drama"]) == "none"
    assert mc.secondary_genre("Crime,Thriller") == "Thriller"
    assert mc.secondary_genre([]) == "none"


def _read_csv_dir(d: Path) -> list[dict]:
    rows = []
    for part in d.glob("part-*.csv"):
        rows.extend(csv.DictReader(part.open()))
    return rows


def test_keyword_analysis_report_via_spark_submit(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id":   ["u1", "u2", "u1"],
        "session_id": ["sess1", "sess2", "sess1"],
        "item_id":   ["item_1", "item_1", "item_2"],
        "clicked":   [1, 0, 1],
        "genres":    [["Sci-Fi", "Action"], ["Sci-Fi", "Action"], ["Drama"]],
        "release_year": [2015, 2015, 1995],
    }).to_parquet(parquet, index=False)

    out = tmp_path / "report"
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"),
         str(Path(__file__).parents[2] / "services/python-modeling/keyword_analysis_report.py"),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300, env={**os.environ, "REDIS_PORT": "6399"},
    )

    # Analysis 1: keyword distribution
    by_kw = {r["keyword"]: r for r in _read_csv_dir(out / "by_keyword")}
    assert int(by_kw["Sci-Fi"]["movie_impressions"]) == 2
    assert int(by_kw["Sci-Fi"]["distinct_movies"]) == 1
    assert int(by_kw["Sci-Fi"]["query_clicks"]) == 1
    assert int(by_kw["Drama"]["movie_impressions"]) == 1

    by_sub = {r["subkeyword"]: r for r in _read_csv_dir(out / "by_subkeyword")}
    assert int(by_sub["Action"]["movie_impressions"]) == 2
    assert int(by_sub["none"]["movie_impressions"]) == 1

    # Analysis 2: category top keywords (l1 = genre family)
    l1 = [r for r in _read_csv_dir(out / "top_keywords_l1")]
    scifi = {r["keyword"]: r for r in l1 if r["l1"] == "SciFi&Fantasy"}
    assert int(scifi["Sci-Fi"]["movie_impressions"]) == 2
    assert int(scifi["Sci-Fi"]["query_clicks"]) == 1
    assert int(scifi["Action"]["movie_impressions"]) == 2
