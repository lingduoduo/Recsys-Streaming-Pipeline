import csv
import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))


def _read_csv_dir(d: Path) -> list[dict]:
    rows = []
    for part in d.glob("part-*.csv"):
        rows.extend(csv.DictReader(part.open()))
    return rows


def test_relevance_analysis_report_via_spark_submit(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    # 4 impressions: ordered(2.0), clicked(1.0), impression_only(0.0)x2
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "item_id": ["item_1", "item_2", "item_3", "item_4"],
        "label":   [2.0, 1.0, 0.0, 0.0],
        "genres":  [["Sci-Fi", "Action"], ["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    }).to_parquet(parquet, index=False)

    out = tmp_path / "report"
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"),
         str(Path(__file__).parents[2] / "services/python-modeling/relevance_analysis_report.py"),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300, env={**os.environ, "REDIS_PORT": "6399"},
    )

    # Analysis 1 — relevance-state distribution
    by_state = {r["relevance_state"]: r for r in _read_csv_dir(out / "by_state")}
    assert float(by_state["impression_only"]["proportion"]) == pytest.approx(0.5)
    assert float(by_state["clicked"]["proportion"]) == pytest.approx(0.25)
    assert float(by_state["ordered"]["proportion"]) == pytest.approx(0.25)

    # Analysis 2 — by query (genre-combo)
    by_query = {r["query"]: r for r in _read_csv_dir(out / "by_query")}
    assert float(by_query["Sci-Fi Action"]["mean_score"]) == pytest.approx(1.0)
    assert float(by_query["Sci-Fi Action"]["ordered_share"]) == pytest.approx(0.5)
    assert float(by_query["Drama"]["mean_score"]) == pytest.approx(0.5)

    # Analysis 2 — by movie genre (exploded)
    by_genre = {r["genre"]: r for r in _read_csv_dir(out / "by_genre")}
    assert float(by_genre["Drama"]["mean_score"]) == pytest.approx(0.5)
    assert float(by_genre["Sci-Fi"]["mean_score"]) == pytest.approx(1.0)
    assert int(by_genre["Action"]["impressions"]) == 2
