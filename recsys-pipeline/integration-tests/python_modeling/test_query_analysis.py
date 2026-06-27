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


def test_query_analysis_report_via_spark_submit(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    # query "Drama" (5 chars → short): 2 impressions, 1 click, 0 orders → ctr 0.5, cvr 0.0
    # query "Sci-Fi Action" (13 chars → long): 1 impression, 1 click, 1 order → ctr 1.0, cvr 1.0
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id":   ["u1", "u2", "u1"],
        "session_id": ["s1", "s2", "s1"],
        "item_id":   ["item_2", "item_2", "item_1"],
        "clicked":   [1, 0, 1],
        "ordered":   [0, 0, 1],
        "label":     [1.0, 0.0, 2.0],
        "genres":    [["Drama"], ["Drama"], ["Sci-Fi", "Action"]],
    }).to_parquet(parquet, index=False)

    out = tmp_path / "report"
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"),
         str(Path(__file__).parents[2] / "services/python-modeling/query_analysis_report.py"),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300, env={**os.environ, "REDIS_PORT": "6399"},
    )

    # Analysis 1 — most common queries
    top = {r["query"]: r for r in _read_csv_dir(out / "top_queries")}
    assert int(top["Drama"]["impressions"]) == 2
    assert int(top["Drama"]["clicks"]) == 1
    assert int(top["Drama"]["query_len"]) == 5
    assert int(top["Sci-Fi Action"]["impressions"]) == 1
    assert int(top["Sci-Fi Action"]["query_len"]) == 13

    # Analysis 2 — short vs long engagement
    buckets = {r["query_length"]: r for r in _read_csv_dir(out / "by_query_length")}
    assert float(buckets["short (<=10)"]["ctr"]) == pytest.approx(0.5)
    assert float(buckets["short (<=10)"]["cvr"]) == pytest.approx(0.0)
    assert float(buckets["long (>10)"]["ctr"]) == pytest.approx(1.0)
    assert float(buckets["long (>10)"]["cvr"]) == pytest.approx(1.0)
