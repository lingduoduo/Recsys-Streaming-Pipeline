import os
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))


def test_session_report_aggregates_multi_slate_sessions(tmp_path):
    """Integration: run session_report.py via $SPARK_HOME/bin/spark-submit on a tiny Parquet
    where one session spans two slates; assert per-session aggregation + the summary CSV."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import csv

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    # session sA (u1) spans 2 slates: req_1 (2 impr, 1 click), req_2 (2 impr, 1 click) → ctr 0.5
    # session sB (u2): 1 slate req_3 (2 impr, 0 click) → ctr 0.0
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "session_id": ["sA", "sA", "sA", "sA", "sB", "sB"],
        "user_id":    ["u1", "u1", "u1", "u1", "u2", "u2"],
        "request_id": ["req_1", "req_1", "req_2", "req_2", "req_3", "req_3"],
        "clicked":    [1, 0, 1, 0, 0, 0],
        "ordered":    [0, 0, 0, 0, 0, 0],
    }).to_parquet(parquet, index=False)

    script = Path(__file__).parents[2] / "services/python-modeling/session_report.py"
    out = tmp_path / "report"
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"), str(script),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300,
    )

    rows = {}
    for part in (out / "by_session").glob("part-*.csv"):
        for r in csv.DictReader(part.open()):
            rows[r["session_id"]] = r

    assert int(rows["sA"]["slates"]) == 2
    assert int(rows["sA"]["impressions"]) == 4
    assert int(rows["sA"]["clicks"]) == 2
    assert float(rows["sA"]["ctr"]) == pytest.approx(0.5)
    assert int(rows["sB"]["slates"]) == 1
    assert float(rows["sB"]["ctr"]) == pytest.approx(0.0)
