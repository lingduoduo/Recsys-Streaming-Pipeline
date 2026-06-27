import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import backfill_producer as bp  # noqa: E402  (path set above)

START = datetime(2026, 6, 1)
END = START + timedelta(days=21)


def test_click_prob_weekend_evening_beats_weekday_morning():
    sat_eve = bp.click_prob(datetime(2026, 6, 6, 20), START, END)   # Sat 20:00
    mon_morn = bp.click_prob(datetime(2026, 6, 1, 8), START, END)   # Mon 08:00
    assert sat_eve > mon_morn


def test_click_prob_changepoint_drops_engagement():
    before = bp.click_prob(datetime(2026, 6, 1, 12), START, END)
    after = bp.click_prob(datetime(2026, 6, 20, 12), START, END)    # >= CHANGEPOINT_DAY=14
    assert after < before


def test_click_prob_is_bounded():
    for day in range(21):
        for hour in range(24):
            p = bp.click_prob(START + timedelta(days=day, hours=hour), START, END)
            assert 0.02 <= p <= 0.95


def test_slate_times_are_sorted_and_span_window():
    import random
    times = bp.slate_times(START, END, random.Random(1))
    assert times == sorted(times)
    assert times[0] >= START and times[-1] < END


def test_pyspark_report_daily_ctr(tmp_path):
    """Integration: run engagement_report_pyspark.py through the project's pinned Spark
    on a tiny Parquet and check the daily CTR CSV. Skipped if Spark isn't available."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import csv
    import os
    import subprocess

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    # Two impressions Mon (1 click), two Sat (2 clicks).
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "impression_time": pd.to_datetime([
            "2026-06-01 09:00", "2026-06-01 09:00",
            "2026-06-06 21:00", "2026-06-06 21:00",
        ]),
        "clicked": [1, 0, 1, 1],
    }).to_parquet(parquet, index=False,
                  coerce_timestamps="us", allow_truncated_timestamps=True)  # Spark 3.5 rejects NANOS

    script = Path(__file__).parents[2] / "services/python-modeling/engagement_report_pyspark.py"
    out = tmp_path / "report"
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"), str(script),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300,
    )

    csv_file = next((out / "ctr_daily").glob("part-*.csv"))
    rows = {r["day"]: float(r["ctr"]) for r in csv.DictReader(csv_file.open())}
    assert rows["2026-06-01"] == pytest.approx(0.5)
    assert rows["2026-06-06"] == pytest.approx(1.0)
