import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import segment_producer as sp  # noqa: E402


def _demo(cohort="existing", age="25-34", sex="F", edu="grad"):
    return {"cohort": cohort, "age_band": age, "sex": sex, "education": edu}


def test_click_prob_existing_beats_new():
    base = _demo(cohort="new")
    better = _demo(cohort="existing")
    assert sp.click_prob(better, "US", "ios") > sp.click_prob(base, "US", "ios")


def test_click_prob_platform_and_age_ordering():
    d = _demo()
    assert sp.click_prob(d, "US", "ios") > sp.click_prob(d, "US", "web")
    young, old = _demo(age="25-34"), _demo(age="55+")
    assert sp.click_prob(young, "US", "ios") > sp.click_prob(old, "US", "ios")


def test_click_prob_is_bounded():
    for cohort in ("new", "existing"):
        for age in sp.AGE_BANDS:
            for geo in sp.GEOS:
                for plat in sp.PLATFORMS:
                    p = sp.click_prob(_demo(cohort=cohort, age=age), geo, plat)
                    assert 0.02 <= p <= 0.95


def test_order_prob_existing_beats_new():
    assert sp.order_prob(_demo(cohort="existing"), "ios") > sp.order_prob(_demo(cohort="new"), "web")


def test_assign_demographics_is_deterministic_and_complete():
    import random
    a = sp.assign_demographics(50, random.Random(1))
    b = sp.assign_demographics(50, random.Random(1))
    assert a == b
    assert len(a) == 50
    for d in a.values():
        assert set(d) == {"cohort", "age_band", "sex", "education"}
        assert d["cohort"] in ("new", "existing")


def test_segment_report_ctr_by_cohort(tmp_path):
    """Integration: run segment_report.py via $SPARK_HOME/bin/spark-submit on a tiny Parquet."""
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import csv
    import os
    import subprocess

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    uf_ex = {"cohort": "existing", "age_band": "25-34", "sex": "F", "education": "grad"}
    uf_new = {"cohort": "new", "age_band": "18-24", "sex": "M", "education": "hs"}
    cf = {"geo": "US", "platform": "ios"}
    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u1", "u2", "u2"],
        "clicked": [1, 0, 1, 1],
        "ordered": [0, 0, 1, 0],
        "user_features": [uf_ex, uf_ex, uf_new, uf_new],
        "context_features": [cf, cf, cf, cf],
    }).to_parquet(parquet, index=False)

    script = Path(__file__).parents[2] / "services/python-modeling/segment_report.py"
    out = tmp_path / "report"
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"), str(script),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300,
    )

    csv_file = next((out / "by_cohort").glob("part-*.csv"))
    rows = {r["cohort"]: float(r["ctr"]) for r in csv.DictReader(csv_file.open())}
    assert rows["existing"] == pytest.approx(0.5)
    assert rows["new"] == pytest.approx(1.0)
