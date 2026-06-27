import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import segment_features as sf  # noqa: E402
import movielens_segment_producer as mp  # noqa: E402


# ── segment_features (pure derivations) ─────────────────────────────────────────
def test_derive_age_band():
    assert sf.derive_age_band(20) == "18-24"
    assert sf.derive_age_band(30) == "25-34"
    assert sf.derive_age_band(60) == "55+"
    assert sf.derive_age_band("33") == "25-34"
    assert sf.derive_age_band(None) == "unknown"


def test_derive_geo_by_zip_first_digit():
    assert sf.derive_geo("90210") == "West"
    assert sf.derive_geo("02139") == "Northeast"
    assert sf.derive_geo("70001") == "South-Central"
    assert sf.derive_geo("") == "unknown"


# ── producer ground-truth model ─────────────────────────────────────────────────
def _demo(age=30, gender="F", occupation="student", zip_code="90210"):
    return {"age": age, "gender": gender, "occupation": occupation, "zip_code": zip_code}


def test_click_prob_age_platform_occupation_geo_orderings():
    assert mp.click_prob(_demo(age=30), "ios") > mp.click_prob(_demo(age=60), "ios")      # 25-34 > 55+
    assert mp.click_prob(_demo(), "ios") > mp.click_prob(_demo(), "web")                   # ios > web
    assert mp.click_prob(_demo(occupation="student"), "web") > \
        mp.click_prob(_demo(occupation="retired"), "web")                                  # student > retired
    assert mp.click_prob(_demo(gender="F"), "web") > mp.click_prob(_demo(gender="M"), "web")
    assert mp.click_prob(_demo(zip_code="90001"), "web") > \
        mp.click_prob(_demo(zip_code="70001"), "web")                                      # West > South-Central


def test_click_prob_bounded():
    for age in (18, 30, 64):
        for occ in mp.OCCUPATIONS:
            for plat in mp.PLATFORMS:
                p = mp.click_prob(_demo(age=age, occupation=occ), plat)
                assert 0.02 <= p <= 0.95


def test_assign_demographics_canonical_fields_and_deterministic():
    import random
    a = mp.assign_demographics(40, random.Random(5))
    b = mp.assign_demographics(40, random.Random(5))
    assert a == b and len(a) == 40
    for d in a.values():
        assert set(d) == {"age", "gender", "occupation", "zip_code"}
        assert 18 <= d["age"] <= 64
        assert len(d["zip_code"]) == 5 and d["zip_code"].isdigit()


def test_demographics_event_shape_matches_userupdated():
    ev = mp.demographics_event("user_1", _demo())
    assert set(ev) == {"user_id", "age", "gender", "occupation", "zip_code", "timestamp"}
    assert isinstance(ev["age"], int)


def test_rating_mean_tracks_engagement_segments():
    assert mp.rating_mean(_demo(age=30)) > mp.rating_mean(_demo(age=60))            # 25-34 > 55+
    assert mp.rating_mean(_demo(occupation="student")) > \
        mp.rating_mean(_demo(occupation="retired"))
    assert 1.0 <= mp.rating_mean(_demo(age=60, occupation="retired")) <= 5.0


def test_rating_event_shape_matches_ratingevent():
    import random
    ev = mp.rating_event("user_1", "movie_3", _demo(), random.Random(0))
    assert set(ev) == {"user_id", "item_id", "event_type", "rating", "timestamp"}
    assert ev["event_type"] == "rating"
    assert 1.0 <= ev["rating"] <= 5.0


# ── report: Redis demographics parsing (no Spark needed) ─────────────────────────
def test_fetch_demographics_parses_and_derives():
    import movielens_segment_report as rep

    fake = MagicMock()
    fake.scan_iter.return_value = ["user:u1:features"]
    fake.hgetall.return_value = {"age": "30", "gender": "F", "occupation": "student",
                                 "zipCode": "90210", "avgRating": "4.5", "ratingCount": "12"}
    with patch("redis.Redis", return_value=fake):
        rows = rep.fetch_demographics("localhost", 6379)

    assert rows == [{"user_id": "u1", "gender": "F", "occupation": "student",
                     "age_band": "25-34", "geo": "West",
                     "user_avg_rating": 4.5, "user_rating_count": 12}]


def test_fetch_demographics_returns_empty_when_redis_down():
    import movielens_segment_report as rep
    # nothing listening on this port → connection refused → [] (platform-only fallback)
    assert rep.fetch_demographics("127.0.0.1", 6399) == []


# ── report: platform breakdown via spark-submit (no Redis) ───────────────────────
def test_segment_report_platform_via_spark_submit(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import csv
    import os
    import subprocess

    spark_home = os.environ.get("SPARK_HOME")
    if not spark_home or not os.path.isfile(os.path.join(spark_home, "bin", "spark-submit")):
        pytest.skip("SPARK_HOME/spark-submit not available")

    parquet = tmp_path / "samples"
    pd.DataFrame({
        "user_id": ["u1", "u1", "u2", "u2"],
        "clicked": [1, 0, 1, 1],
        "ordered": [0, 0, 1, 0],
        "context_features": [{"platform": "ios"}, {"platform": "ios"},
                             {"platform": "web"}, {"platform": "web"}],
    }).to_parquet(parquet, index=False)

    script = Path(__file__).parents[2] / "services/python-modeling/movielens_segment_report.py"
    out = tmp_path / "report"
    env = {**os.environ, "REDIS_PORT": "6399"}  # force the demographics fallback
    subprocess.run(
        [os.path.join(spark_home, "bin", "spark-submit"), str(script),
         "--input", str(parquet), "--outdir", str(out)],
        check=True, capture_output=True, timeout=300, env=env,
    )

    csv_file = next((out / "by_platform").glob("part-*.csv"))
    rows = {r["platform"]: float(r["ctr"]) for r in csv.DictReader(csv_file.open())}
    assert rows["ios"] == pytest.approx(0.5)
    assert rows["web"] == pytest.approx(1.0)
