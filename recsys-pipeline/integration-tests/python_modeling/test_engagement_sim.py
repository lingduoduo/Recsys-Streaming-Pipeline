import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import backfill_producer as bp  # noqa: E402  (path set above)
import engagement_report as er  # noqa: E402

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


def test_build_series_computes_ctr_by_bucket():
    pd = pytest.importorskip("pandas")
    # Two impressions on Mon (1 click), two on Sat (2 clicks).
    df = pd.DataFrame({
        "impression_time": pd.to_datetime([
            "2026-06-01 09:00", "2026-06-01 09:00",   # Mon
            "2026-06-06 21:00", "2026-06-06 21:00",   # Sat
        ]),
        "clicked": [1, 0, 1, 1],
    })
    df["day"] = df["impression_time"].dt.floor("D")
    df["hour"] = df["impression_time"].dt.hour
    df["dow"] = df["impression_time"].dt.dayofweek

    daily, by_hour, by_dow = er.build_series(df)

    mon = daily.loc[daily.day == pd.Timestamp("2026-06-01"), "ctr"].iloc[0]
    sat = daily.loc[daily.day == pd.Timestamp("2026-06-06"), "ctr"].iloc[0]
    assert mon == pytest.approx(0.5)
    assert sat == pytest.approx(1.0)
    # day-of-week frame carries Sat (5) higher than Mon (0)
    assert by_dow.loc[by_dow.dow == 5, "ctr"].iloc[0] > by_dow.loc[by_dow.dow == 0, "ctr"].iloc[0]
