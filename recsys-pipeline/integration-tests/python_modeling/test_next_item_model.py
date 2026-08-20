import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))

import next_item_model as nim  # noqa: E402


def _frame(rows):
    """rows: (user, item, ts, position, clicked, ordered, thumb, abandoned)"""
    return pd.DataFrame(
        rows,
        columns=["user_id", "item_id", "impression_ts", "position",
                 "clicked", "ordered", "thumb", "abandoned"],
    )


def test_positive_is_click_order_or_thumb_up():
    df = _frame([
        ("u1", "i_click", 100, 0, 1, 0, None, 0),
        ("u1", "i_order", 101, 0, 0, 1, None, 0),
        ("u1", "i_thumb", 102, 0, 0, 0, 1, 0),
        ("u1", "i_none", 103, 0, 0, 0, None, 0),
        ("u1", "i_down", 104, 0, 0, 0, -1, 0),
        ("u1", "i_aband", 105, 0, 0, 0, None, 1),
    ])

    kept = df[nim.positive_mask(df)]["item_id"].tolist()

    assert kept == ["i_click", "i_order", "i_thumb"]


def test_slate_ties_break_on_position_then_item():
    # Every impression in one slate shares an impression_ts, so ordering on the
    # timestamp alone would leave these in arbitrary order.
    df = _frame([
        ("u1", "i_b", 100, 2, 1, 0, None, 0),
        ("u1", "i_a", 100, 1, 1, 0, None, 0),
        ("u1", "i_c", 100, 0, 1, 0, None, 0),
    ])

    timelines = nim.build_timelines(df[nim.positive_mask(df)])

    assert timelines["u1"] == [(100, "i_c"), (100, "i_a"), (100, "i_b")]


def test_timelines_are_deterministic_across_runs():
    df = _frame([("u1", f"i{n}", 100, n % 3, 1, 0, None, 0) for n in range(20)])
    positives = df[nim.positive_mask(df)]

    assert nim.build_timelines(positives) == nim.build_timelines(positives.sample(frac=1, random_state=7))


def test_repeat_rate_counts_items_seen_earlier_by_that_user():
    timelines = {"u1": [(1, "a"), (2, "b"), (3, "a")], "u2": [(1, "c")]}

    # 1 of 4 events repeats an item that user already engaged with.
    assert nim.repeat_rate(timelines) == pytest.approx(0.25)


def test_missing_v2_columns_are_treated_as_absent_not_fatal(tmp_path):
    # Parquet written before schema v2 has no thumb/abandoned columns at all.
    legacy = pd.DataFrame(
        [("u1", "i1", 100, 0, 1, 0)],
        columns=["user_id", "item_id", "impression_ts", "position", "clicked", "ordered"],
    )
    legacy.to_parquet(tmp_path / "part.parquet")

    positives = nim.load_positives(str(tmp_path))

    assert positives["item_id"].tolist() == ["i1"]


def test_empty_input_raises_with_the_path(tmp_path):
    pd.DataFrame(
        [], columns=["user_id", "item_id", "impression_ts", "position", "clicked", "ordered"]
    ).to_parquet(tmp_path / "part.parquet")

    with pytest.raises(ValueError, match=str(tmp_path)):
        nim.load_positives(str(tmp_path))


def test_absolute_cutoff_overrides_the_quantile():
    timelines = {"u1": [(10, "a"), (20, "b"), (30, "c")]}

    assert nim.resolve_cutoff(timelines, quantile=0.9, absolute=15) == 15


def test_quantile_cutoff_uses_every_event_not_every_user():
    timelines = {"u1": [(10, "a"), (20, "b")], "u2": [(30, "c"), (40, "d")]}

    # Four events at 10/20/30/40; the 0.5 quantile sits at 25.
    assert nim.resolve_cutoff(timelines, quantile=0.5) == 25


def test_split_puts_every_training_event_before_every_target():
    timelines = {
        "u1": [(10, "a"), (20, "b"), (30, "target1")],
        "u2": [(10, "c"), (40, "target2")],
    }

    split = nim.split_timelines(timelines, cutoff_ts=25)

    assert split.train == {"u1": [(10, "a"), (20, "b")], "u2": [(10, "c")]}
    assert split.targets == {"u1": "target1", "u2": "target2"}
    latest_train = max(ts for events in split.train.values() for ts, _ in events)
    assert latest_train < split.cutoff_ts


def test_target_is_the_first_positive_at_or_after_the_cutoff():
    timelines = {"u1": [(10, "a"), (25, "first"), (30, "second")]}

    split = nim.split_timelines(timelines, cutoff_ts=25)

    assert split.targets["u1"] == "first"


def test_user_without_pre_cutoff_history_is_dropped_and_counted():
    timelines = {"u1": [(10, "a"), (30, "t")], "cold": [(30, "x"), (40, "y")]}

    split = nim.split_timelines(timelines, cutoff_ts=25)

    assert "cold" not in split.targets
    assert split.dropped["no_pre_cutoff_history"] == 1


def test_user_with_a_single_positive_is_dropped_and_counted():
    timelines = {"u1": [(10, "a"), (30, "t")], "thin": [(10, "only")]}

    split = nim.split_timelines(timelines, cutoff_ts=25)

    assert "thin" not in split.targets
    assert split.dropped["fewer_than_two_positives"] == 1


def test_single_distinct_timestamp_raises():
    timelines = {"u1": [(100, "a"), (100, "b")], "u2": [(100, "c"), (100, "d")]}

    with pytest.raises(ValueError, match="distinct"):
        nim.resolve_cutoff(timelines, quantile=0.9)


def test_no_surviving_test_users_raises():
    timelines = {"u1": [(10, "a"), (20, "b")]}

    with pytest.raises(ValueError, match="no test users"):
        nim.split_timelines(timelines, cutoff_ts=100)
