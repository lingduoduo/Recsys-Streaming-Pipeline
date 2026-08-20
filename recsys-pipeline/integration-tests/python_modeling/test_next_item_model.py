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


def test_metrics_match_hand_worked_values():
    # u1's target is ranked 1st, u2's is ranked 3rd.
    rankings = {
        "u1": ["target1"] + [f"x{n}" for n in range(19)],
        "u2": ["x", "y", "target2"] + [f"z{n}" for n in range(17)],
    }
    targets = {"u1": "target1", "u2": "target2"}

    metrics = nim.evaluate_system(rankings, targets)

    assert metrics["hit_rate@5"] == pytest.approx(1.0)
    # 1/1 and 1/3, averaged.
    assert metrics["mrr@10"] == pytest.approx((1.0 + 1.0 / 3.0) / 2)
    # 1/log2(2) = 1.0 and 1/log2(4) = 0.5, averaged.
    assert metrics["ndcg@10"] == pytest.approx(0.75)


def test_target_outside_top_k_scores_zero():
    rankings = {"u1": [f"x{n}" for n in range(20)]}
    targets = {"u1": "never_ranked"}

    metrics = nim.evaluate_system(rankings, targets)

    assert metrics["hit_rate@20"] == 0.0
    assert metrics["mrr@10"] == 0.0
    assert metrics["ndcg@10"] == 0.0


@pytest.mark.parametrize("rank", [1, 2, 3, 5, 10])
def test_ndcg_is_the_reciprocal_log_rank_for_a_single_target(rank):
    """The identity that makes the name honest: with one relevant item, IDCG is 1."""
    import math as _math

    ranking = [f"x{n}" for n in range(rank - 1)] + ["target"] + [f"y{n}" for n in range(20)]

    metrics = nim.evaluate_system({"u": ranking}, {"u": "target"})

    assert metrics["ndcg@10"] == pytest.approx(1.0 / _math.log2(rank + 1))


def test_hit_rate_at_k_respects_the_cutoff():
    # Target sits at rank 7: inside 10 and 20, outside 5.
    ranking = [f"x{n}" for n in range(6)] + ["target"] + [f"y{n}" for n in range(20)]

    metrics = nim.evaluate_system({"u": ranking}, {"u": "target"})

    assert metrics["hit_rate@5"] == 0.0
    assert metrics["hit_rate@10"] == 1.0
    assert metrics["hit_rate@20"] == 1.0


def test_target_absent_from_training_scores_zero_but_still_counts():
    """An item nobody engaged with before the cutoff is unreachable for every system.

    The spec calls for it to be scored normally rather than skipped: it is a real
    property of these baselines, and dropping the user would flatter every score.
    """
    metrics = nim.evaluate_system({"u1": ["a", "b", "c"]}, {"u1": "never_in_training"})

    assert metrics["hit_rate@20"] == 0.0
    assert metrics["ndcg@10"] == 0.0


def test_a_user_with_no_ranking_counts_as_a_miss():
    """A baseline that cannot score a user must not silently shrink the denominator."""
    metrics = nim.evaluate_system({"u1": ["target1"]}, {"u1": "target1", "u2": "target2"})

    assert metrics["hit_rate@5"] == pytest.approx(0.5)


def _split(train, targets):
    return nim.Split(train=train, targets=targets, cutoff_ts=100, dropped={})


def test_most_popular_ranks_by_training_count_and_is_identical_for_every_user():
    split = _split(
        train={"u1": [(1, "a"), (2, "a"), (3, "b")], "u2": [(1, "a"), (2, "c")]},
        targets={"u1": "z", "u2": "z"},
    )

    rankings = nim.most_popular(split, k=3)

    assert rankings["u1"][:2] == ["a", "b"] or rankings["u1"][:2] == ["a", "c"]
    assert rankings["u1"][0] == "a"          # 3 engagements beats 1
    assert rankings["u1"] == rankings["u2"]  # not personalised


def test_repeat_last_returns_most_recent_distinct_items_first():
    split = _split(
        train={"u1": [(1, "old"), (2, "mid"), (3, "new"), (4, "new")]},
        targets={"u1": "z"},
    )

    assert nim.repeat_last(split, k=3) == {"u1": ["new", "mid", "old"]}


def test_item2vec_neighbors_ranks_by_cosine_to_the_last_item():
    split = _split(train={"u1": [(1, "far"), (2, "anchor")]}, targets={"u1": "z"})
    vectors = {
        "anchor": [1.0, 0.0],
        "near": [0.9, 0.1],
        "orthogonal": [0.0, 1.0],
        "far": [-1.0, 0.0],
    }

    ranking = nim.item2vec_neighbors(split, vectors, k=4)["u1"]

    assert ranking.index("near") < ranking.index("orthogonal") < ranking.index("far")


def test_item2vec_neighbors_handles_an_anchor_with_no_embedding():
    split = _split(train={"u1": [(1, "unknown_item")]}, targets={"u1": "z"})

    ranking = nim.item2vec_neighbors(split, {"a": [1.0]}, k=3)

    assert ranking["u1"] == []


def test_load_item_vectors_parses_the_sim_embedding_format(tmp_path):
    path = tmp_path / "item-embedding.txt"
    path.write_text("movie_1:0.5 -0.25 0.75\nmovie_2:1.0 0.0 0.0\n")

    vectors = nim.load_item_vectors(str(path))

    assert vectors["movie_1"] == [0.5, -0.25, 0.75]
    assert len(vectors) == 2


def test_baselines_keep_items_the_user_already_engaged_with():
    """The opposite of recall_eval_report's convention, and deliberate."""
    split = _split(train={"u1": [(1, "seen"), (2, "seen"), (3, "other")]}, targets={"u1": "seen"})

    assert "seen" in nim.most_popular(split, k=5)["u1"]


def test_model_learns_a_deterministic_next_item_pattern():
    """A sequence model that cannot learn a memorised cycle is broken.

    Five users all walk a->b->c->d. Trained on that, the model asked to continue
    a->b->c should rank d first. This is an overfitting check by design: it proves the
    plumbing (masking, shifting, indexing) is right, not that the model generalises.
    """
    cycle = ["a", "b", "c", "d"]
    train = {f"u{n}": [(i, cycle[i]) for i in range(4)] for n in range(5)}
    split = nim.Split(train=train, targets={f"u{n}": "d" for n in range(5)},
                      cutoff_ts=100, dropped={})

    model, item_index = nim.train_next_item(split, epochs=200, seed=nim.SEED)
    rankings = nim.model_rankings(model, item_index, split, k=4)

    assert rankings["u0"][0] == "d"


def test_model_ranking_covers_every_requested_user():
    train = {"u1": [(1, "a"), (2, "b")], "u2": [(1, "b"), (2, "a")]}
    split = nim.Split(train=train, targets={"u1": "a", "u2": "b"}, cutoff_ts=100, dropped={})

    model, item_index = nim.train_next_item(split, epochs=5, seed=nim.SEED)
    rankings = nim.model_rankings(model, item_index, split, k=2)

    assert set(rankings) == {"u1", "u2"}
    assert all(len(r) == 2 for r in rankings.values())


def test_training_is_reproducible_under_the_seed():
    train = {"u1": [(1, "a"), (2, "b"), (3, "c")], "u2": [(1, "c"), (2, "b"), (3, "a")]}
    split = nim.Split(train=train, targets={"u1": "a", "u2": "c"}, cutoff_ts=100, dropped={})

    first = nim.model_rankings(*nim.train_next_item(split, epochs=20, seed=7), split, k=3)
    second = nim.model_rankings(*nim.train_next_item(split, epochs=20, seed=7), split, k=3)

    assert first == second


def _sim_like_frame():
    """Two dozen users walking a shared item cycle, with a late target event each."""
    rows = []
    cycle = [f"m{n}" for n in range(8)]
    ts = 1000
    for u in range(24):
        for step, item in enumerate(cycle):
            rows.append((f"u{u}", item, ts + step, step % 4, 1, 0, None, 0))
        rows.append((f"u{u}", cycle[0], ts + 500, 0, 1, 0, None, 0))
    return _frame(rows)


def test_run_writes_every_system_and_a_support_block(tmp_path):
    (tmp_path / "data").mkdir()
    _sim_like_frame().to_parquet(tmp_path / "data" / "part.parquet")
    metrics_path = tmp_path / "metrics.json"

    report = nim.run(
        input_dir=str(tmp_path / "data"),
        output_path=str(metrics_path),
        quantile=0.9,
        absolute_cutoff=None,
        vectors_path=None,
        epochs=5,
        seed=nim.SEED,
    )

    import json
    written = json.loads(metrics_path.read_text())
    assert written == report
    assert set(report["systems"]) == {
        "most_popular", "repeat_last", "item2vec_neighbors", "next_item_transformer"}
    for name, metrics in report["systems"].items():
        assert set(metrics) == {
            "hit_rate@5", "hit_rate@10", "hit_rate@20", "mrr@10", "ndcg@10"}, name
    support = report["support"]
    assert support["test_users"] > 0
    assert support["catalog_size"] > 0
    assert support["cutoff_ts"] > 0
    assert set(support["dropped_users"]) >= {"fewer_than_two_positives", "no_pre_cutoff_history"}
    assert 0.0 <= support["positive_rate"] <= 1.0
    assert 0.0 <= support["repeat_rate"] <= 1.0


def test_item2vec_baseline_is_empty_without_vectors(tmp_path):
    """No embedding file is a normal state; the system reports zeros rather than failing."""
    (tmp_path / "data").mkdir()
    _sim_like_frame().to_parquet(tmp_path / "data" / "part.parquet")

    report = nim.run(
        input_dir=str(tmp_path / "data"),
        output_path=str(tmp_path / "metrics.json"),
        quantile=0.9,
        absolute_cutoff=None,
        vectors_path=None,
        epochs=5,
        seed=nim.SEED,
    )

    assert report["systems"]["item2vec_neighbors"]["hit_rate@20"] == 0.0


def test_main_accepts_cli_arguments(tmp_path):
    (tmp_path / "data").mkdir()
    _sim_like_frame().to_parquet(tmp_path / "data" / "part.parquet")
    metrics_path = tmp_path / "metrics.json"

    nim.main([
        "--input", str(tmp_path / "data"),
        "--output", str(metrics_path),
        "--epochs", "5",
    ])

    assert metrics_path.exists()
