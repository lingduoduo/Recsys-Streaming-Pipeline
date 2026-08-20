# Next-Item Prediction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An offline experiment that answers "can this pipeline predict what a user engages with next, and how well" — a small sequential transformer measured against three trivial baselines on one shared split and one shared metric implementation.

**Architecture:** One focused Python module builds per-user timelines from `training_samples` Parquet, splits them at a timestamp quantile, scores four systems through a single metric function, and writes `metrics.json`. Nothing touches Spark, Redis, the serving path, or ONNX.

**Tech Stack:** Python 3, pandas + pyarrow for Parquet, PyTorch for the model (imported lazily), pytest.

**Spec:** [.superpowers/docs/specs/2026-08-20-next-item-prediction-design.md](../specs/2026-08-20-next-item-prediction-design.md)

## Global Constraints

- All work lives in two files: `recsys-pipeline/services/python-modeling/next_item_model.py` (new) and `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py` (new). Do not modify any Scala, any producer, any schema, or the serving path.
- Python tests run from the repository root: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
- Test files reach the module with the house pattern used by every test in that directory:
  `sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))` followed by `import next_item_model as nim  # noqa: E402`
- **Metric names are `hit_rate@{5,10,20}`, `mrr@10`, `ndcg@10`.** Never `recall@k` — that name already means a different, set-based quantity in `recall_eval_report.py`, and the spec forbids the collision.
- **A positive is `clicked == 1` OR `ordered == 1` OR `thumb == 1`.** `thumb == -1` and `abandoned == 1` are not positives and are not negatives; they are simply not targets.
- **Items already in a user's history are NOT filtered out of the candidate set.** This is the opposite of `recall_eval_report`'s convention and is deliberate.
- `torch` is imported lazily inside the model code with a `SystemExit` carrying an install hint, matching how `producer.py` handles a missing `kafka` module. It is not added to `requirements.txt` — only `movielens_pipeline.py` depends on it today and it is a very large install.
- Seed every random path. `SEED = 42`, matching `movielens_pipeline.py`.
- Reuse `rank_topk` and `cosine` from `recall_eval_report.py` rather than reimplementing them.

---

### Task 1: Timelines from training samples

**Files:**
- Create: `recsys-pipeline/services/python-modeling/next_item_model.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py`

**Interfaces:**
- Produces: `POSITIVE_MASK_COLUMNS`, `positive_mask(df) -> pd.Series`, `load_positives(input_dir) -> pd.DataFrame`, `build_timelines(positives) -> dict[str, list[tuple[int, str]]]`, `repeat_rate(timelines) -> float`. A timeline is a list of `(impression_ts, item_id)` in ascending order. Every later task consumes that exact shape.

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'next_item_model'`.

- [ ] **Step 3: Write minimal implementation**

Create `next_item_model.py`:

```python
#!/usr/bin/env python3
"""Offline next-item prediction: can this pipeline predict what a user engages with next?

Builds per-user timelines from the training_samples Parquet store, splits them at a
timestamp cutoff, and scores a small sequential transformer against three trivial
baselines through one shared metric implementation.

Offline only: nothing here touches Spark, Redis, the serving path, or ONNX.
"""
from __future__ import annotations

import pandas as pd

SEED = 42

#: Columns whose presence makes a row a positive engagement.
POSITIVE_MASK_COLUMNS = ("clicked", "ordered", "thumb")


def positive_mask(df: pd.DataFrame) -> pd.Series:
    """Rows the user engaged with: a click, an order, or a thumb up.

    A thumb down (-1) states dislike and an abandon states early exit. Neither is a
    positive, and neither is used as a negative — they are simply not prediction targets.
    """
    mask = pd.Series(False, index=df.index)
    if "clicked" in df.columns:
        mask |= df["clicked"].fillna(0).astype(int) == 1
    if "ordered" in df.columns:
        mask |= df["ordered"].fillna(0).astype(int) == 1
    if "thumb" in df.columns:
        mask |= df["thumb"].fillna(0).astype(int) == 1
    return mask


def load_positives(input_dir: str) -> pd.DataFrame:
    """Read the training-samples Parquet directory and keep only positive engagements."""
    df = pd.read_parquet(input_dir)
    positives = df[positive_mask(df)] if len(df) else df
    if positives.empty:
        raise ValueError(
            f"No positive engagements found at {input_dir}. "
            "Run a simulation first, e.g. scripts/run-movie-category-sim.sh"
        )
    return positives


def build_timelines(positives: pd.DataFrame) -> dict[str, list[tuple[int, str]]]:
    """Per-user (impression_ts, item_id) pairs in engagement order.

    Sorted by (impression_ts, position, item_id). The tie-break is load-bearing: every
    impression in one slate carries the same impression_ts, so sorting on the timestamp
    alone leaves a slate's positives in whatever order the Parquet happened to hold them
    and makes "the next item" differ between runs on identical input.
    """
    ordered = positives.sort_values(
        ["user_id", "impression_ts", "position", "item_id"], kind="mergesort")
    timelines: dict[str, list[tuple[int, str]]] = {}
    for user, item, ts in zip(ordered["user_id"], ordered["item_id"], ordered["impression_ts"]):
        timelines.setdefault(str(user), []).append((int(ts), str(item)))
    return timelines


def repeat_rate(timelines: dict[str, list[tuple[int, str]]]) -> float:
    """Share of events whose item that user already engaged with earlier.

    Reported so a reader can judge how much of any system's score is explained by
    re-engagement rather than discovery.
    """
    total = repeats = 0
    for events in timelines.values():
        seen: set[str] = set()
        for _, item in events:
            total += 1
            if item in seen:
                repeats += 1
            seen.add(item)
    return repeats / total if total else 0.0
```

`positive_mask` guards each column with `in df.columns` so Parquet written before schema v2 — which has no `thumb` at all — still yields click and order positives instead of raising.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/next_item_model.py \
        recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py
git commit -m "feat: build per-user engagement timelines for next-item prediction"
```

---

### Task 2: The timestamp-quantile split

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/next_item_model.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py`

**Interfaces:**
- Consumes: `build_timelines` output from Task 1.
- Produces: `Split` (a frozen dataclass with `train: dict[str, list[tuple[int, str]]]`, `targets: dict[str, str]`, `cutoff_ts: int`, `dropped: dict[str, int]`), `resolve_cutoff(timelines, quantile, absolute) -> int`, and `split_timelines(timelines, cutoff_ts) -> Split`. Tasks 4, 5 and 6 all take a `Split`.

- [ ] **Step 1: Write the failing test**

Add to the test file:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: FAIL — `AttributeError: module 'next_item_model' has no attribute 'resolve_cutoff'`.

- [ ] **Step 3: Write minimal implementation**

Add `from dataclasses import dataclass`, then append (`math` arrives with Task 3):

```python
DEFAULT_HOLDOUT_QUANTILE = 0.9


@dataclass(frozen=True)
class Split:
    """One global time cutoff applied to every user.

    `train` holds each user's pre-cutoff timeline, `targets` the single item they engaged
    with first at or after the cutoff. Every system — baselines and model — sees exactly
    this, so their numbers are comparable.
    """

    train: dict[str, list[tuple[int, str]]]
    targets: dict[str, str]
    cutoff_ts: int
    dropped: dict[str, int]


def resolve_cutoff(
    timelines: dict[str, list[tuple[int, str]]],
    quantile: float = DEFAULT_HOLDOUT_QUANTILE,
    absolute: int | None = None,
) -> int:
    """The timestamp that separates training from held-out events.

    A quantile of event timestamps, not a date. The movie-category sim writes its entire
    run inside one UTC date — measured at 4 minutes 50 seconds across 99,415 rows — so a
    date-partition holdout there selects everything or nothing. The engagement backfill
    sim spans 21 days. A quantile works on both.
    """
    if absolute is not None:
        return int(absolute)
    stamps = [ts for events in timelines.values() for ts, _ in events]
    if len(set(stamps)) < 2:
        raise ValueError(
            "Need at least 2 distinct event timestamps to form a time split; "
            f"found {len(set(stamps))}. A single-instant dataset cannot be split in time."
        )
    return int(pd.Series(stamps).quantile(quantile))


def split_timelines(timelines: dict[str, list[tuple[int, str]]], cutoff_ts: int) -> Split:
    """Apply the cutoff, dropping users who cannot contribute a (history, next) pair."""
    train: dict[str, list[tuple[int, str]]] = {}
    targets: dict[str, str] = {}
    dropped = {"fewer_than_two_positives": 0, "no_pre_cutoff_history": 0, "no_target": 0}

    for user, events in timelines.items():
        if len(events) < 2:
            dropped["fewer_than_two_positives"] += 1
            continue
        history = [event for event in events if event[0] < cutoff_ts]
        after = [item for ts, item in events if ts >= cutoff_ts]
        if not history:
            dropped["no_pre_cutoff_history"] += 1
            continue
        if not after:
            dropped["no_target"] += 1
            continue
        train[user] = history
        targets[user] = after[0]

    if not targets:
        raise ValueError(
            f"Split at cutoff_ts={cutoff_ts} left no test users. "
            f"Dropped: {dropped}. Lower the holdout quantile or use a longer-running dataset."
        )
    return Split(train=train, targets=targets, cutoff_ts=int(cutoff_ts), dropped=dropped)
```

`resolve_cutoff` uses pandas' interpolating quantile over **every event**, not every user, so a
user with a long timeline pulls the cutoff proportionally — which is what "a global time cutoff"
means. Interpolation also lets the boundary fall *between* two event timestamps, so `train` stays
strictly before `cutoff_ts` without an off-by-one adjustment. Note the arithmetic: for stamps
`[10, 20, 30, 40]` at quantile `0.5` this returns `25`, not `20` — index-based selection would
return the lower neighbour and quietly move every boundary event into training.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: PASS, 14 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/next_item_model.py \
        recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py
git commit -m "feat: split engagement timelines at a timestamp quantile"
```

---

### Task 3: The shared metric

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/next_item_model.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py`

**Interfaces:**
- Produces: `KS = (5, 10, 20)`, `evaluate_system(rankings: dict[str, list[str]], targets: dict[str, str]) -> dict[str, float]` returning exactly the keys `hit_rate@5`, `hit_rate@10`, `hit_rate@20`, `mrr@10`, `ndcg@10`. Tasks 4, 5 and 6 all score through this one function.

- [ ] **Step 1: Write the failing test**

Add to the test file:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: FAIL — `AttributeError: module 'next_item_model' has no attribute 'evaluate_system'`.

- [ ] **Step 3: Write minimal implementation**

Add `import math`, then append:

```python
KS = (5, 10, 20)
MRR_K = 10
NDCG_K = 10


def evaluate_system(
    rankings: dict[str, list[str]], targets: dict[str, str]
) -> dict[str, float]:
    """Score one system's rankings against the held-out targets.

    Named hit_rate rather than recall on purpose. Each user contributes exactly one
    target, so the fraction of relevant items retrieved is 0 or 1 and its mean is a hit
    rate — and `recall@k` already names a different, set-based quantity in
    recall_eval_report.py. With a single relevant item the ideal DCG is 1, so ndcg@10 is
    the reciprocal log rank.

    The denominator is every user in `targets`, so a system that cannot rank a user
    scores a miss for them rather than quietly excluding them from its own average.
    """
    hits = {k: 0 for k in KS}
    mrr_total = 0.0
    ndcg_total = 0.0

    for user, target in targets.items():
        ranking = rankings.get(user, [])
        try:
            rank = ranking.index(target) + 1
        except ValueError:
            continue
        for k in KS:
            if rank <= k:
                hits[k] += 1
        if rank <= MRR_K:
            mrr_total += 1.0 / rank
        if rank <= NDCG_K:
            ndcg_total += 1.0 / math.log2(rank + 1)

    users = len(targets)
    metrics = {f"hit_rate@{k}": hits[k] / users for k in KS}
    metrics["mrr@10"] = mrr_total / users
    metrics["ndcg@10"] = ndcg_total / users
    return metrics
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: PASS, 24 tests (the ndcg identity is parametrized over 5 ranks).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/next_item_model.py \
        recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py
git commit -m "feat: score next-item rankings with hit_rate, mrr, and ndcg"
```

---

### Task 4: The three baselines

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/next_item_model.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py`

**Interfaces:**
- Consumes: `Split` from Task 2.
- Produces: `TOP_K = 20`, `load_item_vectors(path) -> dict[str, list[float]]`, and three rankers with one shape — `most_popular(split, k=TOP_K)`, `repeat_last(split, k=TOP_K)`, `item2vec_neighbors(split, vectors, k=TOP_K)` — each returning `dict[str, list[str]]` mapping user to ranked item ids. Task 6 calls all three.

- [ ] **Step 1: Write the failing test**

Add to the test file:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: FAIL — `AttributeError: module 'next_item_model' has no attribute 'most_popular'`.

- [ ] **Step 3: Write minimal implementation**

Add `from collections import Counter` and, at the top of the file after the docstring, the shared-helper import that reuses the existing report's functions:

```python
from recall_eval_report import cosine, rank_topk
```

Then append:

```python
TOP_K = 20


def load_item_vectors(path: str) -> dict[str, list[float]]:
    """Read the `item_id:v1 v2 ...` embedding file the sims already write."""
    vectors: dict[str, list[float]] = {}
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or ":" not in line:
                continue
            item, raw = line.split(":", 1)
            values = [float(part) for part in raw.split() if part]
            if values:
                vectors[item] = values
    return vectors


def most_popular(split: Split, k: int = TOP_K) -> dict[str, list[str]]:
    """The null hypothesis: the most-engaged training items, same list for everyone.

    A learned model that does not beat this has demonstrated nothing.
    """
    counts = Counter(item for events in split.train.values() for _, item in events)
    ranking = rank_topk({item: float(n) for item, n in counts.items()}, k)
    return {user: list(ranking) for user in split.targets}


def repeat_last(split: Split, k: int = TOP_K) -> dict[str, list[str]]:
    """The user's own most recent distinct items, most recent first."""
    rankings: dict[str, list[str]] = {}
    for user in split.targets:
        seen: list[str] = []
        for _, item in reversed(split.train.get(user, [])):
            if item not in seen:
                seen.append(item)
            if len(seen) == k:
                break
        rankings[user] = seen
    return rankings


def item2vec_neighbors(
    split: Split, vectors: dict[str, list[float]], k: int = TOP_K
) -> dict[str, list[str]]:
    """Nearest neighbours of the user's last training item in the item2vec space.

    Returns an empty ranking when the anchor has no embedding — that is a real property
    of this baseline, and evaluate_system counts it as a miss rather than dropping the
    user from the denominator.
    """
    rankings: dict[str, list[str]] = {}
    for user in split.targets:
        history = split.train.get(user, [])
        anchor = history[-1][1] if history else None
        if anchor is None or anchor not in vectors:
            rankings[user] = []
            continue
        query = vectors[anchor]
        scores = {item: cosine(query, vec) for item, vec in vectors.items()}
        rankings[user] = rank_topk(scores, k)
    return rankings
```

Importing from `recall_eval_report` works because both modules sit in the same directory and the test inserts that directory on `sys.path`; the CLI in Task 6 does the same for direct execution.

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: PASS, 30 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/next_item_model.py \
        recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py
git commit -m "feat: add popularity, repeat-last, and item2vec next-item baselines"
```

---

### Task 5: The sequential model

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/next_item_model.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py`

**Interfaces:**
- Consumes: `Split` from Task 2, `TOP_K` from Task 4.
- Produces: `train_next_item(split, epochs=..., seed=SEED) -> tuple[object, dict[str, int]]` returning the trained model and the item→index map, and `model_rankings(model, item_index, split, k=TOP_K) -> dict[str, list[str]]`. Task 6 calls both.

- [ ] **Step 1: Write the failing test**

Add to the test file:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: FAIL — `AttributeError: module 'next_item_model' has no attribute 'train_next_item'`.

- [ ] **Step 3: Write minimal implementation**

Append. `torch` is imported inside the function, matching how `producer.py` reports a missing `kafka`:

```python
MAX_SEQUENCE = 64
EMBED_DIM = 64
ATTENTION_HEADS = 4
LAYERS = 2
EPOCHS = 60
LEARNING_RATE = 1e-3


def _torch():
    """Import torch on demand with an actionable message when it is absent.

    Only movielens_pipeline.py depends on torch today and it is a very large install, so
    it stays out of requirements.txt and the producers keep working without it.
    """
    try:
        import torch
        return torch
    except ModuleNotFoundError as exc:  # pragma: no cover - environment dependent
        raise SystemExit(
            "Missing model dependencies. Run: python -m pip install torch"
        ) from exc


def _build_model(torch, catalog_size: int):
    nn = torch.nn

    class NextItemTransformer(nn.Module):
        """Causal transformer over item ids, softmax over the catalog."""

        def __init__(self) -> None:
            super().__init__()
            self.item = nn.Embedding(catalog_size, EMBED_DIM)
            self.position = nn.Embedding(MAX_SEQUENCE, EMBED_DIM)
            layer = nn.TransformerEncoderLayer(
                d_model=EMBED_DIM, nhead=ATTENTION_HEADS,
                dim_feedforward=EMBED_DIM * 2, batch_first=True, dropout=0.0)
            self.encoder = nn.TransformerEncoder(layer, num_layers=LAYERS)
            self.out = nn.Linear(EMBED_DIM, catalog_size)

        def forward(self, ids):
            length = ids.shape[1]
            positions = torch.arange(length, device=ids.device).unsqueeze(0)
            hidden = self.item(ids) + self.position(positions)
            mask = torch.triu(torch.ones(length, length, dtype=torch.bool), diagonal=1)
            return self.out(self.encoder(hidden, mask=mask.to(ids.device)))

    return NextItemTransformer()


def _sequences(split: Split, item_index: dict[str, int]) -> list[list[int]]:
    return [
        [item_index[item] for _, item in events[-MAX_SEQUENCE:]]
        for events in split.train.values()
        if len(events) >= 2
    ]


def train_next_item(split: Split, epochs: int = EPOCHS, seed: int = SEED):
    """Train the causal transformer on the pre-cutoff timelines.

    Returns the model and the item->index map; both are needed to rank.
    """
    torch = _torch()
    torch.manual_seed(seed)

    items = sorted({item for events in split.train.values() for _, item in events})
    item_index = {item: n for n, item in enumerate(items)}
    sequences = _sequences(split, item_index)
    if not sequences:
        raise ValueError("No training sequence has two or more events; nothing to learn from.")

    model = _build_model(torch, len(items))
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
    loss_fn = torch.nn.CrossEntropyLoss()
    width = max(len(seq) for seq in sequences)
    padded = torch.zeros(len(sequences), width, dtype=torch.long)
    keep = torch.zeros(len(sequences), width, dtype=torch.bool)
    for row, seq in enumerate(sequences):
        padded[row, : len(seq)] = torch.tensor(seq, dtype=torch.long)
        keep[row, : len(seq)] = True

    model.train()
    for _ in range(epochs):
        optimizer.zero_grad()
        logits = model(padded[:, :-1])
        targets = padded[:, 1:]
        valid = keep[:, 1:]
        loss = loss_fn(logits[valid], targets[valid])
        loss.backward()
        optimizer.step()
    return model, item_index


def model_rankings(model, item_index: dict[str, int], split: Split, k: int = TOP_K):
    """Rank the catalog for each test user from their training history.

    Seen items stay in the candidate set, matching the baselines and the spec.
    """
    torch = _torch()
    index_item = {n: item for item, n in item_index.items()}
    model.eval()
    rankings: dict[str, list[str]] = {}
    with torch.no_grad():
        for user in split.targets:
            history = [item for _, item in split.train.get(user, [])[-MAX_SEQUENCE:]]
            ids = [item_index[item] for item in history if item in item_index]
            if not ids:
                rankings[user] = []
                continue
            logits = model(torch.tensor([ids], dtype=torch.long))[0, -1]
            order = torch.argsort(logits, descending=True)[:k].tolist()
            rankings[user] = [index_item[n] for n in order]
    return rankings
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: PASS, 33 tests. The model tests train on a handful of tiny sequences and take seconds, not minutes.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/next_item_model.py \
        recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py
git commit -m "feat: train a causal transformer for next-item prediction"
```

---

### Task 6: The CLI and metrics.json

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/next_item_model.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py`

**Interfaces:**
- Consumes: everything from Tasks 1-5.
- Produces: `run(input_dir, output_path, quantile, absolute_cutoff, vectors_path, epochs, seed) -> dict` and `main(argv=None) -> None`. Nothing later consumes them.

- [ ] **Step 1: Write the failing test**

Add to the test file:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py -q`
Expected: FAIL — `AttributeError: module 'next_item_model' has no attribute 'run'`.

- [ ] **Step 3: Write minimal implementation**

Add `import argparse`, `import json`, `import os`, and `from typing import Sequence`, then append:

```python
def run(
    input_dir: str,
    output_path: str,
    quantile: float = DEFAULT_HOLDOUT_QUANTILE,
    absolute_cutoff: int | None = None,
    vectors_path: str | None = None,
    epochs: int = EPOCHS,
    seed: int = SEED,
) -> dict:
    """Build, split, score, and write metrics.json. Returns the same dict it writes."""
    frame = pd.read_parquet(input_dir)
    positives = load_positives(input_dir)
    timelines = build_timelines(positives)
    cutoff = resolve_cutoff(timelines, quantile=quantile, absolute=absolute_cutoff)
    split = split_timelines(timelines, cutoff)

    vectors = load_item_vectors(vectors_path) if vectors_path else {}
    model, item_index = train_next_item(split, epochs=epochs, seed=seed)

    systems = {
        "most_popular": most_popular(split),
        "repeat_last": repeat_last(split),
        "item2vec_neighbors": item2vec_neighbors(split, vectors),
        "next_item_transformer": model_rankings(model, item_index, split),
    }
    report = {
        "systems": {
            name: evaluate_system(rankings, split.targets)
            for name, rankings in systems.items()
        },
        "support": {
            "test_users": len(split.targets),
            "dropped_users": split.dropped,
            "catalog_size": len(item_index),
            "positive_rate": len(positives) / len(frame) if len(frame) else 0.0,
            "repeat_rate": repeat_rate(timelines),
            "cutoff_ts": split.cutoff_ts,
        },
    }

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
    return report


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Offline next-item prediction: a sequential model against three baselines.")
    parser.add_argument(
        "--input", default=os.getenv("NEXT_ITEM_INPUT_PATH", "/tmp/spark-recsys/training-samples"),
        help="training_samples Parquet directory.")
    parser.add_argument(
        "--output", default=os.getenv("NEXT_ITEM_METRICS_PATH", "/tmp/spark-recsys/next-item/metrics.json"),
        help="Where to write metrics.json.")
    parser.add_argument(
        "--holdout-quantile", type=float,
        default=float(os.getenv("NEXT_ITEM_HOLDOUT_QUANTILE", DEFAULT_HOLDOUT_QUANTILE)),
        help="Quantile of event timestamps to split on.")
    parser.add_argument(
        "--cutoff-ts", type=int,
        default=int(os.getenv("NEXT_ITEM_CUTOFF_TS")) if os.getenv("NEXT_ITEM_CUTOFF_TS") else None,
        help="Absolute epoch-second cutoff; overrides --holdout-quantile.")
    parser.add_argument(
        "--vectors", default=os.getenv("NEXT_ITEM_VECTORS_PATH"),
        help="item_id:v1 v2 ... embedding file for the item2vec baseline.")
    parser.add_argument("--epochs", type=int, default=EPOCHS)
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args(argv)

    report = run(
        input_dir=args.input, output_path=args.output, quantile=args.holdout_quantile,
        absolute_cutoff=args.cutoff_ts, vectors_path=args.vectors,
        epochs=args.epochs, seed=args.seed)

    for name, metrics in sorted(report["systems"].items()):
        line = " ".join(f"{key}={value:.4f}" for key, value in sorted(metrics.items()))
        print(f"[next-item] {name:<22} {line}", flush=True)
    print(f"[next-item] wrote {args.output}", flush=True)


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q`
Expected: PASS, whole Python suite.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/next_item_model.py \
        recsys-pipeline/integration-tests/python_modeling/test_next_item_model.py
git commit -m "feat: report next-item metrics for four systems as metrics.json"
```

---

## Verification

- [ ] `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q` passes from the repository root.
- [ ] `git grep -n 'recall@' recsys-pipeline/services/python-modeling/next_item_model.py` returns nothing — the metric collision the spec forbids has not crept back.
- [ ] `git diff master --stat` shows exactly two files, both new.
- [ ] A real run produces a comparison table:
  `python3 recsys-pipeline/services/python-modeling/next_item_model.py --input /tmp/spark-recsys/movie-category-sim/training-samples --output /tmp/spark-recsys/next-item/metrics.json --vectors /tmp/spark-recsys/movie-category-sim/item-embedding.txt`
  Expected: four systems printed with all five metrics each, and a `metrics.json` on disk.

## The result to look for

`most_popular` is the null hypothesis. If `next_item_transformer` does not beat it, the honest
conclusion is that this data does not support a sequential model — not that the model needs
tuning. The catalog is 400 items with every item engaged and a measured median of 107 positives
per user, which is a setting where popularity is genuinely hard to beat. Report whichever way it
lands; a negative result is a real answer to the spec's question.
