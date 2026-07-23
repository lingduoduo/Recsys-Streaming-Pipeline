# Bandit OPE Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Evaluate bandit recommendation policies offline from the logged `rl_experience` replay buffer via a Direct-Method report, and fix the Java↔Python replay contract that breaks `replay_export.py` on real events.

**Architecture:** A shared Python loader reads the replay buffer using the real Java schema (`user`/`action`). A dependency-light pure-numpy logistic regression, fit on logged (taken-action features → observed reward), serves as the Direct-Method reward estimator. A standalone report re-picks target policies from each event's `actionSpace` and reports each policy's estimated value with held-out calibration.

**Tech Stack:** Python 3, numpy (already present via pandas), redis-py. No new dependencies (no sklearn). Tests: pytest.

## Global Constraints

- No new runtime dependency; numpy only (no sklearn). Copied verbatim from spec.
- No serving/wire change: the Java `ReplayEvent` schema (`user`, `action`, `reward`, `timestamp`, `banditScore`, `estimatedReward`, `onlineScore`, `coldStart`, `actionPosition`, `slateSize`, `modelPredictions`, `policy`, `actionSpace[]`) is the source of truth; Python conforms.
- Per-candidate `actionSpace` fields available offline: `item`, `modelPredictions`, `coldStart`, `impressions`, `clicks`. Target policies must be definable over only these.
- Determinism: no RNG. Train/test split and the `random` policy derive from `hashlib.md5(requestId)`.
- All new source lives in `recsys-pipeline/services/python-modeling/`; tests in `recsys-pipeline/integration-tests/python_modeling/`. Tests import modules via `sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))`.

---

## File Structure

- Create `recsys-pipeline/services/python-modeling/replay_buffer.py` — shared loader (Redis list / Parquet → list[dict]).
- Modify `recsys-pipeline/services/python-modeling/replay_export.py` — read via `replay_buffer`; map `user`→userId, `action`→movieId.
- Create `recsys-pipeline/services/python-modeling/logistic.py` — pure-numpy logistic regression + standardize.
- Create `recsys-pipeline/services/python-modeling/ope_eval_report.py` — Direct-Method OPE report.
- Modify `recsys-pipeline/integration-tests/python_modeling/test_replay_export.py` — fixtures to real schema.
- Create `recsys-pipeline/integration-tests/python_modeling/test_replay_buffer.py`.
- Create `recsys-pipeline/integration-tests/python_modeling/test_logistic.py`.
- Create `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`.

---

### Task 1: Shared replay-buffer loader

**Files:**
- Create: `recsys-pipeline/services/python-modeling/replay_buffer.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_replay_buffer.py`

**Interfaces:**
- Produces:
  - `DEFAULT_KEY = "replay:recommendations"`
  - `load_from_redis(client, key=DEFAULT_KEY, limit=-1) -> list[dict]` — calls `client.lrange(key, 0, limit)`, JSON-parses each item (accepts `bytes` or `str`).
  - `load_from_parquet(path) -> list[dict]` — reads a Parquet dump into row dicts (requires pandas).

- [ ] **Step 1: Write the failing test**

```python
# recsys-pipeline/integration-tests/python_modeling/test_replay_buffer.py
import json, sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import replay_buffer


def test_load_from_redis_parses_bytes_and_str():
    entries = [{"user": "u1", "action": "m1", "reward": 0.9},
               {"user": "u2", "action": "m2", "reward": 0.0}]
    client = MagicMock()
    client.lrange.return_value = [json.dumps(entries[0]).encode(), json.dumps(entries[1])]
    out = replay_buffer.load_from_redis(client, limit=-1)
    client.lrange.assert_called_once_with("replay:recommendations", 0, -1)
    assert out == entries


def test_load_from_redis_empty():
    client = MagicMock()
    client.lrange.return_value = []
    assert replay_buffer.load_from_redis(client) == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_replay_buffer.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'replay_buffer'`

- [ ] **Step 3: Write minimal implementation**

```python
# recsys-pipeline/services/python-modeling/replay_buffer.py
"""Shared loader for the RL replay buffer (Redis list or Parquet dump).

Reads the real Java ReplayEvent schema (user/action/reward/...). Single owner of the
Redis key and JSON decoding so replay_export.py and ope_eval_report.py agree.
"""
from __future__ import annotations

import json

DEFAULT_KEY = "replay:recommendations"


def _parse(item) -> dict:
    if isinstance(item, bytes):
        item = item.decode("utf-8")
    return json.loads(item)


def load_from_redis(client, key: str = DEFAULT_KEY, limit: int = -1) -> list[dict]:
    return [_parse(x) for x in client.lrange(key, 0, limit)]


def load_from_parquet(path) -> list[dict]:
    import pandas as pd
    return pd.read_parquet(path).to_dict(orient="records")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_replay_buffer.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/replay_buffer.py \
        recsys-pipeline/integration-tests/python_modeling/test_replay_buffer.py
git commit -m "feat(ope): shared replay-buffer loader (real user/action schema)"
```

---

### Task 2: Fix replay_export contract (user/action)

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/replay_export.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_replay_export.py`

**Interfaces:**
- Consumes: `replay_buffer.load_from_redis`, `replay_buffer.load_from_parquet`, `replay_buffer.DEFAULT_KEY` (Task 1).
- Produces: `entries_to_rows(entries)` maps `e["user"]`→`userId`, `e["action"]`→`movieId`, `reward`→rating, `timestamp`→timestamp (unchanged names).

- [ ] **Step 1: Update the test fixtures + assertions to the real schema**

Replace the `SAMPLE_ENTRIES` block and the two field-name assertions in `test_replay_export.py`:

```python
SAMPLE_ENTRIES = [
    {"user": "u1", "action": "m001", "banditScore": 0.8, "reward": 0.9, "timestamp": 1718300000000},
    {"user": "u1", "action": "m002", "banditScore": 0.5, "reward": 0.2, "timestamp": 1718300001000},
    {"user": "u2", "action": "m001", "banditScore": 0.6, "reward": 1.0, "timestamp": 1718300002000},
]
```

In `test_convert_entries_reward_clipped_to_five` change the entry to
`{"user": "u1", "action": "m1", "reward": 1.5, "timestamp": 0}`.
In `test_write_parquet_roundtrips_raw_tuples` change the `issubset` set to
`{"user", "action", "banditScore", "reward", "timestamp"}`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_replay_export.py -v`
Expected: FAIL with `KeyError: 'userId'` inside `entries_to_rows`.

- [ ] **Step 3: Update `entries_to_rows` and `main` to use the real schema + shared loader**

In `replay_export.py`, replace `entries_to_rows`:

```python
def entries_to_rows(entries: list[dict]) -> list[dict]:
    rows = []
    for e in entries:
        rating = min(float(e.get("reward", 0.0)) * 5.0, 5.0)
        rows.append({
            "userId": str(e["user"]),
            "movieId": str(e["action"]),
            "rating": f"{rating:.1f}",
            "timestamp": str(int(e.get("timestamp", 0))),
        })
    return rows
```

Replace the body of `main` after `cfg = parser.parse_args(args)` with the shared loader:

```python
    import replay_buffer
    client = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=False)
    entries = replay_buffer.load_from_redis(client, cfg.key, cfg.limit)
    if not entries:
        print(f"No entries found at {cfg.key}")
        return
    write_csv(entries, cfg.output)
    if cfg.parquet is not None:
        write_parquet(entries, cfg.parquet)
```

(Delete the now-unused `import json` if no longer referenced; keep `--key` default as `replay_buffer.DEFAULT_KEY` is `"replay:recommendations"`, matching the current default.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_replay_export.py -v`
Expected: PASS (all tests). `test_main_reads_from_redis_mock` still asserts `lrange("replay:recommendations", 0, -1)`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/replay_export.py \
        recsys-pipeline/integration-tests/python_modeling/test_replay_export.py
git commit -m "fix(ope): replay_export reads real user/action schema via shared loader"
```

---

### Task 3: Pure-numpy logistic regression

**Files:**
- Create: `recsys-pipeline/services/python-modeling/logistic.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_logistic.py`

**Interfaces:**
- Produces:
  - `standardize(X) -> (Xs, mean, std)` — column standardize; `std` zeros replaced by 1.
  - `apply_standardize(X, mean, std) -> Xs`.
  - `fit(X, y, l2=1.0, lr=0.5, iters=500) -> w` — batch GD, adds intercept column internally; `y` may be soft (in [0,1]); returns weight vector of length `X.shape[1]+1`.
  - `predict_proba(X, w) -> probs` — sigmoid of `[1|X] @ w`.

- [ ] **Step 1: Write the failing test**

```python
# recsys-pipeline/integration-tests/python_modeling/test_logistic.py
import sys
from pathlib import Path
import numpy as np

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import logistic


def test_separable_data_learns_perfect_ranking():
    X = np.array([[-3.0], [-2.0], [-1.0], [1.0], [2.0], [3.0]])
    y = np.array([0, 0, 0, 1, 1, 1], dtype=float)
    Xs, mean, std = logistic.standardize(X)
    w = logistic.fit(Xs, y, l2=0.0, lr=0.5, iters=2000)
    p = logistic.predict_proba(Xs, w)
    # positives score above negatives
    assert p[3:].min() > p[:3].max()


def test_predict_proba_bounded():
    X = np.array([[10.0], [-10.0]])
    Xs, mean, std = logistic.standardize(X)
    w = logistic.fit(Xs, np.array([1.0, 0.0]), iters=100)
    p = logistic.predict_proba(Xs, w)
    assert np.all((p >= 0) & (p <= 1))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_logistic.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'logistic'`

- [ ] **Step 3: Write minimal implementation**

```python
# recsys-pipeline/services/python-modeling/logistic.py
"""Dependency-light logistic regression (numpy only). Supports soft targets in [0,1]."""
from __future__ import annotations

import numpy as np


def standardize(X):
    X = np.asarray(X, dtype=float)
    mean = X.mean(axis=0)
    std = X.std(axis=0)
    std = np.where(std == 0.0, 1.0, std)
    return (X - mean) / std, mean, std


def apply_standardize(X, mean, std):
    return (np.asarray(X, dtype=float) - mean) / std


def _design(X):
    X = np.asarray(X, dtype=float)
    return np.hstack([np.ones((X.shape[0], 1)), X])


def _sigmoid(z):
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def fit(X, y, l2=1.0, lr=0.5, iters=500):
    D = _design(X)
    y = np.asarray(y, dtype=float)
    w = np.zeros(D.shape[1])
    n = D.shape[0]
    for _ in range(iters):
        p = _sigmoid(D @ w)
        grad = D.T @ (p - y) / n
        grad[1:] += (l2 / n) * w[1:]  # L2 excludes intercept
        w -= lr * grad
    return w


def predict_proba(X, w):
    return _sigmoid(_design(X) @ w)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_logistic.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/logistic.py \
        recsys-pipeline/integration-tests/python_modeling/test_logistic.py
git commit -m "feat(ope): pure-numpy logistic regression helper"
```

---

### Task 4: Reward estimator (features, training, calibration)

**Files:**
- Create: `recsys-pipeline/services/python-modeling/ope_eval_report.py` (estimator half)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`

**Interfaces:**
- Consumes: `logistic.*` (Task 3), `auc` from `ranking_eval_report` (`ranking_eval_report.auc(scores, labels) -> float | None`).
- Produces:
  - `feature_names(events) -> list[str]` — `["coldStart","actionPosition","impressions","clicks"] + sorted(modelPredictions keys seen)`.
  - `taken_features(event, names) -> list[float]` — feature vector of the taken action.
  - `is_test(request_id) -> bool` — `md5(request_id) % 5 == 0`.
  - `RewardModel` with `.predict_one(feat_dict) -> float` and `.calibration -> dict(auc, mse, n_test)`.
  - `fit_reward_model(events) -> RewardModel`.

- [ ] **Step 1: Write the failing test**

```python
# recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import ope_eval_report as ope


def _event(rid, item, mp_rel, impressions, clicks, cold, reward, clicked, pos=0):
    return {
        "requestId": rid, "user": "u", "action": item,
        "actionPosition": pos, "coldStart": cold,
        "impressions": impressions, "clicks": clicks,
        "modelPredictions": {"relevance": mp_rel},
        "reward": reward, "clicked": clicked,
        "actionSpace": [
            {"item": item, "coldStart": cold, "impressions": impressions,
             "clicks": clicks, "modelPredictions": {"relevance": mp_rel}},
        ],
    }


def _dataset(n=200):
    # reward driven by relevance: higher relevance -> reward 1
    events = []
    for i in range(n):
        rel = (i % 10) / 10.0
        reward = 1.0 if rel >= 0.5 else 0.0
        events.append(_event(f"r{i}", f"m{i}", rel, impressions=i % 50,
                             clicks=int(reward), cold=False,
                             reward=reward, clicked=int(reward)))
    return events


def test_feature_names_include_model_prediction_keys():
    names = ope.feature_names(_dataset(5))
    assert names[:4] == ["coldStart", "actionPosition", "impressions", "clicks"]
    assert "relevance" in names


def test_reward_model_calibrates_on_learnable_signal():
    model = ope.fit_reward_model(_dataset(200))
    assert model.calibration["n_test"] > 0
    assert model.calibration["auc"] is None or model.calibration["auc"] >= 0.9
    assert model.calibration["mse"] <= 0.15
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_ope_eval.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ope_eval_report'`

- [ ] **Step 3: Write the estimator half of the module**

```python
# recsys-pipeline/services/python-modeling/ope_eval_report.py
#!/usr/bin/env python3
"""Off-policy evaluation (Direct Method) for bandit policies over the replay buffer.

Fits a dependency-light logistic reward estimator on logged (taken-action features ->
observed reward), then scores target policies re-picked from each event's actionSpace.
Run with plain python (numpy only):

    REDIS_HOST=localhost python services/python-modeling/ope_eval_report.py
"""
from __future__ import annotations

import argparse
import hashlib
import os

import numpy as np

import logistic
from ranking_eval_report import auc

BASE_FEATURES = ["coldStart", "actionPosition", "impressions", "clicks"]


def _model_pred_keys(events) -> list[str]:
    keys = set()
    for e in events:
        mp = e.get("modelPredictions") or {}
        keys.update(k for k, v in mp.items() if isinstance(v, (int, float)))
        for c in e.get("actionSpace", []):
            cmp = c.get("modelPredictions") or {}
            keys.update(k for k, v in cmp.items() if isinstance(v, (int, float)))
    return sorted(keys)


def feature_names(events) -> list[str]:
    return BASE_FEATURES + _model_pred_keys(events)


def _vec(cand_like: dict, names: list[str]) -> list[float]:
    mp = cand_like.get("modelPredictions") or {}
    out = []
    for n in names:
        if n == "coldStart":
            out.append(1.0 if cand_like.get("coldStart") else 0.0)
        elif n in ("actionPosition", "impressions", "clicks"):
            out.append(float(cand_like.get(n, 0) or 0))
        else:
            out.append(float(mp.get(n, 0.0) or 0.0))
    return out


def taken_features(event: dict, names: list[str]) -> list[float]:
    return _vec(event, names)


def is_test(request_id: str) -> bool:
    return int(hashlib.md5(str(request_id).encode()).hexdigest(), 16) % 5 == 0


class RewardModel:
    def __init__(self, names, mean, std, w, calibration):
        self.names = names
        self._mean = mean
        self._std = std
        self._w = w
        self.calibration = calibration

    def predict_one(self, cand_like: dict) -> float:
        X = np.array([_vec(cand_like, self.names)], dtype=float)
        Xs = logistic.apply_standardize(X, self._mean, self._std)
        return float(logistic.predict_proba(Xs, self._w)[0])


def fit_reward_model(events: list[dict]) -> RewardModel:
    names = feature_names(events)
    train = [e for e in events if not is_test(e.get("requestId", ""))]
    test = [e for e in events if is_test(e.get("requestId", ""))]
    if not train:
        train, test = events, events
    Xtr = np.array([taken_features(e, names) for e in train], dtype=float)
    ytr = np.array([float(e.get("reward", 0.0)) for e in train], dtype=float)
    Xs, mean, std = logistic.standardize(Xtr)
    w = logistic.fit(Xs, ytr, l2=1.0, lr=0.5, iters=500)

    Xte = np.array([taken_features(e, names) for e in test], dtype=float)
    pte = logistic.predict_proba(logistic.apply_standardize(Xte, mean, std), w)
    rte = np.array([float(e.get("reward", 0.0)) for e in test], dtype=float)
    clicked = [int(e.get("clicked", e.get("reward", 0.0) > 0)) for e in test]
    calibration = {
        "n_test": len(test),
        "mse": round(float(np.mean((pte - rte) ** 2)), 4) if len(test) else None,
        "auc": auc(list(pte), clicked) if len(test) else None,
    }
    return RewardModel(names, mean, std, w, calibration)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_ope_eval.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "feat(ope): logistic reward estimator with held-out calibration"
```

---

### Task 5: Policies, evaluation table, and CLI

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py` (append policy eval + main)
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py` (add eval + main tests)

**Interfaces:**
- Consumes: `fit_reward_model`, `RewardModel.predict_one`, `feature_names` (Task 4); `replay_buffer.load_from_redis`/`load_from_parquet` (Task 1).
- Produces:
  - `candidates_of(event) -> list[dict]` — `event["actionSpace"]` (list of candidate feature dicts).
  - `pick(policy, event) -> dict` — the chosen candidate dict for a target policy.
  - `POLICY_NAMES(events) -> list[str]` — `["logging","popularity","ctr","random"] + [f"model:{k}" for k in modelPredictions keys]`.
  - `evaluate(events, model) -> list[dict]` — rows `{policy, value, lift_vs_logging, n_events, estimator_auc, estimator_mse}`.
  - `main(argv=None) -> list[dict]` — CLI: load buffer, evaluate, print table, optional `--output` CSV.

- [ ] **Step 1: Write the failing tests (append)**

```python
def test_logging_value_equals_mean_observed_reward():
    events = _dataset(100)
    model = ope.fit_reward_model(events)
    rows = ope.evaluate(events, model)
    logging_row = next(r for r in rows if r["policy"] == "logging")
    mean_reward = sum(e["reward"] for e in events) / len(events)
    assert abs(logging_row["value"] - mean_reward) < 1e-9
    assert logging_row["lift_vs_logging"] == 0.0


def test_relevance_policy_beats_random_on_learnable_signal():
    # candidates carry two items: one high-relevance, one low; reward tracks relevance
    events = []
    for i in range(150):
        hi = {"item": f"h{i}", "coldStart": False, "impressions": 5, "clicks": 5,
              "modelPredictions": {"relevance": 0.9}}
        lo = {"item": f"l{i}", "coldStart": False, "impressions": 5, "clicks": 0,
              "modelPredictions": {"relevance": 0.1}}
        taken = hi if i % 2 == 0 else lo
        events.append({
            "requestId": f"r{i}", "user": "u", "action": taken["item"],
            "actionPosition": 0, "coldStart": False,
            "impressions": taken["impressions"], "clicks": taken["clicks"],
            "modelPredictions": taken["modelPredictions"],
            "reward": 1.0 if taken is hi else 0.0,
            "clicked": 1 if taken is hi else 0,
            "actionSpace": [hi, lo],
        })
    model = ope.fit_reward_model(events)
    rows = {r["policy"]: r["value"] for r in ope.evaluate(events, model)}
    assert rows["model:relevance"] > rows["random"]


def test_main_reads_redis_and_writes_csv(tmp_path):
    import json
    from unittest.mock import MagicMock, patch
    events = _dataset(40)
    raw = [json.dumps(e).encode() for e in events]
    client = MagicMock(); client.lrange.return_value = raw
    out = tmp_path / "ope.csv"
    with patch("ope_eval_report.redis.Redis", return_value=client):
        rows = ope.main(["--output", str(out)])
    assert out.is_file()
    assert any(r["policy"] == "logging" for r in rows)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_ope_eval.py -v`
Expected: FAIL with `AttributeError: module 'ope_eval_report' has no attribute 'evaluate'`

- [ ] **Step 3: Append policy eval + CLI to `ope_eval_report.py`**

Add `import csv` and `import redis` to the imports at the top, then append:

```python
def candidates_of(event: dict) -> list[dict]:
    return event.get("actionSpace") or []


def _ctr(c: dict) -> float:
    imp = float(c.get("impressions", 0) or 0)
    return (float(c.get("clicks", 0) or 0) / imp) if imp > 0 else 0.0


def _rng_index(request_id: str, n: int) -> int:
    h = int(hashlib.md5((str(request_id) + "random").encode()).hexdigest(), 16)
    return h % n


def pick(policy: str, event: dict) -> dict | None:
    cands = candidates_of(event)
    if not cands:
        return None
    if policy == "popularity":
        return max(cands, key=lambda c: float(c.get("impressions", 0) or 0))
    if policy == "ctr":
        return max(cands, key=_ctr)
    if policy == "random":
        return cands[_rng_index(event.get("requestId", ""), len(cands))]
    if policy.startswith("model:"):
        key = policy.split(":", 1)[1]
        return max(cands, key=lambda c: float((c.get("modelPredictions") or {}).get(key, 0.0) or 0.0))
    raise ValueError(f"unknown policy {policy}")


def policy_names(events) -> list[str]:
    return ["logging", "popularity", "ctr", "random"] + [f"model:{k}" for k in _model_pred_keys(events)]


def evaluate(events: list[dict], model: RewardModel) -> list[dict]:
    n = len(events)
    logging_value = sum(float(e.get("reward", 0.0)) for e in events) / n if n else 0.0
    rows = []
    for name in policy_names(events):
        if name == "logging":
            value = logging_value
        else:
            picks = [pick(name, e) for e in events]
            scored = [model.predict_one(c) for c in picks if c is not None]
            value = sum(scored) / len(scored) if scored else 0.0
        lift = (value / logging_value - 1.0) if logging_value else 0.0
        rows.append({
            "policy": name,
            "value": round(value, 4),
            "lift_vs_logging": round(lift, 4),
            "n_events": n,
            "estimator_auc": model.calibration["auc"],
            "estimator_mse": model.calibration["mse"],
        })
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


def main(argv=None) -> list[dict]:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--key", default="replay:recommendations")
    ap.add_argument("--parquet", default=None)
    ap.add_argument("--output", default=None)
    ap.add_argument("--limit", type=int, default=-1)
    args = ap.parse_args(argv)
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))

    import replay_buffer
    if args.parquet:
        events = replay_buffer.load_from_parquet(args.parquet)
    else:
        client = redis.Redis(host=host, port=port, decode_responses=False)
        events = replay_buffer.load_from_redis(client, args.key, args.limit)

    events = [e for e in events if e.get("reward") is not None]
    if not events:
        raise SystemExit("no feedback-completed replay events (with reward) — nothing to evaluate")

    model = fit_reward_model(events)
    rows = evaluate(events, model)

    cal = model.calibration
    print(f"reward estimator calibration: auc={cal['auc']} mse={cal['mse']} (n_test={cal['n_test']})")
    for r in rows:
        print(f"  {r['policy']:16s} value={r['value']:.4f}  lift={r['lift_vs_logging']:+.4f}  n={r['n_events']}")
    if args.output:
        with open(args.output, "w", newline="") as fh:
            wr = csv.DictWriter(fh, fieldnames=["policy", "value", "lift_vs_logging",
                                                "n_events", "estimator_auc", "estimator_mse"])
            wr.writeheader()
            wr.writerows(rows)
        print(f"wrote {args.output}")
    return rows


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_ope_eval.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "feat(ope): Direct-Method policy comparison report + CLI"
```

---

### Task 6: Full-suite green + end-to-end smoke

**Files:** none (verification only)

- [ ] **Step 1: Run the focused Python suite**

Run: `cd recsys-pipeline/services/python-modeling && python -m pytest ../../integration-tests/python_modeling/test_replay_buffer.py ../../integration-tests/python_modeling/test_replay_export.py ../../integration-tests/python_modeling/test_logistic.py ../../integration-tests/python_modeling/test_ope_eval.py -v`
Expected: PASS (all).

- [ ] **Step 2: Smoke the CLI against synthetic events (no Redis)**

Run:
```bash
cd recsys-pipeline/services/python-modeling && python -c "
import json, ope_eval_report as ope
from test_ope_eval import _dataset  # reuse fixture
import sys; sys.path.insert(0, '../../integration-tests/python_modeling')
"
```
If the import path is awkward, instead assert via pytest only (Step 1). Expected: no error.

- [ ] **Step 3: Commit (if any doc/notes updated; otherwise skip)**

No code change expected; this task is a gate.

---

## Self-Review

**Spec coverage:**
- Contract fix → Tasks 1–2. ✓
- Shared loader `replay_buffer.py` → Task 1. ✓
- Pure-numpy logistic, no sklearn → Task 3. ✓
- Reward estimator on taken-action→observed reward + held-out AUC/MSE → Task 4. ✓
- Policies (logging anchor / popularity / ctr / model:<key> / random) → Task 5. ✓
- Table + CSV shape `policy | value | lift_vs_logging | n_events | estimator_auc | estimator_mse` → Task 5. ✓
- CLI reads same buffer, empty/no-reward guard → Task 5 `main`. ✓
- Determinism (md5 split + md5 random) → Tasks 4–5. ✓

**Placeholder scan:** none — every code step is complete.

**Type consistency:** `feature_names`/`taken_features`/`_vec`/`_model_pred_keys` names consistent across Tasks 4–5; `RewardModel.predict_one`, `.calibration` used consistently; `auc` imported from `ranking_eval_report`; loader function names match Task 1.

**Note for implementer:** Task 5 Step 3 says to add `import csv` and `import redis` to the top of `ope_eval_report.py` (Task 4 created it without them).
