# Reward Model Scoring Reuse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Score each replay event once per policy under the fixed reward model and reuse those scores for point rows and every bootstrap replicate, without changing the fit or any reported contract.

**Architecture:** `RewardModel` gains a vectorized `predict_batch`. A helper builds an events × policies score matrix (NaN where a policy has no pick). Point rows and bootstrap replicates both reduce that matrix with NaN-aware counts and sums; replicates index rows with the unchanged RNG sequence.

**Tech Stack:** Python 3.12, numpy, pytest. No new dependencies.

**Spec:** [Reward model scoring reuse](../specs/2026-09-12-reward-model-scoring-reuse-design.md)

## Global Constraints

- Python 3 with numpy only; add no dependencies.
- Keep `fit_reward_model` unchanged: the md5 request split, feature schema, `POLICY_ONLY_PRED_KEYS` exclusion, standardization, `l2=1.0, lr=0.5, iters=500`, and the calibration dictionary.
- Keep `pick`, `policy_names`, `feature_names`, `taken_features`, and `is_test` unchanged; other modules import them.
- Keep the CLI flags, printed lines, CSV columns, and `INTERVAL_FIELDS` unchanged.
- Keep the bootstrap RNG call sequence unchanged: one `rng.integers(0, n, size=n)` per replicate from `np.random.default_rng(seed)`.
- Keep the row semantics: `n_events` is the number of events with a pick, an empty pick set yields value `0.0`, lift is `None` when the logging value is not positive, rows sort by value descending, and `evaluate` rounds only the reported rows.

## Execution notes

All paths are repository-relative. Run pytest from `recsys-pipeline/` with the conda `python3`
(3.12, numpy 2.5). The work is on `perf/reward-model-training`, based on `origin/master`.
Publish a PR against `master`; never commit to `master`.

Baseline before any change (2026-09-12): `python3 -m pytest integration-tests/python_modeling/test_ope_eval.py integration-tests/python_modeling/test_logistic.py integration-tests/python_modeling/test_post_training_q.py integration-tests/python_modeling/test_post_training_dpo.py integration-tests/python_modeling/test_analysis_dashboard.py -q` → 124 passed.

---

### Task 1: Vectorized prediction on `RewardModel`

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:105-116`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`

**Interfaces:**
- Consumes: `_vec(cand_like, names)`, `logistic.apply_standardize`, `logistic.predict_proba` (existing).
- Produces: `RewardModel.predict_batch(cand_likes: list[dict]) -> np.ndarray` of shape `(len(cand_likes),)`; empty input returns an empty float array. `predict_one(cand_like) -> float` unchanged in behavior.

- [ ] **Step 1: Write the failing parity test**

Append to `test_ope_eval.py`:

```python
def test_predict_batch_matches_predict_one():
    import numpy as np
    events = _dataset(60)
    model = ope.fit_reward_model(events)
    candidates = [ope.candidates_of(e)[0] for e in events]
    batch = model.predict_batch(candidates)
    assert batch.shape == (len(candidates),)
    assert all(abs(float(b) - model.predict_one(c)) < 1e-12
               for b, c in zip(batch, candidates))
    assert model.predict_batch([]).shape == (0,)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py::test_predict_batch_matches_predict_one -q`
Expected: FAIL with `AttributeError: 'RewardModel' object has no attribute 'predict_batch'`

- [ ] **Step 3: Implement `predict_batch` and route `predict_one` through it**

Replace the `RewardModel.predict_one` method with:

```python
    def predict_batch(self, cand_likes: list[dict]) -> np.ndarray:
        """Estimated reward for each candidate-like dict, in order.

        One standardize and one sigmoid over the whole list; scoring candidate by candidate
        pays numpy's per-call overhead once per candidate instead of once per policy.
        """
        if not cand_likes:
            return np.zeros(0, dtype=float)
        X = np.array([_vec(c, self.names) for c in cand_likes], dtype=float)
        Xs = logistic.apply_standardize(X, self._mean, self._std)
        return logistic.predict_proba(Xs, self._w)

    def predict_one(self, cand_like: dict) -> float:
        return float(self.predict_batch([cand_like])[0])
```

- [ ] **Step 4: Run the OPE tests**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py -q`
Expected: 22 passed (21 existing + the new one).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "perf: vectorize reward model prediction over candidate lists"
```

---

### Task 2: Per-event policy score matrix for point rows

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:174-198` (`_evaluate_statistics`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py` (new test; update `ExactModel` at lines ~190-203)

**Interfaces:**
- Consumes: `pick(policy, event)`, `RewardModel.predict_batch` from Task 1.
- Produces:
  - `_policy_scores(events, model, policies) -> tuple[np.ndarray, np.ndarray]`: `(rewards, scores)` where `rewards` has shape `(n,)` and `scores` has shape `(n, len(policies))`; the `logging` column equals `rewards`; other columns are `predict_batch` over picks with `NaN` where `pick` returned `None`; `predict_batch` is called exactly once per non-logging policy.
  - `_rows_from_scores(rewards, scores, policies, model) -> list[dict]`: the same row dicts `_evaluate_statistics` returns today (`policy`, `value`, `lift_vs_logging`, `n_events`, `estimator_auc`, `estimator_mse`), sorted by value descending.
  - `_evaluate_statistics(events, model, policies)` keeps its signature and semantics.

- [ ] **Step 1: Write the failing parity test with an empty action space**

Append to `test_ope_eval.py`:

```python
def test_evaluate_statistics_matches_per_event_scoring_with_an_empty_action_space():
    events = _dataset(30)
    events.append({"requestId": "empty", "user": "u", "action": None, "coldStart": False,
                   "modelPredictions": {}, "reward": 1.0, "clicked": 1, "actionSpace": []})
    model = ope.fit_reward_model(events)
    policies = ope.policy_names(events)

    reference = {}
    logging_value = sum(e["reward"] for e in events) / len(events)
    for name in policies:
        if name == "logging":
            reference[name] = (logging_value, len(events))
            continue
        scored = [model.predict_one(c) for c in (ope.pick(name, e) for e in events)
                  if c is not None]
        reference[name] = (sum(scored) / len(scored), len(scored))

    rows = {r["policy"]: r for r in ope._evaluate_statistics(events, model, policies)}
    assert set(rows) == set(policies)
    for name, (value, count) in reference.items():
        assert rows[name]["n_events"] == count
        assert rows[name]["value"] == pytest.approx(value, abs=1e-12)
    assert rows["popularity"]["n_events"] == len(events) - 1
    assert rows["logging"]["n_events"] == len(events)
    assert rows["logging"]["lift_vs_logging"] == 0.0
```

- [ ] **Step 2: Run it to confirm it passes against the current code, then update the stub**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py::test_evaluate_statistics_matches_per_event_scoring_with_an_empty_action_space -q`
Expected: PASS. This is a characterization test; it must keep passing after the rewrite.

Then make the existing `ExactModel` stub in `test_evaluate_uses_raw_statistics_before_rounding_report_rows` batch-capable, replacing its body with:

```python
    class ExactModel:
        calibration = {"auc": None, "mse": None}

        def predict_batch(self, candidates):
            import numpy as np
            return np.array([float(c["impressions"]) / 7.0 for c in candidates])
```

- [ ] **Step 3: Rewrite `_evaluate_statistics` on the score matrix**

Replace `_evaluate_statistics` with:

```python
def _policy_scores(events: list[dict], model, policies: list[str]):
    """Observed rewards and an events x policies matrix of estimated rewards.

    The `logging` column is the observed reward. Every other column is the fixed model's
    estimate of that policy's pick, NaN where the event has no candidates. The model does not
    change during evaluation, so this matrix is scored once and reused by the point rows and by
    every bootstrap replicate; each non-logging policy calls predict_batch exactly once.
    """
    rewards = np.array([float(e.get("reward", 0.0)) for e in events], dtype=float)
    scores = np.full((len(events), len(policies)), np.nan, dtype=float)
    for column, name in enumerate(policies):
        if name == "logging":
            scores[:, column] = rewards
            continue
        picks = [pick(name, e) for e in events]
        present = [i for i, c in enumerate(picks) if c is not None]
        if present:
            scores[present, column] = model.predict_batch([picks[i] for i in present])
    return rewards, scores


def _rows_from_scores(rewards: np.ndarray, scores: np.ndarray, policies: list[str],
                      model) -> list[dict]:
    n = rewards.shape[0]
    logging_value = float(rewards.sum() / n) if n else 0.0
    counts = np.count_nonzero(~np.isnan(scores), axis=0)
    sums = np.nansum(scores, axis=0)
    rows = []
    for column, name in enumerate(policies):
        if name == "logging":
            # Assigned rather than re-derived from the column so the logging lift is exactly 0.0.
            value, count = logging_value, n
        else:
            count = int(counts[column])
            value = float(sums[column] / count) if count else 0.0
        lift = (value / logging_value - 1.0) if logging_value > 0.0 else None
        rows.append({
            "policy": name,
            "value": value,
            "lift_vs_logging": lift,
            "n_events": count,
            "estimator_auc": model.calibration["auc"],
            "estimator_mse": model.calibration["mse"],
        })
    rows.sort(key=lambda r: r["value"], reverse=True)
    return rows


def _evaluate_statistics(events: list[dict], model, policies: list[str]) -> list[dict]:
    rewards, scores = _policy_scores(events, model, policies)
    return _rows_from_scores(rewards, scores, policies, model)
```

Remove the old `_evaluate_statistics` body entirely; `RewardModel` type hints on `model` are dropped because the test stub is duck-typed.

- [ ] **Step 4: Run the OPE tests**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py -q`
Expected: 23 passed.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "perf: score each event once per policy for OPE point rows"
```

---

### Task 3: Bootstrap replicates resample the score matrix

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py` (`bootstrap_intervals`)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`

**Interfaces:**
- Consumes: `_policy_scores`, `_rows_from_scores` from Task 2.
- Produces: `bootstrap_intervals(events, model, point_rows, samples=1000, seed=20260716)` with unchanged signature, return shape, and RNG sequence.

- [ ] **Step 1: Write the failing prediction-count test**

Append to `test_ope_eval.py`:

```python
def test_bootstrap_scores_each_policy_once_regardless_of_sample_count():
    events = _dataset(50)
    model = ope.fit_reward_model(events)
    points = ope.evaluate(events, model)
    non_logging = [row["policy"] for row in points if row["policy"] != "logging"]
    assert len(non_logging) == 4  # popularity, ctr, random, model:relevance

    calls = []
    original = model.predict_batch
    model.predict_batch = lambda candidates: (calls.append(len(candidates)), original(candidates))[1]

    rows = ope.bootstrap_intervals(events, model, points, samples=40, seed=19)

    assert len(calls) == len(non_logging)
    assert all(row["value_ci_low"] is not None for row in rows)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py::test_bootstrap_scores_each_policy_once_regardless_of_sample_count -q`
Expected: FAIL with `assert 160 == 4` (40 replicates × 4 policies).

- [ ] **Step 3: Resample the matrix instead of re-evaluating events**

In `bootstrap_intervals`, replace the block from `policies = [...]` through the end of the `for _ in range(samples)` loop with:

```python
    policies = [row["policy"] for row in point_rows]
    stats = {policy: {"value": [], "lift": []} for policy in policies}
    rewards, scores = _policy_scores(events, model, policies)
    rng = np.random.default_rng(seed)
    for _ in range(samples):
        indexes = rng.integers(0, len(events), size=len(events))
        for row in _rows_from_scores(rewards[indexes], scores[indexes], policies, model):
            stats[row["policy"]]["value"].append(float(row["value"]))
            if row["lift_vs_logging"] is not None:
                stats[row["policy"]]["lift"].append(float(row["lift_vs_logging"]))
```

Leave the `enriched` construction, the `samples == 0 or not events` early return, and the percentile loop untouched. Extend the docstring with one line:

```
    Every event is scored once per policy before resampling; replicates only re-index those scores.
```

- [ ] **Step 4: Run the OPE, post-training, dashboard, and logistic tests**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py integration-tests/python_modeling/test_logistic.py integration-tests/python_modeling/test_post_training_q.py integration-tests/python_modeling/test_post_training_dpo.py integration-tests/python_modeling/test_analysis_dashboard.py -q`
Expected: 127 passed (124 baseline + 3 new).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py
git commit -m "perf: resample precomputed policy scores in the OPE bootstrap"
```

---

### Task 4: Benchmark, docs, full suite, and PR

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md:137-140`
- Modify: `.superpowers/docs/specs/2026-09-12-reward-model-scoring-reuse-design.md` (status and verification record)
- Modify: this plan (check boxes, record measurements)

**Interfaces:**
- Consumes: the finished module from Tasks 1-3.
- Produces: a PR against `master` from `perf/reward-model-training`.

- [ ] **Step 1: Re-run the investigation benchmark**

Write this to the session scratchpad (not the repository) as `bench_rm.py` and run `python3 bench_rm.py 5000` from `recsys-pipeline/`:

```python
import sys, time, random
sys.path.insert(0, "services/python-modeling")
import ope_eval_report as ope

def make(n, k=10, seed=0):
    rnd = random.Random(seed)
    events = []
    for i in range(n):
        cands = []
        for j in range(k):
            rel = rnd.random()
            cands.append({"item": f"m{i}_{j}", "coldStart": rnd.random() < 0.1,
                          "impressions": rnd.randint(0, 100), "clicks": rnd.randint(0, 10),
                          "modelPredictions": {"relevance": rel, "predictionScore": rel*0.8+rnd.random()*0.2,
                                               "i2v": rnd.random(), "als": rnd.random(), "tabQ": rnd.random()}})
        taken = cands[rnd.randrange(k)]
        reward = 1.0 if rnd.random() < taken["modelPredictions"]["relevance"] else 0.0
        events.append({"requestId": f"req-{i}", "user": f"u{i%100}", "action": taken["item"],
                       "actionPosition": 0, "coldStart": taken["coldStart"],
                       "modelPredictions": taken["modelPredictions"], "reward": reward,
                       "clicked": int(reward), "actionSpace": cands})
    return events

events = make(int(sys.argv[1]) if len(sys.argv) > 1 else 5000)
t0 = time.perf_counter(); model = ope.fit_reward_model(events); t1 = time.perf_counter()
print(f"fit_reward_model: {t1-t0:.3f}s")
t0 = time.perf_counter(); pts = ope.evaluate(events, model); t1 = time.perf_counter()
print(f"evaluate: {t1-t0:.3f}s")
t0 = time.perf_counter(); ope.bootstrap_intervals(events, model, pts, samples=1000, seed=1); t1 = time.perf_counter()
print(f"bootstrap 1000 samples: {t1-t0:.3f}s")
```

Expected: fit unchanged near 0.06 s, evaluate below 0.38 s, bootstrap of 1,000 samples in single-digit seconds (baseline extrapolated ~370 s). Record the numbers in the spec's verification record.

- [ ] **Step 2: Document the scoring behavior**

In `Analysis_Report.md`, after the sentence ending `by later `POST /feedback` calls.` in the "Off-policy evaluation" section, add:

```
Each event is scored once under every policy against the fixed estimator; the bootstrap resamples
those per-event scores rather than re-picking and re-scoring the slate in every replicate.
```

- [ ] **Step 3: Run the full Python modeling suite**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling -q`
Expected: all pass. If unrelated tests fail, confirm they fail on `origin/master` too before reporting; do not claim a green suite otherwise.

- [ ] **Step 4: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` section listing the pytest counts and benchmark timings observed in Steps 1 and 3.

```bash
git add recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md .superpowers/docs/specs/2026-09-12-reward-model-scoring-reuse-design.md .superpowers/docs/plans/2026-09-12-reward-model-scoring-reuse.md
git commit -m "docs: record reward model scoring reuse verification"
```

- [ ] **Step 5: Open the PR**

```bash
git push -u origin perf/reward-model-training
gh pr create --base master --title "perf: reuse reward model scores across OPE bootstrap replicates" --body-file <(cat <<'EOF'
## Summary
- Vectorize `RewardModel` prediction and score each replay event once per policy under the fixed reward model.
- Point rows and every bootstrap replicate reduce that per-event score matrix instead of re-picking and re-scoring the slate.
- Fit, feature schema, split, CLI flags, CSV columns, and the bootstrap RNG sequence are unchanged.

## Measurements
Synthetic buffer of 5,000 events × 10 candidates, 9 policies, 1,000 bootstrap samples: <fill from Step 1>.

## Test plan
- [x] `python3 -m pytest integration-tests/python_modeling -q`
- [x] New tests: batch/one parity, per-event parity with an empty action space, one prediction call per policy per bootstrap run.

Spec: `.superpowers/docs/specs/2026-09-12-reward-model-scoring-reuse-design.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)
```

Then record the PR link in the spec's status line and commit it with `docs: link reward model scoring reuse PR`.

---

## Self-review

- Spec coverage: batch prediction (Task 1), score matrix and reduction with preserved row semantics (Task 2), bootstrap reuse with unchanged RNG (Task 3), duck-typed stub (Task 2), docs and benchmark and PR (Task 4). Acceptance criteria 1-7 map to Task 3 Step 1, Task 2 Step 1, Task 1 Step 1, existing tests run in Task 3 Step 4, the CLI test in the same run, Task 3 Step 4, and Task 4 Step 1.
- Placeholders: the PR body has one `<fill from Step 1>` slot that Task 4 Step 1 provides; nothing else is deferred.
- Names used across tasks: `predict_batch`, `_policy_scores`, `_rows_from_scores`, `_evaluate_statistics`, `bootstrap_intervals` are consistent.
