# DPO Plumbing Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the three duplications around the DPO arm (two pair builders, a private feature-vector import in five places, and copied CLI plumbing) without changing any computed value or printed line.

**Architecture:** `ope_eval_report` exposes `candidate_features`. `replay_dataset` becomes the home of the shared CLI helpers `load_events` and `candidate_rows`. `slate_pairs` keeps one `build_pairs` that returns diagnostics. Both CLIs shrink to their key-writing lines. A byte-level parity run on a fixed-seed fixture guards the whole change.

**Tech Stack:** Python 3.12, numpy, pandas, torch 2.11, pytest.

**Spec:** [DPO plumbing simplification](../specs/2026-09-12-dpo-plumbing-simplification-design.md)

## Global Constraints

- Python 3 with numpy, pandas, and torch as already used; add no dependencies.
- Keep `dpo.py` unchanged.
- Keep `PreferencePair`, `JoinDiagnostics`, `is_chosen`, `replay_index`, and `REFERENCE_PRED_KEY` unchanged.
- Keep both CLIs' arguments, returned summary dictionaries, printed lines, and output Parquet contents identical for a fixed seed.
- Keep `ope_eval_report`'s public behavior unchanged apart from the rename; `taken_features` and `predict_batch` keep their results.
- Keep every key written by either `score_events` registered in `POLICY_ONLY_PRED_KEYS`.

## Execution notes

All paths are repository-relative. Run pytest from `recsys-pipeline/` with the conda `python3`.
The work is on `simplify/dpo-plumbing`, based on `origin/master`. Publish a PR against `master`;
never commit to `master`.

Baselines (2026-09-12): `test_post_training_dpo.py` + `test_post_training_q.py` → 74 passed.
The parity script `dpo_parity.py` in the session scratchpad was run on the unchanged code and its
output saved as `dpo_parity_before.json` (6,846 bytes; 24 pairs from 12 slates). It runs
`post_train_q.main` then `post_train_dpo.main` on a joined fixture with one null `modelPredictions`,
and dumps both summaries, both stdouts, and every candidate's `modelPredictions` as sorted JSON.

---

### Task 1: Rename `_vec` to `candidate_features`

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:74,98,121`
- Modify: `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py:21-22`
- Modify: `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py:86`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py:87`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_q.py:96`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py:327`

**Interfaces:**
- Produces: `ope_eval_report.candidate_features(cand_like: dict, names: list[str]) -> list[float]`, same body as the old `_vec`.

- [x] **Step 1: Rename the definition and its two internal callers**

In `ope_eval_report.py` change `def _vec(` to `def candidate_features(`, and the two calls `_vec(_taken_candidate(event), names)` and `_vec(c, self.names)` to `candidate_features(...)`.

- [x] **Step 2: Update the external callers**

In `replay_dataset.py` replace

```python
from ope_eval_report import _vec as candidate_features
from ope_eval_report import feature_names, taken_features
```

with

```python
from ope_eval_report import candidate_features, feature_names, taken_features
```

In `slate_pairs.py`, `post_train_dpo.py`, and `post_train_q.py` replace `ope_eval_report._vec(candidate, names)` with `ope_eval_report.candidate_features(candidate, names)`. In `test_post_training_q.py` replace `ope_eval_report._vec(candidate, names)` with `ope_eval_report.candidate_features(candidate, names)`.

- [x] **Step 3: Verify no private name remains and the suites pass**

Run: `grep -rn "_vec\b" recsys-pipeline/services/python-modeling recsys-pipeline/integration-tests --include='*.py'`
Expected: no output.

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py integration-tests/python_modeling/test_post_training_q.py integration-tests/python_modeling/test_ope_eval.py -q`
Expected: 97 passed (74 + 23). Observed: 97 passed.

- [x] **Step 4: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py recsys-pipeline/services/python-modeling/post-training/replay_dataset.py recsys-pipeline/services/python-modeling/post-training/slate_pairs.py recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py recsys-pipeline/services/python-modeling/post-training/post_train_q.py recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py
git commit -m "refactor: expose candidate_features instead of a private _vec"
```

---

### Task 2: One pair builder

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py:92-151`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py:140`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py`

**Interfaces:**
- Produces: `slate_pairs.build_pairs(slates, events, names=None) -> tuple[list[PreferencePair], int, JoinDiagnostics]`.

- [x] **Step 1: Update the tests to the three-value shape**

In `test_post_training_dpo.py`:

- every `pairs, dropped = slate_pairs.build_pairs(...)` becomes `pairs, dropped, _ = slate_pairs.build_pairs(...)`
- every `pairs, _ = slate_pairs.build_pairs(...)` becomes `pairs, _, _ = slate_pairs.build_pairs(...)`
- the two `assert slate_pairs.build_pairs(slates, events) == ([], 0)` lines become

```python
    pairs, dropped, _ = slate_pairs.build_pairs(slates, events)
    assert (pairs, dropped) == ([], 0)
```

- every `slate_pairs.build_pairs_with_diagnostics(` becomes `slate_pairs.build_pairs(`.

- [x] **Step 2: Run the DPO tests to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -q`
Expected: failures with `ValueError: not enough values to unpack` on the two-value returns and `AttributeError` on the removed name. Observed: 19 failed, 15 passed.

- [x] **Step 3: Collapse the builders**

In `slate_pairs.py` delete the `build_pairs` wrapper (its `def`, docstring, and two-line body) and rename `build_pairs_with_diagnostics` to `build_pairs`, replacing its docstring's first line with:

```python
    """Cross every chosen item with every rejected item WITHIN each slate.

    Returns (pairs, dropped, JoinDiagnostics). `dropped` counts pairs discarded because one side
    had no replay row to supply features. A low join yield is a finding worth reporting, never
    something to swallow -- and a zero yield is usually a namespace mismatch rather than sparsity,
    which is what the matched-request_id count exposes.
    """
```

In `post_train_dpo.py` change `slate_pairs.build_pairs_with_diagnostics(slates, events, names)` to `slate_pairs.build_pairs(slates, events, names)`.

- [x] **Step 4: Verify**

Run: `grep -rn "build_pairs_with_diagnostics" recsys-pipeline --include='*.py' --include='*.md'`
Expected: no output.

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -q`
Expected: 34 passed. Observed: 34 passed.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/slate_pairs.py recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py
git commit -m "refactor: one build_pairs that returns its diagnostics"
```

---

### Task 3: Shared loader and candidate rows

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py:31,77-109`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_q.py:22,86-123`

**Interfaces:**
- Produces: `replay_dataset.load_events(args) -> list[dict]` (reads `args.parquet`, `args.key`, `args.limit`) and `replay_dataset.candidate_rows(events, names)` yielding `(event, candidate, features, predictions)`.
- Consumes: `ope_support.load_from_parquet`, `ope_support.load_from_redis`, `candidate_features` from Task 1.

- [x] **Step 1: Add the helpers to `replay_dataset.py`**

Add `import os` after `from __future__ import annotations` (before `from collections import defaultdict`), and `import ope_support` next to the `ope_eval_report` import. Append at the end of the file:

```python
def load_events(args) -> list[dict]:
    """Replay events from `--parquet` when given, otherwise from Redis at REDIS_HOST / REDIS_PORT."""
    if args.parquet:
        return ope_support.load_from_parquet(args.parquet)
    import redis
    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        decode_responses=False,
    )
    return ope_support.load_from_redis(client, args.key, args.limit)


def candidate_rows(events, names):
    """Every candidate in every event's actionSpace, as (event, candidate, features, predictions).

    `predictions` is the candidate's modelPredictions dict, created in place when it is None --
    which is what Parquet yields for an absent nested struct -- so a scorer can write its policy
    key straight into it. Every key written there must be registered in
    ope_eval_report.POLICY_ONLY_PRED_KEYS, or the reward model is fit on the scores it grades.
    """
    for event in events:
        for candidate in as_list(event.get("actionSpace")):
            predictions = candidate.get("modelPredictions")
            if predictions is None:
                predictions = candidate["modelPredictions"] = {}
            yield event, candidate, candidate_features(candidate, names), predictions
```

- [x] **Step 2: Slim the DPO CLI**

In `post_train_dpo.py` delete `import os`. Replace `score_events` and `_load_events` (from `def score_events` through the `return ope_support.load_from_redis(...)` line) with:

```python
def score_events(events, names, policy):
    """Write dpoScore into every candidate's modelPredictions, in place."""
    rows = list(replay_dataset.candidate_rows(events, names))
    scores = policy.score_many([features for _, _, features, _ in rows])
    for (_, _, _, predictions), score in zip(rows, scores):
        predictions[ope_eval_report.DPO_PRED_KEY] = float(score)
    return events
```

In `main`, change `events = _load_events(args)` to `events = replay_dataset.load_events(args)`.

- [x] **Step 3: Slim the Q CLI**

In `post_train_q.py` delete `import os`. Replace `score_events` and `_load_events` (from `def score_events` through the `return ope_support.load_from_redis(...)` line) with:

```python
def score_events(events, names, q, model):
    """Write the two policy-score keys into every candidate's modelPredictions, in place."""
    rows = list(replay_dataset.candidate_rows(events, names))
    fqi_scores = model.score_many([features for _, _, features, _ in rows])
    for (event, candidate, _, predictions), q_value in zip(rows, fqi_scores):
        state = replay_dataset.state_key(event.get("state"))
        # Key names come from ope_eval_report so the producer and the schema-exclusion list cannot
        # drift apart; see POLICY_ONLY_PRED_KEYS there for why that matters.
        predictions[ope_eval_report.TABULAR_Q_PRED_KEY] = tabular_q.score(
            q, state, str(candidate.get("item")))
        predictions[ope_eval_report.FQI_Q_PRED_KEY] = float(q_value)
    return events
```

In `main`, change `events = _load_events(args)` to `events = replay_dataset.load_events(args)`. If `ope_support` is no longer referenced in `post_train_q.py`, delete its import; in `post_train_dpo.py` it is still used for `--slates`.

- [x] **Step 4: Run the suites and the parity script**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py integration-tests/python_modeling/test_post_training_q.py -q`
Expected: 74 passed. Observed: 74 passed.

Run: `cd recsys-pipeline && python3 <scratchpad>/dpo_parity.py > <scratchpad>/dpo_parity_after.json && cmp <scratchpad>/dpo_parity_before.json <scratchpad>/dpo_parity_after.json && echo IDENTICAL`
Expected: `IDENTICAL`. Observed: identical.

Run: `grep -n "_load_events\|^import os" recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py recsys-pipeline/services/python-modeling/post-training/post_train_q.py`
Expected: no output.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/replay_dataset.py recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py recsys-pipeline/services/python-modeling/post-training/post_train_q.py
git commit -m "refactor: share the replay loader and candidate rows across post-training CLIs"
```

---

### Task 4: Full suite, records, and PR

**Files:**
- Modify: `.superpowers/docs/specs/2026-09-12-dpo-plumbing-simplification-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)

**Interfaces:**
- Consumes: the finished work from Tasks 1-3.
- Produces: a PR against `master` from `simplify/dpo-plumbing`.

- [x] **Step 1: Run the full Python modeling suite and count the line change**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling -q`
Expected: 501 passed. Observed: 501 passed.

Run: `git diff origin/master --stat -- recsys-pipeline/services/python-modeling`
Expected: `post_train_dpo.py` plus `post_train_q.py` net at least 30 lines removed. Observed: 54 net removed across the two; branch total 70 insertions, 101 deletions.

- [x] **Step 2: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` section with the test counts, the parity result, the grep results, and the diff stat.

```bash
git add .superpowers/docs/specs/2026-09-12-dpo-plumbing-simplification-design.md .superpowers/docs/plans/2026-09-12-dpo-plumbing-simplification.md
git commit -m "docs: record DPO plumbing simplification verification"
```

- [x] **Step 3: Open the PR**

```bash
git push -u origin simplify/dpo-plumbing
gh pr create --base master --title "refactor: simplify the DPO plumbing" --body "$(cat <<'EOF'
## Summary
- Investigated DPO: the loss and trainer in `dpo.py` are already minimal and stay untouched. The complexity was in the plumbing around them.
- One `build_pairs` that returns its diagnostics, replacing a wrapper that discarded them.
- `ope_eval_report.candidate_features` replaces the private `_vec` that five modules and a test reached into.
- `replay_dataset.load_events` and `replay_dataset.candidate_rows` replace the loader and candidate-scoring loop that the DPO and Q CLIs each carried a copy of.
- No change to computed values, printed lines, CLI flags, or written Parquet columns.

## Test plan
- [x] DPO + Q test files: 74 pass, same count as before, call-site edits only.
- [x] Byte-identical parity: both CLIs run on a 12-slate joined fixture with a null `modelPredictions` and fixed seeds produce the same summaries, stdout, and per-candidate predictions before and after (`cmp` on sorted JSON dumps).
- [x] Full Python modeling suite: <fill from Step 1>.
- [x] No `_vec`, `build_pairs_with_diagnostics`, or `_load_events` remains.

Spec: `.superpowers/docs/specs/2026-09-12-dpo-plumbing-simplification-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-dpo-plumbing-simplification.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link DPO plumbing simplification PR`.

---

## Self-review

- Spec coverage: rename (Task 1), one builder (Task 2), shared helpers and slimmer CLIs (Task 3), parity, full suite, PR (Tasks 3-4). Acceptance criteria 1-6 map to Task 1 Step 3 plus Task 2 Step 4, Task 3 Step 4, Task 3 Step 4, Task 1 Step 3, Task 3 Step 4 plus Task 4 Step 1, Task 4 Step 1.
- Placeholders: one `<fill from Step 1>` in the PR body supplied by Task 4 Step 1; `<scratchpad>` is the session scratchpad directory.
- Names used across tasks: `candidate_features`, `build_pairs`, `load_events`, `candidate_rows`, `JoinDiagnostics` are consistent with the spec.
