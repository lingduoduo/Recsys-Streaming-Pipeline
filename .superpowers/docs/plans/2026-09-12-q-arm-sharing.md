# Q Arm Sharing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the two duplications between the offline Q arm and the DPO arm: the copied scorer class and the copied held-out split.

**Architecture:** `dpo.py` aliases `fqi.FittedQ` as `PreferencePolicy`. `replay_dataset.split_held_out` becomes the one request-id split; both CLIs and their tests use it. A JSON parity dump guards both CLIs' outputs.

**Tech Stack:** Python 3.12, numpy, torch 2.11, pandas, pytest.

**Spec:** [Offline Q arm: share the scorer and the split with DPO](../specs/2026-09-12-q-arm-sharing-design.md)

## Global Constraints

- Python 3 with numpy, pandas, and torch as already used; add no dependencies.
- Keep `tabular_q.py` and the fitting code in `fqi.py` and `dpo.py` unchanged.
- Keep both CLIs' arguments, returned summaries, printed lines, and written Parquet identical for a fixed seed.
- Keep the split semantics: train is every item whose `request_id` is not held out by `ope_eval_report.is_test`; when that leaves nothing, both sides are the whole input and the degenerate flag is true.

## Execution notes

All paths are repository-relative. Run pytest from `recsys-pipeline/`. The work is on
`simplify/q-arm-sharing`, based on `origin/master`. Publish a PR against `master`.

Baselines (2026-09-12): DPO + Q test files → 74 passed. The parity script `dpo_parity.py` in the
session scratchpad (runs `post_train_q.main` then `post_train_dpo.main` on a 12-slate joined
fixture with one null `modelPredictions`) was run on the unchanged code and saved as
`q_parity_before.json` (6,846 bytes).

---

### Task 1: One scorer class

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/dpo.py:29-62`

**Interfaces:**
- Produces: `dpo.PreferencePolicy` is `fqi.FittedQ`; `dpo.fit` still returns it.

- [x] **Step 1: Replace the class with the alias**

Change

```python
from fqi import QNetwork as ScoreNetwork
```

to

```python
from fqi import FittedQ as PreferencePolicy
from fqi import QNetwork as ScoreNetwork
```

and delete the whole `class PreferencePolicy:` block (from `class PreferencePolicy:` through `return self.score_many([features])[0]` and the blank lines after it), leaving `dpo_loss` immediately after the constants.

- [x] **Step 2: Verify**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py -q`
Expected: 34 passed. Observed: 34 passed.

- [x] **Step 3: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/dpo.py
git commit -m "refactor: DPO reuses the FQI scorer class instead of copying it"
```

---

### Task 2: One held-out split

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py:45-57,160`
- Modify: `recsys-pipeline/services/python-modeling/post-training/post_train_q.py:35-42,128`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py:238-245,461-467`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py:251-258`

**Interfaces:**
- Produces: `replay_dataset.split_held_out(items) -> tuple[list, list, bool]`.

- [x] **Step 1: Point the three tests at the shared helper first**

In `test_post_training_dpo.py`, add `import replay_dataset` after `import post_train_dpo`, and change both `post_train_dpo.split_pairs(pairs)` calls to `replay_dataset.split_held_out(pairs)`. In `test_post_training_q.py`, change `train, test = post_train_q.split_transitions(transitions)` to `train, test, _ = replay_dataset.split_held_out(transitions)`.

- [x] **Step 2: Run them to verify they fail**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py integration-tests/python_modeling/test_post_training_q.py -q -k "split"`
Expected: 3 failed with `AttributeError: module 'replay_dataset' has no attribute 'split_held_out'`. Observed: 3 failed, 1 passed, 70 deselected.

- [x] **Step 3: Add the helper and use it in both CLIs**

In `replay_dataset.py`, change the import line to

```python
from ope_eval_report import candidate_features, feature_names, is_test, taken_features
```

and append after `candidate_rows`:

```python
def split_held_out(items):
    """Train/held-out split on the requestId hash ope_eval_report uses: (train, held_out, degenerate).

    When the hash leaves no training item -- which a tiny input can do -- the whole set is
    returned for BOTH sides rather than crashing, and `degenerate` says so: anything reported as
    held-out is then in-sample, and the caller must label it as such.
    """
    train = [item for item in items if not is_test(item.request_id)]
    held_out = [item for item in items if is_test(item.request_id)]
    if not train:
        return items, items, True
    return train, held_out, False
```

In `post_train_dpo.py`, delete `split_pairs` (its `def` through `return train, held_out, False`) and change `train, held_out, degenerate_split = split_pairs(pairs)` to `train, held_out, degenerate_split = replay_dataset.split_held_out(pairs)`.

In `post_train_q.py`, delete `split_transitions` (its `def` through `return train, test`) and change `train, held_out = split_transitions(transitions)` to `train, held_out, _ = replay_dataset.split_held_out(transitions)`.

- [x] **Step 4: Verify tests, parity, and the grep**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_post_training_dpo.py integration-tests/python_modeling/test_post_training_q.py -q`
Expected: 74 passed. Observed: 74 passed.

Run: `cd recsys-pipeline && python3 <scratchpad>/dpo_parity.py > <scratchpad>/q_parity_after.json && cmp <scratchpad>/q_parity_before.json <scratchpad>/q_parity_after.json && echo IDENTICAL`
Expected: `IDENTICAL`. Observed: identical.

Run: `grep -rn "class PreferencePolicy\|def split_pairs\|def split_transitions" recsys-pipeline/services/python-modeling`
Expected: no output. Observed: no output.

- [x] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/post-training/replay_dataset.py recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py recsys-pipeline/services/python-modeling/post-training/post_train_q.py recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py
git commit -m "refactor: one held-out split for the post-training CLIs"
```

---

### Task 3: Full suite, records, and PR

**Files:**
- Modify: `.superpowers/docs/specs/2026-09-12-q-arm-sharing-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)

- [x] **Step 1: Run the full Python modeling suite**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling -q`
Expected: 501 passed. Observed: 501 passed.

- [x] **Step 2: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` with the test counts, the parity result, the grep, and the diff stat.

```bash
git add .superpowers/docs/specs/2026-09-12-q-arm-sharing-design.md .superpowers/docs/plans/2026-09-12-q-arm-sharing.md
git commit -m "docs: record Q arm sharing verification"
```

- [x] **Step 3: Open the PR**

```bash
git push -u origin simplify/q-arm-sharing
gh pr create --base master --title "refactor: share the scorer class and held-out split between the Q and DPO arms" --body "$(cat <<'EOF'
## Summary
- Investigated the offline Q arm: the tabular sweep and fitted Q iteration are compact and pinned by analytic tests. What remained was duplication with the DPO arm.
- `dpo.PreferencePolicy` was a line-for-line copy of `fqi.FittedQ`; DPO now aliases it, as it already did for the network.
- `post_train_q.split_transitions` and `post_train_dpo.split_pairs` were the same request-id split; `replay_dataset.split_held_out` replaces both.
- No change to either CLI's arguments, printed lines, summary keys, or written Parquet.

## Test plan
- [x] DPO + Q test files: 74 pass, same count as before; three split tests now call the shared helper.
- [x] Byte-identical parity: both CLIs on a 12-slate joined fixture with a null `modelPredictions` and fixed seeds produce the same summaries, stdout, and per-candidate predictions before and after.
- [x] Full Python modeling suite: <fill from Step 1>.

Spec: `.superpowers/docs/specs/2026-09-12-q-arm-sharing-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-q-arm-sharing.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link Q arm sharing PR`.

---

## Self-review

- Spec coverage: scorer alias (Task 1), shared split and test moves (Task 2), suite and PR (Task 3). Acceptance criteria 1-4 map to Task 2 Step 4, Task 2 Step 4, Task 2 Step 4, Task 3 Step 1.
- Placeholders: one `<fill from Step 1>` in the PR body supplied by Task 3 Step 1; `<scratchpad>` is the session scratchpad directory.
- Names: `PreferencePolicy`, `FittedQ`, `split_held_out`, `is_test` are consistent between spec and tasks.
