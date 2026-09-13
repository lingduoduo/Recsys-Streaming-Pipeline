# Reward model scoring reuse

**Date:** 2026-09-12
**Status:** Implemented and verified; PR pending

## Problem and scope

`ope_eval_report.py` is the repository's reward model (RM) stage: `fit_reward_model` fits a
numpy logistic estimator on each logged event's taken-action features and observed reward, then
the fitted model grades every policy by re-picking each event's slate and scoring the pick. The
analysis dashboard reuses the same functions for its OPE card. The model is an offline artifact
held in memory for one run; this change does not add persistence or serving integration.

Investigation timed each stage on a synthetic replay buffer of 5,000 events with 10 candidates
each and 5 prediction keys:

| Stage | Time |
|---|---|
| `fit_reward_model` (split, features, 500 GD iterations, calibration) | 0.06 s |
| `evaluate` point estimates over 9 policies | 0.38 s |
| `bootstrap_intervals` at the default 1,000 samples | ~370 s (extrapolated from 5 samples) |

Fitting is already vectorized and is cheap even at the 10,000-event replay cap. The cost is in
applying the fitted model:

- `RewardModel.predict_one` builds a one-row array and runs standardization and the sigmoid per
  candidate, so scoring is dominated by per-call numpy overhead.
- Every bootstrap replicate re-runs `pick` and `predict_one` for every event and policy, although
  the model is fixed and an event's score under a policy never changes between replicates.

This measures redundant computation on a local fixture, not production throughput.

## Global constraints

- Python 3 with numpy only; add no dependencies.
- Keep `fit_reward_model` unchanged: the md5 request split, feature schema, `POLICY_ONLY_PRED_KEYS`
  exclusion, standardization, `l2=1.0, lr=0.5, iters=500`, and the calibration dictionary.
- Keep `pick`, `policy_names`, `feature_names`, `taken_features`, and `is_test` unchanged; other
  modules import them.
- Keep the CLI flags, printed lines, CSV columns, and `INTERVAL_FIELDS` unchanged.
- Keep the bootstrap RNG call sequence unchanged: one `rng.integers(0, n, size=n)` per replicate
  from `np.random.default_rng(seed)`, so a seed selects the same events as before.
- Keep the row semantics: `n_events` is the number of events with a pick, an empty pick set
  yields value `0.0`, lift is `None` when the logging value is not positive, rows sort by value
  descending, and `evaluate` rounds only the reported rows.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Vectorize `predict_one` only | Cuts per-call overhead but every replicate still re-picks and re-scores every event. |
| Score each event under each policy once; resample the scores | Selected: the model is fixed during evaluation, so the per-event score matrix is the sufficient statistic for both point rows and replicates. |
| Vectorize all replicates as one index matrix | Fastest but allocates samples × events × policies doubles (hundreds of MB at the replay cap). |
| Change the fit (convergence stopping, fewer iterations) | Saves milliseconds and changes the reported numbers; rejected. |

## Design

### Batch prediction

Add `RewardModel.predict_batch(candidates)` that builds one feature matrix with the existing
`_vec`, applies one `apply_standardize`, and returns one `predict_proba` array. Given no
candidates it returns an empty float array. `predict_one` becomes a wrapper over `predict_batch`
and keeps its return type.

### Per-event policy scores

Add a helper that, for a list of events and policy names, returns the observed reward vector and
an events × policies float matrix. The `logging` column holds each event's reward. Every other
column holds `model.predict_batch` over that policy's picks, in event order, with `NaN` where
`pick` returned `None`. Each non-logging policy calls `predict_batch` exactly once.

`_evaluate_statistics` builds this matrix and reduces it with NaN-aware counts and sums, keeping
the existing row dictionary keys and ordering. `bootstrap_intervals` builds the matrix once, then
reduces the row subset selected by each replicate's indexes. Replicate values may differ from the
previous implementation at the floating-point rounding level because numpy sums replace Python
sums; percentile bounds are still rounded to four decimals.

### Duck-typed models

`_evaluate_statistics` accepts any object with `predict_batch` and `calibration`. The existing
test stub gains `predict_batch`.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/python-modeling/ope_eval_report.py` | Batch prediction, per-event score matrix, matrix-based point rows and replicates. |
| `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py` | Prediction-count regression, parity with per-event scoring, empty action space, stub update. |
| `recsys-pipeline/docs/recommendation_architecture/Analysis_Report.md` | One sentence on scoring once and resampling. |
| `.superpowers/docs/plans/2026-09-12-reward-model-scoring-reuse.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. A bootstrap of 40 samples over events with 4 non-logging policies invokes `predict_batch`
   4 times in total, not 160.
2. `_evaluate_statistics` values and counts match a reference computed with `predict_one` per pick
   within `1e-12`, on a fixture that includes an event with an empty action space.
3. `predict_batch` over a list equals `predict_one` element-wise within `1e-12`.
4. Existing bootstrap tests pass unchanged: determinism under a seed, point rows not mutated,
   intervals containing point values, single-event and zero-sample edges, and zero-reward lift.
5. The CLI test still prints the calibration line and writes the same CSV columns.
6. `test_post_training_q.py`, `test_post_training_dpo.py`, `test_analysis_dashboard.py`, and
   `test_logistic.py` pass.
7. The benchmark script from the investigation reports the 1,000-sample bootstrap in seconds,
   not minutes, on the 5,000-event fixture.

## Risks and limits

The score matrix holds events × policies doubles, which is small at the replay cap. Picks remain a
Python loop, so point evaluation stays proportional to events × candidates × policies; only the
bootstrap changes complexity. Rollback consists of reverting this change; no data or artifact
migration is required.

## Verification record

- `python3 -m pytest integration-tests/python_modeling -q`: 501 passed, 0 failures, on 2026-09-12
  with Python 3.12.2 and numpy 2.5.1.
- Final `test_ope_eval.py` run: 23 tests passed (20 pre-existing + 3 new).
- Regression evidence: a 40-sample bootstrap over 4 non-logging policies called prediction 160 times
  before the change and 4 times afterward.
- Parity: on a 2,000-event fixture with one empty action space, 9 policies, 30 samples, seed 5,
  the reported rows and intervals from `origin/master` and this branch are byte-identical.
- Benchmark (synthetic buffer, 10 candidates per event, 9 policies, 1,000 bootstrap samples):

| Events | Fit | Point rows | Bootstrap before | Bootstrap after |
|---|---|---|---|---|
| 5,000 | 0.06 s | 0.38 s → 0.12 s | ~370 s (extrapolated) | 0.30 s |
| 10,000 (replay cap) | 0.14 s | 0.24 s | not measured | 0.57 s |
