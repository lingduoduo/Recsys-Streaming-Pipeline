# DPO plumbing simplification

**Date:** 2026-09-12
**Status:** Approved design; implementation in progress on `simplify/dpo-plumbing`

## Problem and scope

The offline DPO arm is three modules under `services/python-modeling/post-training/`: the
reference-anchored pairwise loss and trainer in `dpo.py`, the slate-to-pair join in
`slate_pairs.py`, and the CLI in `post_train_dpo.py`, as designed in
[the DPO design](2026-08-20-post-training-dpo-design.md). The loss is three lines and the trainer
a full-batch Adam loop, both pinned by analytic tests; neither changes here. The avoidable
complexity is in the plumbing around them:

- `slate_pairs.build_pairs` is a wrapper that discards the diagnostics returned by
  `build_pairs_with_diagnostics`. Production calls only the diagnostics version; the wrapper serves
  twelve test call sites.
- Four modules (`slate_pairs`, `replay_dataset`, `post_train_dpo`, `post_train_q`) and one test reach
  into `ope_eval_report._vec` to build a candidate's feature vector. `replay_dataset` already imports
  it under the alias `candidate_features`.
- `post_train_dpo` and `post_train_q` each carry a copy of `_load_events` (Parquet or Redis) and of
  the candidate-scoring loop in `score_events`, including the null `modelPredictions` guard and its
  comment. About forty lines exist twice.

This change removes those three duplications without changing any computed value, printed line,
CLI flag, or written Parquet column. It does not touch the loss, the pair definition, `beta`, the
split, or the Q arm's algorithms.

## Global constraints

- Python 3 with numpy, pandas, and torch as already used; add no dependencies.
- Keep `dpo.py` unchanged.
- Keep `PreferencePair`, `JoinDiagnostics`, `is_chosen`, `replay_index`, and `REFERENCE_PRED_KEY`
  unchanged.
- Keep both CLIs' arguments, returned summary dictionaries, printed lines, and output Parquet
  contents identical for a fixed seed.
- Keep `ope_eval_report`'s public behavior unchanged apart from the rename; `taken_features` and
  `predict_batch` keep their results.
- Keep every key written by either `score_events` registered in `POLICY_ONLY_PRED_KEYS`.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Leave the plumbing; the tests pass | No risk, but the next reader still meets two builders, a private import in five places, and two copies of the CLI loop. |
| Remove the three duplications in place, guarded by the existing tests plus a byte-level parity run | Selected: smallest diff, and both CLIs are deterministic under a seed so parity is checkable. |
| Rename only DPO's own uses of `_vec` | Leaves the Q arm and the replay dataset on the private name; inconsistent for no gain. |
| Rewrite the scorer in numpy to drop torch | The FQI arm shares the network; dropping torch there is a separate decision and would change the arms' comparability. |

## Design

### One pair builder

`slate_pairs.build_pairs(slates, events, names=None)` returns `(pairs, dropped, JoinDiagnostics)`.
`build_pairs_with_diagnostics` is removed; its body and docstring become `build_pairs`. The CLI and
the tests call the one function.

### A public feature-vector function

`ope_eval_report._vec` is renamed `candidate_features`. `taken_features`, `predict_batch`, and the
four external modules call the new name. `replay_dataset` imports it directly instead of aliasing.
No `_vec` alias remains.

### Shared CLI helpers in `replay_dataset`

`replay_dataset` gains:

- `load_events(args)`: returns `ope_support.load_from_parquet(args.parquet)` when `args.parquet` is
  set, otherwise a Redis read from `REDIS_HOST` / `REDIS_PORT` with `args.key` and `args.limit`.
  This is the shared body of the two `_load_events` functions.
- `candidate_rows(events, names)`: a generator over every candidate in every event's `actionSpace`,
  yielding `(event, candidate, features, predictions)` where `predictions` is the candidate's
  `modelPredictions` dict, created in place when it is `None`. The null guard and its explanation
  live here once.

Each CLI's `score_events` collects the rows, scores the features in one batch as before, and writes
its own keys into `predictions`. The Q arm's `state_key` is computed per candidate from the yielded
event instead of once per event; the value is identical. Both CLIs drop their `_load_events` and
the `os` import it needed.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/python-modeling/ope_eval_report.py` | Rename `_vec` to `candidate_features`. |
| `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py` | Direct import; `load_events`; `candidate_rows`. |
| `recsys-pipeline/services/python-modeling/post-training/slate_pairs.py` | One `build_pairs`; new feature-vector name. |
| `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py` | Use shared loader, rows, and builder. |
| `recsys-pipeline/services/python-modeling/post-training/post_train_q.py` | Use shared loader and rows. |
| `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py` | Three-value unpacking; one builder name. |
| `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py` | New feature-vector name. |
| `.superpowers/docs/plans/2026-09-12-dpo-plumbing-simplification.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. `grep -rn "_vec\b"` over `services/python-modeling` and `integration-tests` returns nothing;
   `grep -rn "build_pairs_with_diagnostics"` returns nothing.
2. Both CLIs, run on a twelve-slate joined fixture with one null `modelPredictions` and fixed seeds,
   return the same summary dictionaries, print the same stdout, and write the same per-candidate
   `modelPredictions` before and after the change, compared byte for byte as JSON.
3. `test_post_training_dpo.py` and `test_post_training_q.py` pass: 74 tests, the same count as
   before, with only call-site edits.
4. `test_ope_eval.py` passes unchanged.
5. Neither CLI keeps a private `_load_events`, and `post_train_dpo.py` plus `post_train_q.py`
   together lose at least thirty lines.
6. The full Python modeling suite passes; unrelated failures are reported against a baseline
   rather than claimed green.

## Risks and limits

The rename touches the Q arm and the replay dataset, so a missed call site fails at import or
at first use; the suites and the grep in criterion 1 cover it. Computing `state_key` per candidate
instead of per event is a small amount of repeated string work on a path that is not
performance-sensitive. Rollback is a revert; no data or artifact changes.
