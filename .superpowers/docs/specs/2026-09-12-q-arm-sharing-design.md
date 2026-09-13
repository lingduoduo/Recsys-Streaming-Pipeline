# Offline Q arm: share the scorer and the split with DPO

**Date:** 2026-09-12
**Status:** Approved design; implementation in progress on `simplify/q-arm-sharing`

## Problem and scope

The offline Q arm (`tabular_q.py`, `fqi.py`, `post_train_q.py`) was the first post-training
component, per [the offline Q design](2026-08-20-post-training-offline-q-design.md). Its
algorithms are compact and pinned by analytic tests. What remains is duplication between it and
the DPO arm that [the DPO plumbing pass](2026-09-12-dpo-plumbing-simplification-design.md) did
not reach:

- `dpo.PreferencePolicy` is a line-for-line copy of `fqi.FittedQ`: the same constructor, the same
  `score_many` with standardization, the same `score_one`. `dpo.py` already imports the network
  from `fqi` under an alias.
- `post_train_q.split_transitions` and `post_train_dpo.split_pairs` apply the same request-id hash
  with the same all-in-train fallback; the DPO version also returns a degenerate flag that the Q
  version silently drops.

This change removes both duplications. It does not touch the tabular sweep, fitted Q iteration,
the DPO loss, either CLI's arguments, printed lines, summary keys, or output Parquet contents.

## Global constraints

- Python 3 with numpy, pandas, and torch as already used; add no dependencies.
- Keep `tabular_q.py` and the fitting code in `fqi.py` and `dpo.py` unchanged.
- Keep both CLIs' arguments, returned summaries, printed lines, and written Parquet identical for a
  fixed seed.
- Keep the split semantics: train is every item whose `request_id` is not held out by
  `ope_eval_report.is_test`; when that leaves nothing, both sides are the whole input and the
  degenerate flag is true.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Leave the copies | No risk, but a future fix to one scorer or split silently misses the other. |
| Alias `fqi.FittedQ` as `dpo.PreferencePolicy` and move the split to `replay_dataset` | Selected: the smallest diff, mirroring the alias `dpo.py` already uses for the network; `replay_dataset` is already the shared data module for both CLIs. |
| Introduce a neutral `StandardizedScorer` base class | A third name for the same thing; rejected. |
| Keep thin `split_pairs` / `split_transitions` wrappers | Preserves two names for one function; the three tests move to the shared helper instead. |

## Design

### One scorer class

`dpo.py` replaces its `PreferencePolicy` class with `from fqi import FittedQ as PreferencePolicy`,
alongside the existing `from fqi import QNetwork as ScoreNetwork`. `fit` keeps returning
`PreferencePolicy(net, mean, std)`; nothing outside `dpo.py` names the class.

### One held-out split

`replay_dataset.split_held_out(items)` returns `(train, held_out, degenerate)` using
`ope_eval_report.is_test(item.request_id)`, with the DPO docstring's explanation of the degenerate
case. `post_train_dpo` calls it where `split_pairs` was; `post_train_q` calls it as
`train, held_out, _ = ...` where `split_transitions` was. Both local functions are removed, and
the three tests that called them call `replay_dataset.split_held_out`.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/python-modeling/post-training/dpo.py` | Alias the scorer instead of copying it. |
| `recsys-pipeline/services/python-modeling/post-training/replay_dataset.py` | `split_held_out`. |
| `recsys-pipeline/services/python-modeling/post-training/post_train_dpo.py` | Use the shared split. |
| `recsys-pipeline/services/python-modeling/post-training/post_train_q.py` | Use the shared split. |
| `recsys-pipeline/integration-tests/python_modeling/test_post_training_dpo.py` | Two split tests call the shared helper. |
| `recsys-pipeline/integration-tests/python_modeling/test_post_training_q.py` | One split test calls the shared helper. |
| `.superpowers/docs/plans/2026-09-12-q-arm-sharing.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. `grep -rn "class PreferencePolicy\|def split_pairs\|def split_transitions"` over
   `services/python-modeling` returns nothing.
2. The DPO and Q test files pass with 74 tests, the same count as before.
3. Both CLIs, run on the twelve-slate joined fixture with a null `modelPredictions` and fixed seeds,
   return the same summaries, print the same stdout, and write the same per-candidate predictions
   before and after, compared byte for byte as JSON.
4. The full Python modeling suite passes; unrelated failures are reported against a baseline
   rather than claimed green.

## Risks and limits

The alias means a future change to `FittedQ` reaches DPO too, which is the point: the two arms
were designed to share one function class so their comparison stays fair. Rollback is a revert;
no data or artifact changes.
