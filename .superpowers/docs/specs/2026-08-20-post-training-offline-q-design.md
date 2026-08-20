# Post-training track, component 1: offline Q-learning

**Date:** 2026-08-20
**Status:** Approved, ready for implementation planning

## Context

The repository already runs reinforcement learning in the serving path: `HybridRecommendationService`
supports a `q-learning` bandit algorithm that applies an online Bellman update on every `/feedback`
call. An audit of that implementation found it unsuitable as the basis of a post-training track:

- Reward is bounded `[0, 1]` and no state is terminal, so the Bellman fixed point is `r/(1-gamma)` —
  up to 10.0 at the default `gamma = 0.9` — while the ranking path clamps Q to `[0, 1]`. Every value
  above 1.0 is ordering information that is discarded.
- The state key is the set of genres and tags over the user's last 50 items, which barely changes
  between consecutive interactions. Simulating the exact update rule against the shipped catalog,
  88.5% of transitions had `s' == s`, so the update bootstraps off the row it is writing to and Q
  grows with selection frequency rather than reward rate.
- `max_a' Q(s', a')` ranges over every action stored for the next state, including items the user has
  already watched and the policy can never serve.
- No terminal handling: every feedback bootstraps.

Offline batch fitting avoids all four by construction. Values are never clamped because evaluation is
argmax-based; episodes have real terminal transitions; and the logged action space bounds the max.

This document specifies the first component of a **post-training track**: offline policy improvement
trained on logged data. DPO follows as a sibling component reusing the same dataset layer and
evaluation hookup.

## Scope

**In scope.** A new `services/python-modeling/post-training/` directory holding offline algorithms
that read the RL replay buffer, fit a Q function, score the logged replay, and report off-policy
value estimates alongside the existing baselines.

**Out of scope.** No change to the serving path. The fitted Q is not published to Redis and does not
affect live ranking. Repairing the online `q-learning` implementation is separate work. Existing
modules (`ope_support.py`, `ope_eval_report.py`, `replay_export.py`) stay where they are; nothing
moves.

## Data layer — `replay_dataset.py`

Reads the replay buffer through the existing loader in `ope_support.py` (Redis list or Parquet dump),
so no new I/O paths are introduced. Its job is to turn a flat event list into episodes.

The logged `ReplayEvent` schema already carries `user`, `requestId`, `state`, `nextState`, `action`,
`actionSpace`, `candidates`, `policy`, `reward`, `clicked`, `timestamp`, and `slateSize`.

Episode construction: group events by `user`, sort by `timestamp`, and split into sessions on an
inactivity gap (default 30 minutes, CLI-configurable). Chaining events within a session yields three
properties the online implementation lacks:

1. **Terminal transitions.** The final transition of a session is terminal; its target is `r` alone,
   with no `gamma * maxQ` term.
2. **A feasible next-state action space.** A logged event carries `actionSpace` at `s` only. The next
   event in the chained session supplies the action space at `s'`, so `max_a' Q(s', a')` ranges over
   actions the policy could actually take.
3. **Explicit off-policy handling.** The logging policy is recorded per event (`policy`, in practice
   `ucb`). Learning a different policy from it is the point; the field is retained for reporting and
   for later importance-weighted estimators.

Feature vectors reuse the per-candidate schema that `ope_eval_report.py` already defines —
`coldStart`, `impressions`, `clicks`, and the numeric keys of `modelPredictions` — so the trainer and
the evaluator read identical features.

## Algorithms

Both arms ship together. The tabular arm is the control that establishes whether function
approximation earns its complexity, following the baseline-comparison pattern of `next_item_model.py`.

**`tabular_q.py` — batch tabular Q-learning.** Q is a dict keyed by `(state_key, action)`, swept over
the dataset until the maximum value change falls below a tolerance or an iteration cap is reached.
The state key is the logged `state` signature, matching what serving uses, which makes the comparison
against the online implementation direct.

**`fqi.py` — Fitted Q Iteration.** A small torch MLP over the candidate feature vector. For K
iterations, regression targets are computed as `r + gamma * max_a' Q_prev(s', a')` (zero for terminal
transitions) against a frozen copy of the previous iterate, then the network is refit against them.
Freezing the target network per iteration is what makes the procedure a contraction rather than a
moving-target regression.

Neither arm clamps Q. Offline evaluation ranks by argmax, so scale is irrelevant and the saturation
that breaks the serving path cannot occur.

Both arms take `gamma` from a single CLI flag defaulting to `0.9`, matching the serving default, so
the offline and online value scales stay comparable.

## Evaluation

Two metrics, because neither alone is sufficient.

**Off-policy value via the existing harness.** `pick()` in `ope_eval_report.py` already supports a
generic `model:{key}` policy that argmaxes over `modelPredictions[key]` per candidate. The trainer
writes its predictions back into each candidate's `modelPredictions` as `tabQ` and `fqiQ`. The
policies `model:tabQ` and `model:fqiQ` then evaluate with **no change to `pick()`**, inheriting the
logistic reward model, the `requestId`-hash held-out split, and the bootstrap confidence intervals,
and landing in the same table as `ctr`, `popularity`, `random`, and the existing `model:qValue`.

This also preserves an existing property: `ope_eval_report.py` is deliberately numpy-only. Torch stays
in the trainer, which emits a scored replay file the evaluator reads.

**Held-out Bellman/TD residual.** The Direct Method estimator above is single-step: it cannot credit
long-horizon value, which is precisely what Q-learning claims to add. Mean absolute TD error on the
held-out split, reported per algorithm, is the RL-native check on fit quality.

Neither number is asserted in tests as a quality threshold; both are reported. The tests assert
correctness of the machinery, not the outcome of a stochastic fit.

## File layout

```
services/python-modeling/post-training/
├── replay_dataset.py   # replay buffer → episodes (terminal flags, s' action space, features)
├── tabular_q.py        # batch tabular Q-learning (baseline arm)
├── fqi.py              # neural Fitted Q Iteration (torch, main arm)
└── post_train_q.py     # CLI: train → score replay → write tabQ/fqiQ → report TD residual
```

The directory name is hyphenated, matching `python-modeling` itself. Its modules are therefore
imported flat after a `sys.path` insert — the convention already used by every test under
`integration-tests/python_modeling/` — not as a `post_training.` package, since hyphens are not valid
in Python package names. Callers insert both `python-modeling/` (for `ope_support`) and
`python-modeling/post-training/`.

Trained artifacts are written to `sampledata/`, alongside the existing `.onnx` and `.pt` files, so all
model artifacts remain in one place.

DPO lands later as a sibling `dpo.py` in this directory, reusing `replay_dataset.py` for slate-level
chosen/rejected pairs and the same `model:{key}` evaluation hookup.

## Success criteria

1. Terminal transitions do not bootstrap — unit test over a hand-built episode.
2. Tabular Q on a synthetic two-state chain recovers the analytically optimal Q — test against
   closed-form expected values.
3. FQI matches tabular on that same chain within tolerance — test.
4. Every candidate in the scored replay gains `tabQ` and `fqiQ`, and `model:fqiQ` appears in the OPE
   output table — test.
5. Held-out TD residual is reported per algorithm — verified by inspection of the report output.

Tests live at `integration-tests/python_modeling/test_post_training_q.py`.

## Risks and open items

- **Training data availability.** `sampledata/` currently holds no replay Parquet dump; the buffer is
  produced by a simulation run and exported via `scripts/run-retrain.sh`. If the buffer is empty at
  implementation time, a simulation run must generate one before the evaluation can be wired, and
  that should be stated rather than worked around with synthetic data.
- **Dependency declaration.** `torch` is imported by `movielens_pipeline.py` but absent from
  `services/python-modeling/requirements.txt`, which lists only kafka-python, lz4, fastavro, and
  pyarrow. This component adds a second torch consumer, so `torch` should be added to that file.
- **Small-sample fits.** Both the DM reward model and FQI are fit on whatever the replay buffer holds.
  Bootstrap intervals are reported for the former; wide intervals should be read as inconclusive
  rather than negative.
- **DPO is not traditional RL.** It replaces the RL step rather than extending it — classic
  post-training is SFT → reward model → PPO/GRPO, whereas DPO derives the optimal policy in closed
  form and drops both the reward model and the rollouts. The track's ordering should reflect that DPO
  is a fork from this component, not a continuation of it.
