# Post-training track, component 2: DPO

**Date:** 2026-08-20
**Status:** Approved, ready for implementation planning
**Depends on:** component 1, `.superpowers/docs/specs/2026-08-20-post-training-offline-q-design.md`
(branch `feat/post-training-offline-q`, PR #200, unmerged at time of writing)

## Context

Component 1 established the post-training track: offline policy improvement fit on logged data,
evaluated through the existing off-policy harness. It shipped two arms — batch tabular Q-learning
and Fitted Q Iteration — both reading the RL replay buffer.

DPO is the second component. It is worth stating plainly that DPO is **not** traditional RL and is
not a continuation of component 1: classic post-training runs SFT → reward model → PPO/GRPO,
whereas DPO derives the optimal policy in closed form and drops both the reward model and the
rollouts. It is a fork from component 1, not a step beyond it. The two share a directory, a feature
schema, and an evaluation hookup; they do not share an algorithmic lineage.

## Why the loss is unusually simple here

DPO's objective is a difference of log-ratios between a policy and a frozen reference:

```
-log σ( β · [ (log π_θ(w|s) − log π_ref(w|s)) − (log π_θ(l|s) − log π_ref(l|s)) ] )
```

Model the policy as a softmax over the candidates of the slate the pair came from. Both terms then
carry the same partition function over the same candidate set, and both cancel in the difference:

```
log π_θ(w|s) − log π_θ(l|s) = f_θ(w) − f_θ(l)
```

so the loss reduces **exactly** to

```
-log σ( β · [ (f_θ(w) − f_θ(l)) − (f_ref(w) − f_ref(l)) ] )
```

This is the repository's existing BPR loss (`train_two_tower` in `movielens_pipeline.py`) with a
reference margin subtracted. No partition function has to be estimated or approximated — the
property that makes DPO delicate on language models does not arise on slate data. The relationship
to BPR is an exact identity rather than a limit: when the reference assigns both items of a pair the
same score, the reference margin is zero and the objective *is* BPR at `β = 1`. That identity is the
sanity anchor to test, since it holds exactly and needs no tolerance.

## Data

**Pairs come from slates, not from the replay action space.** `ExperienceCollectorStreamingJob.buildSlates`
emits one row per `(request_id, user_id)` carrying an ordered `items` array with per-position
`clicked`, `ordered`, `label`, `rating`, `dwell_millis`, `completion_rate`, and
`negative_feedback_reason`. Every item in that array was **exposed**. A clicked item paired against
an exposed-but-not-clicked item from the same slate is therefore a genuine preference, with user,
context, and time held constant.

An item counts as **chosen** when `clicked` or `ordered` is truthy, or `label > 0`. Every other item
in the same slate is a **rejected** candidate, including one carrying a `thumb_down` or a
`negative_feedback_reason`: a disliked item is a rejected item, and the binary formulation makes no
attempt to rank degrees of rejection. Grading rejection strength was considered and deferred — it
needs a defensible ordering across heterogeneous signals, which the binary version avoids.

The replay buffer's `actionSpace` was rejected as a pair source. Its candidates were never shown,
so treating them as "rejected" would conflate *not preferred* with *not exposed* — which trains a
ranker to reproduce its own logging policy.

**Features come from the replay buffer, not from the slate.** The two sources carry different
per-item structures: slate items have an `item_features` map, replay candidates have
`modelPredictions`. Training on one and scoring on the other would silently compare different
quantities. The component therefore joins them on `requestId` + item: the slate decides *who won*,
the replay row supplies the feature vector, using the identical `ope_eval_report.feature_names`
schema every other arm uses. Pairs whose replay row is missing are dropped and **counted**; that
count is reported, never swallowed.

**Reference scores.** `f_ref` is `modelPredictions["predictionScore"]` — the per-candidate score the
ranker actually ordered by. It cannot be the top-level `banditScore`: `candidateFeatures` in
`MovieLensServingSideEffects` logs only `item`, `modelPredictions`, `coldStart`, `impressions`, and
`clicks` per candidate, so `banditScore` exists solely for the selected item and the rejected item
of a pair would have none. `predictionScore` is clamped to `[0, 1]` at serve time, so the reference
margin lies in `[-1, 1]`.

## Policy

`f_θ` is a small torch MLP over the candidate feature vector — the same shape and the same feature
schema as component 1's FQI arm, so all three arms are scored on identical inputs and the
comparison between them is meaningful.

Post-training the *shipped* ranking transformer was considered and deferred. It is the more literal
reading of "post-training" and `movielens_pipeline.py` already persists `.pt` checkpoints for
warm-init, but it would require the slate Parquet's item ids to map onto that pipeline's index
space, which is unverified. It remains the natural component 3.

`β` defaults to `1.0`, exposed as a CLI flag. Scores live in roughly `[0, 1]`, so the margin is
small and a large `β` saturates the sigmoid immediately.

## Evaluation

Two metrics, for the same reason component 1 used two.

**Held-out pairwise accuracy** — the fraction of held-out pairs where `f_θ(w) > f_θ(l)`. This is
DPO's native measure, needs no reward model, and is honest on its own terms. Report the reference
policy's own pairwise accuracy alongside it: if `f_ref` already separates the pairs, the fitted
policy has learned nothing the logging policy did not already know.

**Off-policy value** — `dpoScore` is written into each candidate's `modelPredictions`, making
`model:dpoScore` an evaluable policy in the existing harness alongside `model:tabQ`, `model:fqiQ`,
`ctr`, `popularity`, and `random`.

`dpoScore` **must** be registered in `ope_eval_report.POLICY_ONLY_PRED_KEYS` in the same commit that
starts writing it. Component 1's final review found that injected policy scores were entering
`feature_names()` and being handed to the reward model as input features, which reported +17.4%
lift for a policy that had learned nothing. The guard test added afterwards fails when any
unregistered key is written, so this is enforced rather than remembered.

**Split discipline.** The same `is_test(requestId)` split as every other arm; fit on the
non-held-out portion only. `ope_eval_report._evaluate_statistics` scores policies over all events
while the reward model trains on 80%, so fitting on everything would hand `model:dpoScore` an
in-sample advantage the baselines do not get.

## File layout

```
services/python-modeling/post-training/
├── replay_dataset.py    (component 1, reused for the feature schema)
├── tabular_q.py         (component 1)
├── fqi.py               (component 1)
├── post_train_q.py      (component 1)
├── slate_pairs.py       # NEW: slate Parquet → (chosen, rejected) pairs joined to replay features
├── dpo.py               # NEW: the reference-anchored pairwise loss and its trainer
└── post_train_dpo.py    # NEW: CLI — load → pair → split → fit → score → report
```

Trained artifacts go to `sampledata/`, matching component 1.

## Composition

Because `feature_names()` now excludes every policy-only key, the two components chain safely:

```
post_train_q.py   --parquet replay.parquet  --output-parquet scored1.parquet
post_train_dpo.py --parquet scored1.parquet --slates slates.parquet --output-parquet scored2.parquet
ope_eval_report.py --parquet scored2.parquet
```

The final table carries `model:tabQ`, `model:fqiQ`, and `model:dpoScore` together. This chaining is
only safe because of the circularity fix; before it, each scoring pass silently altered every other
policy's number.

## Success criteria

1. A slate with one clicked and two exposed-not-clicked items yields exactly two pairs, both with
   the clicked item as `chosen` — test.
2. A slate where every item was clicked, or none was, yields no pairs — test.
3. Pairs never cross slates: two slates each with one click produce no cross-slate pair — test.
4. A pair whose replay row is missing is dropped and counted in the reported total — test.
5. With a constant reference — `f_ref` equal for both items of a pair, so the reference margin is
   exactly zero — the loss equals plain BPR at `β = 1` — test, analytic identity rather than a
   tolerance.
6. A policy that exactly reproduces `f_ref` yields a zero reference-adjusted margin, so the loss
   equals `-log σ(0) = log 2` — test, analytic.
7. Held-out pairwise accuracy is reported for both the fitted policy and the reference — verified
   by inspection of the report output.
8. `dpoScore` appears in `POLICY_ONLY_PRED_KEYS` and `model:dpoScore` appears in the OPE table —
   test.

## Risks

- **Neither data source exists locally.** No slate Parquet, no replay Parquet, Redis not running —
  the same wall component 1 hit, and worse here because DPO needs *both*, correlated by
  `requestId`. Tests must therefore build synthetic fixtures for both sides, and the real-data run
  must be reported as outstanding rather than quietly skipped.
- **Join yield is unknown.** If the replay buffer and the slate Parquet are produced by different
  paths with different retention, the join could drop most pairs. Reporting the dropped count is
  the guard; a low yield is a finding, not a failure to hide.
- **`predictionScore` includes an exploration bonus**, which for the Thompson algorithm is a random
  draw. Using it as `f_ref` means the reference is stochastic for Thompson-logged data.
  `estimatedReward` is the deterministic alternative. Starting with `predictionScore` because it is
  what the ranker ordered by; revisit if the logged algorithm is Thompson.
- **Dependency on unmerged work.** This branch stacks on `feat/post-training-offline-q`. If PR #200
  changes during review, this component rebases onto the result.
