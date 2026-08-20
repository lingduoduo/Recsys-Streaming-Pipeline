# Next-Item Prediction — Offline Evaluation — Design

**Date:** 2026-08-20
**Status:** approved
**Depends on:** [2026-08-19-event-schema-v2-design.md](2026-08-19-event-schema-v2-design.md)

## Problem

An audit of the data this pipeline generates measured it against the fields and
tasks a recommendation training record is normally expected to support. Every
category was represented except one: there is no next-item prediction anywhere in
the repo. Searches for `next_item`, `sasrec`, `gru4rec`, `bert4rec`, and `seq2seq`
return nothing.

The pieces exist but were never assembled. `ItemSequencePreprocessingJob` builds
per-user item sequences, but only to feed `Item2VecTrainingJob`'s word2vec. The
columnar sequence store (`seq:{user}:{kind}:{day}`) is a *serving* feature — query
hydration reads it to build request context — not a training target. And
`UserEventStreamingJob` filters to `event_type === "click"`, so the store never
saw orders, and now never sees the thumbs and abandons schema v2 added.

So the question "can this pipeline predict what a user engages with next, and how
well" has no answer, and nothing in the repo can produce one.

## Goal

An offline experiment that answers that question with a number, and with enough
context to interpret the number: a learned sequential model measured against three
trivial baselines on the same split, the same candidate set, and the same metrics.

## Non-goals

Deliberately excluded, each deserving its own spec:

| Deferred | Why separate |
|---|---|
| ONNX export | The serving path loads ONNX; exporting before the model has earned its place adds an artifact nobody consumes |
| Serving integration | Wiring predictions into candidate generation or scoring is a ranking change with latency budgets, not a data question |
| Hyperparameter search | Answers "how good can this get", a different question from "does this beat popularity" |
| Cold-start users | A user with no pre-cutoff history cannot be scored by a sequence model at all; they are excluded and counted |
| Thumbs-down as a training signal | A negative-preference signal deserves a considered treatment, not a sign flip |
| Extending the sequence store to carry thumbs | A contract change to a serving-side store, which would make this no longer offline-only |

## Approach

Three alternatives were weighed.

**Baselines plus one learned model** (chosen). Build the harness — sequence
construction, split, metrics — plus three cheap baselines, then one small
sequential transformer, and report all four side by side.

**Learned model only** (rejected). Less work, but a next-item number with nothing
to compare against is uninterpretable. On a 400-item catalog, most-popular is
often brutally strong; without it in the table there is no way to know whether the
model did anything.

**Reuse item2vec as the model** (rejected as the whole project, adopted as a
baseline). Nearest neighbours in the existing embedding space is already a
next-item predictor and costs nothing. It belongs in the comparison, but shipping
only that leaves the actual question open.

The training source is `training_samples` Parquet rather than the sequence store's
Parquet mirror. The mirror is one row per event and already day-bucketed, which is
convenient, but it carries clicks only — the signals this project just spent four
PRs adding would be invisible. `training_samples` carries every engagement signal,
is date-partitioned, and is already the source for the recall, ranking, and
relevance datasets.

## 1. Sequence construction

Read the `training_samples` Parquet directory with `pd.read_parquet`, matching
`analysis_dashboard_report.load_samples`.

**A positive is `clicked == 1` or `ordered == 1` or `thumb == 1`.** A `thumb == -1`
is an explicit statement of dislike and is not a positive; `abandoned == 1` is not
either. Neither is treated as a negative signal in this spec — they are simply not
targets.

A user's timeline is their positives ordered by `(impression_ts, position,
item_id)`. The tie-break is not decorative: every impression in one slate shares a
single `impression_ts`, so ordering on the timestamp alone leaves a slate's
positives in arbitrary order and makes the "next" item non-deterministic between
runs.

Repeated engagement with the same item is kept. Rewatching is real behaviour and
the ranking model in `movielens_pipeline` already has a `rewatch` head. The repeat
rate is reported so a reader can judge how much of any score it explains.

A user needs at least two positives to yield one (history, next) pair. Users below
that are dropped and counted.

## 2. Split: a timestamp quantile

The split is a global time cutoff — every training event precedes every test
target in wall-clock terms, so no user's held-out event is predicted using another
user's later behaviour.

**The cutoff is a quantile of `impression_ts`, not a date.** A date-partition
holdout, as `CtrRankingModelTrainingJob` uses, degenerates here: the
movie-category sim writes its entire run inside one UTC date — a measured span of
4 minutes 50 seconds across 99,415 rows. A date-based holdout on that data selects
either everything or nothing. The engagement backfill sim, by contrast, spans 21
days. A quantile works on both.

Default `NEXT_ITEM_HOLDOUT_QUANTILE = 0.9`. An absolute
`NEXT_ITEM_CUTOFF_TS` overrides it for multi-day data where a specific boundary is
wanted.

| Split | Contents |
|---|---|
| Train | Every positive strictly before the cutoff |
| Test | Per user, their first positive at or after the cutoff |
| History | That user's pre-cutoff positives, in order |

Users with no pre-cutoff history are excluded from the test set and counted in the
support block. Every system — baselines and model alike — sees exactly this split.

## 3. Baselines

All three are fit on pre-cutoff data only.

| Baseline | Prediction |
|---|---|
| `most_popular` | Top-k items by training-set positive count, identical for every user |
| `item2vec_neighbors` | Cosine top-k around the user's last training positive, using the embeddings the sim already writes |
| `repeat_last` | The user's most recent distinct training items, most recent first |

`most_popular` is the one that matters. It is the null hypothesis: a learned model
that does not beat it has demonstrated nothing.

## 4. The model

A small causal transformer over item ids: an item embedding plus a learned
positional encoding, two self-attention layers with a causal mask, and a softmax
over the catalog. Trained with cross-entropy on next-item within each training
sequence.

PyTorch, seeded, module-level constants, following the conventions already in
`movielens_pipeline.py` — that file is the house style for models in this repo and
its `_TransformerLayer` is a reasonable reference for the attention block.

The model is deliberately small. The catalog is 400 items and sequences have a
median length of 107 positives per user; capacity is not the binding constraint
and tuning it is a non-goal.

## 5. Metrics and output

`hit_rate@5`, `hit_rate@10`, `hit_rate@20`, `mrr@10`, and `ndcg@10`, computed by
one shared function so all four systems are measured identically.

**The metric is named `hit_rate`, not `recall`, and the distinction is not
pedantic here.** Each user contributes exactly one test target, so the fraction of
relevant items retrieved is either 0 or 1 — the mean over users is a hit rate.
More importantly, `recall@k` already means something else in this repo:
`recall_eval_report.evaluate` runs leave-one-out over *every* click a user made,
tracks `sum_recall` and `sum_hit` as separate quantities, and filters previously
seen items out of the candidate set. Publishing a different quantity under the
same name in a sibling report is how two numbers end up compared that were never
comparable.

`mrr@10` and `ndcg@10` keep their standard names, which are correct as-is:
reciprocal rank and NDCG are both well defined for a single relevant item, where
the ideal DCG is 1 and NDCG@10 is therefore the reciprocal log rank.

Reuse `rank_topk` and the cosine helper from `recall_eval_report.py` rather than
writing a second implementation of either; write the scoring functions fresh,
since the candidate convention differs.

**Items already in a user's history are not filtered out of the candidate set.**
This is stated because it moves the numbers substantially, and because
`recall_eval_report` takes the opposite convention — it excludes seen items
explicitly. That report asks "can we retrieve a held-out item the user has not
been shown"; this one asks "what does the user engage with next", and with a
measured repeat rate in the data, re-engagement is part of the answer. The two
conventions are both right for their own question, which is the second reason
these metrics must not share a name.

Output is `metrics.json`, in the shape `CtrRankingModelTrainingJob` already
writes, extended with a support block:

| Field | Meaning |
|---|---|
| `systems` | One entry per baseline and the model, each with `hit_rate@{5,10,20}`, `mrr@10`, `ndcg@10` |
| `test_users` | Users contributing a test target |
| `dropped_users` | Split by reason: fewer than two positives, or no pre-cutoff history |
| `catalog_size` | Distinct items in the training split |
| `positive_rate` | Positives over total rows |
| `repeat_rate` | Share of positives whose item already appears earlier in that user's history |
| `cutoff_ts` | The resolved cutoff, so a run is reproducible |

## Error handling

| Condition | Behavior |
|---|---|
| Input directory empty or missing | Raise with the path and the hint to run a sim first, matching `load_samples` |
| A required column absent (`thumb`, `abandoned`) | Treat as all-null — Parquet written before schema v2 lacks them, and clicks alone still form sequences |
| Fewer than two distinct timestamps | Raise: no time cutoff can split the data, and a silent all-train run would report meaningless metrics |
| No test users survive the split | Raise rather than emit a metrics file with empty systems |
| An item in test but absent from training | Scored normally; it is simply unreachable for `most_popular` and `item2vec_neighbors`, which is a real property of those baselines |

## Testing

- **Sequence construction** — positives selected correctly from a frame mixing
  clicks, orders, thumbs up and down, and abandons; the `(ts, position, item_id)`
  tie-break makes slate ordering deterministic across two runs of the same input.
- **Split** — every training event precedes every test target; a user with no
  pre-cutoff history is excluded and counted; the absolute-cutoff override wins
  over the quantile.
- **Baselines** — `most_popular` returns the training-count order on a
  hand-built frame; `repeat_last` returns most-recent-first; `item2vec_neighbors`
  falls back cleanly when an item has no embedding.
- **Metrics** — `hit_rate@k`, `mrr@10`, and `ndcg@10` computed against hand-worked
  expected values on a tiny fixture, not against the implementation's own output.
  One case pins the single-target identity `ndcg@10 == 1 / log2(rank + 1)`, which
  is the property that makes the name honest.
- **End to end** — the CLI runs on a small fixture and writes a `metrics.json`
  containing all four systems and a complete support block.
