# Spec: Ranking-Performance Evaluation (logloss + ROC-AUC)

## Objective

Show ranking performance by computing **logloss** and **ROC-AUC** of several ranking signals
against the click label, and comparing them.

Delivered as a self-contained pandas/Python job (`ranking_eval_report.py`).

## Setup (chosen scope)

- **Grain:** one `training_samples` row per recommended impression.
- **Label:** positive = **click** (`clicked == 1`, i.e. label ≥ 1).
- **Signals compared** (each scores an impression's click-likelihood):

  | Signal | Score | Source |
  |--------|-------|--------|
  | `popularity` | item global popularity | Redis `global:item_popularity` (ZSET) |
  | `position` | `-position` (lower position = higher rank) | `training_samples` |
  | `embedding` | `dot(uEmb[user], i2vEmb[item])` | Redis `uEmb:*`, `i2vEmb:*` (rows with both) |

## Metrics (per signal)

- **ROC-AUC** — rank-based (Mann–Whitney, average ranks for ties), calibration-free → the headline
  ranking metric. `None` if only one class present.
- **logloss** — on a per-signal calibration of the score (z-score → sigmoid → probability), so
  different-scale signals are comparable. (AUC is the rigorous metric; logloss is indicative.)
- **coverage** — fraction of impressions the signal could score (≈1 for popularity/position; may be
  < 1 for embedding when `uEmb`/`i2vEmb` are missing).

## Inputs / outputs

- **Input:** `training_samples` Parquet (`user_id`, `item_id`, `position`, `clicked`/`label`);
  Redis `global:item_popularity`, `uEmb:*`, `i2vEmb:*`.
- **Output:** `ranking_eval.csv` — `signal`, `n`, `positives`, `coverage`, `auc`, `logloss`.

## Design notes

- Pure functions: `auc` (rank-based w/ ties), `logloss`, `zsigmoid`, `evaluate_signal` —
  unit-tested in-process (no Spark/Redis) against hand-computed values (AUC of the classic
  `[.1,.4,.35,.8]/[0,0,1,1]` example = 0.75).
- Single-node pandas/Python, hand-rolled metrics (no sklearn dependency). Run with plain `python`.
- Signals lacking data (e.g. no embeddings) report `auc=None` with their coverage.

## Boundaries

- **In:** the report. Additive — reads existing Parquet/Redis, writes a CSV.
- **Out:** training a learned ranker; NDCG/MRR; calibrated logloss via Platt/Isotonic; a Spark variant.

## Success criteria (testable)

- [ ] `auc` matches known values (perfect=1, reversed=0, ties=0.5, one-class=None).
- [ ] `logloss` matches a hand value; `zsigmoid` is bounded and 0.5 on zero variance.
- [ ] `evaluate_signal` returns auc/logloss/n/positives; report emits one row per signal with coverage.
