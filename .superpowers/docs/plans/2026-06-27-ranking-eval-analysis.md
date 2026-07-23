# Plan: Ranking-Performance Evaluation

> Implementation plan for [2026-06-27-ranking-eval-analysis.md](../specs/2026-06-27-ranking-eval-analysis.md).
> Shipped in PR #106 (branch `feat/ranking-eval-analysis`).

**Status:** ✅ shipped. New tests 5 passed (full suite green; note: independent of the still-open recall-eval PR #105).

## Decisions

- Compare signals (popularity / position / embedding) on logloss + ROC-AUC; positive = click;
  pandas/Python with hand-rolled metrics; implemented now.

## Tasks

- [x] `ranking_eval_report.py`:
  - Pure metrics: `auc` (rank-based Mann–Whitney, average ranks for ties; None if one class),
    `logloss` (clipped), `zsigmoid` (per-signal calibration), `evaluate_signal`.
  - Signals: `popularity` (Redis `global:item_popularity`), `position` (`-position`),
    `embedding` (`dot(uEmb, i2vEmb)`, rows with both); per-signal coverage.
  - `fetch_popularity` / `fetch_embeddings` (Redis); `main` reads `training_samples` Parquet,
    label = click, writes `ranking_eval.csv`.
- [x] `test_ranking_eval.py` — pure in-process tests: AUC known value (0.75) + perfect/reversed/
  ties/one-class, logloss hand value, zsigmoid bounds, `evaluate_signal`.

## Verification

- `pytest integration-tests/python_modeling/test_ranking_eval.py` → 5 passed.
- Run: `REDIS_HOST=localhost python services/python-modeling/ranking_eval_report.py --input <parquet>`
  (needs `global:item_popularity`; `uEmb:*`+`i2vEmb:*` for the embedding signal).

## Future scope

- Train a learned ranker (logistic/GBDT) and report its holdout logloss/AUC; NDCG@k / MRR;
  calibrated logloss (Platt/Isotonic); a Spark variant.
