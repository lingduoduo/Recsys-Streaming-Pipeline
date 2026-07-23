# Plan: Recall-Task Evaluation

> Implementation plan for [2026-06-27-recall-eval-analysis.md](../specs/2026-06-27-recall-eval-analysis.md).
> Shipped in PR #105 (branch `feat/recall-eval-analysis`).

**Status:** ✅ shipped. Python suite 88 passed / 1 skipped (5 new).

## Decisions

- Per-user, leave-one-out, query derived from the other clicks (genres+title / mean i2vEmb),
  hybrid = RRF; recall@k + hitrate@k; pandas/Python (hand-rolled BM25), implemented now.

## Tasks

- [x] `recall_eval_report.py`:
  - Retrieval primitives: `tokenize`, `build_bm25`/`bm25_score` (Okapi), `cosine`, `mean_vec`,
    `rank_topk` (deterministic tie-break), `rrf`.
  - `evaluate(clicks_by_user, corpus, item_vecs, ks)` — LOO per user → recall@k / hitrate@k per
    method (bm25/embedding/hybrid), aggregated over users; skips < 2-click users; embedding/hybrid
    degrade if vectors absent.
  - `fetch_corpus_and_vecs` (Redis `movie:*:features` + `i2vEmb:*`); `main` reads `training_samples`
    Parquet (relevant = clicked), writes `recall_eval.csv`.
- [x] `test_recall_eval.py` — pure in-process tests: BM25 ranking, cosine/RRF, deterministic top-k,
  and a hand-computed LOO scenario (bm25 recall/hitrate 0.5/0.5; embedding 0.75/1.0) + <2-click skip.

## Verification

- `pytest integration-tests/python_modeling/test_recall_eval.py` → 5 passed; full suite 88/1 skipped.
- Run: `REDIS_HOST=localhost python services/python-modeling/recall_eval_report.py --input <parquet>`
  (needs `movie:*:features` for BM25; `i2vEmb:*` for embedding/hybrid).

## Future scope

- NDCG@k / MRR; user-profile (uEmb / inferred-genres) query representation; ANN index; a Spark variant.
