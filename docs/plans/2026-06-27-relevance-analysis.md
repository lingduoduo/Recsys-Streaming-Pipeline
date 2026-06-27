# Plan: Relevance Analysis

> Implementation plan for [2026-06-27-relevance-analysis.md](../specs/2026-06-27-relevance-analysis.md).
> Shipped in PR #104 (branch `feat/relevance-analysis`).

**Status:** ✅ shipped. Python suite 83 passed / 1 skipped (1 new).

## Decisions

- relevance_state = 3-level graded (impression_only / clicked / ordered) from `label`;
  query = genre-combo; two breakdowns (by query + by genre); implemented now.

## Tasks

- [x] `relevance_analysis_report.py` (PySpark):
  - `ensure_genres` — Parquet `genres` column if present, else join Redis movie meta.
  - `with_relevance` — `relevance_state` from `label`; `query` = genre-combo.
  - `state_distribution` — per state: score, impressions, proportion.
  - `_relevance_breakdown(df, dim)` → `by_query` (dim=query) and `by_genre` (explode genres):
    impressions, mean_score, impression_only/clicked/ordered shares.
  - No UDFs (plain Spark expressions) → no `addPyFile`.
- [x] `test_relevance_analysis.py` — spark-submit integration on a tiny Parquet (no Redis):
  asserts state proportions and by-query / by-genre mean_score + shares.

## Verification

- `pytest integration-tests/python_modeling/test_relevance_analysis.py` → 1 passed; full suite 83/1 skipped.
- Run: `REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit"
  services/python-modeling/relevance_analysis_report.py --input <training-samples parquet>`.

## Future scope

- Explicit-rating relevance level (join RatingEvent); a query×genre overlap/match score;
  significance testing; a streaming/topic variant.
