# Plan: Query Analysis

> Implementation plan for [2026-06-27-query-analysis.md](../specs/2026-06-27-query-analysis.md).
> Shipped in PR #103 (branch `feat/query-analysis`).

**Status:** ✅ shipped. Python suite 82 passed / 1 skipped (1 new).

## Decisions

- query = genre-combo string (`concat_ws(" ", genres)`); CVR = orders/impressions; short ≤ 10 chars;
  implemented now.

## Tasks

- [x] `query_analysis_report.py` (PySpark):
  - `ensure_genres` — use the Parquet `genres` column if present, else join Redis movie meta.
  - `with_query` — `query` (genre-combo, `unknown` if empty), `query_len`, `query_length` bucket.
  - `most_common_queries` — per query: impressions, users, sessions, clicks, orders, CTR, CVR, avg_rating.
  - `length_engagement` — per short/long bucket: same metrics + distinct_queries.
  - No UDFs (plain `concat_ws`/`length`) → no `addPyFile` needed.
- [x] `test_query_analysis.py` — spark-submit integration on a tiny Parquet (no Redis): asserts
  most-common impressions/len and short-vs-long CTR/CVR.

## Verification

- `pytest integration-tests/python_modeling/test_query_analysis.py` → 1 passed; full suite 82/1 skipped.
- Run: `REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit"
  services/python-modeling/query_analysis_report.py --input <training-samples parquet>`.

## Future scope

- Real free-text queries if a search surface is added; title/tag-based query text; per-query
  significance testing; a streaming/topic variant.
