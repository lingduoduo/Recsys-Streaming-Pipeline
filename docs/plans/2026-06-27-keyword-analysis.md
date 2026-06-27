# Plan: Keyword Analysis

> Implementation plan for [2026-06-27-keyword-analysis.md](../specs/2026-06-27-keyword-analysis.md).
> Shipped in PR #102 (branch `feat/keyword-analysis`).

**Status:** ✅ shipped. Python suite 81 passed / 1 skipped (2 new).

## Decisions

- keyword = first genre, subkeyword = second genre; query keywords = genres of the session's
  clicked movies; one PySpark job covering both analyses (shared input/join).

## Tasks

- [x] `movie_categories.secondary_genre(genres)` — second genre / `none`. (Unit-tested.)
- [x] `genre_meta.fetch_movie_meta(host, port)` — Redis `movie:*:features` → `{item_id, genres, release_year}` (`[]` if down).
- [x] `keyword_analysis_report.py` (PySpark):
  - `ensure_meta` — use the Parquet `genres`/`release_year` columns if present, else join Redis.
  - `with_keywords_and_categories` — `keyword`/`subkeyword` + `l1`/`l2`/`l3` (UDFs over `movie_categories`).
  - `keyword_distribution(df, dim)` — movie impressions + distinct movies + query clicks per keyword/subkeyword.
  - `category_top_keywords(df, level)` — explode genres, rank per category by movie impressions, with query clicks.
  - `main` ships `movie_categories.py` + `genre_meta.py` via `addPyFile` (UDFs run on workers); writes CSVs.
- [x] `test_keyword_analysis.py` — unit (`secondary_genre`) + spark-submit integration on a tiny
  Parquet with `genres`/`release_year` (no Redis), asserting both analyses.

## Verification

- `pytest integration-tests/python_modeling/test_keyword_analysis.py` → 2 passed; full suite 81/1 skipped.
- Run: `REDIS_HOST=localhost "$SPARK_HOME/bin/spark-submit"
  services/python-modeling/keyword_analysis_report.py --input <training-samples parquet>`.

## Future scope

- Catalog tags/keywords or title-token keywords; an l3 (genre×decade) drill-down UI; a topic/streaming variant.
