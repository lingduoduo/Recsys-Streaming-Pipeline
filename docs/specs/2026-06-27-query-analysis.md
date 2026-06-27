# Spec: Query Analysis (most-common + short-vs-long engagement)

## Objective

A second query-focused analysis (after keyword analysis). Two analyses over the engagement stream:

1. **Most common queries** — the top queries by impressions (with users / sessions / CTR / CVR).
2. **Short vs long query engagement** — engagement (CTR, CVR, avg rating) for short queries
   (≤10 chars) vs long queries (>10 chars).

Delivered as one PySpark batch report (`query_analysis_report.py`) writing CSVs.

## What a "query" is (chosen scope)

The events carry no free-text query, so the **query = the search intent of a recommended
impression**, represented as its movie's **genre-combo string** — `concat_ws(" ", genres)`
(e.g. `"Sci-Fi Action"`; `"unknown"` when a movie has no genres). This both repeats (so "most
common" is meaningful) and varies in length (so short vs long is meaningful).

## Entity definitions (counted at the training_samples grain)

`training_samples` has one row per recommended movie in a request. From that grain:

| Term | Definition |
|------|-----------|
| **impression** | one `training_samples` row (a recommended movie shown) — the counting unit |
| **query** | `concat_ws(" ", genres)` of the impression's movie |
| **user / session** | distinct `user_id` / `session_id` |
| **clicks / orders** | `Σ clicked` / `Σ ordered` |
| **CTR** | `clicks / impressions` |
| **CVR** | `orders / impressions` (conversion over impressions) |
| **avg_rating** | `avg(label)` (implicit label: click → 1.0, order → 2.0, else 0.0) |

`query_len` = character length of the query; **short** = `query_len ≤ 10`, **long** = `> 10`.

## Inputs / outputs

- **Input:** `training_samples` Parquet (`user_id`, `session_id`, `item_id`, `clicked`, `ordered`,
  `label`); genres from the Parquet `genres` column if present, else Redis `movie:{id}:features`
  (`genre_meta.fetch_movie_meta`).
- **Output CSVs** (under `<input>/../report-queries`):

  | File | Columns |
  |------|---------|
  | `top_queries` | `query`, `query_len`, `impressions`, `users`, `sessions`, `clicks`, `orders`, `ctr`, `cvr`, `avg_rating` |
  | `by_query_length` | `query_length` (short/long), `impressions`, `users`, `sessions`, `distinct_queries`, `clicks`, `orders`, `ctr`, `cvr`, `avg_rating` |

## Design notes

- Pure DataFrame transforms (`with_query`, `most_common_queries`, `length_engagement`); the query
  is a plain Spark expression (`concat_ws` + `length`) — **no UDF**, so no `addPyFile` needed.
- Run through the pinned Spark (`"$SPARK_HOME/bin/spark-submit"`).

## Boundaries

- **In:** the report (+ reuse `genre_meta`). Additive — reads existing Parquet/Redis, writes CSVs.
- **Out:** free-text/search queries (none exist); title- or tag-based query text; a streaming variant.

## Success criteria (testable)

- [ ] `top_queries` ranks genre-combo queries by impressions with users/sessions/CTR/CVR/avg_rating.
- [ ] `by_query_length` splits short (≤10) vs long (>10) with CTR/CVR/avg_rating.
- [ ] Runs via spark-submit on a Parquet with a `genres` column (no Redis needed).
