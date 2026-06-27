# Spec: Keyword Analysis (distribution + category top keywords)

## Objective

Two analyses over the engagement stream, exposing genre-keyword structure for movies and queries:

1. **Keyword / subkeyword distribution** — how often each genre appears as a movie's first
   (`keyword`) vs second (`subkeyword`) genre, counted for movies (impressions / distinct movies)
   and queries (clicks).
2. **Category top keywords (movies vs queries)** — within each category value (l1/l2/l3), the top
   keywords ranked by movie impressions, with the query-click count beside them.

Delivered as one PySpark batch report (`keyword_analysis_report.py`) producing CSVs.

## Definitions (chosen scope)

- **keyword** = a movie's first genre; **subkeyword** = its second genre (`none` if only one).
- **category**: `l1` = genre family, `l2` = primary genre, `l3` = genre × release decade
  (via `movie_categories`).
- **query** = `(user_id, session_id)`; a query's keywords are the genres of the movies the user
  **clicked** in the session — so the "query" side is the clicked rows (`clicked == 1`).
- For analysis 2, "keyword" is each genre of a movie (exploded), so it is not trivially equal to
  the category itself.

## Inputs / sources

- `training_samples` Parquet — `user_id`, `session_id`, `item_id`, `clicked`.
- Movie genres (+ release year for `l3`): from the Parquet `genres` column when present
  (e.g. the OnlineJoiner catalog join), otherwise fetched from Redis `movie:{id}:features`
  (`genre_meta.fetch_movie_meta`). Missing → `genres = []` → `keyword = "unknown"`.

## Outputs (CSV under `<input>/../report-keywords`)

| File | Columns |
|------|---------|
| `by_keyword` | `keyword`, `movie_impressions`, `distinct_movies`, `query_clicks` |
| `by_subkeyword` | `subkeyword`, `movie_impressions`, `distinct_movies`, `query_clicks` |
| `top_keywords_l1` / `_l2` / `_l3` | `<level>`, `keyword`, `movie_impressions`, `query_clicks`, `rank` (top-N per category) |

## Design notes

- Pure DataFrame transforms (`with_keywords_and_categories`, `keyword_distribution`,
  `category_top_keywords`) for testability; genre logic reused from `movie_categories`.
- UDFs reference `movie_categories`, so the report ships `movie_categories.py` + `genre_meta.py`
  to the Spark workers via `addPyFile` (else `ModuleNotFoundError` on executors).
- Run through the pinned Spark: `"$SPARK_HOME/bin/spark-submit"` (pip pyspark may differ).

## Boundaries

- **In:** the report + `movie_categories.secondary_genre` + `genre_meta`. Additive — reads existing
  Parquet / Redis keys, writes CSVs only.
- **Out:** non-genre keyword sources (catalog tags/keywords, title tokens); a streaming/topic
  variant; a REST surface.

## Success criteria (testable)

- [ ] `secondary_genre` returns second genre / `none`. (Unit-tested.)
- [ ] `by_keyword`/`by_subkeyword` count movie impressions, distinct movies, and query clicks.
- [ ] `top_keywords_l1/l2/l3` rank exploded genres per category with movie + query counts.
- [ ] Report runs via spark-submit on a Parquet with a `genres` column (no Redis needed).
