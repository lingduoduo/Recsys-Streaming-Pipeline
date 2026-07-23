# Spec: Relevance Analysis (state distribution + query/genre relevance)

## Objective

A third relevance-focused analysis. Two analyses over the engagement stream:

1. **Relevance-state distribution** — per relevance state: its score, impressions, and proportion
   (score vs proportions).
2. **Query / genre relevance** — relevance-state mix + mean score broken down by query AND
   (separately) by movie genre, indicating which queries and genres are most relevant.

Delivered as one PySpark batch report (`relevance_analysis_report.py`) writing CSVs.

## Definitions (chosen scope)

- **relevance_state** (graded, from the training_samples `label`):
  `0.0 → impression_only`, `1.0 → clicked`, `2.0 → ordered`.
- **query** = the impression's genre-combo intent, `concat_ws(" ", genres)` (`"unknown"` if no
  genres) — consistent with the query-analysis job.
- **mean_score** = `avg(label)` (relevance strength); state **share** = fraction of impressions in
  that state.
- Counting grain = one `training_samples` row per recommended movie (an impression).

## Inputs / outputs

- **Input:** `training_samples` Parquet (`item_id`, `label`); genres from the Parquet `genres`
  column if present, else Redis `movie:{id}:features` (`genre_meta.fetch_movie_meta`).
- **Output CSVs** (under `<input>/../report-relevance`):

  | File | Columns |
  |------|---------|
  | `by_state` | `relevance_state`, `score`, `impressions`, `proportion` |
  | `by_query` | `query`, `impressions`, `mean_score`, `impression_only_share`, `clicked_share`, `ordered_share` |
  | `by_genre` | `genre`, `impressions`, `mean_score`, `impression_only_share`, `clicked_share`, `ordered_share` |

## Design notes

- Pure DataFrame transforms (`with_relevance`, `state_distribution`, `by_query`, `by_genre`);
  `by_genre` explodes `genres`. All plain Spark expressions — **no UDF**, so no `addPyFile` needed.
- Run through the pinned Spark (`"$SPARK_HOME/bin/spark-submit"`).

## Boundaries

- **In:** the report (+ reuse `genre_meta`). Additive — reads existing Parquet/Redis, writes CSVs.
- **Out:** explicit-rating relevance level (label only here); a query×genre match score; a
  streaming variant.

## Success criteria (testable)

- [ ] `by_state` gives score + proportion per relevance state (sums to 1).
- [ ] `by_query` / `by_genre` give mean_score + per-state shares per query / genre.
- [ ] Runs via spark-submit on a Parquet with a `genres` column (no Redis needed).
