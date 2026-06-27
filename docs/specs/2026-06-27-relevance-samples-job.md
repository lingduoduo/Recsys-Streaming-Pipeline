# Spec: Relevance-Samples Streaming Job

> Third derived dataset off `training_samples`, after recall
> ([recall](2026-06-27-recall-samples-job.md)) and ranking
> ([ranking](2026-06-27-ranking-samples-job.md)) — an LTR/relevance-judgment shape
> (query → document, with a score).

## Objective

Derive **relevance data** from the existing recsys events and emit it to a new Kafka topic
`relevance_samples` — one row per recommended impression as `(query, document, score)` for
relevance/LTR analysis.

## Relevance record (per recommended impression)

| field | source | meaning |
|-------|--------|---------|
| `query` | `user_id:session_id` | personalized query (the (user, session) is the query) |
| `event_ts` | `impression_ts` | time info |
| `recommended_movie_id` | `item_id` | the document (movie) |
| `title` | Redis `movie:{id}:features` | movie title (null if absent) |
| `genres` | Redis `movie:{id}:features` | movie genres (array; `[]` if absent) |
| `release_year` | Redis `movie:{id}:features` | release year (null if absent) |
| `score` | `label` | graded relevance: click → 1.0, order → 2.0, else 0.0 |

## Design

- **Source:** `training_samples` (OnlineJoiner output; reuse
  `ExperienceCollectorStreamingJob.TrainingSampleSchema`).
- **Document text:** per micro-batch, distinct `item_id`s are collected on the driver and their
  `movie:{id}:features` hashes fetched from Redis (`title`, `genres` comma-string, `releaseYear`),
  written by `MovieLensContextCollectorStreamingJob` from the movie-metadata path. Attached via
  `buildRelevanceSamples` (a pure function over three lookup maps → unit-testable). Missing →
  null title / empty genres / null year.
- **Query:** `concat_ws(":", user_id, session_id)` (personalized-retrieval framing).
- **Score = engagement label** (chosen scope).
- **Job:** `com.demo.process.RelevanceSampleStreamingJob` — `readStream(training_samples)` →
  `foreachBatch` (fetch movie features → `buildRelevanceSamples`) → `write` JSON to
  `relevance_samples`.

## Scope / boundaries

- **In:** the new job + spec; new topic `relevance_samples`. Reads existing `training_samples` +
  existing `movie:{id}:features` keys — **additive**, no schema/topic/key change.
- **Out:** explicit-rating or model/similarity scores; producing movie metadata (the movie
  context path owns that). Run standalone:
  `SPARK_MAIN_CLASS=com.demo.process.RelevanceSampleStreamingJob ./run-streaming-job.sh`.

## Success criteria (testable)

- [ ] `buildRelevanceSamples` emits the 7 fields; `query` = user_id:session_id; title/genres/year
      from the lookups (null/empty when absent); `score` = label. (Unit-tested.)
- [ ] `fetchMovieFeatures` HGETALLs `movie:{id}:features`; empty ids → no Redis call.
- [ ] Job consumes `training_samples`, writes JSON to `relevance_samples`.
- [ ] `sbt test` green (existing specs unaffected).
