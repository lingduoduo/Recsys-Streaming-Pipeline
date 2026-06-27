# Spec: Ranking-Samples Streaming Job

> Ranking-stage sibling of the recall-samples job
> ([2026-06-27-recall-samples-job.md](2026-06-27-recall-samples-job.md)) — same source/grain,
> enriched with embeddings and both `is_click` + `rating`.

## Objective

Derive **ranking data** from the existing recsys events and emit it to a new Kafka topic
`ranking_samples` — one row per recommended impression, with user/item features + embeddings and
the engagement labels, for ranking-model training.

## Ranking record (per recommended impression)

| field | source | meaning |
|-------|--------|---------|
| `user_id` | training_samples | who |
| `user_features` | training_samples (`user_features` map) | captured user features |
| `user_embedding` | Redis `uEmb:{user_id}` | user embedding vector (`[]` if absent) |
| `session_id` | training_samples (PR #97) | session |
| `event_ts` | `impression_ts` | time info |
| `recommended_movie_id` | `item_id` | the recommended movie |
| `item_features` | training_samples (`item_features` map) | captured item features |
| `item_embedding` | Redis `i2vEmb:{item_id}` | movie embedding vector (`[]` if absent) |
| `is_click` | `clicked == 1` | binary click label |
| `rating` | `label` | implicit feedback: click → 1.0, order → 2.0, else 0.0 |

## Design

- **Source:** `training_samples` (the OnlineJoiner output) — already joins a slate's impressions +
  feedback per `(request, user, item)` and carries `session_id`, `label`, and the feature maps.
  Reuse `ExperienceCollectorStreamingJob.TrainingSampleSchema` to parse.
- **Embeddings:** looked up per micro-batch from Redis (`uEmb:{user}`, `i2vEmb:{item}`,
  space-separated floats written by the offline embedding jobs). Distinct user/item ids per batch
  are collected on the driver, fetched once each, and attached via `buildRankingSamples`
  (a pure function over two lookup maps → unit-testable). Missing keys → empty vector.
  Prefixes are env-tunable (`USER_EMBEDDING_PREFIX=uEmb`, `ITEM_EMBEDDING_PREFIX=i2vEmb`).
- **Job:** `com.demo.process.RankingSampleStreamingJob` — `readStream(training_samples)` →
  `foreachBatch` (fetch embeddings → `buildRankingSamples`) → `write` JSON to `ranking_samples`.
- **rating = implicit label** (chosen scope); `is_click` is the binary. Explicit-rating join is
  out of scope.

## Scope / boundaries

- **In:** the new job + spec; new output topic `ranking_samples`. Reads existing
  `training_samples` + existing Redis embedding keys — **additive**, no schema/topic/key change.
- **Out:** explicit-rating join; writing embeddings (offline jobs own that); wiring into
  `run-data-pipeline.sh`. Run standalone:
  `SPARK_MAIN_CLASS=com.demo.process.RankingSampleStreamingJob ./run-streaming-job.sh`.

## Success criteria (testable)

- [ ] `buildRankingSamples` emits the 10 fields above; `user_embedding`/`item_embedding` from the
      lookups (empty when absent); `is_click` boolean; `rating` = label. (Unit-tested.)
- [ ] `fetchEmbeddings` parses `prefix:id` space-separated floats; empty ids → no Redis call.
- [ ] Job consumes `training_samples`, writes JSON to `ranking_samples`.
- [ ] `sbt test` green (existing specs unaffected).

## Notes

- Embeddings are attached via a UDF over a driver-fetched map (small per-batch). For very large
  batches a broadcast join would scale better — future optimization.
