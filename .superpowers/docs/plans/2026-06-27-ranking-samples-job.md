# Plan: Ranking-Samples Streaming Job

> Implementation plan for [2026-06-27-ranking-samples-job.md](../specs/2026-06-27-ranking-samples-job.md).
> Shipped in PR #99 (branch `feat/ranking-samples-job`).

**Status:** ✅ shipped. `sbt test` green (43 = 41 + 2 new); `sbt assembly` builds.

## Task — `RankingSampleStreamingJob`

**Files:** `src/main/scala/com/demo/process/RankingSampleStreamingJob.scala`,
`src/test/scala/com/demo/process/RankingSampleStreamingJobSpec.scala`.

- [x] `buildRankingSamples(samples, userEmb, itemEmb)` — pure `select` mapping training samples →
  ranking rows: `user_id`, `user_features`, `user_embedding` (UDF lookup), `session_id`,
  `event_ts` (= `impression_ts`), `recommended_movie_id` (= `item_id`), `item_features`,
  `item_embedding` (UDF lookup), `is_click` (= `clicked == 1`), `rating` (= `label`). Missing
  embedding → empty vector.
- [x] `fetchEmbeddings(ids, prefix, host, port, poolMax)` — driver-side Redis GET of
  `prefix:id` (space-separated floats, via `com.demo.task.RedisPool`); missing keys omitted;
  empty ids → empty map (no Redis call).
- [x] `parseSamples` — `EventParsing.fromJson(raw, ExperienceCollectorStreamingJob.TrainingSampleSchema)`
  filtered on non-null `user_id`/`item_id`.
- [x] `main` — `readStream(RANKING_INPUT_TOPIC=training_samples)` → `foreachBatch`: collect
  distinct user/item ids, fetch embeddings (`USER_EMBEDDING_PREFIX=uEmb`,
  `ITEM_EMBEDDING_PREFIX=i2vEmb`), `buildRankingSamples`, `to_json` keyed by
  `user_id:session_id:recommended_movie_id` → `write` to `RANKING_OUTPUT_TOPIC=ranking_samples`;
  BatchMetricsListener registered; ProcessingTime trigger.
- [x] Spec: field set + values (embeddings from lookups, empty when absent; `is_click` boolean;
  `rating` = label incl. order → 2.0); `fetchEmbeddings` empty-ids path.

## Verification

- `sbt test` → 43 passed (2 new in `RankingSampleStreamingJobSpec`); `sbt assembly` builds.
- Run: `SPARK_MAIN_CLASS=com.demo.process.RankingSampleStreamingJob ./run-streaming-job.sh`
  (with `training_samples` flowing and embeddings present in Redis, e.g. from the offline jobs).

## Future scope

- Explicit-rating variant (join `RatingEvent` by `(user_id, item_id)`).
- Broadcast the embedding maps for very large batches; optionally add to `run-data-pipeline.sh`.
