# Plan: Recall-Samples Streaming Job

> Implementation plan for [2026-06-27-recall-samples-job.md](../specs/2026-06-27-recall-samples-job.md).
> Shipped in PR #98 (branch `feat/recall-samples-job`).

**Status:** ✅ shipped. `sbt test` green (41 = 39 + 2 new).

## Task — `RecallSampleStreamingJob`

**Files:** `src/main/scala/com/demo/process/RecallSampleStreamingJob.scala`,
`src/test/scala/com/demo/process/RecallSampleStreamingJobSpec.scala`.

- [x] `buildRecallSamples(samples)` — pure `select` mapping training samples → recall rows:
  `user_id`, `session_id`, `event_ts` (= `impression_ts`), `recommended_movie_id` (= `item_id`),
  `click_movie_id` (= `item_id` iff `clicked == 1`, else null), `rating` (= `label`).
- [x] `parseSamples` — `EventParsing.fromJson(raw, ExperienceCollectorStreamingJob.TrainingSampleSchema)`
  filtered on non-null `user_id`/`item_id` (reuses the schema, which already has `session_id`).
- [x] `main` — `readStream(RECALL_INPUT_TOPIC=training_samples)` → `foreachBatch` →
  `to_json(struct(*))` keyed by `user_id:session_id:recommended_movie_id` → `write` to
  `RECALL_OUTPUT_TOPIC=recall_samples`; BatchMetricsListener registered; ProcessingTime trigger.
- [x] Spec: per-impression rows, `click_movie_id` null unless clicked, `rating` = label
  (click→1.0, order→2.0, miss→0.0), and the recall schema columns.

## Verification

- `sbt test` → 41 passed (2 new in `RecallSampleStreamingJobSpec`); `sbt assembly` builds.
- Run: `SPARK_MAIN_CLASS=com.demo.process.RecallSampleStreamingJob ./run-streaming-job.sh`
  (with `training_samples` flowing, e.g. via `run-data-pipeline.sh`).

## Future scope

- Explicit-rating variant: join `RatingEvent` (movielens_context) by `(user_id, item_id)` to add a
  true 1–5 `rating` alongside the implicit label.
- Optionally add the job to `run-data-pipeline.sh` as a 4th pipeline stage.
