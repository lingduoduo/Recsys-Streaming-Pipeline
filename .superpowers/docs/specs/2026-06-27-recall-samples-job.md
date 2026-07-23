# Spec: Recall-Samples Streaming Job

## Objective

Derive **recall data** from the existing recsys events and emit it to a new Kafka topic
`recall_samples` — one row per recommended impression, for recall/ranking analysis and training.

## Recall record (per recommended impression)

| field | source | meaning |
|-------|--------|---------|
| `user_id` | training_samples | who |
| `session_id` | training_samples (threaded in PR #97) | session |
| `event_ts` | `impression_ts` | time info (seconds) |
| `recommended_movie_id` | `item_id` | the recommended movie shown |
| `click_movie_id` | `item_id` iff `clicked == 1`, else null | which recommendation was clicked |
| `rating` | `label` | implicit feedback: click → 1.0, order → 2.0, else 0.0 |

## Design

- **Source:** `training_samples` (the OnlineJoiner output) — it already joins a slate's
  impressions + feedback per `(request, user, item)` and carries `session_id` and `label`. Same
  source as `ExperienceCollectorStreamingJob`; reuse its `TrainingSampleSchema` to parse.
- **Job:** `com.demo.process.RecallSampleStreamingJob` — `readStream(training_samples)` →
  `buildRecallSamples` (a pure `select`) → `foreachBatch` → `write` JSON to `recall_samples`.
  `buildRecallSamples` is extracted as a pure function for unit testing.
- **Rating = implicit label** (chosen scope): no rating join; the `label` column carries
  click/order. (Explicit `RatingEvent` ratings live on `movielens_context`; out of scope here.)
- Env knobs (with defaults): `RECALL_INPUT_TOPIC=training_samples`,
  `RECALL_OUTPUT_TOPIC=recall_samples`, plus the usual offsets/trigger/checkpoint vars.

## Scope

- **In:** the new `RecallSampleStreamingJob` + its spec; a new output topic `recall_samples`.
- **Out:** explicit-rating join; changes to existing jobs/schemas; wiring into
  `run-data-pipeline.sh` (runnable standalone via `run-streaming-job.sh`).

## Boundaries

- **Additive:** new topic only; reads existing `training_samples` (no schema change). Does not
  rename/alter `recsys_events` / `training_samples` / `training_experiences`.
- Run: `SPARK_MAIN_CLASS=com.demo.process.RecallSampleStreamingJob ./run-streaming-job.sh`.

## Success criteria (testable)

- [ ] `buildRecallSamples` emits one row per impression with the six fields above; `click_movie_id`
      null unless clicked; `rating` = label. (Unit-tested.)
- [ ] Job consumes `training_samples`, writes JSON to `recall_samples`.
- [ ] `sbt test` green (existing specs unaffected).
