# Plan: Relevance-Samples Streaming Job

> Implementation plan for [2026-06-27-relevance-samples-job.md](../specs/2026-06-27-relevance-samples-job.md).
> Shipped in PR #100 (branch `feat/relevance-samples-job`).

**Status:** ✅ shipped. `sbt test` green (45 = 43 + 2 new); `sbt assembly` builds.

## Task — `RelevanceSampleStreamingJob`

**Files:** `src/main/scala/com/demo/process/RelevanceSampleStreamingJob.scala`,
`src/test/scala/com/demo/process/RelevanceSampleStreamingJobSpec.scala`.

- [x] `buildRelevanceSamples(samples, titleMap, genresMap, yearMap)` — pure `select`:
  `query` (= `concat_ws(":", user_id, session_id)`), `event_ts` (= `impression_ts`),
  `recommended_movie_id` (= `item_id`), `title` (lookup, null if absent),
  `genres` (lookup comma-string → array, `[]` if absent), `release_year` (lookup → nullable int),
  `score` (= `label`).
- [x] `fetchMovieFeatures(ids, host, port, poolMax)` — driver-side Redis HGETALL of
  `movie:{id}:features` (via `RedisPool`); missing keys omitted; empty ids → empty map.
- [x] `parseSamples` — `EventParsing.fromJson(raw, ExperienceCollectorStreamingJob.TrainingSampleSchema)`
  filtered on non-null `user_id`/`item_id`.
- [x] `main` — `readStream(RELEVANCE_INPUT_TOPIC=training_samples)` → `foreachBatch`: distinct
  `item_id`s → fetch features → split into title/genres/year maps → `buildRelevanceSamples` →
  `to_json` keyed by `query:recommended_movie_id` → `write` to
  `RELEVANCE_OUTPUT_TOPIC=relevance_samples`; BatchMetricsListener; ProcessingTime trigger.
- [x] Spec: field set + values (query, joined title/genres/year incl. absent cases, score = label);
  `fetchMovieFeatures` empty-ids path.

## Verification

- `sbt test` → 45 passed (2 new in `RelevanceSampleStreamingJobSpec`); `sbt assembly` builds.
- Run: `SPARK_MAIN_CLASS=com.demo.process.RelevanceSampleStreamingJob ./run-streaming-job.sh`
  (with `training_samples` flowing and `movie:{id}:features` populated, e.g. by the movie sim /
  MovieLensContextCollector).

## Future scope

- Alternate scores: explicit rating, or user×movie embedding similarity.
- A real text query (e.g. from user context/recent genres) instead of the user:session key.
