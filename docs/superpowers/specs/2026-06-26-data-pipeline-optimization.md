# Spec: Data Pipeline Optimization

> Saved here (not root `SPEC.md`) because root `SPEC.md` already holds the
> training-consolidation spec. This spec is scoped to the streaming data pipeline.

## Objective

Optimize the streaming **Data Pipeline** across all five dimensions — throughput,
cost, correctness, code quality, operability — **without changing external
contracts** (Kafka topic names, output schemas, Redis keys).

Pipeline under optimization:

```
producer.py ──► Kafka recsys_events (:9092)
                  ├─► UserEventStreamingJob        ──► Redis :6379  (global:item_popularity)
                  └─► OnlineJoinerStreamingJob     ──► Kafka training_samples + Parquet
                                                        └─► ExperienceCollectorStreamingJob ──► Kafka training_experiences
```

**Users:** developers running the pipeline locally (`local[*]`) and operators
running it on a Spark cluster.

**Success:** every work item's acceptance check passes; the existing ScalaTest
specs stay green; no topic/schema/Redis-key change ships without explicit approval.

## Scope

- **In:** the 3 streaming jobs (`UserEventStreamingJob`, `OnlineJoinerStreamingJob`,
  `ExperienceCollectorStreamingJob`), their shared utils (`SparkSessions`, `Env`),
  and `run-streaming-job.sh`.
- **Out:** `producer.py` internals, the modeling/experiment pipelines, the Flink
  user-history path. (The other two `from_json` jobs —
  `RecommendationResponseStatsJob`, `MovieLensContextCollectorStreamingJob` — may
  adopt the shared parser opportunistically but are not required.)

## Tech Stack

Scala 2.12.18 · Spark 3.5.1 Structured Streaming · Jedis 5.1.5 · sbt + sbt-assembly ·
JDK 17 · ScalaTest 3.2.18. No new runtime dependencies expected.

## Commands

```bash
# from recsys-pipeline/services/spark-streaming-job
sbt assembly        # build fat jar
sbt test            # run all ScalaTest specs
sbt "testOnly com.demo.*StreamingJobSpec"   # run the streaming-job specs

# from recsys-pipeline (run a single job)
SPARK_MAIN_CLASS=com.demo.process.OnlineJoinerStreamingJob ./run-streaming-job.sh
```

## Project Structure

```
services/spark-streaming-job/src/main/scala/com/demo/
  event/                     # NEW — shared event parsing (extracted from the jobs)
    EventSchemas.scala       #   base event StructType + per-job extensions
    EventParsing.scala       #   parseEvents(raw): DataFrame, timestamp normalisation, null filter
  task/UserEventStreamingJob.scala
  process/OnlineJoinerStreamingJob.scala
  process/ExperienceCollectorStreamingJob.scala
  util/{SparkSessions,Env}.scala      # gains AQE config + metrics listener helper
src/test/scala/com/demo/
  event/EventParsingSpec.scala         # NEW — unit tests for the shared parser
  task/UserEventStreamingJobSpec.scala         # existing, must stay green
  process/OnlineJoinerStreamingJobSpec.scala   # existing, must stay green
  process/ExperienceCollectorStreamingJobSpec.scala  # existing, must stay green
recsys-pipeline/
  run-streaming-job.sh        # unchanged single-job entry
  run-data-pipeline.sh        # NEW — launch all 3 jobs with distinct checkpoints
```

## Work Items (prioritized; each independently shippable)

### Phase 1 — behavior-preserving (default scope)

**1. Code quality — shared event parsing.**
Extract the duplicated event `StructType` + `from_json` + timestamp-normalisation +
null-filter into `com.demo.event.{EventSchemas, EventParsing}`. The base schema
holds the common fields; jobs that need extra columns (`OnlineJoiner`:
`request_id`, `position`, `*_features`) compose on top. The two timestamp
conventions differ today (UserEvent normalises to **millis**, OnlineJoiner to
**seconds**) — preserve each job's existing output exactly; do not unify the unit.
- *Acceptance:* no `from_json(... EventSchema ...)` schema literal duplicated across
  the 3 jobs; `EventParsingSpec` covers millis+legacy-seconds+null-filter paths.
- *Verify:* `sbt test` green (existing job specs unchanged).
- *Files:* `event/*` (new), the 3 jobs, `event/EventParsingSpec.scala` (new).

**2. Cost — bound `OnlineJoinerStreamingJob` Parquet small-files.**
Today each micro-batch does `write.mode(append).partitionBy(date).parquet(...)`,
emitting up to `shuffle.partitions` files **per batch per date** → small-files blowup.
Coalesce/repartition to an env-tunable file count before the Parquet write
(`ONLINE_JOINER_OUTPUT_FILES`, default e.g. 1). Kafka write path unchanged.
- *Acceptance:* a batch writes ≤ `ONLINE_JOINER_OUTPUT_FILES` files per date partition;
  output rows/schema identical.
- *Verify:* unit test asserts file count on a temp dir; `sbt test` green.
- *Files:* `OnlineJoinerStreamingJob.scala`, its spec.

**3. Cost/throughput — enable Adaptive Query Execution.**
Turn on `spark.sql.adaptive.enabled` (and `coalescePartitions`) in `SparkSessions`,
env-overridable. Lets Spark right-size shuffle partitions instead of the fixed 4–8.
- *Acceptance:* AQE configs present and overridable; `sbt test` green.
- *Files:* `SparkSessions.scala`.

**4. Operability — per-batch streaming metrics.**
Add a `StreamingQueryListener` (registered via a small `SparkSessions`/util helper)
that logs `numInputRows`, `processedRowsPerSecond`, and `batchDuration` per batch
for each query. Off-by-default-noise avoided via existing logger levels.
- *Acceptance:* running any job logs a one-line metrics record per micro-batch.
- *Files:* `util/` (new listener), the 3 job `main`s (register it).

**5. Operability — run all three jobs from one command.**
Add `run-data-pipeline.sh` that launches the 3 jobs (each its own `spark-submit`
process + distinct `SPARK_CHECKPOINT_LOCATION`), prints PIDs, and traps
SIGINT/SIGTERM to shut them down together.
- *Acceptance:* one command starts all 3 with separate checkpoints; Ctrl-C stops all.
- *Files:* `run-data-pipeline.sh` (new); README note (separate docs PR).

### Phase 2 — semantics-changing (ASK FIRST, not in default scope)

**6. Correctness — event dedup + late-data handling.**
Optional: `event_id`-based dedup with a watermark, and explicit corrupt-record
accounting (count/log `from_json`-null rows instead of silently dropping). Changes
streaming state + checkpoint layout, so gated behind approval.
- *Acceptance:* duplicates within watermark are dropped; corrupt records counted.

## Code Style

Match the existing jobs:

```scala
// env-tunable knob with a safe default + a short comment on the perf rationale
val outputFiles = math.max(1, Env.int("ONLINE_JOINER_OUTPUT_FILES", 1))
// ... coalesce keeps per-batch Parquet file count bounded (avoids small-files blowup)
samples.coalesce(outputFiles).write.mode("append").partitionBy("date")...
```

- Config via the `Env` util; every new knob has a default and is documented at the call site.
- Comments explain *why* (the non-obvious perf choice), not *what*.
- No new dependencies without approval.

## Testing Strategy

- ScalaTest under `src/test/scala`, using the existing `SparkTestSupport` trait.
- **Behavior-preserving items (1–5) must leave the existing job specs passing
  unchanged** — that is the regression gate.
- New units: `EventParsingSpec` (parser), a file-count assertion for item 2.
- No cluster/load benchmark in scope; throughput is validated qualitatively via the
  new per-batch metrics (item 4) and the bounded file count (item 2).

## Boundaries

- **Always:** keep output schemas, topic names, and Redis keys stable; gate every new
  knob with a safe default; run `sbt test` before each commit; one work item per commit.
- **Ask first:** anything in Phase 2 (event semantics, checkpoint-layout changes),
  adding a dependency, changing trigger/checkpoint defaults.
- **Never:** rename `recsys_events` / `training_samples` / `training_experiences` /
  `global:item_popularity`; delete or weaken existing tests; commit secrets.

## Success Criteria (testable)

- [ ] `sbt test` green before and after every work item.
- [ ] No duplicated event-schema `from_json` literal across the 3 jobs.
- [ ] `OnlineJoinerStreamingJob` writes ≤ `ONLINE_JOINER_OUTPUT_FILES` files per date per batch.
- [ ] AQE enabled (and overridable) in `SparkSessions`.
- [ ] Each job logs per-batch `numInputRows` / `rps` / `batchDuration`.
- [ ] `run-data-pipeline.sh` starts all 3 jobs (distinct checkpoints) and stops them cleanly.

## Open Questions

1. **Spec location** — using `docs/superpowers/specs/` since root `SPEC.md` is taken. OK?
2. **Phase 2** — in scope now, or defer the semantics-changing dedup/late-data work?
3. **Throughput target** — is "no regression + bounded files + visible metrics" sufficient,
   or do you want a concrete events/sec benchmark (would add a load harness)?
