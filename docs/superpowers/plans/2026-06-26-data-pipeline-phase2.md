# Data Pipeline Optimization — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add `event_id` dedup with a watermark to both `recsys_events` consumers, and per-batch corrupt-record accounting — the approved Phase 2 of `docs/superpowers/specs/2026-06-26-data-pipeline-optimization.md`.

**Architecture:** Each consumer's streaming DataFrame gains `.withWatermark(event_time, delay).dropDuplicatesWithinWatermark("event_id")` (Spark 3.5) before its `foreachBatch`. Corrupt records (rows where `from_json` produced a null struct) are counted via `Dataset.observe(...)` and surfaced through the existing `BatchMetricsListener`. Behavior of `buildTrainingSamples` / popularity counting is otherwise unchanged.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1 Structured Streaming, ScalaTest 3.2.18.

## Global Constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18. No new runtime dependencies.
- **Never** rename topics `recsys_events` / `training_samples` / `training_experiences` or the Redis key `global:item_popularity`.
- Output schemas of `training_samples` / `training_experiences` / Parquet UNCHANGED (dedup removes duplicate input rows; it must not add or rename output columns — drop the helper `event_time` column before it can reach an output).
- New knob: `EVENT_WATERMARK_DELAY` (default `"10 minutes"`), read via `sys.env.getOrElse`.
- Build/test dir: `recsys-pipeline/services/spark-streaming-job`. Build `sbt assembly`, test `sbt test`. One commit per task.
- Streaming dedup makes the queries STATEFUL → existing checkpoints are incompatible. This is a documented operational change (Task 4), not a code concern.

---

### Task 1: `dropDuplicatesWithinWatermark` in `UserEventStreamingJob`

Splits `parseEvents` into reusable `fromJson` + `normalize`, adds a dedup helper to `EventParsing`, and a testable `dedupedClicks` transform used by `main`. A `MemoryStream` test proves duplicates are dropped across batches.

**Files:**
- Modify: `src/main/scala/com/demo/event/EventParsing.scala` (add `dedupeWithinWatermark`)
- Modify: `src/main/scala/com/demo/task/UserEventStreamingJob.scala`
- Test: `src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala` (add streaming case)

**Interfaces:**
- Produces:
  - `EventParsing.dedupeWithinWatermark(df: DataFrame, eventTime: Column, delay: String): DataFrame`
  - `UserEventStreamingJob.normalize(df: DataFrame): DataFrame` (the coalesce+filter tail of the old `parseEvents`)
  - `UserEventStreamingJob.dedupedClicks(raw: DataFrame, watermarkDelay: String): DataFrame`
- `parseEvents` is kept (`= normalize(EventParsing.fromJson(raw, EventSchemas.userEvent))`) so existing batch tests still pass.

- [ ] **Step 1: Write the failing streaming test**

Add to `UserEventStreamingJobSpec.scala` (add imports `org.apache.spark.sql.execution.streaming.MemoryStream`):

```scala
it should "drop duplicate event_id within the watermark across micro-batches" in {
  val s = spark; import s.implicits._
  implicit val sqlCtx = s.sqlContext
  val input = MemoryStream[String]
  val deduped = UserEventStreamingJob.dedupedClicks(input.toDF(), "10 minutes")
  val q = deduped.writeStream.format("memory").queryName("ue_out").outputMode("append").start()
  try {
    val e = """{"event_id":"e1","user_id":"u1","item_id":"i1","event_type":"click","timestamp_ms":1718400000000}"""
    input.addData(e); q.processAllAvailable()
    input.addData(e); q.processAllAvailable()   // identical event_id again
    s.table("ue_out").count() shouldBe 1
  } finally q.stop()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.task.UserEventStreamingJobSpec"`
Expected: FAIL — `value dedupedClicks is not a member of object UserEventStreamingJob`.

- [ ] **Step 3: Add the dedup helper**

In `EventParsing.scala` add (imports: `org.apache.spark.sql.Column`, `org.apache.spark.sql.functions._`):

```scala
/** Watermarked event-id de-duplication. Adds a transient `event_time` column from
  * `eventTime`, sets the watermark, and drops duplicate `event_id`s seen within it. */
def dedupeWithinWatermark(df: DataFrame, eventTime: Column, delay: String): DataFrame =
  df.withColumn("event_time", eventTime)
    .withWatermark("event_time", delay)
    .dropDuplicatesWithinWatermark("event_id")
```

- [ ] **Step 4: Refactor `UserEventStreamingJob` and add `dedupedClicks`**

Split `parseEvents` and add the transforms:

```scala
import com.demo.event.{EventParsing, EventSchemas}
import org.apache.spark.sql.functions._

// the coalesce + null-filter tail of the old parseEvents
def normalize(df: DataFrame): DataFrame =
  df.withColumn("timestamp_ms", coalesce(col("timestamp_ms"), col("timestamp") * 1000L))
    .filter(col("user_id").isNotNull && col("item_id").isNotNull)

def parseEvents(raw: DataFrame): DataFrame =
  normalize(EventParsing.fromJson(raw, EventSchemas.userEvent))

/** Parse → watermark-dedup on event_id → keep clicks. event_time derived from millis. */
def dedupedClicks(raw: DataFrame, watermarkDelay: String): DataFrame = {
  val valid = parseEvents(raw)
  // event_time from millis → seconds → timestamp, matching the codebase idiom
  EventParsing.dedupeWithinWatermark(valid, to_timestamp(from_unixtime(col("timestamp_ms") / 1000)), watermarkDelay)
    .filter(col("event_type") === "click")
}
```

In `main`, replace the `val parsed = parseEvents(df).filter(col("event_type") === "click")` line with:

```scala
val watermarkDelay = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes")
val parsed = dedupedClicks(df, watermarkDelay)
```

(The `parsed.writeStream.foreachBatch { ... }` popularity code below is unchanged — it never reads `event_time`, and `ZINCRBY` is the same.)

- [ ] **Step 5: Run tests to verify pass**

Run: `sbt "testOnly com.demo.task.UserEventStreamingJobSpec"`
Expected: PASS (the two existing `parseEvents` cases + the new streaming dedup case).

- [ ] **Step 6: Full suite (no regressions)**

Run: `sbt test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/demo/event/EventParsing.scala src/main/scala/com/demo/task/UserEventStreamingJob.scala src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala
git commit -m "feat: watermarked event_id dedup in UserEventStreamingJob"
```

---

### Task 2: `dropDuplicatesWithinWatermark` in `OnlineJoinerStreamingJob`

Adds `event_id` to the joiner schema, moves parse+dedup onto the streaming DataFrame (so dedup is cross-batch), and calls `buildTrainingSamples` on the deduped batch inside `foreachBatch`.

**Files:**
- Modify: `src/main/scala/com/demo/event/EventSchemas.scala` (add `event_id` to `joiner`)
- Modify: `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
- Test: `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala` (add streaming case)

**Interfaces:**
- Consumes: `EventParsing.dedupeWithinWatermark` (Task 1).
- Produces: `OnlineJoinerStreamingJob.dedupedEvents(raw: DataFrame, watermarkDelay: String): DataFrame` (= `parseEvents` → watermark dedup on `event_id`, event_time from the seconds `timestamp`).

- [ ] **Step 1: Write the failing streaming test**

Add to `OnlineJoinerStreamingJobSpec.scala` (import `MemoryStream`; the spec already has its own `spark`):

```scala
it should "drop duplicate event_id within the watermark across micro-batches" in {
  val s = spark; import s.implicits._
  implicit val sqlCtx = s.sqlContext
  val input = MemoryStream[String]
  val deduped = OnlineJoinerStreamingJob.dedupedEvents(input.toDF(), "10 minutes")
  val q = deduped.writeStream.format("memory").queryName("oj_out").outputMode("append").start()
  try {
    val e = """{"event_id":"x1","request_id":"r1","user_id":"u1","item_id":"i1","event_type":"impression","timestamp_ms":1718400000000,"position":0}"""
    input.addData(e); q.processAllAvailable()
    input.addData(e); q.processAllAvailable()
    s.table("oj_out").count() shouldBe 1
  } finally q.stop()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: FAIL — `value dedupedEvents is not a member of object OnlineJoinerStreamingJob`.

- [ ] **Step 3: Add `event_id` to the joiner schema**

In `EventSchemas.scala`, change `joiner` so `event_id` is the first field (nullable), keeping all others:

```scala
val joiner: StructType = StructType(
  (StructField("event_id", StringType, nullable = true) +:
    StructField("request_id", StringType, nullable = false) +: baseFields) ++ Seq(
    StructField("position", IntegerType, nullable = true),
    StructField("user_features", MapType(StringType, StringType), nullable = true),
    StructField("item_features", MapType(StringType, StringType), nullable = true),
    StructField("context_features", MapType(StringType, StringType), nullable = true)
  )
)
```

- [ ] **Step 4: Add `dedupedEvents` and restructure `main`**

In `OnlineJoinerStreamingJob.scala` add:

```scala
def dedupedEvents(raw: DataFrame, watermarkDelay: String): DataFrame =
  // parseEvents yields `timestamp` in seconds; derive event_time the codebase way
  EventParsing.dedupeWithinWatermark(parseEvents(raw), to_timestamp(from_unixtime(col("timestamp"))), watermarkDelay)
```

In `main`, change the pipeline so parse+dedup happen on the streaming `raw` BEFORE `foreachBatch`, and `buildTrainingSamples` runs on the already-parsed batch:

```scala
val watermarkDelay = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes")
val events = dedupedEvents(raw, watermarkDelay)

events.writeStream
  .foreachBatch { (batch: DataFrame, batchId: Long) =>
    val samples = buildTrainingSamples(batch)        // was buildTrainingSamples(parseEvents(batch))
      .withColumn("batch_id", lit(batchId))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    // ... existing Kafka write + writeParquet(...) + finally unpersist UNCHANGED ...
  }
  // ... existing checkpoint + trigger + start + awaitTermination UNCHANGED ...
```

(`buildTrainingSamples` ignores the extra `event_id` / `event_time` columns — it selects explicit columns. Do NOT change `buildTrainingSamples`.)

- [ ] **Step 5: Run tests to verify pass**

Run: `sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: PASS (existing `buildTrainingSamples` cases + the new streaming dedup case).

- [ ] **Step 6: Full suite (no regressions)**

Run: `sbt test`
Expected: PASS — confirms Task 1's `EventParsingSpec` etc. still green with the joiner schema change.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/demo/event/EventSchemas.scala src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "feat: watermarked event_id dedup in OnlineJoinerStreamingJob"
```

---

### Task 3: Corrupt-record accounting via `observe` + metrics listener

Counts rows where `from_json` failed (struct null → `user_id` null) per batch using `Dataset.observe`, and logs it through `BatchMetricsListener`.

**Files:**
- Modify: `src/main/scala/com/demo/util/BatchMetricsListener.scala`
- Modify: `src/main/scala/com/demo/task/UserEventStreamingJob.scala`, `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala` (insert the `observe`)
- Test: `src/test/scala/com/demo/util/BatchMetricsListenerSpec.scala` (extend `format` test)

**Interfaces:**
- Produces: `BatchMetricsListener.format(name, numInputRows, rowsPerSecond, durationMs, corrupt)` — `corrupt: Long` added as the last param.
- `EventParsing.observeIngest(df: DataFrame): DataFrame` — passthrough that attaches an `"ingest"` observed metric with a `corrupt` count.

- [ ] **Step 1: Extend the failing formatter test**

Replace the assertion block in `BatchMetricsListenerSpec.scala`'s test with:

```scala
"BatchMetricsListener.format" should "render the per-batch metrics line incl. corrupt count" in {
  val line = BatchMetricsListener.format("UserEventStreamingJob", 5000L, 1234.5, 405L, 7L)
  line should include ("UserEventStreamingJob")
  line should include ("rows=5000")
  line should include ("rps=1234.5")
  line should include ("batchMs=405")
  line should include ("corrupt=7")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.util.BatchMetricsListenerSpec"`
Expected: FAIL — too many arguments / missing `corrupt=` substring.

- [ ] **Step 3: Update `BatchMetricsListener`**

In `BatchMetricsListener.scala`:

```scala
def format(name: String, numInputRows: Long, rowsPerSecond: Double, durationMs: Long, corrupt: Long): String =
  s"[batch-metrics] query=$name rows=$numInputRows rps=$rowsPerSecond batchMs=$durationMs corrupt=$corrupt"
```

In `onQueryProgress`, read the observed metric (null-safe; default 0):

```scala
val batchMs = Option(p.durationMs.get("triggerExecution")).map(_.toLong).getOrElse(0L)
val corrupt = Option(p.observedMetrics.get("ingest")).map(_.getAs[Long]("corrupt")).getOrElse(0L)
log.info(format(p.name, p.numInputRows, p.processedRowsPerSecond, batchMs, corrupt))
```

Add an `observeIngest` helper:

```scala
// in EventParsing.scala
def observeIngest(df: DataFrame): DataFrame =
  df.observe("ingest", sum(when(col("user_id").isNull, 1L).otherwise(0L)).as("corrupt"))
```

- [ ] **Step 4: Wire `observeIngest` into both jobs**

In `UserEventStreamingJob.dedupedClicks`, observe right after `fromJson` (before the null filter drops corrupt rows):

```scala
def dedupedClicks(raw: DataFrame, watermarkDelay: String): DataFrame = {
  val parsedAll = EventParsing.observeIngest(EventParsing.fromJson(raw, EventSchemas.userEvent))
  val valid = normalize(parsedAll)
  EventParsing.dedupeWithinWatermark(valid, to_timestamp(from_unixtime(col("timestamp_ms") / 1000)), watermarkDelay)
    .filter(col("event_type") === "click")
}
```

In `OnlineJoinerStreamingJob`, do the same: make `parseEvents` start from `EventParsing.observeIngest(EventParsing.fromJson(rawKafka, EventSchemas.joiner))` (insert `observeIngest` between `fromJson` and the existing `.withColumn("timestamp", ...)`).

> Note: `observe` only emits when the query actually runs; the unit tests cover `format`. Do not assert observed metrics in unit tests.

- [ ] **Step 5: Run tests to verify pass**

Run: `sbt test`
Expected: PASS — `BatchMetricsListenerSpec` green, all streaming/batch specs still green.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/demo/util/BatchMetricsListener.scala src/main/scala/com/demo/event/EventParsing.scala src/main/scala/com/demo/task/UserEventStreamingJob.scala src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala src/test/scala/com/demo/util/BatchMetricsListenerSpec.scala
git commit -m "feat: per-batch corrupt-record accounting via observe + metrics listener"
```

---

### Task 4: Document the new knob + checkpoint-reset requirement

**Files:**
- Modify: `recsys-pipeline/README.md` (Data Pipeline section)

- [ ] **Step 1: Add an operational note**

In `recsys-pipeline/README.md`, under the data-pipeline / configuration area, add:

```markdown
**Event de-duplication (Phase 2):** `UserEventStreamingJob` and
`OnlineJoinerStreamingJob` drop duplicate `event_id`s within
`EVENT_WATERMARK_DELAY` (default `10 minutes`). Because this makes the queries
stateful, **existing checkpoints are incompatible** — on first deploy of this
change, point `SPARK_CHECKPOINT_LOCATION` at a fresh directory. Per-batch
`corrupt=<n>` counts are logged by the metrics listener.
```

- [ ] **Step 2: Commit**

```bash
git add recsys-pipeline/README.md
git commit -m "docs: note EVENT_WATERMARK_DELAY + checkpoint reset for Phase 2 dedup"
```

---

## Final verification

- [ ] `cd recsys-pipeline/services/spark-streaming-job && sbt test` → all PASS (incl. the two new `MemoryStream` dedup tests).
- [ ] `sbt assembly` → jar builds.
- [ ] `git grep -n "recsys_events\|training_samples\|training_experiences\|global:item_popularity"` → only pre-existing references; no topic/key renamed.
- [ ] Confirm no output column named `event_time` leaks into `training_samples` (it is only on the dedup stream; `buildTrainingSamples` selects explicit columns).
