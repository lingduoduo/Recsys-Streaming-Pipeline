# Data Pipeline Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Optimize the streaming data pipeline (the 3 `recsys_events`/`training_samples` jobs) for code quality, cost, throughput, and operability — without changing any topic, output schema, or Redis key.

**Architecture:** Extract the duplicated Kafka-JSON parse mechanic into a shared `com.demo.event` module; bound `OnlineJoinerStreamingJob`'s per-batch Parquet file count; enable Adaptive Query Execution in the shared session factory; add a per-batch `StreamingQueryListener` for visibility; add a `run-data-pipeline.sh` that runs all three jobs together. Every change is guarded by the existing ScalaTest specs, which must stay green.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1 Structured Streaming, Jedis 5.1.5, sbt + sbt-assembly, JDK 17, ScalaTest 3.2.18.

## Global Constraints

- Scala `2.12.18`, Spark `3.5.1`, ScalaTest `3.2.18`. No new runtime dependencies.
- **Never** rename topics `recsys_events` / `training_samples` / `training_experiences`, or the Redis key `global:item_popularity`.
- **Never** change the output schema of `training_samples`, `training_experiences`, or the Parquet output.
- Every new tuning knob is read via `com.demo.util.Env`, has a safe default, and is documented at the call site.
- All work runs from `recsys-pipeline/services/spark-streaming-job/` unless a path says otherwise.
- Build: `sbt assembly`. Test: `sbt test`. One work item per commit.

---

### Task 1: Shared event-parsing module (`com.demo.event`)

Creates the shared parse mechanic and the two event schemas. No job touches it yet — this task is independently testable: the module compiles and its spec passes.

**Files:**
- Create: `src/main/scala/com/demo/event/EventSchemas.scala`
- Create: `src/main/scala/com/demo/event/EventParsing.scala`
- Test: `src/test/scala/com/demo/event/EventParsingSpec.scala`

**Interfaces:**
- Produces:
  - `com.demo.event.EventSchemas.userEvent: org.apache.spark.sql.types.StructType`
  - `com.demo.event.EventSchemas.joiner: org.apache.spark.sql.types.StructType`
  - `com.demo.event.EventParsing.fromJson(rawKafka: DataFrame, schema: StructType): DataFrame`
    — casts the Kafka `value` column to a JSON string, applies `from_json`, and flattens to top-level columns.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/event/EventParsingSpec.scala`:

```scala
package com.demo.event

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventParsingSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "EventParsing.fromJson" should "flatten a userEvent JSON value into columns" in {
    val s = spark; import s.implicits._
    val raw = Seq(
      """{"event_id":"e1","user_id":"user_5","item_id":"movie_3","event_type":"click","timestamp_ms":1718400000000}"""
    ).toDF("value")

    val out = EventParsing.fromJson(raw, EventSchemas.userEvent)

    out.columns should contain allOf ("event_id", "user_id", "item_id", "event_type", "timestamp_ms", "timestamp")
    val row = out.collect().head
    row.getAs[String]("user_id") shouldBe "user_5"
    row.getAs[String]("item_id") shouldBe "movie_3"
    row.getAs[Long]("timestamp_ms") shouldBe 1718400000000L
  }

  it should "expose joiner-specific columns (request_id, position, feature maps)" in {
    val s = spark; import s.implicits._
    val raw = Seq(
      """{"request_id":"req_1","user_id":"u1","item_id":"i1","event_type":"impression","timestamp":100,"position":2,"user_features":{"tier":"gold"}}"""
    ).toDF("value")

    val out = EventParsing.fromJson(raw, EventSchemas.joiner)

    out.columns should contain allOf ("request_id", "position", "user_features", "item_features", "context_features")
    out.collect().head.getAs[Int]("position") shouldBe 2
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.event.EventParsingSpec"`
Expected: FAIL — `object EventParsing is not a member of package com.demo.event` (does not compile).

- [ ] **Step 3: Write the schemas**

Create `src/main/scala/com/demo/event/EventSchemas.scala`:

```scala
package com.demo.event

import org.apache.spark.sql.types._

/** Schemas for the unified `recsys_events` payloads, shared by the jobs that
  * consume them. `timestamp_ms` (millis) is primary; `timestamp` (seconds) is
  * legacy compatibility. */
object EventSchemas {

  /** Fields present on every recsys_events record. */
  val baseFields: Seq[StructField] = Seq(
    StructField("user_id", StringType, nullable = false),
    StructField("item_id", StringType, nullable = false),
    StructField("event_type", StringType, nullable = false),
    StructField("timestamp_ms", LongType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  )

  /** UserEventStreamingJob view: adds event_id. Order matches the original schema. */
  val userEvent: StructType =
    StructType(StructField("event_id", StringType, nullable = true) +: baseFields)

  /** OnlineJoinerStreamingJob view: adds request_id, position, and feature maps. */
  val joiner: StructType = StructType(
    (StructField("request_id", StringType, nullable = false) +: baseFields) ++ Seq(
      StructField("position", IntegerType, nullable = true),
      StructField("user_features", MapType(StringType, StringType), nullable = true),
      StructField("item_features", MapType(StringType, StringType), nullable = true),
      StructField("context_features", MapType(StringType, StringType), nullable = true)
    )
  )
}
```

- [ ] **Step 4: Write the parse mechanic**

Create `src/main/scala/com/demo/event/EventParsing.scala`:

```scala
package com.demo.event

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/** The Kafka-JSON parse mechanic shared by every streaming job:
  * cast the `value` column to a JSON string, apply `from_json`, flatten. */
object EventParsing {

  def fromJson(rawKafka: DataFrame, schema: StructType): DataFrame =
    rawKafka
      .selectExpr("CAST(value AS STRING) AS value")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `sbt "testOnly com.demo.event.EventParsingSpec"`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/demo/event src/test/scala/com/demo/event
git commit -m "feat: shared com.demo.event parse module (EventSchemas + EventParsing)"
```

---

### Task 2: Adopt the shared parser in the three jobs

Replaces the per-job schema literals and `from_json` mechanics with `com.demo.event`, preserving each job's distinct post-parse logic (UserEvent keeps millis `timestamp_ms`; OnlineJoiner keeps its seconds `timestamp`; ExperienceCollector keeps its `TrainingSampleSchema`). The existing job specs are the regression gate — they must pass unchanged.

**Files:**
- Modify: `src/main/scala/com/demo/task/UserEventStreamingJob.scala`
- Modify: `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
- Modify: `src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala`

**Interfaces:**
- Consumes: `EventSchemas.userEvent`, `EventSchemas.joiner`, `EventParsing.fromJson` (Task 1).
- Produces: unchanged public methods `UserEventStreamingJob.parseEvents`, `OnlineJoinerStreamingJob.parseEvents`/`buildTrainingSamples`, `ExperienceCollectorStreamingJob.parseSamples`/`buildSlates` — same signatures, same outputs.

- [ ] **Step 1: Run the existing specs to confirm green baseline**

Run: `sbt "testOnly com.demo.task.UserEventStreamingJobSpec com.demo.process.OnlineJoinerStreamingJobSpec com.demo.process.ExperienceCollectorStreamingJobSpec"`
Expected: PASS (baseline before refactor).

- [ ] **Step 2: Refactor `UserEventStreamingJob`**

In `UserEventStreamingJob.scala`: delete the local `schema` val (lines ~33-40) and import the shared module. Replace `parseEvents` to delegate the mechanic:

```scala
import com.demo.event.{EventParsing, EventSchemas}
// ...
// (the private `schema` val is deleted)

def parseEvents(raw: DataFrame): DataFrame =
  EventParsing.fromJson(raw, EventSchemas.userEvent)
    .withColumn(
      "timestamp_ms",
      coalesce(col("timestamp_ms"), col("timestamp") * 1000L)
    )
    .filter(col("user_id").isNotNull && col("item_id").isNotNull)
```

In `main`, change the read so `parseEvents` receives the raw Kafka frame directly (the cast now lives in `fromJson`):

```scala
val df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", kafkaTopic)
  .option("kafka.group.id", "training-user-history")
  .option("startingOffsets", "earliest")
  .option("failOnDataLoss", "false")
  .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
  .load()                       // raw `value` (binary) — fromJson casts it
```

(Remove the trailing `.selectExpr("CAST(value AS STRING) as value")`.)

- [ ] **Step 3: Refactor `OnlineJoinerStreamingJob`**

Delete the local `EventSchema` val (lines ~12-23), import the module, and rewrite `parseEvents` to use the shared mechanic + shared schema while keeping the seconds normalisation and null filter exactly:

```scala
import com.demo.event.{EventParsing, EventSchemas}
// ...
// (the local `EventSchema` val is deleted)

def parseEvents(rawKafka: DataFrame): DataFrame =
  EventParsing.fromJson(rawKafka, EventSchemas.joiner)
    .withColumn("timestamp",
      coalesce(col("timestamp_ms") / 1000L, col("timestamp")))
    .drop("timestamp_ms")
    .filter(
      col("request_id").isNotNull &&
        col("user_id").isNotNull &&
        col("item_id").isNotNull &&
        col("event_type").isNotNull &&
        col("timestamp").isNotNull
    )
```

`buildTrainingSamples` is unchanged.

- [ ] **Step 4: Refactor `ExperienceCollectorStreamingJob`**

Keep `TrainingSampleSchema` where it is (it is a training-sample schema, not an event schema), but route its parse through the shared mechanic:

```scala
import com.demo.event.EventParsing
// ...
def parseSamples(rawKafka: DataFrame): DataFrame =
  EventParsing.fromJson(rawKafka, TrainingSampleSchema)
    .filter(
      col("request_id").isNotNull &&
        col("user_id").isNotNull &&
        col("item_id").isNotNull
    )
```

- [ ] **Step 5: Run the full suite to verify behavior is preserved**

Run: `sbt test`
Expected: PASS — all existing specs (UserEvent, OnlineJoiner, ExperienceCollector, plus the rest) unchanged and green.

- [ ] **Step 6: Verify no duplicated event-schema literal remains**

Run: `grep -rn "StructField(\"user_id\"" src/main/scala/com/demo/task src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
Expected: no matches (the only `user_id` `StructField` now lives in `com/demo/event/EventSchemas.scala`).

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/demo
git commit -m "refactor: route the 3 streaming jobs through com.demo.event parser"
```

---

### Task 3: Bound `OnlineJoinerStreamingJob` Parquet file count

Each micro-batch currently appends a `partitionBy(date)` Parquet write, emitting up to `shuffle.partitions` files per date per batch (small-files blowup). Add an env-tunable coalesce, extracted into a testable function.

**Files:**
- Modify: `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
- Test: `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala` (add a case)

**Interfaces:**
- Produces: `OnlineJoinerStreamingJob.writeParquet(samples: DataFrame, path: String, outputFiles: Int): Unit`
  — coalesces to `outputFiles` partitions, then `append`/`partitionBy("date")`/Parquet.

- [ ] **Step 1: Write the failing test**

Add to `OnlineJoinerStreamingJobSpec.scala`:

```scala
it should "write at most ONLINE_JOINER_OUTPUT_FILES parquet files per date partition" in {
  val sparkSession = spark
  import sparkSession.implicits._
  import java.nio.file.Files

  val dir = Files.createTempDirectory("joiner-parquet").toFile
  val out = new java.io.File(dir, "samples").getAbsolutePath

  val samples = Seq(
    ("s1", java.sql.Date.valueOf("2026-06-26")),
    ("s2", java.sql.Date.valueOf("2026-06-26")),
    ("s3", java.sql.Date.valueOf("2026-06-26"))
  ).toDF("sample_id", "date")

  OnlineJoinerStreamingJob.writeParquet(samples, out, outputFiles = 1)

  val partDir = new java.io.File(out, "date=2026-06-26")
  val parquetFiles = partDir.listFiles().filter(_.getName.endsWith(".parquet"))
  parquetFiles.length shouldBe 1
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: FAIL — `value writeParquet is not a member of object OnlineJoinerStreamingJob`.

- [ ] **Step 3: Extract `writeParquet` and add the knob**

In `OnlineJoinerStreamingJob.scala`, add the function:

```scala
def writeParquet(samples: DataFrame, path: String, outputFiles: Int): Unit =
  samples
    .coalesce(math.max(1, outputFiles))   // bound per-batch file count (avoid small-files blowup)
    .write
    .mode("append")
    .partitionBy("date")
    .format("parquet")
    .save(path)
```

Read the knob in `main` (next to the other env reads):

```scala
val outputFiles = math.max(1, com.demo.util.Env.int("ONLINE_JOINER_OUTPUT_FILES", 1))
```

Replace the inline Parquet write inside `foreachBatch` with a call:

```scala
writeParquet(samples.withColumn("date", to_date(col("impression_time"))), outputPath, outputFiles)
```

(Delete the old `samples.withColumn("date"...)...save(outputPath)` block.)

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: PASS (including the new case and the existing ones).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "perf: bound OnlineJoiner per-batch parquet files via coalesce knob"
```

---

### Task 4: Enable Adaptive Query Execution in `SparkSessions`

AQE lets Spark right-size shuffle partitions at runtime instead of the fixed value. Make the config set a testable pure map, applied in `create`.

**Files:**
- Modify: `src/main/scala/com/demo/util/SparkSessions.scala`
- Test: `src/test/scala/com/demo/util/SparkSessionsSpec.scala` (create)

**Interfaces:**
- Produces: `SparkSessions.adaptiveConfigs: Map[String, String]` — the AQE settings applied by `create`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/util/SparkSessionsSpec.scala`:

```scala
package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSessionsSpec extends AnyFlatSpec with Matchers {

  "SparkSessions.adaptiveConfigs" should "enable AQE and partition coalescing" in {
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.enabled") shouldBe "true"
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.coalescePartitions.enabled") shouldBe "true"
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.util.SparkSessionsSpec"`
Expected: FAIL — `value adaptiveConfigs is not a member of object SparkSessions`.

- [ ] **Step 3: Add the config map and apply it**

Rewrite `SparkSessions.scala`:

```scala
package com.demo.util

import org.apache.spark.sql.SparkSession

object SparkSessions {

  /** AQE settings applied to every session; env-overridable per key. */
  val adaptiveConfigs: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true"
  )

  def create(defaultAppName: String, defaultShufflePartitions: Int = 8): SparkSession = {
    val builder = SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", defaultAppName))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config(
        "spark.sql.shuffle.partitions",
        sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", defaultShufflePartitions.toString)
      )
    adaptiveConfigs.foreach { case (k, v) => builder.config(k, sys.env.getOrElse(envKeyFor(k), v)) }
    builder.getOrCreate()
  }

  // spark.sql.adaptive.enabled -> SPARK_SQL_ADAPTIVE_ENABLED
  private def envKeyFor(confKey: String): String =
    confKey.toUpperCase.replace('.', '_')
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly com.demo.util.SparkSessionsSpec"`
Expected: PASS.

- [ ] **Step 5: Run full suite (no regressions)**

Run: `sbt test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/demo/util/SparkSessions.scala src/test/scala/com/demo/util/SparkSessionsSpec.scala
git commit -m "perf: enable Adaptive Query Execution in shared SparkSessions factory"
```

---

### Task 5: Per-batch streaming metrics listener

Adds a `StreamingQueryListener` that logs `numInputRows`, `processedRowsPerSecond`, and batch duration per micro-batch. The log line is built by a pure formatter so it can be unit-tested without constructing Spark event objects.

**Files:**
- Create: `src/main/scala/com/demo/util/BatchMetricsListener.scala`
- Test: `src/test/scala/com/demo/util/BatchMetricsListenerSpec.scala`
- Modify: `src/main/scala/com/demo/task/UserEventStreamingJob.scala`, `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`, `src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala` (register the listener)

**Interfaces:**
- Produces:
  - `BatchMetricsListener.format(name: String, numInputRows: Long, rowsPerSecond: Double, durationMs: Long): String`
  - `BatchMetricsListener.register(spark: SparkSession): Unit`

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/util/BatchMetricsListenerSpec.scala`:

```scala
package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BatchMetricsListenerSpec extends AnyFlatSpec with Matchers {

  "BatchMetricsListener.format" should "render the per-batch metrics line" in {
    val line = BatchMetricsListener.format("UserEventStreamingJob", 5000L, 1234.5, 405L)
    line should include ("UserEventStreamingJob")
    line should include ("rows=5000")
    line should include ("rps=1234.5")
    line should include ("batchMs=405")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `sbt "testOnly com.demo.util.BatchMetricsListenerSpec"`
Expected: FAIL — `object BatchMetricsListener is not a member of package com.demo.util`.

- [ ] **Step 3: Implement the listener**

Create `src/main/scala/com/demo/util/BatchMetricsListener.scala`:

```scala
package com.demo.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.slf4j.LoggerFactory

/** Logs one line per micro-batch: input rows, throughput, and batch duration. */
object BatchMetricsListener {
  private val log = LoggerFactory.getLogger(getClass)

  def format(name: String, numInputRows: Long, rowsPerSecond: Double, durationMs: Long): String =
    s"[batch-metrics] query=$name rows=$numInputRows rps=$rowsPerSecond batchMs=$durationMs"

  def register(spark: SparkSession): Unit =
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(e: StreamingQueryListener.QueryStartedEvent): Unit = ()
      override def onQueryTerminated(e: StreamingQueryListener.QueryTerminatedEvent): Unit = ()
      override def onQueryProgress(e: StreamingQueryListener.QueryProgressEvent): Unit = {
        val p = e.progress
        // durationMs is a Map of phase -> millis; "triggerExecution" is the batch wall time.
        val batchMs = Option(p.durationMs.get("triggerExecution")).map(_.toLong).getOrElse(0L)
        log.info(format(p.name, p.numInputRows, p.processedRowsPerSecond, batchMs))
      }
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `sbt "testOnly com.demo.util.BatchMetricsListenerSpec"`
Expected: PASS.

- [ ] **Step 5: Register the listener in each job**

In each of the three job `main`s, immediately after the `SparkSession` is obtained (e.g. after `val spark = SparkSessions.create(...)`, and for `UserEventStreamingJob` after the `spark` lazy val is first referenced — add `BatchMetricsListener.register(spark)` before `.start()`):

```scala
import com.demo.util.BatchMetricsListener
// ...
BatchMetricsListener.register(spark)
```

- [ ] **Step 6: Run full suite (no regressions)**

Run: `sbt test`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/demo/util/BatchMetricsListener.scala src/test/scala/com/demo/util/BatchMetricsListenerSpec.scala src/main/scala/com/demo
git commit -m "feat: per-batch streaming metrics listener on the 3 data-pipeline jobs"
```

---

### Task 6: `run-data-pipeline.sh` — run all three jobs together

One command launches the three jobs as separate `spark-submit` processes with distinct checkpoints, prints PIDs, and shuts them down together on Ctrl-C.

**Files:**
- Create: `recsys-pipeline/run-data-pipeline.sh`
- Test: `recsys-pipeline/integration-tests/test_run_data_pipeline.sh`

**Interfaces:**
- Consumes: `run-streaming-job.sh` (one process per `SPARK_MAIN_CLASS`).
- Produces: `run-data-pipeline.sh` launching classes `com.demo.task.UserEventStreamingJob`, `com.demo.process.OnlineJoinerStreamingJob`, `com.demo.process.ExperienceCollectorStreamingJob`.

- [ ] **Step 1: Write the failing test**

Create `recsys-pipeline/integration-tests/test_run_data_pipeline.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."   # recsys-pipeline/

# Stub run-streaming-job.sh on PATH: record the class it would launch, then exit.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cat > "$tmp/run-streaming-job.sh" <<'STUB'
#!/usr/bin/env bash
echo "launched ${SPARK_MAIN_CLASS}" >> "$RECORD"
STUB
chmod +x "$tmp/run-streaming-job.sh"

export RECORD="$tmp/record.txt"
: > "$RECORD"

# DRY_RUN makes the script use the stubbed launcher and not wait/trap.
DRY_RUN=1 RUN_STREAMING_JOB="$tmp/run-streaming-job.sh" bash run-data-pipeline.sh

grep -q "com.demo.task.UserEventStreamingJob" "$RECORD"          || { echo "FAIL: UserEvent not launched"; exit 1; }
grep -q "com.demo.process.OnlineJoinerStreamingJob" "$RECORD"    || { echo "FAIL: OnlineJoiner not launched"; exit 1; }
grep -q "com.demo.process.ExperienceCollectorStreamingJob" "$RECORD" || { echo "FAIL: ExperienceCollector not launched"; exit 1; }
echo "PASS"
```

Make it executable: `chmod +x recsys-pipeline/integration-tests/test_run_data_pipeline.sh`

- [ ] **Step 2: Run test to verify it fails**

Run: `bash recsys-pipeline/integration-tests/test_run_data_pipeline.sh`
Expected: FAIL — `run-data-pipeline.sh: No such file or directory`.

- [ ] **Step 3: Write the script**

Create `recsys-pipeline/run-data-pipeline.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

LAUNCHER="${RUN_STREAMING_JOB:-./run-streaming-job.sh}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark-recsys}"

CLASSES=(
  "com.demo.task.UserEventStreamingJob:user-event-streaming-job"
  "com.demo.process.OnlineJoinerStreamingJob:online-joiner"
  "com.demo.process.ExperienceCollectorStreamingJob:experience-collector"
)

pids=()
for entry in "${CLASSES[@]}"; do
  class="${entry%%:*}"
  ckpt="${entry##*:}"
  echo "Starting ${class} (checkpoint ${CHECKPOINT_ROOT}/${ckpt})"
  SPARK_MAIN_CLASS="$class" \
    SPARK_CHECKPOINT_LOCATION="${CHECKPOINT_ROOT}/${ckpt}" \
    "$LAUNCHER" &
  pids+=("$!")
done

echo "Launched PIDs: ${pids[*]}"

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  wait "${pids[@]}" 2>/dev/null || true
  exit 0
fi

shutdown() { echo "Stopping ${pids[*]}"; kill "${pids[@]}" 2>/dev/null || true; }
trap shutdown INT TERM
wait
```

Make it executable: `chmod +x recsys-pipeline/run-data-pipeline.sh`

- [ ] **Step 4: Run test to verify it passes**

Run: `bash recsys-pipeline/integration-tests/test_run_data_pipeline.sh`
Expected: `PASS` — all three classes recorded.

- [ ] **Step 5: Syntax-check the script**

Run: `bash -n recsys-pipeline/run-data-pipeline.sh`
Expected: no output (valid syntax).

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/run-data-pipeline.sh recsys-pipeline/integration-tests/test_run_data_pipeline.sh
git commit -m "feat: run-data-pipeline.sh launches all 3 streaming jobs together"
```

---

## Final verification

- [ ] Run the whole suite: `cd recsys-pipeline/services/spark-streaming-job && sbt test` → all PASS.
- [ ] Build the jar: `sbt assembly` → produces `target/scala-2.12/spark-recsys-job.jar`.
- [ ] Confirm no topic/schema/Redis-key strings changed:
  `git diff master --stat` and `git grep -n "recsys_events\|training_samples\|training_experiences\|global:item_popularity"` → only pre-existing references.
