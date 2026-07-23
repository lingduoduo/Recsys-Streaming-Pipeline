# Shared Execution Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract the streaming jobs' shared connect→filter/feature→enrich→persist pipeline into a reusable `com.demo.engine` package, and prove it by refactoring `OnlineJoinerStreamingJob` onto it with no behavior change.

**Architecture:** A config-driven engine with small SAM traits — `Source` (produces a streaming DataFrame), `Stage` (streaming `DataFrame⇒DataFrame`), `BatchStage` (`(DataFrame, batchId)⇒DataFrame`), `Sink` (`write(batch, batchId)`). `ExecutionEngine.run` wires `source.read` → fold streaming stages → `foreachBatch { processBatch }`, where `processBatch` folds batch stages, persists, and writes each sink under a bounded retry. `EngineConfig.validate` fails fast before any query starts.

**Tech Stack:** Scala 2.12, Spark 3.5.x Structured Streaming, ScalaTest (`AnyFlatSpec`), `com.demo.util.Env`/`SparkSessions`.

## Global Constraints

- Module: `recsys-pipeline/services/spark-streaming-job` (sbt). All Scala paths below are relative to `src/main/scala` / `src/test/scala` in that module.
- New package: `com.demo.engine` — one small file per responsibility.
- Test pattern: `AnyFlatSpec with Matchers with BeforeAndAfterAll`, a `local[1]` SparkSession built in `beforeAll` with `spark.sql.shuffle.partitions=1` and `spark.ui.enabled=false`, stopped in `afterAll`.
- `OnlineJoinerStreamingJob` must stay behavior-identical: same env vars, same Kafka value schema, same Parquet `partitionBy("date")` layout. Its spec stays green EXCEPT the single `writeParquet` test, which relocates to `SinkSpec` because that logic moves into `ParquetSink` (documented in Task 4/Task 6).
- Out of scope: no new source/sink connectors beyond Kafka + Parquet; no other job changes; no serving changes.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- Run tests from the module dir: `cd recsys-pipeline/services/spark-streaming-job`.

## File Structure

- Create: `com/demo/engine/EngineConfig.scala` — config case class + `validate` (Task 1)
- Create: `com/demo/engine/Source.scala` — `Source` trait + `KafkaSource` (Task 2)
- Create: `com/demo/engine/Stage.scala` — `Stage` + `BatchStage` traits (Task 3)
- Create: `com/demo/engine/Sink.scala` — `Sink` trait + `KafkaSink` + `ParquetSink` (Task 4)
- Create: `com/demo/engine/ExecutionEngine.scala` — `withRetry`, `processBatch`, `run` (Task 5)
- Modify: `com/demo/process/OnlineJoinerStreamingJob.scala` — `main` runs on the engine; drop `writeParquet` + inline stream/foreachBatch (Task 6)
- Tests: `com/demo/engine/{EngineConfigSpec,SourceSpec,StageSpec,SinkSpec,ExecutionEngineSpec}.scala`; modify `com/demo/process/OnlineJoinerStreamingJobSpec.scala` (Task 6)

---

### Task 1: EngineConfig + validate

**Files:**
- Create: `com/demo/engine/EngineConfig.scala`
- Test: `com/demo/engine/EngineConfigSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `final case class EngineConfig(bootstrapServers, inputTopic, startingOffsets, groupId, maxOffsetsPerTrigger: Int, triggerInterval, checkpointLocation, watermarkDelay, sinkMaxRetries: Int)` (all `String` unless typed); `object EngineConfig { def validate(cfg: EngineConfig): Either[List[String], EngineConfig] }` accumulating ALL errors.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/EngineConfigSpec.scala`:

```scala
package com.demo.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineConfigSpec extends AnyFlatSpec with Matchers {

  private def good = EngineConfig(
    bootstrapServers = "localhost:9092", inputTopic = "in", startingOffsets = "earliest",
    groupId = "g", maxOffsetsPerTrigger = 5000, triggerInterval = "10 seconds",
    checkpointLocation = "/tmp/ck", watermarkDelay = "10 minutes", sinkMaxRetries = 0)

  "validate" should "accept a well-formed config" in {
    EngineConfig.validate(good) shouldBe Right(good)
  }

  it should "accumulate all errors for an invalid config" in {
    val bad = good.copy(inputTopic = "  ", maxOffsetsPerTrigger = 0, sinkMaxRetries = -1)
    val errs = EngineConfig.validate(bad).left.getOrElse(Nil)
    errs should have size 3
    errs.exists(_.contains("inputTopic")) shouldBe true
    errs.exists(_.contains("maxOffsetsPerTrigger")) shouldBe true
    errs.exists(_.contains("sinkMaxRetries")) shouldBe true
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.EngineConfigSpec"`
Expected: FAIL — `EngineConfig` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/engine/EngineConfig.scala`:

```scala
package com.demo.engine

final case class EngineConfig(
    bootstrapServers: String,
    inputTopic: String,
    startingOffsets: String,
    groupId: String,
    maxOffsetsPerTrigger: Int,
    triggerInterval: String,
    checkpointLocation: String,
    watermarkDelay: String,
    sinkMaxRetries: Int
)

object EngineConfig {
  def validate(cfg: EngineConfig): Either[List[String], EngineConfig] = {
    val errors = List.newBuilder[String]
    def nonBlank(name: String, v: String): Unit =
      if (v == null || v.trim.isEmpty) errors += s"$name must not be blank"
    nonBlank("bootstrapServers", cfg.bootstrapServers)
    nonBlank("inputTopic", cfg.inputTopic)
    nonBlank("checkpointLocation", cfg.checkpointLocation)
    nonBlank("triggerInterval", cfg.triggerInterval)
    nonBlank("watermarkDelay", cfg.watermarkDelay)
    if (cfg.maxOffsetsPerTrigger <= 0) errors += "maxOffsetsPerTrigger must be > 0"
    if (cfg.sinkMaxRetries < 0) errors += "sinkMaxRetries must be >= 0"
    val es = errors.result()
    if (es.isEmpty) Right(cfg) else Left(es)
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.EngineConfigSpec"`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/EngineConfig.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/EngineConfigSpec.scala
git commit -m "feat: engine EngineConfig + validate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Source trait + KafkaSource

**Files:**
- Create: `com/demo/engine/Source.scala`
- Test: `com/demo/engine/SourceSpec.scala`

**Interfaces:**
- Consumes: `EngineConfig` (Task 1).
- Produces: `trait Source { def read(spark: SparkSession, cfg: EngineConfig): DataFrame }`; `object KafkaSource extends Source` reading `readStream.format("kafka")` with `kafka.bootstrap.servers`, `subscribe`, `startingOffsets`, `kafka.group.id`, `failOnDataLoss=false`, `maxOffsetsPerTrigger`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/SourceSpec.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SourceSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("SourceSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  private def cfg = EngineConfig("localhost:9092", "in", "earliest", "g", 5000,
    "10 seconds", "/tmp/ck", "10 minutes", 0)

  "KafkaSource" should "build a streaming DataFrame carrying the kafka value column" in {
    val df = KafkaSource.read(spark, cfg)
    df.isStreaming shouldBe true
    df.columns should contain ("value")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.SourceSpec"`
Expected: FAIL — `KafkaSource` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/engine/Source.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Source {
  def read(spark: SparkSession, cfg: EngineConfig): DataFrame
}

object KafkaSource extends Source {
  def read(spark: SparkSession, cfg: EngineConfig): DataFrame =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", cfg.bootstrapServers)
      .option("subscribe", cfg.inputTopic)
      .option("startingOffsets", cfg.startingOffsets)
      .option("kafka.group.id", cfg.groupId)
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", cfg.maxOffsetsPerTrigger.toString)
      .load()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.SourceSpec"`
Expected: PASS (1 test). (The `spark-sql-kafka` connector is already on the module classpath — every streaming job uses it. If `load()` reports a missing data source, STOP and report BLOCKED: the connector isn't a test-scope dependency.)

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/Source.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/SourceSpec.scala
git commit -m "feat: engine Source trait + KafkaSource

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Stage + BatchStage traits

**Files:**
- Create: `com/demo/engine/Stage.scala`
- Test: `com/demo/engine/StageSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `trait Stage { def apply(df: DataFrame): DataFrame }` (streaming, SAM) and `trait BatchStage { def apply(df: DataFrame, batchId: Long): DataFrame }` (per-micro-batch, SAM). Both are Scala-2.12 SAM interfaces so a lambda can be assigned to them.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/StageSpec.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("StageSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "a Stage lambda" should "transform a DataFrame" in {
    val s = spark; import s.implicits._
    val stage: Stage = (df) => df.filter(col("x") > 1)
    val out = stage(Seq(1, 2, 3).toDF("x")).collect().map(_.getInt(0)).sorted
    out shouldBe Array(2, 3)
  }

  "a BatchStage lambda" should "receive the batchId" in {
    val s = spark; import s.implicits._
    val stage: BatchStage = (df, id) => df.withColumn("bid", lit(id))
    val out = stage(Seq("a").toDF("x"), 7L).select("bid").first().getLong(0)
    out shouldBe 7L
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.StageSpec"`
Expected: FAIL — `Stage` / `BatchStage` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/engine/Stage.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.DataFrame

trait Stage {
  def apply(df: DataFrame): DataFrame
}

trait BatchStage {
  def apply(df: DataFrame, batchId: Long): DataFrame
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.StageSpec"`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/Stage.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/StageSpec.scala
git commit -m "feat: engine Stage + BatchStage traits

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Sink trait + KafkaSink + ParquetSink

**Files:**
- Create: `com/demo/engine/Sink.scala`
- Test: `com/demo/engine/SinkSpec.scala`

**Interfaces:**
- Consumes: nothing from prior tasks (uses Spark only).
- Produces:
  - `trait Sink { def write(batch: DataFrame, batchId: Long): Unit }`
  - `class KafkaSink(bootstrapServers: String, topic: String, keyCol: String) extends Sink` with a pure `payload(df: DataFrame): DataFrame` returning columns `key` (= `keyCol`) and `value` (= `to_json(struct(all df columns))`); `write` saves `payload(batch)` to Kafka.
  - `class ParquetSink(path: String, partitionCol: String, outputFiles: Int, transform: DataFrame => DataFrame) extends Sink` whose `write` does `transform(batch).coalesce(max(1,outputFiles)).write.mode("append").partitionBy(partitionCol).parquet(path)`.

This task moves the Parquet-write logic previously in `OnlineJoinerStreamingJob.writeParquet` into `ParquetSink`; the parquet-file-count assertion is written here against `ParquetSink` (the equivalent test is removed from `OnlineJoinerStreamingJobSpec` in Task 6).

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/SinkSpec.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SinkSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("SinkSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "KafkaSink.payload" should "produce key/value with the value carrying all columns as JSON" in {
    val s = spark; import s.implicits._
    val df = Seq(("k1", "v1")).toDF("id", "other")
    val out = new KafkaSink("localhost:9092", "t", "id").payload(df)
    out.columns should contain allOf ("key", "value")
    val row = out.first()
    row.getAs[String]("key") shouldBe "k1"
    val json = row.getAs[String]("value")
    json should include ("\"id\":\"k1\"")
    json should include ("\"other\":\"v1\"")
  }

  "ParquetSink" should "write at most outputFiles parquet files per partition" in {
    import java.nio.file.Files
    val s = spark; import s.implicits._
    val dir = Files.createTempDirectory("parquet-sink").toFile
    val out = new java.io.File(dir, "samples").getAbsolutePath

    val batch = Seq(
      ("s1", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s2", java.sql.Timestamp.valueOf("2026-06-26 00:00:00")),
      ("s3", java.sql.Timestamp.valueOf("2026-06-26 00:00:00"))
    ).toDF("sample_id", "impression_time")

    val sink = new ParquetSink(out, "date", outputFiles = 1,
      transform = df => df.withColumn("date", to_date(col("impression_time"))))
    sink.write(batch, 0L)

    val partDir = new java.io.File(out, "date=2026-06-26")
    partDir.listFiles().count(_.getName.endsWith(".parquet")) shouldBe 1
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.SinkSpec"`
Expected: FAIL — `KafkaSink` / `ParquetSink` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/engine/Sink.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, to_json}

trait Sink {
  def write(batch: DataFrame, batchId: Long): Unit
}

/** Writes each row as key=keyCol, value=JSON of ALL columns, to a Kafka topic. */
class KafkaSink(bootstrapServers: String, topic: String, keyCol: String) extends Sink {
  def payload(df: DataFrame): DataFrame =
    df.select(
      col(keyCol).as("key"),
      to_json(struct(df.columns.map(col): _*)).as("value")
    )

  def write(batch: DataFrame, batchId: Long): Unit =
    payload(batch).write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", topic)
      .save()
}

/** Applies a pre-write transform, then appends partitioned Parquet, bounding file count. */
class ParquetSink(path: String, partitionCol: String, outputFiles: Int,
                  transform: DataFrame => DataFrame) extends Sink {
  def write(batch: DataFrame, batchId: Long): Unit =
    transform(batch)
      .coalesce(math.max(1, outputFiles))
      .write
      .mode("append")
      .partitionBy(partitionCol)
      .format("parquet")
      .save(path)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.SinkSpec"`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/Sink.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/SinkSpec.scala
git commit -m "feat: engine Sink trait + KafkaSink + ParquetSink

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: ExecutionEngine (withRetry, processBatch, run)

**Files:**
- Create: `com/demo/engine/ExecutionEngine.scala`
- Test: `com/demo/engine/ExecutionEngineSpec.scala`

**Interfaces:**
- Consumes: `EngineConfig`, `Source`, `Stage`, `BatchStage`, `Sink` (Tasks 1–4).
- Produces:
  - `ExecutionEngine.withRetry(maxRetries: Int)(op: => Unit): Unit` — runs `op`; on exception retries up to `maxRetries` more times (total `maxRetries+1` attempts) then rethrows.
  - `ExecutionEngine.processBatch(batch: DataFrame, batchId: Long, batchStages: Seq[BatchStage], sinks: Seq[Sink], maxRetries: Int): Unit` — folds `batchStages`, persists `MEMORY_AND_DISK_SER`, writes each sink under `withRetry`, always unpersists.
  - `ExecutionEngine.run(spark, cfg, source, streamingStages: Seq[Stage], batchStages: Seq[BatchStage], sinks: Seq[Sink]): Unit` — `source.read` → fold `streamingStages` → `writeStream.foreachBatch(processBatch)` with checkpoint + `ProcessingTime(triggerInterval)` → `start().awaitTermination()`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/ExecutionEngineSpec.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExecutionEngineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("ExecutionEngineSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "withRetry" should "retry until success within the budget" in {
    var calls = 0
    ExecutionEngine.withRetry(2) { calls += 1; if (calls <= 2) throw new RuntimeException("boom") }
    calls shouldBe 3
  }

  it should "rethrow after exhausting retries" in {
    var calls = 0
    an[RuntimeException] should be thrownBy
      ExecutionEngine.withRetry(1) { calls += 1; throw new RuntimeException("boom") }
    calls shouldBe 2
  }

  "processBatch" should "apply batch stages and write to every sink with the batchId" in {
    val s = spark; import s.implicits._
    val received = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    val collecting: Sink = (b, id) =>
      b.collect().foreach(r => received += ((r.getAs[String]("x"), r.getAs[Long]("bid"))))
    val addBid: BatchStage = (df, id) => df.withColumn("bid", lit(id))

    ExecutionEngine.processBatch(Seq("a", "b").toDF("x"), 5L, Seq(addBid), Seq(collecting), maxRetries = 0)

    received.toSet shouldBe Set(("a", 5L), ("b", 5L))
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.ExecutionEngineSpec"`
Expected: FAIL — `ExecutionEngine` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/engine/ExecutionEngine.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.storage.StorageLevel

object ExecutionEngine {

  /** Run `op`; on failure retry up to `maxRetries` more times, then rethrow. */
  def withRetry(maxRetries: Int)(op: => Unit): Unit = {
    var attempt = 0
    var done = false
    while (!done) {
      try { op; done = true }
      catch {
        case e: Throwable =>
          if (attempt >= maxRetries) throw e
          attempt += 1
      }
    }
  }

  /** Fold batch stages, persist, write each sink under retry, always unpersist. */
  def processBatch(
      batch: DataFrame, batchId: Long,
      batchStages: Seq[BatchStage], sinks: Seq[Sink], maxRetries: Int
  ): Unit = {
    val records = batchStages.foldLeft(batch)((df, s) => s(df, batchId))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      sinks.foreach(sink => withRetry(maxRetries)(sink.write(records, batchId)))
    } finally {
      records.unpersist()
    }
  }

  /** Wire source -> streaming stages -> foreachBatch(processBatch) -> start/await. */
  def run(
      spark: SparkSession, cfg: EngineConfig, source: Source,
      streamingStages: Seq[Stage], batchStages: Seq[BatchStage], sinks: Seq[Sink]
  ): Unit = {
    val streamed = streamingStages.foldLeft(source.read(spark, cfg))((df, s) => s(df))
    streamed.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        processBatch(batch, batchId, batchStages, sinks, cfg.sinkMaxRetries)
      }
      .option("checkpointLocation", cfg.checkpointLocation)
      .trigger(Trigger.ProcessingTime(cfg.triggerInterval))
      .start()
      .awaitTermination()
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.ExecutionEngineSpec"`
Expected: PASS (3 tests). (`run` is covered end-to-end by the OnlineJoiner refactor in Task 6.)

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/ExecutionEngine.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/ExecutionEngineSpec.scala
git commit -m "feat: engine ExecutionEngine (withRetry, processBatch, run)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Refactor OnlineJoinerStreamingJob onto the engine (proof)

**Files:**
- Modify: `com/demo/process/OnlineJoinerStreamingJob.scala`
- Modify: `com/demo/process/OnlineJoinerStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `EngineConfig`, `KafkaSource`, `Stage`, `BatchStage`, `KafkaSink`, `ParquetSink`, `ExecutionEngine` (Tasks 1–5).
- Produces: no new API. `OnlineJoinerStreamingJob.main` now runs on the engine; the pure methods `loadCatalog`, `withCatalog`, `enrichWithCatalog`, `parseEvents`, `dedupedEvents`, `buildTrainingSamples` are unchanged and retained. `writeParquet` is removed (its logic now lives in `ParquetSink`).

Behavior must be identical: Kafka value = `to_json(struct(...))` of `buildTrainingSamples(batch) + batch_id`; Parquet = `withCatalog(...).withColumn("date", to_date(impression_time))` appended `partitionBy("date")`, `coalesce(outputFiles)`.

- [ ] **Step 1: Update the spec (remove the relocated test)**

In `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`, DELETE the test block:
`it should "write at most ONLINE_JOINER_OUTPUT_FILES parquet files per date partition" in { ... }`
(its equivalent now lives in `com.demo.engine.SinkSpec` against `ParquetSink`). If, after deletion, the imports `org.apache.spark.sql.functions.{coalesce, col}` or `MemoryStream` are only partly used, leave the still-used ones and drop only the now-unused. Do NOT change any other test in this file.

- [ ] **Step 2: Run the spec to confirm the remaining tests still compile/pass**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`
Expected: PASS (the remaining `buildTrainingSamples`/`enrichWithCatalog`/`withCatalog`/`loadCatalog`/`dedupedEvents` tests). This confirms the retained methods are untouched before we rewrite `main`.

- [ ] **Step 3: Rewrite `main` to run on the engine and remove `writeParquet`**

In `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`:

(a) Add imports:
```scala
import com.demo.engine.{BatchStage, EngineConfig, ExecutionEngine, KafkaSink, KafkaSource, ParquetSink, Sink, Stage}
import com.demo.util.Env
```

(b) Replace the entire body of `def main(args: Array[String]): Unit = { ... }` with:
```scala
  def main(args: Array[String]): Unit = {
    val spark = SparkSessions.create("OnlineJoinerStreamingJob")
    BatchMetricsListener.register(spark)

    val cfg = EngineConfig(
      bootstrapServers     = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      inputTopic           = sys.env.getOrElse("ONLINE_JOINER_INPUT_TOPIC", "recsys_events"),
      startingOffsets      = sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"),
      groupId              = sys.env.getOrElse("KAFKA_GROUP_ID", "training-joiner"),
      maxOffsetsPerTrigger = Env.int("MAX_OFFSETS_PER_TRIGGER", 5000),
      triggerInterval      = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds"),
      checkpointLocation   = sys.env.getOrElse("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-recsys/online-joiner"),
      watermarkDelay       = sys.env.getOrElse("EVENT_WATERMARK_DELAY", "10 minutes"),
      sinkMaxRetries       = Env.int("SINK_MAX_RETRIES", 0)
    )
    EngineConfig.validate(cfg) match {
      case Left(errors) =>
        errors.foreach(e => System.err.println(s"[config] $e"))
        sys.exit(1)
      case Right(_) => ()
    }

    val outputTopic = sys.env.getOrElse("ONLINE_JOINER_OUTPUT_TOPIC", "training_samples")
    val outputPath  = sys.env.getOrElse("ONLINE_JOINER_HDFS_OUTPUT_PATH", "/tmp/spark-recsys/training-samples")
    val outputFiles = math.max(1, Env.int("ONLINE_JOINER_OUTPUT_FILES", 1))
    val catalogPath = sys.env.getOrElse("ONLINE_JOINER_CATALOG_PATH", "")
    val catalog: Option[DataFrame] = if (catalogPath.nonEmpty) Some(loadCatalog(spark, catalogPath)) else None

    val streamingStages: Seq[Stage] = Seq((df: DataFrame) => dedupedEvents(df, cfg.watermarkDelay))
    val batchStages: Seq[BatchStage] =
      Seq((df: DataFrame, id: Long) => buildTrainingSamples(df).withColumn("batch_id", lit(id)))
    val sinks: Seq[Sink] = Seq(
      new KafkaSink(cfg.bootstrapServers, outputTopic, "sample_id"),
      new ParquetSink(outputPath, "date", outputFiles,
        (df: DataFrame) => withCatalog(df, catalog).withColumn("date", to_date(col("impression_time"))))
    )

    ExecutionEngine.run(spark, cfg, KafkaSource, streamingStages, batchStages, sinks)
  }
```

(c) DELETE the `def writeParquet(samples: DataFrame, path: String, outputFiles: Int): Unit = ...` method.

(d) Remove now-unused imports flagged by the compiler (expected: `org.apache.spark.sql.streaming.Trigger`, `org.apache.spark.storage.StorageLevel`, and `to_json`/`struct` if no longer referenced). Keep `col`, `lit`, `to_date`, `coalesce`, `broadcast`, `typedLit`, etc. that the retained pure methods still use. Let the compiler guide.

- [ ] **Step 4: Run the full module suite**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt test`
Expected: ALL specs PASS, including the engine specs and the trimmed `OnlineJoinerStreamingJobSpec`. Output pristine.

- [ ] **Step 5: Sanity-check behavior preservation by reading the diff**

Run: `git -C ../../.. diff --stat` (from the module dir) — confirm only `OnlineJoinerStreamingJob.scala` and its spec changed in `com/demo/process`, plus the new `com/demo/engine` files. Confirm no change to `ONLINE_JOINER_*` env var names, the `training_samples` default output topic, or the `partitionBy("date")` layout.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "refactor: run OnlineJoinerStreamingJob on the shared execution engine

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Run the whole module suite once more:

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt test`
Expected: all specs PASS (engine specs + unchanged behavior for every other job).

## Self-Review (completed by plan author)

- **Spec coverage:** bullet 6 config parse/validate → Task 1 (+ wired in Task 6 `main`); bullet 1 connect (Kafka) → Task 2; bullet 2 filter/feature → Task 3 + OnlineJoiner stages in Task 6; bullet 3 enrich/offline join → `ParquetSink` transform (`withCatalog`) in Tasks 4/6; bullet 4 persist → Task 4; bullet 5 concurrency/retry/backpressure → `ExecutionEngine` (Task 5) + `maxOffsetsPerTrigger`/`triggerInterval` in config; proof refactor → Task 6.
- **Deviations from the spec, flagged:** (1) The spec's single `Stage` type is split into `Stage` + `BatchStage` so the per-batch stage can receive `batchId` (needed for the `batch_id` column) — this is the honest model of the streaming-vs-batch split the spec already describes. (2) `shufflePartitions` is NOT added to `EngineConfig`; `SparkSessions.create` already owns `spark.sql.shuffle.partitions` via `SPARK_SQL_SHUFFLE_PARTITIONS`, so duplicating it would be an unused field. (3) `EngineConfig.fromEnv` is not built; the one job constructs its config inline (env-reading stays in the job, keeping Task 1 pure/testable) — add `fromEnv` when a second job adopts the engine (YAGNI). (4) The `writeParquet` test relocates from `OnlineJoinerStreamingJobSpec` to `SinkSpec` because that logic moves into `ParquetSink`; all other OnlineJoiner tests are unchanged.
- **Placeholder scan:** none — every step has concrete code/commands.
- **Type consistency:** `EngineConfig`, `Source.read(spark,cfg)`, `Stage.apply(df)`, `BatchStage.apply(df,batchId)`, `Sink.write(batch,batchId)`, `ExecutionEngine.{withRetry,processBatch,run}` signatures are used identically across Tasks 1–6.
- **Note:** Task 2's `KafkaSource` test depends on the `spark-sql-kafka` connector being on the test classpath (it is — every streaming job uses it); the task says to report BLOCKED if not.
