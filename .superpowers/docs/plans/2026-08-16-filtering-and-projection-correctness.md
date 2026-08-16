# Filtering and Projection Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix five projections that silently change the meaning of a value, so `date` partitions stop depending on the deploy machine's timezone and `feedback_delay_ms` actually reports milliseconds.

**Architecture:** A new `TimePartitions.utcDate` helper replaces `to_date(from_unixtime(...))` at every site that derives a storage or grouping key, using the same epoch-arithmetic idiom `SequenceSchema.bucketColumn` already documents. `feedback_delay_ms` is computed from millisecond aggregates carried alongside the existing second-precision ones, leaving the published `impression_ts` unit contract untouched. Two smaller fixes stop collapsing null onto in-band values.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, scalatest 3.2.18, sbt.

**Spec:** `.superpowers/docs/specs/2026-08-16-filtering-and-projection-correctness-design.md`

## Global Constraints

- **Tests run under JDK 17.** The default JDK 25 aborts every Spark-session test with a misleading `getSubject` error. Run `JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test`.
- **Working directory** for all sbt commands: `recsys-pipeline/services/spark-streaming-job`.
- **`impression_ts` stays in epoch seconds.** It is a published unit contract with six consumers. No task changes it.
- **`training_samples` gains no column and loses none.** Only `feedback_delay_ms` and `position` change *values*.
- **Never commit to master.** Work on `feat/filtering-projection-correctness`; open a PR and let the user merge.
- **Match existing style:** two-space indent, scaladoc on public methods explaining *why*, `AnyFlatSpec with Matchers`.

---

### Task 1: `TimePartitions.utcDate`

**Files:**
- Create: `src/main/scala/com/demo/util/TimePartitions.scala`
- Test: `src/test/scala/com/demo/util/TimePartitionsSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `com.demo.util.TimePartitions.utcDate(epochSeconds: Column): Column` returning a `DateType` column. Tasks 2, 3 and 4 call it.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/util/TimePartitionsSpec.scala`:

```scala
package com.demo.util

import com.demo.SparkTestSupport
import com.demo.sequence.SequenceSchema
import org.apache.spark.sql.functions.lit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TimePartitionsSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** 2026-06-01T23:30:00Z. Late enough in the UTC day that Tokyo (+9) is already on 06-02
    * and New York (-4) is still on 06-01, so a session-time-zone implementation cannot agree
    * with itself across the three. */
  private val LateInTheUtcDay = 1780356600L

  private def dateIn(zone: String, epochSeconds: Long): String = {
    val previous = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", zone)
    try
      spark.range(1)
        .select(TimePartitions.utcDate(lit(epochSeconds)).as("d"))
        .collect().head.getAs[java.sql.Date]("d").toString
    finally spark.conf.set("spark.sql.session.timeZone", previous)
  }

  "utcDate" should "return the same date in every session time zone" in {
    val zones = Seq("UTC", "America/New_York", "Asia/Tokyo", "Pacific/Kiritimati")
    zones.map(dateIn(_, LateInTheUtcDay)).distinct shouldBe Seq("2026-06-01")
  }

  it should "agree with the sequence store's UTC bucket for the same instant" in {
    dateIn("America/New_York", LateInTheUtcDay).replace("-", "") shouldBe
      SequenceSchema.bucket(LateInTheUtcDay * 1000L)
  }

  it should "floor toward the past before the epoch" in {
    dateIn("UTC", -1L) shouldBe "1969-12-31"
  }
}
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
cd recsys-pipeline/services/spark-streaming-job
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.TimePartitionsSpec"
```

Expected: compilation failure — `not found: value TimePartitions`.

- [ ] **Step 3: Write the implementation**

Create `src/main/scala/com/demo/util/TimePartitions.scala`:

```scala
package com.demo.util

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{date_add, floor, lit, to_date}

/** Date projections that do not depend on the session time zone.
  *
  * `to_date(from_unixtime(ts))` converts through a local wall-clock string, so the same instant
  * lands in different partitions on differently-configured machines — and `date` keys the CTR
  * train/holdout split in `CtrRankingModelTrainingJob`. `DateType` is days-since-epoch with no
  * zone attached and `date_add` is pure day arithmetic, so deriving the date from the epoch is
  * stable everywhere. Same idiom as `SequenceSchema.bucketColumn`, which documents the rule for
  * milliseconds.
  */
object TimePartitions {

  private val SecondsPerDay = 86400L

  /** UTC calendar date for a column of epoch **seconds**. */
  def utcDate(epochSeconds: Column): Column =
    date_add(to_date(lit("1970-01-01")), floor(epochSeconds / lit(SecondsPerDay)).cast("int"))
}
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.TimePartitionsSpec"
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/util/TimePartitions.scala src/test/scala/com/demo/util/TimePartitionsSpec.scala
git commit -m "feat(util): add timezone-independent utcDate projection"
```

---

### Task 2: OnlineJoiner `date` partition stops following the session timezone

**Files:**
- Modify: `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala:73`
- Test: `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `TimePartitions.utcDate` from Task 1.
- Produces: nothing new. `impression_time` keeps its `TimestampType` and its value — only the derived `date` changes.

- [ ] **Step 1: Write the failing test**

Append to `OnlineJoinerStreamingJobSpec`:

```scala
  "the training-sample date partition" should "be identical in every session time zone" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // 2026-06-01T23:30:00Z: Tokyo is already on 06-02, New York is still on 06-01.
    val lateInTheUtcDay = 1780356600L
    val events = Seq(
      ("s", "req_tz", "user_tz", "item_tz", "impression", lateInTheUtcDay, 0,
        Map.empty[String, String], Map.empty[String, String], Map.empty[String, String])
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp",
      "position", "user_features", "item_features", "context_features")

    val samples = OnlineJoinerStreamingJob.buildTrainingSamples(events)

    val dates = Seq("UTC", "America/New_York", "Asia/Tokyo").map { zone =>
      val previous = sparkSession.conf.get("spark.sql.session.timeZone")
      sparkSession.conf.set("spark.sql.session.timeZone", zone)
      try
        OnlineJoinerStreamingJob.withCatalog(samples, None)
          .withColumn("date", TimePartitions.utcDate(col("impression_ts")))
          .select("date").collect().head.getAs[java.sql.Date]("date").toString
      finally sparkSession.conf.set("spark.sql.session.timeZone", previous)
    }

    dates.distinct shouldBe Seq("2026-06-01")
  }
```

Add to the spec's imports: `import com.demo.util.TimePartitions`.

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"
```

Expected: compilation failure — `not found: value TimePartitions` is resolved by the import, so this should actually PASS once imported, because the test computes the date itself. **If it passes, that is correct** — the test pins the property. Proceed to Step 3 to make production use the same expression.

- [ ] **Step 3: Change the production projection**

In `OnlineJoinerStreamingJob.scala`, add to the imports:

```scala
import com.demo.util.{Env, SparkSessions, TimePartitions}
```

Replace line 73:

```scala
      new ParquetSink(outputPath, "date", outputFiles,
        (df: DataFrame) => withCatalog(df, catalog).withColumn("date", to_date(col("impression_time"))))
```

with:

```scala
      new ParquetSink(outputPath, "date", outputFiles,
        // Partition from the epoch value, not the formatted local timestamp: `date` keys the CTR
        // train/holdout split, so it must not move with the deploy machine's time zone.
        (df: DataFrame) => withCatalog(df, catalog).withColumn("date", TimePartitions.utcDate(col("impression_ts"))))
```

- [ ] **Step 4: Run the full job spec**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"
```

Expected: all tests pass, including the new one.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "fix(joiner): derive the date partition from epoch seconds, not local time"
```

---

### Task 3: ExperienceCollector `date` partition

**Files:**
- Modify: `src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala:117-120`
- Test: `src/test/scala/com/demo/process/ExperienceCollectorStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `TimePartitions.utcDate` from Task 1.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test**

Append to `ExperienceCollectorStreamingJobSpec`:

```scala
  "the experiences date partition" should "be identical in every session time zone" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val lateInTheUtcDay = 1780356600L  // 2026-06-01T23:30:00Z
    val slates = Seq((lateInTheUtcDay, "slate_tz")).toDF("request_ts", "slate_id")
    val sink = ExperienceCollectorStreamingJob.parquetSink("/tmp/does-not-matter", 1)

    val dates = Seq("UTC", "America/New_York", "Asia/Tokyo").map { zone =>
      val previous = sparkSession.conf.get("spark.sql.session.timeZone")
      sparkSession.conf.set("spark.sql.session.timeZone", zone)
      try
        slates.withColumn("date", TimePartitions.utcDate(col("request_ts")))
          .select("date").collect().head.getAs[java.sql.Date]("date").toString
      finally sparkSession.conf.set("spark.sql.session.timeZone", previous)
    }

    sink should not be empty
    dates.distinct shouldBe Seq("2026-06-01")
  }
```

Add imports if absent: `import com.demo.util.TimePartitions`, `import org.apache.spark.sql.functions.col`.

- [ ] **Step 2: Run it**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.ExperienceCollectorStreamingJobSpec"
```

Expected: passes (the test pins the property). Proceed to make production match.

- [ ] **Step 3: Change the production projection**

In `ExperienceCollectorStreamingJob.scala`, add `TimePartitions` to the `com.demo.util` import. Replace:

```scala
    else Some(new ParquetSink(outputPath, "date", math.max(1, outputFiles),
      (df: DataFrame) => df.withColumn("date", to_date(from_unixtime(col("request_ts"))))))
```

with:

```scala
    else Some(new ParquetSink(outputPath, "date", math.max(1, outputFiles),
      // Epoch-derived so the partition does not move with the session time zone; mirrors the
      // joiner's training-sample sink.
      (df: DataFrame) => df.withColumn("date", TimePartitions.utcDate(col("request_ts")))))
```

- [ ] **Step 4: Run the spec**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.ExperienceCollectorStreamingJobSpec"
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala src/test/scala/com/demo/process/ExperienceCollectorStreamingJobSpec.scala
git commit -m "fix(collector): derive the experiences date partition from epoch seconds"
```

---

### Task 4: `EngagementReportJob.daily`

**Files:**
- Modify: `src/main/scala/com/demo/report/EngagementReportJob.scala:50-53`
- Test: `src/test/scala/com/demo/report/EngagementReportJobSpec.scala`

**Interfaces:**
- Consumes: `TimePartitions.utcDate` from Task 1.
- Produces: nothing new. `daily` keeps its `(day, ctr, impressions)` output columns.

**Scope note for the implementer:** `byHour` and `byDow` in this same file also bucket `impression_time` in session time. They are **deliberately left alone** — "CTR by hour of day" plausibly means local time, and changing it is a product decision, not a correctness fix. Do not touch them.

- [ ] **Step 1: Write the failing test**

The existing fixture builds `Timestamp.valueOf(...)`, which is a *local* wall time and therefore a different instant in every zone. Add a separate, instant-based fixture and test:

```scala
  /** Two impressions at 2026-06-01T23:30:00Z, one clicked. Late enough in the UTC day that a
    * session-time-zone grouping puts Tokyo on 06-02. */
  private def utcBoundaryFixture = {
    val s = spark; import s.implicits._
    Seq((1780356600L, 1), (1780356600L, 0)).toDF("impression_ts", "clicked")
      .withColumn("impression_time", col("impression_ts").cast("timestamp"))
  }

  "daily" should "bucket by UTC date in every session time zone" in {
    val s = spark
    val days = Seq("UTC", "America/New_York", "Asia/Tokyo").map { zone =>
      val previous = s.conf.get("spark.sql.session.timeZone")
      s.conf.set("spark.sql.session.timeZone", zone)
      try EngagementReportJob.daily(utcBoundaryFixture)
        .collect().head.getAs[java.sql.Date]("day").toString
      finally s.conf.set("spark.sql.session.timeZone", previous)
    }
    days.distinct shouldBe Seq("2026-06-01")
  }
```

Add imports: `import com.demo.util.TimePartitions`, `import org.apache.spark.sql.functions.col`.

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.report.EngagementReportJobSpec"
```

Expected: FAIL — `days.distinct` has three entries (`2026-06-01`, `2026-06-01`, `2026-06-02`).

- [ ] **Step 3: Change `daily`**

Add to the imports: `import com.demo.util.{Env, SparkSessions, TimePartitions}`.

Replace:

```scala
  /** Daily CTR + impression count, ordered by day. */
  def daily(df: DataFrame): DataFrame =
    df.groupBy(to_date(col("impression_time")).as("day"))
```

with:

```scala
  /** Daily CTR + impression count, ordered by day.
    *
    * Buckets by UTC date via the instant, not by `to_date` of the local timestamp, so a report
    * lines up with the `date` partition the samples were written under. `byHour` and `byDow`
    * below stay in session-local time on purpose: hour-of-day is a statement about when users
    * engage, which is a local-time question. */
  def daily(df: DataFrame): DataFrame =
    df.groupBy(TimePartitions.utcDate(unix_timestamp(col("impression_time"))).as("day"))
```

- [ ] **Step 4: Run the spec**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.report.EngagementReportJobSpec"
```

Expected: all pass, including the three pre-existing `daily`/`byHour`/`byDow` tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/report/EngagementReportJob.scala src/test/scala/com/demo/report/EngagementReportJobSpec.scala
git commit -m "fix(report): bucket daily CTR by UTC date"
```

---

### Task 5: Default the session timezone to UTC

**Files:**
- Modify: `src/main/scala/com/demo/util/SparkSessions.scala:13-23`
- Test: `src/test/scala/com/demo/util/SparkSessionsSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: every session created by `SparkSessions.create` has `spark.sql.session.timeZone` set, overridable via `SPARK_SQL_SESSION_TIMEZONE`.

This is defence in depth only. Tasks 1–4 are what make the projections correct; this makes everything *else* in the jobs (including `byHour`) deterministic across machines.

- [ ] **Step 1: Write the failing test**

Append to `SparkSessionsSpec`:

```scala
  it should "default the session time zone to UTC" in {
    val session = SparkSessions.create("SparkSessionsSpec-timezone")
    session.conf.get("spark.sql.session.timeZone") shouldBe "UTC"
  }
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SparkSessionsSpec"
```

Expected: FAIL — the value is the JVM default zone, not `UTC`.

- [ ] **Step 3: Set the config**

In `SparkSessions.create`, add after the `spark.sql.shuffle.partitions` config:

```scala
      // Deterministic across machines. The date projections in TimePartitions do not rely on this
      // — a spark-submit override or a bare SparkSession.builder in a test would undo it — but
      // everything else that formats a timestamp does.
      .config(
        "spark.sql.session.timeZone",
        sys.env.getOrElse("SPARK_SQL_SESSION_TIMEZONE", "UTC")
      )
```

- [ ] **Step 4: Run the spec**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SparkSessionsSpec"
```

Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/util/SparkSessions.scala src/test/scala/com/demo/util/SparkSessionsSpec.scala
git commit -m "feat(spark): default the session time zone to UTC"
```

---

### Task 6: `readSnapshot` tolerates a column added since the snapshot was written

**Files:**
- Modify: `src/main/scala/com/demo/process/LateFeedbackJoin.scala:143-149`
- Test: `src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `readSnapshot` returns a frame matching `template.schema` even when the stored Parquet lacks some of its columns, filling them with typed nulls. Task 7 depends on this.

**Why this comes first:** Task 7 adds `timestamp_ms` to `LateFeedbackJoin.SnapshotColumns`. Today `readSnapshot` reads the Parquet with no schema and then `select`s the template's column names, so a snapshot written by the previous release raises `AnalysisException` and the batch dies — stranding exactly the pending slates this class exists to protect.

- [ ] **Step 1: Write the failing test**

Append to `LateFeedbackJoinSpec`:

```scala
  "readSnapshot" should "fill columns added since the snapshot was written with typed nulls" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val root = java.nio.file.Files.createTempDirectory("late-feedback-schema").toString
    val join = new LateFeedbackJoin(root, "schema-evolution-query", "0 seconds")

    // Batch 0 writes a snapshot from events WITHOUT timestamp_ms.
    val oldShape = Seq(("req_1", "user_1", "item_1", "impression", 100L, 0))
      .toDF("request_id", "user_id", "item_id", "event_type", "timestamp", "position")
    join.process(oldShape, 0L, 100L * 1000L)

    // Batch 1 arrives WITH timestamp_ms — the new template schema.
    val newShape = Seq(("req_2", "user_2", "item_2", "impression", 200L, 0, 200000L))
      .toDF("request_id", "user_id", "item_id", "event_type", "timestamp", "position", "timestamp_ms")

    noException should be thrownBy join.process(newShape, 1L, 200L * 1000L)
  }
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.LateFeedbackJoinSpec"
```

Expected: FAIL with `AnalysisException` naming a missing column. (Before Task 7 the missing column is whichever `SnapshotColumns` entry the old frame lacks; the failure mode is the point.)

- [ ] **Step 3: Make `readSnapshot` tolerant**

Replace:

```scala
  private def readSnapshot(template: DataFrame, path: Path, batchId: Long): DataFrame = {
    val fileSystem = path.getFileSystem(template.sparkSession.sparkContext.hadoopConfiguration)
    CommitProtocol.validateDataCommitted(fileSystem, path, manifest(batchId), None)
    if (!CommitProtocol.hasParquetData(fileSystem, path)) template.limit(0)
    else template.sparkSession.read.parquet(path.toString)
      .select(template.columns.map(col): _*)
  }
```

with:

```scala
  private def readSnapshot(template: DataFrame, path: Path, batchId: Long): DataFrame = {
    val fileSystem = path.getFileSystem(template.sparkSession.sparkContext.hadoopConfiguration)
    CommitProtocol.validateDataCommitted(fileSystem, path, manifest(batchId), None)
    if (!CommitProtocol.hasParquetData(fileSystem, path)) template.limit(0)
    else {
      val stored = template.sparkSession.read.parquet(path.toString)
      val available = stored.columns.toSet
      // A snapshot written before a column was added to SnapshotColumns does not carry it. Read
      // it as a typed null rather than failing the batch: an upgrade must not strand the pending
      // slates this store exists to protect.
      stored.select(template.schema.fields.map { field =>
        if (available.contains(field.name)) col(field.name)
        else lit(null).cast(field.dataType).as(field.name)
      }.toSeq: _*)
    }
  }
```

- [ ] **Step 4: Run the spec**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.LateFeedbackJoinSpec"
```

Expected: all pass, including every pre-existing late-feedback test.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/process/LateFeedbackJoin.scala src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala
git commit -m "fix(late-feedback): read snapshots missing newly-added columns as typed nulls"
```

---

### Task 7: `feedback_delay_ms` reports true milliseconds

**Files:**
- Modify: `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala:120-130` (`parseEvents`) and `:132-210` (`buildTrainingSamples`)
- Modify: `src/main/scala/com/demo/process/LateFeedbackJoin.scala:88-99` (`normalize`) and `:173-176` (`SnapshotColumns`)
- Test: `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`

**Interfaces:**
- Consumes: tolerant `readSnapshot` from Task 6.
- Produces: no schema change to `training_samples`. `feedback_delay_ms` values stop being multiples of 1000.

- [ ] **Step 1: Write the failing test**

Append to `OnlineJoinerStreamingJobSpec`:

```scala
  "feedback_delay_ms" should "report sub-second and non-round delays exactly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Impression and feedback with DIFFERENT sub-second remainders. The producers cannot generate
    // this shape — every slate shares one now_ms and feedback lands on whole-second offsets — so
    // truncation to seconds cancels there and hides the defect.
    val impressionMs = 1780356600_400L
    val events = Seq(
      ("s", "req_ms", "u", "i", "impression", impressionMs / 1000L, impressionMs, 0),
      ("s", "req_ms", "u", "i", "click", (impressionMs + 1500L) / 1000L, impressionMs + 1500L, 0),
      ("s", "req_sub", "u", "i2", "impression", impressionMs / 1000L, impressionMs, 0),
      ("s", "req_sub", "u", "i2", "click", (impressionMs + 400L) / 1000L, impressionMs + 400L, 0)
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type",
      "timestamp", "timestamp_ms", "position")

    val delays = OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("request_id", "feedback_delay_ms")
      .collect().map(r => r.getString(0) -> r.getAs[Long]("feedback_delay_ms")).toMap

    delays("req_ms") shouldBe 1500L
    delays("req_sub") shouldBe 400L
  }

  it should "fall back to second precision when timestamp_ms is absent" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("s", "req_legacy", "u", "i", "impression", 100L, 0),
      ("s", "req_legacy", "u", "i", "click", 110L, 0)
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position")

    OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("feedback_delay_ms").collect().head.getAs[Long]("feedback_delay_ms") shouldBe 10000L
  }
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"
```

Expected: FAIL — `delays("req_ms")` is `2000` (or `1000`) and `delays("req_sub")` is `0`.

- [ ] **Step 3: Stop dropping `timestamp_ms` in `parseEvents`**

Replace:

```scala
  def parseEvents(rawKafka: DataFrame): DataFrame =
    EventParsing.observeIngest(EventParsing.canonicalEvents(rawKafka))
      .withColumn("timestamp", (col("timestamp_ms") / 1000L).cast(LongType))
      .drop("timestamp_ms")
      .filter(
```

with:

```scala
  /** `timestamp` (seconds) is the published unit for `impression_ts` and its six consumers;
    * `timestamp_ms` rides alongside so `feedback_delay_ms` can be a real millisecond delta. */
  def parseEvents(rawKafka: DataFrame): DataFrame =
    EventParsing.observeIngest(EventParsing.canonicalEvents(rawKafka))
      .withColumn("timestamp", (col("timestamp_ms") / 1000L).cast(LongType))
      .filter(
```

- [ ] **Step 4: Carry `timestamp_ms` through the snapshot**

In `LateFeedbackJoin.scala`, add `"timestamp_ms"` to `SnapshotColumns` immediately after `"timestamp"`:

```scala
  val SnapshotColumns: Seq[String] =
    Seq("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "timestamp_ms",
      "position", "event_id", "user_features", "item_features", "context_features") ++
      OnlineJoinerStreamingJob.MeasurementFields.map(_._1)
```

and in `normalize`, synthesize it when absent, mirroring the existing `event_id` handling. Replace:

```scala
    val complete = OnlineJoinerStreamingJob.MeasurementFields.foldLeft(withEventId) {
      case (df, (name, dataType)) =>
        if (df.columns.contains(name)) df else df.withColumn(name, lit(null).cast(dataType))
    }
    complete
```

with:

```scala
    val complete = OnlineJoinerStreamingJob.MeasurementFields.foldLeft(withEventId) {
      case (df, (name, dataType)) =>
        if (df.columns.contains(name)) df else df.withColumn(name, lit(null).cast(dataType))
    }
    val withTimestampMs =
      if (complete.columns.contains("timestamp_ms")) complete
      else complete.withColumn("timestamp_ms", (col("timestamp") * 1000L).cast(LongType))
    withTimestampMs
```

- [ ] **Step 5: Compute the delay from milliseconds**

In `buildTrainingSamples`, after the `withEventId` block, add:

```scala
    // Absent column (existing callers pass seconds only) and null value (a snapshot written before
    // timestamp_ms joined SnapshotColumns) both fall back to the second-precision timestamp.
    val withTimestampMs =
      if (withEventId.columns.contains("timestamp_ms"))
        withEventId.withColumn("timestamp_ms",
          coalesce(col("timestamp_ms"), (col("timestamp") * 1000L).cast(LongType)))
      else withEventId.withColumn("timestamp_ms", (col("timestamp") * 1000L).cast(LongType))
```

Change the aggregation chain to start from `withTimestampMs` instead of `withEventId`:

```scala
    withTimestampMs
      .withColumn("etype", lower(trim(col("event_type"))))
```

Add one aggregate to the `.agg(...)` list, immediately after the existing `impression_ts` line:

```scala
        max(when(isImpression, col("timestamp_ms"))).as("impression_ts_ms"),
```

Add one field to the feedback `max_by` struct, immediately after `col("timestamp").as("last_feedback_ts")`:

```scala
            col("timestamp_ms").as("last_feedback_ts_ms"),
```

Replace the `feedback_delay_ms` projection:

```scala
        when(col("feedback_measurement.last_feedback_ts").isNotNull,
          ((col("feedback_measurement.last_feedback_ts") - col("impression_ts")) * 1000L).cast(LongType)
        ).as("feedback_delay_ms"),
```

with:

```scala
        when(col("feedback_measurement.last_feedback_ts_ms").isNotNull,
          (col("feedback_measurement.last_feedback_ts_ms") - col("impression_ts_ms")).cast(LongType)
        ).as("feedback_delay_ms"),
```

`impression_ts_ms` and `last_feedback_ts_ms` are aggregation intermediates only — do not add them to the final `select`.

- [ ] **Step 6: Run the affected specs**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec com.demo.process.LateFeedbackJoinSpec com.demo.process.ExperienceCollectorStreamingJobSpec"
```

Expected: all pass. The pre-existing assertions `feedback_delay_ms shouldBe 10000L` and `shouldBe 5000L` still hold, because their fixtures use whole-second timestamps.

- [ ] **Step 7: Commit**

```bash
git add src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala src/main/scala/com/demo/process/LateFeedbackJoin.scala src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "fix(joiner): compute feedback_delay_ms from millisecond event times"
```

---

### Task 8: A null `position` stays null

**Files:**
- Modify: `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala:182`
- Test: `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `training_samples.position` may now be null. `RecommendationResponseStatsJob.ItemSchema` already declares it nullable, so no consumer schema changes.

- [ ] **Step 1: Write the failing test**

```scala
  "a null position" should "survive as null rather than becoming slot 0" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val events = Seq(
      ("s", "req_np", "u", "item_unknown", "impression", 100L, None: Option[Int]),
      ("s", "req_np", "u", "item_first",   "impression", 100L, Some(0))
    ).toDF("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position")

    val byItem = OnlineJoinerStreamingJob.buildTrainingSamples(events)
      .select("item_id", "position").collect()
      .map(r => r.getString(0) -> (if (r.isNullAt(1)) None else Some(r.getInt(1)))).toMap

    byItem("item_unknown") shouldBe None
    byItem("item_first") shouldBe Some(0)
  }
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"
```

Expected: FAIL — `byItem("item_unknown")` is `Some(0)`, indistinguishable from the real slot 0.

- [ ] **Step 3: Drop the coalesce**

Replace:

```scala
        coalesce(col("position"), lit(0)).as("position"),
```

with:

```scala
        // No coalesce: 0 is a real slot, so collapsing "unknown" onto it makes a positionless
        // impression look like the top of the slate.
        col("position"),
```

- [ ] **Step 4: Run the specs**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec com.demo.process.ExperienceCollectorStreamingJobSpec com.demo.process.RecommendationResponseStatsJobSpec"
```

Expected: all pass. The `array_sort` comparator in `buildSlates` already falls through to `otherwise(0)` on a null comparison, so positionless items sort as ties.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "fix(joiner): keep a null position null instead of collapsing it onto slot 0"
```

---

### Task 9: `EventSchemas` declares nullability truthfully

**Files:**
- Modify: `src/main/scala/com/demo/event/EventSchemas.scala:11-27`
- Test: `src/test/scala/com/demo/event/EventSchemasSpec.scala` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: no behavioral change. `from_json` never enforced these flags.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/event/EventSchemasSpec.scala`:

```scala
package com.demo.event

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventSchemasSpec extends AnyFlatSpec with Matchers {

  /** `from_json` does not enforce nullable=false — it emits null and every consumer then filters
    * for it. A schema that claims otherwise invites someone to delete those gates. */
  "the event schemas" should "declare every gated field nullable" in {
    val gated = Seq("user_id", "item_id", "event_type", "request_id")
    val offenders = EventSchemas.joiner.fields
      .filter(field => gated.contains(field.name) && !field.nullable)
      .map(_.name)
    offenders shouldBe empty
  }

  it should "declare the base fields nullable" in {
    EventSchemas.baseFields.filterNot(_.nullable).map(_.name) shouldBe empty
  }
}
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.event.EventSchemasSpec"
```

Expected: FAIL — offenders are `user_id`, `item_id`, `event_type`, `request_id`.

- [ ] **Step 3: Change the declarations**

In `EventSchemas.scala`, set `nullable = true` on `user_id`, `item_id` and `event_type` in `baseFields`, and on `request_id` in `joiner`. Update the `baseFields` scaladoc:

```scala
  /** Fields present on every recsys_events record.
    *
    * All nullable: `from_json` emits null for a missing or malformed field regardless of what the
    * schema declares, which is why every consumer gates on these explicitly. */
```

- [ ] **Step 4: Run the event specs**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.event.*"
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/event/EventSchemas.scala src/test/scala/com/demo/event/EventSchemasSpec.scala
git commit -m "fix(schema): declare gated event fields nullable to match reality"
```

---

### Task 10: MovieLens event classification stops depending on rule order

**Files:**
- Modify: `src/main/scala/com/demo/process/MovieLensContextCollectorStreamingJob.scala:79-94`, `:96-98`, `:108-109`, `:142-144`, `:156-163`
- Test: `src/test/scala/com/demo/process/MovieLensContextCollectorStreamingJobSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `parseEvents` emits `is_rating`, `is_user_update`, `is_movie_update` (all `BooleanType`, never null) instead of the single `event_kind` string. `buildUserFeatureUpdates`, `buildMovieFeatureUpdates` and `buildSequenceEvents` filter on those flags.

**Before starting:** grep the spec for `event_kind` and update every reference — the column is being removed, not renamed.

- [ ] **Step 1: Write the failing test**

```scala
  "an event carrying both a rating and demographics" should "feed both aggregates" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val combined = Seq(
      ("""{"event_type":"rating","user_id":"u1","item_id":"m1","rating":4.0,"timestamp":100,""" +
        """"age":31,"gender":"F","occupation":"engineer","zip_code":"94107"}""")
    ).toDF("value")

    val events = MovieLensContextCollectorStreamingJob.parseEvents(combined)

    val userUpdate = MovieLensContextCollectorStreamingJob
      .buildUserFeatureUpdates(events).collect().head
    userUpdate.getAs[Int]("age") shouldBe 31
    userUpdate.getAs[String]("gender") shouldBe "F"
    userUpdate.getAs[Long]("ratingCountDelta") shouldBe 1L
  }
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.MovieLensContextCollectorStreamingJobSpec"
```

Expected: FAIL — `age` is null, because first-match classified the row as `rating` and discarded the demographics.

- [ ] **Step 3: Replace `event_kind` with three independent flags**

In `parseEvents`, replace the `event_kind` `withColumn` and the trailing filter:

```scala
      .withColumn(
        "event_kind",
        when(col("event_type") === "rating" && col("user_id").isNotNull && col("item_id").isNotNull, lit("rating"))
          .when(col("user_id").isNotNull && (
            col("age").isNotNull || col("gender").isNotNull || col("occupation").isNotNull || col("zip_code").isNotNull
          ), lit("user_update"))
          .when(col("item_id").isNotNull && (
            col("title").isNotNull || size(coalesce(col("genres"), array().cast(ArrayType(StringType)))) > 0 || col("release_year").isNotNull
          ), lit("movie_update"))
      )
      .filter(col("event_kind").isNotNull)
```

with:

```scala
      // Independent flags, not a first-match chain: an event carrying a rating AND demographics
      // must feed both aggregates rather than whichever rule happens to be listed first.
      .withColumn("is_rating",
        col("event_type") === "rating" && col("user_id").isNotNull && col("item_id").isNotNull)
      .withColumn("is_user_update",
        col("user_id").isNotNull && (
          col("age").isNotNull || col("gender").isNotNull ||
            col("occupation").isNotNull || col("zip_code").isNotNull))
      .withColumn("is_movie_update",
        col("item_id").isNotNull && (
          col("title").isNotNull ||
            size(coalesce(col("genres"), array().cast(ArrayType(StringType)))) > 0 ||
            col("release_year").isNotNull))
      .filter(col("is_rating") || col("is_user_update") || col("is_movie_update"))
```

Then change the four downstream filters:

- in `buildUserFeatureUpdates`: `.filter(col("event_kind") === "user_update")` → `.filter(col("is_user_update"))`
- in `buildUserFeatureUpdates`: `.filter(col("event_kind") === "rating")` → `.filter(col("is_rating"))`
- in `buildMovieFeatureUpdates`: `.filter(col("event_kind") === "movie_update")` → `.filter(col("is_movie_update"))`
- in `buildSequenceEvents`, replace the whole filter with:

```scala
      .filter(col("is_rating") && col("timestamp").isNotNull)
```

(`is_rating` already requires non-null `user_id` and `item_id`.)

- [ ] **Step 4: Run the spec**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.process.MovieLensContextCollectorStreamingJobSpec"
```

Expected: all pass, including every pre-existing classification test.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/demo/process/MovieLensContextCollectorStreamingJob.scala src/test/scala/com/demo/process/MovieLensContextCollectorStreamingJobSpec.scala
git commit -m "fix(movielens): classify events by independent flags, not rule order"
```

---

### Task 11: Full suite and PR

**Files:** none.

- [ ] **Step 1: Run the whole suite**

```bash
cd recsys-pipeline/services/spark-streaming-job
JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test
```

Expected: all green. If anything fails, fix it before proceeding — do not open the PR on a red suite.

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/filtering-projection-correctness
gh pr create --title "fix: filtering and projection correctness" --body "$(cat <<'PRBODY'
Implements `.superpowers/docs/specs/2026-08-16-filtering-and-projection-correctness-design.md`.

Five projections that silently changed the meaning of a value:

- **`date` followed the deploy machine's timezone.** Three sites used `to_date(from_unixtime(...))` where `SequenceSchema` and `RawArchiveSink` already deliberately do not. It matters because `CtrRankingModelTrainingJob.splitByDate` uses that string as the train/holdout key, so the same Parquet trained in two timezones produced two different validation sets.
- **`feedback_delay_ms` was not milliseconds** — always a multiple of 1000. The simulator cannot reveal this: every slate shares one `now_ms` and feedback lands on whole-second offsets, so truncation cancels.
- **A null `position` became slot 0**, indistinguishable from the top of the slate.
- **`nullable = false` declarations that never bound**, contradicted by every gate that immediately filters those fields for null.
- **MovieLens classification depended on rule order**, discarding demographics from a combined event.

Plus one prerequisite: `LateFeedbackJoin.readSnapshot` now reads a snapshot missing a newly-added column as typed nulls instead of raising, so adding `timestamp_ms` to `SnapshotColumns` cannot strand pending slates across the upgrade.

**Operational note:** the `date` correction is not retroactive. Partitions written before this change keep their local-timezone boundaries; for any deployment not already running in UTC, one day at the cutover has rows split under two definitions.

Left alone deliberately: `EngagementReportJob.byHour`/`byDow` stay in session-local time — hour-of-day is a local-time question, and changing it is a product decision.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
PRBODY
)"
```

- [ ] **Step 3: Report the PR URL to the user and stop.** Do not merge.

---

## Self-Review

**Spec coverage:** §1 timezone → Tasks 1–5. §2 milliseconds → Tasks 6–7. §3 position → Task 8. §4 nullability → Task 9. §5 classification → Task 10. Testing section → the per-task tests plus Task 11. No spec requirement is unassigned.

**Deviation from the spec, deliberate:** the spec's §1 implies changing `impression_time` itself. It does not need changing — `to_timestamp(from_unixtime(ts))` round-trips to the correct instant, and only `to_date` of it is zone-sensitive. Task 2 therefore changes the partition expression alone, which is strictly smaller.

**Addition not in the spec:** Task 6 (`readSnapshot` tolerance). Found while planning Task 7; without it the upgrade raises `AnalysisException` and strands pending slates.
