# Late-Feedback Join Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `OnlineJoinerStreamingJob` join feedback to its impression across micro-batch boundaries, so a click or order arriving after its impression's batch produces one correctly labelled training sample instead of being dropped.

**Architecture:** A new `LateFeedbackJoin` becomes the joiner's batch stage. It keeps the raw events of slates whose feedback window is still open in a durable per-batch snapshot under the archive root, unions that snapshot with the incoming batch, publishes only the slates whose window has closed by feeding them to the existing `buildTrainingSamples`, and re-commits the remainder. The snapshot commit protocol is extracted out of `RawArchiveSink` so both stores share one implementation.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1 (`spark-sql`, DataFrame API, Hadoop `FileContext`), ScalaTest (`AnyFlatSpec`), sbt, bash, pytest.

**Spec:** `.superpowers/docs/specs/2026-08-11-late-feedback-join-design.md`

> **As built — three corrections the whole-branch review forced.** The spec records the final design.
>
> 1. **The wall-clock arm cannot drain an idle stream.** Task 2 and Task 5 were written believing
>    empty micro-batches still fire on the trigger interval. They do not: the joiner's streaming plan
>    is a stateless `foreachBatch`, and Spark constructs a no-data micro-batch only for a plan whose
>    stateful operators ask for one. When the topic goes quiet, no batch runs and `process` is never
>    called. The arm still bounds a slate's wait whenever batches are running; it cannot flush a
>    stopped stream. Task 5's sim window is therefore derived from the producer's maximum order delay
>    (`120 × scale`, not `180 × scale`) so the feedback still arriving is what closes every window.
> 2. **The drain floor outgrew `DRAIN_TIMEOUT`.** Task 5's floor scales with `FEEDBACK_DELAY_SCALE`
>    while the timeout did not, so past `scale ≈ 1.8` the drain expired below its own floor and killed
>    the joiner early. `DRAIN_TIMEOUT` now defaults to the larger of 600 and the floor plus 300.
> 3. **The event-time arm is clamped to the wall clock.** Task 2's rule compared every slate against a
>    global maximum event timestamp, so one clock-skewed future event flushed the entire pending store
>    at once. Observed event time may lag the wall clock, as an archive backfill does, but never runs
>    ahead of it.

## Global Constraints

- One row per `sample_id`, ever. Nothing published is ever restated. This is what keeps all eleven `training_samples` consumers unchanged.
- `buildTrainingSamples` keeps its current body, signature, and output columns. The join wraps it; it does not rewrite it.
- The Kafka key (`sample_id`), the Parquet `date` partition (`to_date(impression_time)`), and the `batch_id` column stay exactly as they are.
- Existing `_COMMITTED` manifest bytes must not change. `RawArchiveSink`'s manifest stays `version=2\nquery=<ns>\nkind=<kind>\nbatch_id=<id>\n` verbatim — archives on disk are validated byte-for-byte against it.
- `RawArchiveSinkSpec` (545 lines) and `ExecutionEngineSpec` must pass **unmodified**. They are the regression gate on the extraction.
- Do not modify `ExecutionEngine.scala`, `EngineConfig.scala`, the event schema, the Avro decoder, `run_replay`, or any consumer of `training_samples`.
- `FEEDBACK_JOIN_WAIT` defaults to `3 minutes`; `0 seconds` must reproduce the pre-change per-batch behaviour exactly.
- A retried micro-batch must publish exactly the rows it published before the retry.
- All Scala goes under `recsys-pipeline/services/spark-streaming-job/`. Run sbt from that directory.
- Preserve existing untracked `.ua/` directories and `recsys-pipeline/kafka.png`; they are outside this implementation.

## File Structure

- `src/main/scala/com/demo/engine/CommitProtocol.scala` — **new.** The snapshot commit protocol lifted verbatim out of `RawArchiveSink`: attempt directory, manifest with row count and SHA-256 inventory, atomic non-overwriting rename, validation on read.
- `src/main/scala/com/demo/engine/RawArchiveSink.scala` — **modified.** Delegates to `CommitProtocol`; keeps its own manifest strings and paths.
- `src/main/scala/com/demo/process/LateFeedbackJoin.scala` — **new.** The pending-slate store, the close rule, the due/open split, and the orphan counter.
- `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala` — **modified.** Builds the join, uses it as the batch stage, exposes `MeasurementFields` to the package.
- `src/test/scala/com/demo/engine/CommitProtocolSpec.scala` — **new.**
- `src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala` — **new.**
- `src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala` — **modified.** Comment only, on the two pinned cross-batch tests.
- `recsys-pipeline/scripts/run-movielens-segment-sim.sh`, `run-movie-category-sim.sh` — **modified.** Pass `FEEDBACK_JOIN_WAIT`; extend the parquet drain floor.
- `recsys-pipeline/integration-tests/test_service_scripts.py` — **modified.** Assert the new drain floor and the joiner's env wiring.
- `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`, `recsys-pipeline/README.md` — **modified.**

---

### Task 1: Extract the Commit Protocol

Pure refactor. No behaviour changes, no new features. `RawArchiveSinkSpec` passing unmodified is the proof.

**Files:**
- Create: `services/spark-streaming-job/src/main/scala/com/demo/engine/CommitProtocol.scala`
- Modify: `services/spark-streaming-job/src/main/scala/com/demo/engine/RawArchiveSink.scala`
- Test: `services/spark-streaming-job/src/test/scala/com/demo/engine/CommitProtocolSpec.scala`

**Interfaces:**
- Produces:
  - `CommitProtocol.CommitMarker: String` (`"_COMMITTED"`)
  - `CommitProtocol.writeDirectory(df: DataFrame, attemptsRoot: Path, finalPath: Path, partitionByDate: Boolean, description: String, expectedManifest: String): Unit`
  - `CommitProtocol.validateCommitted(fileSystem: FileSystem, directory: Path, expectedManifest: String): Unit`
  - `CommitProtocol.validateDataCommitted(fileSystem: FileSystem, directory: Path, expectedBase: String, expectedRowCount: Option[Long]): Unit`
  - `CommitProtocol.writeManifest(fileSystem: FileSystem, directory: Path, contents: String): Unit`
  - `CommitProtocol.hasParquetData(fileSystem: FileSystem, path: Path): Boolean`
- Consumed by: Task 2's `LateFeedbackJoin`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/CommitProtocolSpec.scala`:

```scala
package com.demo.engine

import java.nio.file.{Files, Paths}

import com.demo.SparkTestSupport
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommitProtocolSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val Manifest = "version=2\nquery=test-namespace\nkind=pending\nbatch_id=7\n"

  private def frame(values: Seq[(String, Long)]) = {
    val session = spark
    import session.implicits._
    values.toDF("event_id", "timestamp_ms")
  }

  private def commit(root: java.nio.file.Path, values: Seq[(String, Long)]): Path = {
    val finalPath = new Path(new Path(root.toString), "7")
    CommitProtocol.writeDirectory(
      frame(values),
      new Path(new Path(root.toString), "_attempts/7"),
      finalPath,
      partitionByDate = false,
      "pending snapshot 7",
      Manifest)
    finalPath
  }

  private def fileSystem(path: Path) =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)

  "CommitProtocol.writeDirectory" should "commit a readable, validating directory" in {
    val root = Files.createTempDirectory("commit-protocol")
    val committed = commit(root, Seq("a" -> 1L, "b" -> 2L))

    CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(2L))
    CommitProtocol.hasParquetData(fileSystem(committed), committed) shouldBe true
    spark.read.parquet(committed.toString).count() shouldBe 2L
  }

  it should "commit an empty directory without parquet data" in {
    val root = Files.createTempDirectory("commit-protocol-empty")
    val committed = commit(root, Seq.empty)

    CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(0L))
    CommitProtocol.hasParquetData(fileSystem(committed), committed) shouldBe false
  }

  it should "be a no-op when the same content is committed twice" in {
    val root = Files.createTempDirectory("commit-protocol-retry")
    commit(root, Seq("a" -> 1L, "b" -> 2L))
    commit(root, Seq("a" -> 1L, "b" -> 2L))

    val committed = new Path(new Path(root.toString), "7")
    CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(2L))
  }

  "CommitProtocol.validateDataCommitted" should "reject a directory whose inventory grew after the commit" in {
    val root = Files.createTempDirectory("commit-protocol-extra")
    val committed = commit(root, Seq("a" -> 1L))
    val extra = new Path(committed, "part-99-not-in-the-manifest.parquet")
    val output = fileSystem(extra).create(extra, false)
    try output.write(Array[Byte](1, 2, 3)) finally output.close()

    an[IllegalStateException] should be thrownBy
      CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(1L))
  }

  it should "never silently accept a tampered data file" in {
    val root = Files.createTempDirectory("commit-protocol-tamper")
    val committed = commit(root, Seq("a" -> 1L))
    val parquet = Files.list(Paths.get(committed.toString)).filter(_.toString.endsWith(".parquet"))
      .findFirst().orElseThrow(() => new AssertionError("no parquet file written"))
    Files.write(parquet, Files.readAllBytes(parquet) ++ Array[Byte](0))

    // LocalFileSystem keeps a .crc sidecar, so re-reading the corrupted file raises Hadoop's own
    // ChecksumException before the SHA-256 comparison runs. The exception type is Hadoop's, not
    // ours; what this pins is that validation never returns normally on a corrupted file.
    an[Exception] should be thrownBy
      CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(1L))
  }

  it should "reject a directory with no commit marker" in {
    val root = Files.createTempDirectory("commit-protocol-missing")
    val bare = new Path(new Path(root.toString), "7")
    fileSystem(bare).mkdirs(bare)

    an[IllegalStateException] should be thrownBy
      CommitProtocol.validateDataCommitted(fileSystem(bare), bare, Manifest, None)
  }
}
```

- [ ] **Step 2: Run the test to verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.engine.CommitProtocolSpec'`

Expected: compilation failure — `CommitProtocol` is not a member of `com.demo.engine`.

- [ ] **Step 3: Create `CommitProtocol` by moving code out of `RawArchiveSink`**

Create `src/main/scala/com/demo/engine/CommitProtocol.scala`. Move the bodies of `writeDirectory`, `writeManifest`, `validateCommitted`, `validateDataCommitted`, `dataManifest`, `parquetInventory`, `fileSha256`, `parseInventoryEntry`, `hasParquetData`, the `CommitMarker` constant, and the `ArchiveFileIdentity` case class **verbatim** — do not rewrite them, do not "improve" them. Only the enclosing object and the visibility change.

```scala
package com.demo.engine

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.UUID

import org.apache.hadoop.fs.{FileAlreadyExistsException, FileContext, FileSystem, Options, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.util.control.NonFatal

/** Atomic directory-commit protocol shared by every durable store in this engine.
  *
  * A commit writes a UUID attempt directory, stamps it with a manifest carrying the row count and
  * a SHA-256 inventory of every Parquet file, then renames it into place non-overwritingly.
  * Requires a filesystem with atomic, non-overwriting directory rename semantics (local/HDFS
  * through Hadoop FileContext).
  */
object CommitProtocol {

  val CommitMarker = "_COMMITTED"

  final case class ArchiveFileIdentity(path: String, size: Long, sha256: String)

  def writeDirectory(
      df: DataFrame,
      attemptsRoot: Path,
      finalPath: Path,
      partitionByDate: Boolean,
      description: String,
      expectedManifest: String
  ): Unit = {
    // body moved verbatim from RawArchiveSink.writeDirectory
  }

  def writeManifest(fileSystem: FileSystem, directory: Path, contents: String): Unit = {
    // body moved verbatim from RawArchiveSink.writeManifest
  }

  def validateCommitted(fileSystem: FileSystem, directory: Path, expectedManifest: String): Unit = {
    // body moved verbatim from RawArchiveSink.validateCommitted
  }

  def validateDataCommitted(
      fileSystem: FileSystem,
      directory: Path,
      expectedBase: String,
      expectedRowCount: Option[Long]
  ): Unit = {
    // body moved verbatim from RawArchiveSink.validateDataCommitted
  }

  def hasParquetData(fileSystem: FileSystem, path: Path): Boolean = {
    // body moved verbatim from the private RawArchiveSink object
  }

  private def dataManifest(base: String, rowCount: Long, files: Seq[ArchiveFileIdentity]): String = {
    // body moved verbatim
  }

  private def parquetInventory(fileSystem: FileSystem, directory: Path): Seq[ArchiveFileIdentity] = {
    // body moved verbatim
  }

  private def fileSha256(fileSystem: FileSystem, path: Path): String = {
    // body moved verbatim
  }

  private def parseInventoryEntry(value: String): Option[ArchiveFileIdentity] = {
    // body moved verbatim
  }
}
```

Note the moved `writeDirectory` body references `org.apache.spark.sql.functions` for nothing and `StorageLevel` for its persist — keep the imports it needs, drop the ones it does not.

- [ ] **Step 4: Delete the moved code from `RawArchiveSink` and delegate**

In `RawArchiveSink.scala`, delete the nine moved members and the `ArchiveFileIdentity` case class, then point the remaining call sites at `CommitProtocol`:

- `deduplicateValid` / `readPreviousDedupeState`: `validateDataCommitted(fileSystem, path, manifest("dedupe", batchId - 1L), None)` → `CommitProtocol.validateDataCommitted(...)`; `RawArchiveSink.hasParquetData(...)` → `CommitProtocol.hasParquetData(...)`.
- `writeDedupeState` and `writeBatch`: `writeDirectory(...)` → `CommitProtocol.writeDirectory(...)` with the same six arguments.
- `isBusinessSinkComplete` / `completeBusinessSink`: `validateCommitted(...)` → `CommitProtocol.validateCommitted(...)`, `writeManifest(...)` → `CommitProtocol.writeManifest(...)`.
- `RawArchiveSink.CommitMarker` → `CommitProtocol.CommitMarker`.

`completeBusinessSink` keeps its own attempt-directory/rename block — it commits a marker with no data, which `writeDirectory` does not model. Leave it exactly as it is apart from the two delegated calls.

Keep `manifest`, `queryRoot`, `dedupeRoot`, `businessSinkPath`, `businessSinkManifest`, `queryNamespace`, `toUtcDate`, and `watermarkCutoff` in `RawArchiveSink`. Their strings must not change.

- [ ] **Step 5: Verify GREEN and no regression**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.engine.*'`

Expected: `CommitProtocolSpec` passes; `RawArchiveSinkSpec`, `ExecutionEngineSpec`, `SinkSpec`, `EngineConfigSpec`, `SourceSpec`, `StageSpec`, `RedisSinkSpec` all pass with no source edits. If `RawArchiveSinkSpec` fails, the extraction was not verbatim — fix the extraction, never the spec.

- [ ] **Step 6: Confirm the refactor changed no manifest bytes**

Run: `cd recsys-pipeline/services/spark-streaming-job && git diff -- src/main/scala/com/demo/engine/RawArchiveSink.scala | grep -E '^\+.*(version=|kind=|batch_id=|query=|row_count=|file=)'`

Expected: no output. Any hit means a manifest string moved or changed and existing archives would fail validation.

- [ ] **Step 7: Commit**

```bash
git add services/spark-streaming-job/src/main/scala/com/demo/engine/CommitProtocol.scala \
        services/spark-streaming-job/src/main/scala/com/demo/engine/RawArchiveSink.scala \
        services/spark-streaming-job/src/test/scala/com/demo/engine/CommitProtocolSpec.scala
git commit -m "refactor: extract the snapshot commit protocol from RawArchiveSink"
```

---

### Task 2: `LateFeedbackJoin` — the Pending Store and the Close Rule

**Files:**
- Create: `services/spark-streaming-job/src/main/scala/com/demo/process/LateFeedbackJoin.scala`
- Modify: `services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala` (visibility of `MeasurementFields` only)
- Test: `services/spark-streaming-job/src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala`

**Interfaces:**
- Consumes: `CommitProtocol` from Task 1; `OnlineJoinerStreamingJob.buildTrainingSamples(df: DataFrame): DataFrame` and `OnlineJoinerStreamingJob.MeasurementFields: Seq[(String, DataType)]`.
- Produces:
  - `new LateFeedbackJoin(archiveRoot: String, queryNamespace: String, feedbackJoinWait: String)`
  - `LateFeedbackJoin.process(events: DataFrame, batchId: Long, nowMs: Long): DataFrame` — returns published training samples, identical in schema to `buildTrainingSamples`'s output.
  - `LateFeedbackJoin.waitSeconds(interval: String): Long` (companion object).
- Consumed by: Task 3 (orphan counting) and Task 4 (job wiring).

**Why `nowMs` is a parameter:** the wall-clock arm must be deterministic in tests, and the caller is the only place that should read the clock.

**Why the committed snapshot decides the split:** on a retry, `nowMs` differs, so recomputing the open set could produce a different one — and `CommitProtocol.writeDirectory` would then throw `commit inventory mismatch` against the already-committed snapshot, wedging the batch forever. Instead, when snapshot `N` already exists the join reads it back and publishes exactly its complement. The first attempt's split is authoritative.

- [ ] **Step 1: Write the failing tests**

Create `src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala`:

```scala
package com.demo.process

import java.nio.file.Files

import com.demo.SparkTestSupport
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LateFeedbackJoinSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val Columns = Seq("session_id", "request_id", "user_id", "item_id", "event_type",
    "timestamp", "position", "user_features", "item_features", "context_features")

  private val Empty = Map.empty[String, String]

  /** One event row in the shape the joiner's staged events arrive in. */
  private def event(
      itemId: String,
      eventType: String,
      timestamp: Long,
      requestId: String = "req_1"
  ) = ("sess_1", requestId, "user_1", itemId, eventType, timestamp, 0, Empty, Empty, Empty)

  private def batch(rows: Seq[(String, String, String, String, String, Long, Int,
      Map[String, String], Map[String, String], Map[String, String])]): DataFrame = {
    val session = spark
    import session.implicits._
    // A typed empty Seq still carries the schema, so limit(0) is not needed for the empty case.
    rows.toDF(Columns: _*)
  }

  private def join(wait: String): LateFeedbackJoin =
    new LateFeedbackJoin(
      Files.createTempDirectory("late-feedback-join").toString, "test-namespace", wait)

  private def labels(samples: DataFrame): Seq[(String, Int, Int, Double)] =
    samples.select("item_id", "clicked", "ordered", "label").collect()
      .map(row => (row.getString(0), row.getInt(1), row.getInt(2), row.getDouble(3))).toSeq

  "LateFeedbackJoin" should "join a click to an impression from an earlier batch" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    val first = subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs)
    val second = subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 1000L)
    val third = subject.process(batch(Seq.empty), 2L, startMs + 61000L)

    // Nothing publishes while the window is open.
    first.count() shouldBe 0L
    second.count() shouldBe 0L
    // The wall clock closes the window, and the click lands on its impression.
    labels(third) shouldBe Seq(("item_1", 1, 0, 1.0))
  }

  it should "close a window when event time advances past the deadline" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs).count() shouldBe 0L
    // req_2's impression at 1000 pushes observed event time past req_1's deadline of 160.
    val published = subject.process(
      batch(Seq(
        event("item_1", "click", 105L),
        event("item_9", "impression", 1000L, requestId = "req_2"))),
      1L, startMs + 1000L)

    // req_1 is due and correctly labelled; req_2's own window (deadline 1060) is still open.
    labels(published) shouldBe Seq(("item_1", 1, 0, 1.0))
  }

  it should "land a click and a later order on one published sample" in {
    val subject = join("120 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs).count() shouldBe 0L
    subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 5000L).count() shouldBe 0L
    subject.process(batch(Seq(event("item_1", "order", 190L))), 2L, startMs + 90000L).count() shouldBe 0L
    val published = subject.process(batch(Seq.empty), 3L, startMs + 121000L)

    labels(published) shouldBe Seq(("item_1", 1, 1, 2.0))
  }

  it should "publish nothing for feedback whose impression never arrives" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "click", 105L))), 0L, startMs).count() shouldBe 0L
    subject.process(batch(Seq.empty), 1L, startMs + 61000L).count() shouldBe 0L
  }

  it should "reproduce per-batch behaviour when the wait is zero" in {
    val subject = join("0 seconds")

    val together = subject.process(
      batch(Seq(event("item_1", "impression", 100L), event("item_1", "click", 105L))), 0L, 1000000L)
    val feedbackOnly = subject.process(batch(Seq(event("item_2", "click", 205L))), 1L, 1000000L)

    labels(together) shouldBe Seq(("item_1", 1, 0, 1.0))
    feedbackOnly.count() shouldBe 0L
  }

  it should "publish the same rows when a batch is retried under a later clock" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(batch(Seq(event("item_1", "impression", 100L))), 0L, startMs).count() shouldBe 0L
    val attempt = subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 1000L)
      .collect().toSeq
    // A retry of batch 1 runs with a clock past the deadline; the committed split still wins.
    val retry = subject.process(batch(Seq(event("item_1", "click", 105L))), 1L, startMs + 61000L)
      .collect().toSeq

    attempt shouldBe Seq.empty[Row]
    retry shouldBe attempt
  }

  "LateFeedbackJoin.waitSeconds" should "parse the watermark interval syntax" in {
    LateFeedbackJoin.waitSeconds("3 minutes") shouldBe 180L
    LateFeedbackJoin.waitSeconds("0 seconds") shouldBe 0L
    LateFeedbackJoin.waitSeconds("2 hours") shouldBe 7200L
  }

  it should "reject a negative or month-based wait" in {
    an[IllegalArgumentException] should be thrownBy LateFeedbackJoin.waitSeconds("-5 seconds")
    an[IllegalArgumentException] should be thrownBy LateFeedbackJoin.waitSeconds("1 month")
  }
}
```

- [ ] **Step 2: Run the tests to verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.process.LateFeedbackJoinSpec'`

Expected: compilation failure — `LateFeedbackJoin` is not a member of `com.demo.process`.

- [ ] **Step 3: Widen `MeasurementFields` to the package**

In `OnlineJoinerStreamingJob.scala`, change the one line:

```scala
  private val MeasurementFields: Seq[(String, DataType)] = Seq(
```

to:

```scala
  private[process] val MeasurementFields: Seq[(String, DataType)] = Seq(
```

Change nothing else in that file in this task.

- [ ] **Step 4: Implement `LateFeedbackJoin`**

Create `src/main/scala/com/demo/process/LateFeedbackJoin.scala`:

```scala
package com.demo.process

import com.demo.engine.CommitProtocol
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType}
import org.apache.spark.storage.StorageLevel

/** Joins feedback to its impression across micro-batch boundaries.
  *
  * A slate's raw events are held in a durable per-batch snapshot until its feedback window closes,
  * then handed to `buildTrainingSamples` unchanged. Holding raw events — rather than merged
  * partial aggregates — means the aggregation, including `max_by` last-feedback-wins and its
  * `(timestamp, event_id)` tiebreak, is reused exactly as it is already tested.
  *
  * `queryNamespace` must be the archive's own namespace so the store shares its query root, and
  * must not be shared by concurrently active queries.
  */
class LateFeedbackJoin(archiveRoot: String, queryNamespace: String, feedbackJoinWait: String) {

  require(Option(queryNamespace).exists(_.trim.nonEmpty), "queryNamespace must not be blank")

  private val waitSeconds: Long = LateFeedbackJoin.waitSeconds(feedbackJoinWait)

  private val pendingRoot: Path =
    new Path(new Path(new Path(archiveRoot), s"_queries/$queryNamespace"), "_pending")

  private val slateKeys: Seq[String] = Seq("request_id", "user_id", "item_id")

  /** Publish the slates whose window has closed; hold the rest for a later batch.
    *
    * `nowMs` is the caller's wall clock, passed in so the close rule stays testable.
    */
  def process(events: DataFrame, batchId: Long, nowMs: Long): DataFrame = {
    val prepared = normalize(events, nowMs)
    val all = prepared.unionByName(readPending(prepared, batchId))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      val open = commitOpenSlates(all, batchId, nowMs)
      // localCheckpoint materializes the due rows and cuts their lineage, so the samples this
      // returns carry no dependency on a snapshot directory that compaction may later delete.
      // Its blocks are released when the RDD is garbage collected; losing them costs a batch
      // retry, which recomputes from the committed snapshot anyway.
      val due = all.join(open.select(slateKeys.map(col): _*).distinct(), slateKeys, "left_anti")
        .drop("first_seen_ms")
        .localCheckpoint(eager = true)
      compactOlderSnapshots(all, batchId)
      OnlineJoinerStreamingJob.buildTrainingSamples(due)
    } finally all.unpersist()
  }

  /** Project to the stable snapshot schema and stamp arrival time on rows entering the store. */
  private def normalize(events: DataFrame, nowMs: Long): DataFrame = {
    val withEventId =
      if (events.columns.contains("event_id")) events
      else events.withColumn("event_id", lit(null).cast(StringType))
    val complete = OnlineJoinerStreamingJob.MeasurementFields.foldLeft(withEventId) {
      case (df, (name, dataType)) =>
        if (df.columns.contains(name)) df else df.withColumn(name, lit(null).cast(dataType))
    }
    complete
      .select(LateFeedbackJoin.SnapshotColumns.map(col): _*)
      .withColumn("first_seen_ms", lit(nowMs).cast(LongType))
  }

  /** Commit this batch's snapshot of still-open slates, or reuse the one a prior attempt wrote. */
  private def commitOpenSlates(all: DataFrame, batchId: Long, nowMs: Long): DataFrame = {
    val path = new Path(pendingRoot, batchId.toString)
    val fileSystem = path.getFileSystem(all.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) {
      val slates = all.groupBy(slateKeys.map(col): _*).agg(
        max(when(LateFeedbackJoin.isImpression, col("timestamp"))).as("impression_ts"),
        min(col("timestamp")).as("first_event_ts"),
        min(col("first_seen_ms")).as("slate_first_seen_ms"))
      val latest = all.agg(max(col("timestamp"))).first()
      val deadline = coalesce(col("impression_ts"), col("first_event_ts")) + lit(waitSeconds)
      val eventTimeDue = if (latest.isNullAt(0)) lit(false) else lit(latest.getLong(0)) >= deadline
      val wallClockDue = lit(nowMs) - col("slate_first_seen_ms") >= lit(waitSeconds * 1000L)
      val openKeys = slates
        .filter(not(coalesce(eventTimeDue || wallClockDue, lit(false))))
        .select(slateKeys.map(col): _*)
      writeSnapshot(all.join(openKeys, slateKeys, "left_semi"), batchId)
    }
    readSnapshot(all, path, batchId)
  }

  private def writeSnapshot(open: DataFrame, batchId: Long): Unit =
    CommitProtocol.writeDirectory(
      open,
      new Path(pendingRoot, s"_attempts/$batchId"),
      new Path(pendingRoot, batchId.toString),
      partitionByDate = false,
      s"pending slate snapshot $batchId",
      manifest(batchId))

  /** Read the snapshot written by the previous batch; empty before the first one exists. */
  private def readPending(template: DataFrame, batchId: Long): DataFrame = {
    if (batchId <= 0L) return template.limit(0)
    val path = new Path(pendingRoot, (batchId - 1L).toString)
    val fileSystem = path.getFileSystem(template.sparkSession.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(path)) template.limit(0) else readSnapshot(template, path, batchId - 1L)
  }

  private def readSnapshot(template: DataFrame, path: Path, batchId: Long): DataFrame = {
    val fileSystem = path.getFileSystem(template.sparkSession.sparkContext.hadoopConfiguration)
    CommitProtocol.validateDataCommitted(fileSystem, path, manifest(batchId), None)
    if (!CommitProtocol.hasParquetData(fileSystem, path)) template.limit(0)
    else template.sparkSession.read.parquet(path.toString)
      .select(template.columns.map(col): _*)
  }

  /** Batch N needs only N-1 and N: a restart replays N and rereads N-1. */
  private def compactOlderSnapshots(df: DataFrame, batchId: Long): Unit =
    try {
      val fileSystem = pendingRoot.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
      if (fileSystem.exists(pendingRoot))
        fileSystem.listStatus(pendingRoot).iterator
          .filter(_.isDirectory)
          .filter(status => status.getPath.getName.forall(_.isDigit))
          .filter(_.getPath.getName.toLong < batchId - 1L)
          .foreach(status => fileSystem.delete(status.getPath, true))
    } catch {
      case scala.util.control.NonFatal(_) => ()
    }

  private def manifest(batchId: Long): String =
    s"version=2\nquery=$queryNamespace\nkind=pending\nbatch_id=$batchId\n"
}

object LateFeedbackJoin {

  /** The columns a snapshot carries: everything `buildTrainingSamples` reads, in a fixed order so
    * a snapshot written by one batch unions cleanly with the next. */
  val SnapshotColumns: Seq[String] =
    Seq("session_id", "request_id", "user_id", "item_id", "event_type", "timestamp", "position",
      "event_id", "user_features", "item_features", "context_features") ++
      OnlineJoinerStreamingJob.MeasurementFields.map(_._1)

  def isImpression: org.apache.spark.sql.Column =
    lower(trim(col("event_type"))).isin("impression", "exposure")

  /** Parse the wait using the same interval syntax as EVENT_WATERMARK_DELAY. */
  def waitSeconds(interval: String): Long = {
    val parsed = IntervalUtils.fromIntervalString(interval)
    require(parsed.months == 0,
      s"FEEDBACK_JOIN_WAIT must not use months, whose length is ambiguous: $interval")
    val seconds = parsed.days.toLong * 86400L + parsed.microseconds / 1000000L
    require(seconds >= 0L, s"FEEDBACK_JOIN_WAIT must not be negative: $interval")
    seconds
  }
}
```

- [ ] **Step 5: Run the tests to verify GREEN**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.process.LateFeedbackJoinSpec'`

Expected: all nine assertions pass.

If "close a window when event time advances" fails with both slates published, the deadline is being compared against the wrong column — it must be `coalesce(impression_ts, first_event_ts)`, per slate, not a global minimum.

- [ ] **Step 6: Confirm the joiner's own suite still passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.process.OnlineJoinerStreamingJobSpec'`

Expected: all pass unchanged — this task only widened a `private` to `private[process]`.

- [ ] **Step 7: Commit**

```bash
git add services/spark-streaming-job/src/main/scala/com/demo/process/LateFeedbackJoin.scala \
        services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
        services/spark-streaming-job/src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala
git commit -m "feat: join late feedback to its impression across micro-batches"
```

---

### Task 3: Count and Log Orphan Feedback

Feedback that comes due with no impression is dropped, exactly as before. This task makes the residual loss measurable.

**Files:**
- Modify: `services/spark-streaming-job/src/main/scala/com/demo/process/LateFeedbackJoin.scala`
- Test: `services/spark-streaming-job/src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala`

**Interfaces:**
- Produces: `LateFeedbackJoin.formatOrphans(batchId: Long, slates: Long, events: Long): String`.

Follows `BatchMetricsListener`: a pure `format` function that tests assert on, logged through slf4j.

- [ ] **Step 1: Write the failing tests**

Append to `LateFeedbackJoinSpec.scala`:

```scala
  "LateFeedbackJoin.formatOrphans" should "name the batch and both counts" in {
    LateFeedbackJoin.formatOrphans(7L, 2L, 3L) shouldBe
      "[late-feedback] batch=7 orphan_slates=2 orphan_events=3"
  }

  "LateFeedbackJoin" should "count a due slate that never received an impression" in {
    val subject = join("60 seconds")
    val startMs = 1000000L

    subject.process(
      batch(Seq(event("item_1", "click", 105L), event("item_1", "order", 110L))), 0L, startMs)
      .count() shouldBe 0L
    subject.orphanCounts shouldBe (0L, 0L)

    subject.process(batch(Seq.empty), 1L, startMs + 61000L).count() shouldBe 0L
    subject.orphanCounts shouldBe (1L, 2L)
  }

  it should "count no orphans when every due slate has its impression" in {
    val subject = join("0 seconds")

    subject.process(
      batch(Seq(event("item_1", "impression", 100L), event("item_1", "click", 105L))), 0L, 1000000L)
      .count() shouldBe 1L
    subject.orphanCounts shouldBe (0L, 0L)
  }
```

- [ ] **Step 2: Run the tests to verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.process.LateFeedbackJoinSpec'`

Expected: compilation failure — `formatOrphans` and `orphanCounts` do not exist.

- [ ] **Step 3: Implement the counter**

In `LateFeedbackJoin.scala`, add the logger and the last-batch counts to the class:

```scala
  private val log = org.slf4j.LoggerFactory.getLogger(getClass)

  @volatile private var lastOrphanSlates: Long = 0L
  @volatile private var lastOrphanEvents: Long = 0L

  /** The most recent batch's (slates, events) orphan counts. Exposed for tests and diagnostics. */
  def orphanCounts: (Long, Long) = (lastOrphanSlates, lastOrphanEvents)

  private def countOrphans(due: DataFrame, batchId: Long): Unit = {
    val orphanKeys = due.groupBy(slateKeys.map(col): _*)
      .agg(max(when(LateFeedbackJoin.isImpression, lit(1)).otherwise(lit(0))).as("has_impression"))
      .filter(col("has_impression") === 0)
      .select(slateKeys.map(col): _*)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
    try {
      lastOrphanSlates = orphanKeys.count()
      // Only pay for the second count when there is something to report.
      lastOrphanEvents =
        if (lastOrphanSlates == 0L) 0L
        else due.join(orphanKeys, slateKeys, "left_semi").count()
      if (lastOrphanSlates > 0L)
        log.info(LateFeedbackJoin.formatOrphans(batchId, lastOrphanSlates, lastOrphanEvents))
    } finally orphanKeys.unpersist()
  }
```

Call it in `process`, between the split and the publish:

```scala
      val due = all.join(open.select(slateKeys.map(col): _*).distinct(), slateKeys, "left_anti")
        .drop("first_seen_ms")
        .localCheckpoint(eager = true)
      countOrphans(due, batchId)
      compactOlderSnapshots(all, batchId)
```

And add to the companion object:

```scala
  def formatOrphans(batchId: Long, slates: Long, events: Long): String =
    s"[late-feedback] batch=$batchId orphan_slates=$slates orphan_events=$events"
```

- [ ] **Step 4: Run the tests to verify GREEN**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch 'testOnly com.demo.process.LateFeedbackJoinSpec'`

Expected: all pass, including the six from Task 2.

- [ ] **Step 5: Commit**

```bash
git add services/spark-streaming-job/src/main/scala/com/demo/process/LateFeedbackJoin.scala \
        services/spark-streaming-job/src/test/scala/com/demo/process/LateFeedbackJoinSpec.scala
git commit -m "feat: count and log feedback that comes due with no impression"
```

---

### Task 4: Wire the Join into `OnlineJoinerStreamingJob`

**Files:**
- Modify: `services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
- Modify: `services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala` (comment only)

**Interfaces:**
- Consumes: `LateFeedbackJoin` from Tasks 2 and 3, `RawArchiveSink.stableQueryNamespace`.
- Produces: the `FEEDBACK_JOIN_WAIT` environment variable, default `3 minutes`.

- [ ] **Step 1: Hoist the archive root and build the join**

In `main`, replace:

```scala
    val archive = new RawArchiveSink(
      sys.env.getOrElse("RECSYS_EVENT_ARCHIVE_PATH", "/tmp/spark-recsys/recsys-events-archive"),
      sys.env.getOrElse("RECSYS_EVENT_DEAD_LETTER_PATH", "/tmp/spark-recsys/recsys-events-dead-letter"),
      cfg.checkpointLocation
    )
```

with:

```scala
    val archiveRoot =
      sys.env.getOrElse("RECSYS_EVENT_ARCHIVE_PATH", "/tmp/spark-recsys/recsys-events-archive")
    val archive = new RawArchiveSink(
      archiveRoot,
      sys.env.getOrElse("RECSYS_EVENT_DEAD_LETTER_PATH", "/tmp/spark-recsys/recsys-events-dead-letter"),
      cfg.checkpointLocation
    )
    // Feedback arriving within this window joins its impression; a sample publishes once, when
    // its window closes. "0 seconds" restores the pre-change per-batch behaviour.
    val lateFeedbackJoin = new LateFeedbackJoin(
      archiveRoot,
      archive.stableQueryNamespace,
      sys.env.getOrElse("FEEDBACK_JOIN_WAIT", "3 minutes"))
```

- [ ] **Step 2: Use the join as the batch stage**

Replace:

```scala
    val batchStages: Seq[BatchStage] =
      Seq((df: DataFrame, id: Long) => buildTrainingSamples(df).withColumn("batch_id", lit(id)))
```

with:

```scala
    val batchStages: Seq[BatchStage] =
      Seq((df: DataFrame, id: Long) =>
        lateFeedbackJoin.process(df, id, System.currentTimeMillis()).withColumn("batch_id", lit(id)))
```

- [ ] **Step 3: Label the pinned per-batch tests**

In `OnlineJoinerStreamingJobSpec.scala`, above `it should "drop feedback whose impression fell in an earlier batch"`, add:

```scala
  // buildTrainingSamples is batch-local by design and stays that way. LateFeedbackJoin is the
  // layer that spans batches: see LateFeedbackJoinSpec for the end-to-end cross-batch guarantee.
```

Change no assertion in this file.

- [ ] **Step 4: Verify the job compiles and every Scala suite passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt -batch test`

Expected: all suites pass.

- [ ] **Step 5: Commit**

```bash
git add services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala \
        services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "feat: run the online joiner through the late-feedback join"
```

---

### Task 5: Teach the Sims to Wait for the Join

Both sims drain the joiner's Parquet output on a floor of `150s × FEEDBACK_DELAY_SCALE`. The joiner now holds each slate a further `FEEDBACK_JOIN_WAIT` past its impression, so that floor would end the drain with samples still pending.

**Files:**
- Modify: `recsys-pipeline/scripts/run-movielens-segment-sim.sh`
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh`
- Test: `recsys-pipeline/integration-tests/test_service_scripts.py`

**Interfaces:**
- Consumes: `FEEDBACK_JOIN_WAIT` from Task 4.
- Produces: shell variables `FEEDBACK_JOIN_WAIT_SECONDS` and `SAMPLE_DRAIN_SECONDS` in both sims.

**Note:** `/bin/bash` on macOS is 3.2 — no associative arrays, no floating-point arithmetic. Scale with `awk`, exactly as `FEEDBACK_TAIL_SECONDS` already does.

- [ ] **Step 1: Write the failing tests**

In `integration-tests/test_service_scripts.py`, replace the body of `test_sim_drain_waits_out_the_feedback_tail` with:

```python
@pytest.mark.parametrize("sim", SIMS)
def test_sim_drain_waits_out_the_feedback_tail(sim: str) -> None:
    """Stability alone declares completion in ~18s, well before a 120s order arrives — and the
    joiner then holds the slate a further FEEDBACK_JOIN_WAIT before publishing it."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    assert "FEEDBACK_TAIL_SECONDS" in script
    assert "min_wait" in script
    # A plain substring check would pass even if no drain call site actually forwarded the
    # floor — assert the joiner's parquet drain receives $SAMPLE_DRAIN_SECONDS as its min_wait.
    assert re.search(
        r'drain parquet .*"\$SAMPLE_DRAIN_SECONDS"', script
    ), "the joiner's drain call must receive $SAMPLE_DRAIN_SECONDS as its min_wait"
    # ...and that the floor is built from both waits, not just the producer's tail.
    assert re.search(
        r'SAMPLE_DRAIN_SECONDS=.*FEEDBACK_TAIL_SECONDS', script, re.DOTALL
    ), "the drain floor must include the producer's feedback tail"
    assert re.search(
        r'SAMPLE_DRAIN_SECONDS=.*FEEDBACK_JOIN_WAIT_SECONDS', script, re.DOTALL
    ), "the drain floor must include the joiner's feedback window"


@pytest.mark.parametrize("sim", SIMS)
def test_sim_passes_the_join_wait_to_the_joiner(sim: str) -> None:
    """A sim that scales the producer's feedback tail must scale the joiner's window with it,
    or a compressed run waits three real minutes for every sample."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    assert re.search(
        r'FEEDBACK_JOIN_WAIT_SECONDS="\$\{FEEDBACK_JOIN_WAIT_SECONDS:-\$\(awk', script
    ), "the join wait must scale with FEEDBACK_DELAY_SCALE like the tail does"
    joiner_call = re.search(
        r'start_job com\.demo\.process\.OnlineJoinerStreamingJob.*?\n\n', script, re.DOTALL
    )
    assert joiner_call, "expected the joiner start_job call"
    assert "FEEDBACK_JOIN_WAIT=" in joiner_call.group(0), \
        "the joiner must receive FEEDBACK_JOIN_WAIT"
```

- [ ] **Step 2: Run the tests to verify RED**

Run: `cd recsys-pipeline && python -m pytest -q integration-tests/test_service_scripts.py -k "feedback_tail or join_wait"`

Expected: four failures (two sims × two tests) — `SAMPLE_DRAIN_SECONDS` and `FEEDBACK_JOIN_WAIT_SECONDS` do not exist.

- [ ] **Step 3: Add the knobs to both sims**

In each of `scripts/run-movielens-segment-sim.sh` and `scripts/run-movie-category-sim.sh`, immediately after the existing `FEEDBACK_TAIL_SECONDS` assignment, add:

```bash
# The joiner holds each slate FEEDBACK_JOIN_WAIT past its impression before publishing its
# sample, so it scales with FEEDBACK_DELAY_SCALE exactly like the producer's tail does.
FEEDBACK_JOIN_WAIT_SECONDS="${FEEDBACK_JOIN_WAIT_SECONDS:-$(awk -v scale="$FEEDBACK_DELAY_SCALE" \
  'BEGIN { v = 180 * scale; if (v < 10) v = 10; printf "%d", v }')}"
# The parquet drain must outlast the last order's arrival *and* the window the joiner then holds
# it for, plus one trigger interval for the publishing batch itself.
SAMPLE_DRAIN_SECONDS="${SAMPLE_DRAIN_SECONDS:-$((FEEDBACK_TAIL_SECONDS + FEEDBACK_JOIN_WAIT_SECONDS + 10))}"
```

- [ ] **Step 4: Pass the window to the joiner and widen the drain**

In each sim, add the env argument to the joiner's `start_job` call. In `run-movie-category-sim.sh`:

```bash
start_job com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC" \
  "ONLINE_JOINER_OUTPUT_TOPIC=$SAMPLES_TOPIC" \
  "FEEDBACK_JOIN_WAIT=$FEEDBACK_JOIN_WAIT_SECONDS seconds"
```

In `run-movielens-segment-sim.sh` the same call has two env arguments, not three:

```bash
start_job com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC" \
  "FEEDBACK_JOIN_WAIT=$FEEDBACK_JOIN_WAIT_SECONDS seconds"
```

Then change the parquet drain in both scripts from:

```bash
drain parquet "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 "$FEEDBACK_TAIL_SECONDS"
```

to:

```bash
drain parquet "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 "$SAMPLE_DRAIN_SECONDS"
```

- [ ] **Step 5: Run the tests to verify GREEN**

Run: `cd recsys-pipeline && python -m pytest -q integration-tests/test_service_scripts.py`

Expected: all pass, including `test_sim_starts_jobs_before_producing`, `test_sim_exit_trap_covers_every_job_pid`, and `test_movie_category_sim_wires_every_measurement_input`.

- [ ] **Step 6: Check both scripts still parse**

Run: `cd recsys-pipeline && bash -n scripts/run-movielens-segment-sim.sh && bash -n scripts/run-movie-category-sim.sh`

Expected: no output.

- [ ] **Step 7: Commit**

```bash
git add scripts/run-movielens-segment-sim.sh scripts/run-movie-category-sim.sh \
        integration-tests/test_service_scripts.py
git commit -m "test: hold each sim's drain open for the joiner's feedback window"
```

---

### Task 6: Document, Verify, and Open the PR

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`
- Modify: `recsys-pipeline/README.md`

- [ ] **Step 1: Replace the "dropped" section in `Data_Pipeline.md`**

Find the section added yesterday, `#### Feedback that arrives in a later micro-batch is dropped`, in the `OnlineJoinerStreamingJob` part of the document. Replace the heading and its whole body with:

```markdown
#### Feedback that arrives in a later micro-batch still joins

`buildTrainingSamples` is batch-local: step 2 drops groups with no impression in the current batch.
`LateFeedbackJoin` wraps it and spans batches. A slate's raw events are held in a durable pending
snapshot under `<archive>/_queries/<namespace>/_pending/<batchId>` until its feedback window closes,
and only then handed to `buildTrainingSamples`. A click or order arriving within
`FEEDBACK_JOIN_WAIT` of its impression therefore lands on the same training sample.

Each sample is published exactly once, when its window closes, so every `sample_id` remains unique
and no consumer of `training_samples` has to dedupe. The cost is latency: a sample reaches the topic
and the Parquet sink one `FEEDBACK_JOIN_WAIT` after its impression rather than in the impression's
own batch. Its `date` partition is still `to_date(impression_time)`, so it lands in its impression's
date regardless of when it publishes.

A slate's window closes when either arm fires: observed event time advances past
`impression_ts + FEEDBACK_JOIN_WAIT`, or the slate has been held that long in wall-clock time. The
event-time arm keeps an archive backfill fast; the wall-clock arm drains the store when the stream
goes idle, which is what stops a finished sim from leaving samples pending forever.

Feedback arriving *more* than `FEEDBACK_JOIN_WAIT` after its impression is still dropped — the
sample it belongs to has already published, and the one-row contract rules out restating it. That
residual is counted rather than silent; each batch that sees any logs

    [late-feedback] batch=<id> orphan_slates=<n> orphan_events=<n>

`EVENT_WATERMARK_DELAY` remains unrelated to this: it governs deduplication only, not join buffering.
```

- [ ] **Step 2: Document the knob in `README.md`**

In the env-var table, next to the other simulation entries added for `FEEDBACK_DELAY_SCALE` and `FEEDBACK_TAIL_SECONDS`, add:

```markdown
| `FEEDBACK_JOIN_WAIT` | `3 minutes` | `OnlineJoinerStreamingJob` | How long a slate's feedback window stays open before its training sample publishes. Feedback arriving inside the window joins its impression; feedback after it is dropped and counted. `0 seconds` restores the old per-batch behaviour. Both sims scale it with `FEEDBACK_DELAY_SCALE` |
```

- [ ] **Step 3: Run every relevant suite**

```bash
cd recsys-pipeline/services/spark-streaming-job && sbt -batch test
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline && \
  python -m pytest -q integration-tests/python_modeling/ integration-tests/test_service_scripts.py
```

Expected: all pass. The Avro/Kafka round-trip test skips without a broker.

- [ ] **Step 4: Confirm scope**

Run: `cd /Users/linghuang/Git/Recsys-Streaming-Pipeline && git diff --stat master`

Expected: only `CommitProtocol.scala`, `RawArchiveSink.scala`, `LateFeedbackJoin.scala`, `OnlineJoinerStreamingJob.scala`, the four test files, the two sim scripts, `test_service_scripts.py`, `Data_Pipeline.md`, `README.md`, and the two `.superpowers/docs/` documents. Specifically **not** `ExecutionEngine.scala`, `EngineConfig.scala`, any event schema, or any consumer of `training_samples`.

- [ ] **Step 5: Commit the documentation**

```bash
git add docs/recommendation_architecture/Data_Pipeline.md README.md
git commit -m "docs: document the cross-batch feedback join and its window"
```

- [ ] **Step 6: Request code review**

Use superpowers:requesting-code-review. Ask specifically about the two properties this change must not break:

1. Can any `sample_id` be published twice — across a retry, a restart from checkpoint, or a slate whose events span more than two batches?
2. Can a slate be dropped without ever publishing — held in a snapshot that is then compacted, or excluded from both the due set and the next snapshot?

- [ ] **Step 7: Open the PR**

Open a PR against `master` from `feature/late-feedback-join`. Do not merge; wait for the user.

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: §1 one-row semantics → Tasks 2 and 4; §2 the store → Task 2; §3 the close rule → Task 2; §4 orphan counting → Task 3; §5 configuration → Tasks 2 (`waitSeconds` validation) and 4 (the env var); §6 the extraction → Task 1; §7 the sims → Task 5; Testing and Success Criteria → the specs written in Tasks 1–3 plus Task 6's full run; Accepted Limitations → documented in Task 6.

**Type consistency.** `process(events, batchId, nowMs)`, `waitSeconds(interval)`, `formatOrphans(batchId, slates, events)`, `orphanCounts`, `SnapshotColumns`, and `isImpression` are used in Tasks 3–5 exactly as Task 2 defines them. `CommitProtocol`'s six public members are used in Task 2 exactly as Task 1 defines them.
