# Spark Failure Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make shuffle spill, task failures, and stage retries visible in every Spark job, add opt-in event logging, and derive the shuffle-partition default from the master instead of a constant tuned for a laptop.

**Architecture:** Three changes attach at `SparkSessions.create`, the seam all ~20 jobs already pass through, so none of them needs per-job wiring. A new `SparkListener` reports per-stage spill, shuffle volume, failed tasks and attempt number; event logging becomes available behind an env flag that is off by default; and `shufflePartitionsFor` picks the partition count from the master string.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1 (`Provided`), ScalaTest 3.2.18, sbt. No new dependencies.

**Spec:** `.superpowers/docs/specs/2026-08-31-spark-failure-observability-design.md`

## Global Constraints

- **Tests require JDK 17.** The default JDK 25 aborts Spark-session tests with a misleading `getSubject` error. Run everything as
  `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly <spec>"`.
- **No new library dependencies.** Everything here uses Spark's own listener API and the standard library.
- **Defaults are inert.** Running any existing job unchanged must produce no new INFO output and no behavior change. `SPARK_EVENT_LOG_ENABLED` defaults `false`; `local[*]` keeps its current partition count.
- **`ClusterShufflePartitions = 200`**, applied only to non-local masters. `SPARK_SQL_SHUFFLE_PARTITIONS` overrides everything, exactly as today.
- **Comments explain *why*, not *what*** — match the density and voice of `DropMetrics.scala`, which is the closest sibling to this work.
- **Prefer pure functions over session-level tests.** `SparkSessions.create` calls `getOrCreate`, which returns whatever session another suite already built in this JVM. `SparkSessionsSpec` says so in a comment and deliberately asserts constants rather than wiring. Any logic worth testing must be reachable without constructing a session.

---

### Task 1: SpillMetrics — formatting and emission policy

The pure core: how a stage's numbers are rendered, and the rule deciding whether a stage is worth an INFO line. No Spark types, no session, tests run in milliseconds.

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/SpillMetrics.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/SpillMetricsSpec.scala`

**Interfaces:**
- Produces:
  - `final case class StageCost(job: String, stageId: Int, attempt: Int, tasks: Int, spillMemBytes: Long, spillDiskBytes: Long, shuffleWriteBytes: Long, shuffleReadBytes: Long, failedTasks: Int)`
  - `def humanBytes(n: Long): String`
  - `def format(c: StageCost): String`
  - `def worthInfo(c: StageCost): Boolean`

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpillMetricsSpec extends AnyFlatSpec with Matchers {

  private def cost(
      spillMem: Long = 0L, spillDisk: Long = 0L, failed: Int = 0, attempt: Int = 0) =
    SpillMetrics.StageCost(
      job = "SessionReportJob", stageId = 4, attempt = attempt, tasks = 8,
      spillMemBytes = spillMem, spillDiskBytes = spillDisk,
      shuffleWriteBytes = 2100000000L, shuffleReadBytes = 2100000000L, failedTasks = failed)

  "humanBytes" should "render plain bytes below a kilobyte" in {
    SpillMetrics.humanBytes(0L) shouldBe "0B"
    SpillMetrics.humanBytes(512L) shouldBe "512B"
  }

  it should "step through K, M and G at the boundaries" in {
    SpillMetrics.humanBytes(1024L) shouldBe "1.0K"
    SpillMetrics.humanBytes(1024L * 1024L) shouldBe "1.0M"
    SpillMetrics.humanBytes(1024L * 1024L * 1024L) shouldBe "1.0G"
  }

  it should "not overflow on a value larger than Int.MaxValue" in {
    // 8 GiB. Any intermediate Int arithmetic wraps negative here.
    SpillMetrics.humanBytes(8L * 1024L * 1024L * 1024L) shouldBe "8.0G"
  }

  "format" should "name the job, stage, attempt and every cost" in {
    val line = SpillMetrics.format(cost(spillMem = 1288490188L, spillDisk = 880803840L))
    line should include("[spill-metrics]")
    line should include("job=SessionReportJob")
    line should include("stage=4")
    line should include("attempt=0")
    line should include("tasks=8")
    line should include("spillMem=1.2G")
    line should include("spillDisk=840.0M")
    line should include("failedTasks=0")
  }

  "worthInfo" should "stay quiet for a clean stage" in {
    // A streaming query produces stages forever; an INFO line per clean stage would bury the
    // lines this listener exists to surface.
    SpillMetrics.worthInfo(cost()) shouldBe false
  }

  it should "fire on memory spill" in {
    SpillMetrics.worthInfo(cost(spillMem = 1L)) shouldBe true
  }

  it should "fire on disk spill" in {
    SpillMetrics.worthInfo(cost(spillDisk = 1L)) shouldBe true
  }

  it should "fire on a failed task" in {
    SpillMetrics.worthInfo(cost(failed = 1)) shouldBe true
  }

  it should "fire on a retried stage that spilled nothing" in {
    // attemptNumber > 0 IS a stage retry, and it is the direct answer to "are stages retrying".
    // A retry with no spill is exactly the case a spill-only rule would hide.
    SpillMetrics.worthInfo(cost(attempt = 1)) shouldBe true
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SpillMetricsSpec"`
Expected: FAIL — `SpillMetrics` is not a member of `com.demo.util`.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.util

/** What one completed stage cost, and whether that is worth saying out loud.
  *
  * Separated from the listener so the rendering and the emission rule are testable without a
  * SparkSession — `SparkSessions.create` calls `getOrCreate`, so session-level assertions in this
  * module are unreliable by construction.
  */
object SpillMetrics {

  /** The shuffle and spill cost of a single completed stage attempt. */
  final case class StageCost(
      job: String,
      stageId: Int,
      attempt: Int,
      tasks: Int,
      spillMemBytes: Long,
      spillDiskBytes: Long,
      shuffleWriteBytes: Long,
      shuffleReadBytes: Long,
      failedTasks: Int)

  private val Unit1K = 1024L
  private val Unit1M = Unit1K * 1024L
  private val Unit1G = Unit1M * 1024L

  /** Raw byte counts are unreadable at the scale that matters: 1288490188 versus 1.2G. */
  def humanBytes(n: Long): String =
    if (n >= Unit1G) f"${n.toDouble / Unit1G}%.1fG"
    else if (n >= Unit1M) f"${n.toDouble / Unit1M}%.1fM"
    else if (n >= Unit1K) f"${n.toDouble / Unit1K}%.1fK"
    else s"${n}B"

  def format(c: StageCost): String =
    s"[spill-metrics] job=${c.job} stage=${c.stageId} attempt=${c.attempt} tasks=${c.tasks} " +
      s"spillMem=${humanBytes(c.spillMemBytes)} spillDisk=${humanBytes(c.spillDiskBytes)} " +
      s"shuffleWrite=${humanBytes(c.shuffleWriteBytes)} shuffleRead=${humanBytes(c.shuffleReadBytes)} " +
      s"failedTasks=${c.failedTasks}"

  /** Emit at INFO only when something happened.
    *
    * This deliberately breaks the rule DropMetrics states -- that a silent counter is
    * indistinguishable from a broken one -- and the reason is cardinality, not disagreement.
    * DropMetrics fires once per micro-batch per gate; this fires once per STAGE, and a streaming
    * query produces stages continuously and forever. Emitting every clean stage at INFO would bury
    * the lines this exists to surface. The caller logs the quiet case at DEBUG, which keeps the
    * counter provably alive.
    */
  def worthInfo(c: StageCost): Boolean =
    c.spillMemBytes > 0L || c.spillDiskBytes > 0L || c.failedTasks > 0 || c.attempt > 0
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SpillMetricsSpec"`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/SpillMetrics.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/SpillMetricsSpec.scala
git commit -m "feat: render and gate per-stage spill metrics"
```

---

### Task 2: FailureTally — per-stage failure accounting that releases its state

`StageInfo` exposes no failed-task count: `failureReason` is set only when the whole stage failed, which is absent for a stage that succeeded after retrying tasks. So failures must be accumulated from task-end events. This task is that accumulator, kept separate from the listener so it can be tested without constructing Spark event objects.

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/FailureTally.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/FailureTallySpec.scala`

**Interfaces:**
- Produces:
  - `class FailureTally` with:
    - `def recordFailure(stageId: Int, attempt: Int): Unit`
    - `def drain(stageId: Int, attempt: Int): Int` — returns the count and forgets the key
    - `def size: Int` — number of keys currently held; exists so a test can prove state is released

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FailureTallySpec extends AnyFlatSpec with Matchers {

  "drain" should "return zero for a stage that never failed" in {
    new FailureTally().drain(1, 0) shouldBe 0
  }

  it should "count failures for one stage attempt" in {
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.recordFailure(1, 0)
    tally.drain(1, 0) shouldBe 2
  }

  it should "keep attempts of the same stage separate" in {
    // A retried stage is a different attempt; merging them would misreport both.
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.recordFailure(1, 1)
    tally.drain(1, 0) shouldBe 1
    tally.drain(1, 1) shouldBe 1
  }

  it should "release the key once drained" in {
    // A listener that accumulates per-stage state for the life of a long-running streaming query
    // and never releases it is itself a memory leak -- an absurd way for an observability feature
    // to fail.
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.size shouldBe 1
    tally.drain(1, 0)
    tally.size shouldBe 0
  }

  it should "return zero on a second drain of the same key" in {
    val tally = new FailureTally()
    tally.recordFailure(1, 0)
    tally.drain(1, 0) shouldBe 1
    tally.drain(1, 0) shouldBe 0
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.FailureTallySpec"`
Expected: FAIL — `FailureTally` not found.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.util

import java.util.concurrent.ConcurrentHashMap

/** Counts failed tasks per stage attempt, and forgets each one as soon as it is reported.
  *
  * Spark's StageInfo carries no failed-task count -- `failureReason` is set only when the whole
  * stage failed, which is not the same thing and is absent for a stage that succeeded after
  * retrying tasks. So failures have to be accumulated from task-end events and matched up when the
  * stage completes.
  *
  * `drain` removes the key rather than merely reading it: the listener holding this runs for the
  * life of the query, and a streaming query's stages are unbounded.
  *
  * Spark delivers listener events on a single dispatch thread, but a ConcurrentHashMap costs
  * nothing here and removes the question.
  */
class FailureTally {

  private val counts = new ConcurrentHashMap[(Int, Int), Int]()

  def recordFailure(stageId: Int, attempt: Int): Unit = {
    counts.merge((stageId, attempt), 1, (a: Int, b: Int) => a + b)
    ()
  }

  /** The failure count for this stage attempt, forgetting it in the same step. */
  def drain(stageId: Int, attempt: Int): Int = {
    val previous = counts.remove((stageId, attempt))
    if (previous == null) 0 else previous
  }

  /** Keys currently held. Exists so a test can prove `drain` releases state. */
  def size: Int = counts.size()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.FailureTallySpec"`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/FailureTally.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/FailureTallySpec.scala
git commit -m "feat: tally per-stage task failures and release the state"
```

---

### Task 3: SpillMetricsListener — the Spark adapter

A thin `SparkListener` that pulls numbers off Spark's event objects and hands them to the two pure pieces above. Thin on purpose: everything worth asserting was already tested in Tasks 1 and 2, because Spark's `TaskMetrics` cannot be constructed in a test.

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/SpillMetricsListener.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/SpillMetricsListenerSpec.scala`

**Interfaces:**
- Consumes: `SpillMetrics.StageCost` / `format` / `worthInfo` (Task 1); `FailureTally` (Task 2).
- Produces: `class SpillMetricsListener(jobName: String) extends SparkListener`, and `object SpillMetricsListener { def register(spark: SparkSession, jobName: String): Unit }`

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.util

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpillMetricsListenerSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "register" should "attach without disturbing the session" in {
    // The listener's arithmetic is covered by SpillMetricsSpec and FailureTallySpec, which need no
    // session. Spark's TaskMetrics has no public constructor, so a unit test cannot synthesise a
    // realistic stage-completed event; what is worth asserting here is that registering the
    // listener does not break a job that then runs.
    SpillMetricsListener.register(spark, "SpillMetricsListenerSpec")
    val s = spark; import s.implicits._
    val counted = Seq(("a", 1), ("b", 2), ("a", 3)).toDF("k", "v")
      .groupBy("k").count().collect()
    counted.length shouldBe 2
  }

  it should "be safe to register twice" in {
    // ExecutionEngine registers BatchMetricsListener per query; a job that opens two queries would
    // otherwise double-register and double-log.
    SpillMetricsListener.register(spark, "SpillMetricsListenerSpec")
    SpillMetricsListener.register(spark, "SpillMetricsListenerSpec")
    val s = spark; import s.implicits._
    Seq(1, 2, 3).toDF("v").count() shouldBe 3L
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SpillMetricsListenerSpec"`
Expected: FAIL — `SpillMetricsListener` not found.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.util

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

// NOTE: SparkContext.listenerBus is private[spark] and cannot be read from this package. Do not
// try to ask Spark which listeners are attached -- it will not compile.

/** One line per completed stage: what it spilled, what it shuffled, what failed, which attempt.
  *
  * Complements BatchMetricsListener rather than replacing it. That one is a
  * StreamingQueryListener, whose progress event carries no spill or shuffle-byte fields at all, and
  * which never fires for the batch report jobs -- where the most spill-prone aggregation in this
  * codebase lives.
  */
class SpillMetricsListener(jobName: String) extends SparkListener {

  private val log = LoggerFactory.getLogger(classOf[SpillMetricsListener])
  private val failures = new FailureTally

  // TaskInfo.successful rather than matching on `reason`: it is plain public API, where the
  // TaskEndReason subclasses are a DeveloperApi that has moved between Spark versions.
  override def onTaskEnd(event: SparkListenerTaskEnd): Unit =
    if (!event.taskInfo.successful)
      failures.recordFailure(event.stageId, event.stageAttemptId)

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    val info = event.stageInfo
    val metrics = info.taskMetrics
    val cost = SpillMetrics.StageCost(
      job = jobName,
      stageId = info.stageId,
      attempt = info.attemptNumber(),
      tasks = info.numTasks,
      spillMemBytes = metrics.memoryBytesSpilled,
      spillDiskBytes = metrics.diskBytesSpilled,
      shuffleWriteBytes = metrics.shuffleWriteMetrics.bytesWritten,
      shuffleReadBytes = metrics.shuffleReadMetrics.totalBytesRead,
      failedTasks = failures.drain(info.stageId, info.attemptNumber()))

    val line = SpillMetrics.format(cost)
    if (SpillMetrics.worthInfo(cost)) log.info(line) else log.debug(line)
  }
}

object SpillMetricsListener {

  // Which sessions already have a listener. Weak keys so a stopped session can still be collected
  // -- this object outlives any one session, and holding them strongly would leak every session
  // the JVM ever created. SparkContext.listenerBus is private[spark], so asking Spark what is
  // already attached is not an option from here.
  private val registered: java.util.Set[SparkSession] =
    java.util.Collections.newSetFromMap(new java.util.WeakHashMap[SparkSession, java.lang.Boolean]())

  /** Attach one listener per session. Idempotent: a job that opens two queries must not
    * double-register and double-log every stage. */
  def register(spark: SparkSession, jobName: String): Unit =
    registered.synchronized {
      if (registered.add(spark)) spark.sparkContext.addSparkListener(new SpillMetricsListener(jobName))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SpillMetricsListenerSpec"`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/SpillMetricsListener.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/SpillMetricsListenerSpec.scala
git commit -m "feat: report per-stage spill, shuffle and retries from a SparkListener"
```

---

### Task 4: Wire it into SparkSessions — partitions, event logging, listener

The seam. After this task every job gets all three behaviours with no per-job change.

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/SparkSessions.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/SparkSessionsSpec.scala` (extend)
- Modify: `recsys-pipeline/README.md`

**Interfaces:**
- Consumes: `SpillMetricsListener.register` (Task 3).
- Produces:
  - `val ClusterShufflePartitions: Int = 200`
  - `def shufflePartitionsFor(master: String, localDefault: Int): Int`
  - `def ensureEventLogDir(dir: String): Option[String]`

- [ ] **Step 1: Write the failing test**

Append to `SparkSessionsSpec.scala`:

```scala
  "shufflePartitionsFor" should "keep the local default for every local master form" in {
    // local[*] is the default master and the only thing that executes today. 200 partitions on a
    // laptop is hundreds of tiny tasks and more scheduling overhead than work.
    SparkSessions.shufflePartitionsFor("local", 8) shouldBe 8
    SparkSessions.shufflePartitionsFor("local[1]", 8) shouldBe 8
    SparkSessions.shufflePartitionsFor("local[*]", 8) shouldBe 8
    SparkSessions.shufflePartitionsFor("local[4]", 4) shouldBe 4
  }

  it should "raise the default on every cluster master" in {
    SparkSessions.shufflePartitionsFor("yarn", 8) shouldBe SparkSessions.ClusterShufflePartitions
    SparkSessions.shufflePartitionsFor("k8s://https://host:6443", 8) shouldBe SparkSessions.ClusterShufflePartitions
    SparkSessions.shufflePartitionsFor("spark://host:7077", 8) shouldBe SparkSessions.ClusterShufflePartitions
    SparkSessions.shufflePartitionsFor("mesos://host:5050", 8) shouldBe SparkSessions.ClusterShufflePartitions
  }

  it should "treat an unrecognised master as a cluster" in {
    // Guessing "local" for something unknown funnels a real cluster's shuffles through 8
    // partitions, which is the failure this exists to prevent. Guessing "cluster" only costs a
    // laptop some scheduling overhead.
    SparkSessions.shufflePartitionsFor("something-new", 8) shouldBe SparkSessions.ClusterShufflePartitions
  }

  it should "not mistake a master merely containing the word local" in {
    SparkSessions.shufflePartitionsFor("spark://localhost:7077", 8) shouldBe SparkSessions.ClusterShufflePartitions
  }

  "ensureEventLogDir" should "create a missing directory and return it" in {
    val dir = java.nio.file.Files.createTempDirectory("evlog-test").resolve("nested").toString
    SparkSessions.ensureEventLogDir(dir) shouldBe Some(dir)
    java.nio.file.Files.isDirectory(java.nio.file.Paths.get(dir)) shouldBe true
  }

  it should "accept a directory that already exists" in {
    val dir = java.nio.file.Files.createTempDirectory("evlog-existing").toString
    SparkSessions.ensureEventLogDir(dir) shouldBe Some(dir)
  }

  it should "return None rather than throwing when the directory cannot be created" in {
    // Spark throws at session creation if spark.eventLog.dir does not exist -- it does not create
    // it. Letting that propagate would turn an observability feature into an outage: every job
    // would fail to start. A logging feature must never be the reason a job cannot run.
    val file = java.nio.file.Files.createTempFile("evlog-not-a-dir", ".txt")
    SparkSessions.ensureEventLogDir(file.resolve("under-a-file").toString) shouldBe None
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SparkSessionsSpec"`
Expected: FAIL — `shufflePartitionsFor` and `ensureEventLogDir` are not members of `SparkSessions`.

- [ ] **Step 3: Write the implementation**

Replace the body of `SparkSessions.scala` with this, keeping `adaptiveConfigs`, `defaultTimeZone` and `envKeyFor` exactly as they are:

```scala
package com.demo.util

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Paths}

object SparkSessions {

  private val log = LoggerFactory.getLogger(getClass)

  /** AQE settings applied to every session; env-overridable per key. */
  val adaptiveConfigs: Map[String, String] = Map(
    "spark.sql.adaptive.enabled" -> "true",
    "spark.sql.adaptive.coalescePartitions.enabled" -> "true"
  )

  /** Session time zone applied to every session; override with `SPARK_SQL_SESSION_TIMEZONE`.
    *
    * Defence in depth only, so timestamp formatting does not vary by deploy host. The date
    * projections in [[TimePartitions]] deliberately do not rely on it — a `spark-submit` override
    * or a bare `SparkSession.builder` in a test would silently undo it. */
  val defaultTimeZone: String = "UTC"

  /** Shuffle partitions for a non-local master. */
  val ClusterShufflePartitions: Int = 200

  /** Partition count chosen from the master, not from a constant.
    *
    * The old flat default of 8 is right for `local[*]` -- the default master, and the only thing
    * that executes in this repo today -- and wrong for a cluster, where every wide shuffle would
    * funnel through 8 partitions and spill. Raising it flat would instead slow every local run and
    * every test. An unrecognised master is treated as a cluster: guessing "local" wrongly causes
    * the spill this exists to prevent, while guessing "cluster" wrongly costs a laptop some
    * scheduling overhead.
    */
  def shufflePartitionsFor(master: String, localDefault: Int): Int =
    if (master == "local" || master.startsWith("local[")) localDefault
    else ClusterShufflePartitions

  /** Make sure Spark's event-log directory exists, returning it only if it is usable.
    *
    * Spark throws at session creation when `spark.eventLog.dir` is missing -- it does not create
    * the directory. Propagating that would mean enabling event logging on a fresh machine breaks
    * every job at startup, turning an observability feature into an outage. So a directory that
    * cannot be created disables event logging with a warning instead.
    */
  def ensureEventLogDir(dir: String): Option[String] =
    try {
      Files.createDirectories(Paths.get(dir))
      Some(dir)
    } catch {
      case scala.util.control.NonFatal(e) =>
        log.warn(s"event log dir '$dir' is unusable, event logging stays off: ${e.getMessage}")
        None
    }

  def create(defaultAppName: String, defaultShufflePartitions: Int = 8): SparkSession = {
    val appName = sys.env.getOrElse("SPARK_APP_NAME", defaultAppName)
    val master = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val builder = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config(
        "spark.sql.shuffle.partitions",
        sys.env.getOrElse(
          "SPARK_SQL_SHUFFLE_PARTITIONS",
          shufflePartitionsFor(master, defaultShufflePartitions).toString)
      )
      .config(
        "spark.sql.session.timeZone",
        sys.env.getOrElse("SPARK_SQL_SESSION_TIMEZONE", defaultTimeZone)
      )
    adaptiveConfigs.foreach { case (k, v) => builder.config(k, sys.env.getOrElse(envKeyFor(k), v)) }

    if (sys.env.get("SPARK_EVENT_LOG_ENABLED").contains("true")) {
      ensureEventLogDir(sys.env.getOrElse("SPARK_EVENT_LOG_DIR", "/tmp/spark-events")).foreach { dir =>
        builder.config("spark.eventLog.enabled", "true")
        builder.config("spark.eventLog.dir", dir)
      }
    }

    val spark = builder.getOrCreate()
    SpillMetricsListener.register(spark, appName)
    spark
  }

  // spark.sql.adaptive.enabled -> SPARK_SQL_ADAPTIVE_ENABLED
  private def envKeyFor(confKey: String): String =
    confKey.toUpperCase.replace('.', '_')
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.util.SparkSessionsSpec"`
Expected: PASS, 9 tests (2 pre-existing plus 7 new).

- [ ] **Step 5: Run the whole Scala suite for regressions**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test`
Expected: PASS. Every job in the repo now constructs its session through the modified `create`; this run is how you learn whether any of them broke.

- [ ] **Step 6: Document the three new environment variables**

In `recsys-pipeline/README.md`, add to the environment-variable table that
`test_readme_documents_every_measurement_environment_variable` checks — read that test under `recsys-pipeline/integration-tests/` first and match the format it requires:

| Variable | Default | Meaning |
|---|---|---|
| `SPARK_EVENT_LOG_ENABLED` | `false` | Write Spark event logs, so a completed run can be inspected after the fact. Off by default; a long-running stream's logs grow without bound |
| `SPARK_EVENT_LOG_DIR` | `/tmp/spark-events` | Where those logs go. Created if missing; if it cannot be created, event logging stays off rather than failing the job |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | derived from `SPARK_MASTER` | Overrides the partition count. Without it, local masters keep the per-job default and cluster masters get 200 |

Then run: `cd recsys-pipeline && python3 -m pytest integration-tests -k "readme or documentation" -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/util/SparkSessions.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/util/SparkSessionsSpec.scala \
        recsys-pipeline/README.md
git commit -m "feat: derive shuffle partitions from the master and add opt-in event logging"
```

---

## Verification

```bash
cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test
cd recsys-pipeline && python3 -m pytest integration-tests -q
```

Both pass, and running any existing job with no new environment variables set produces no new INFO
output and no behavior change — which is the point: the defaults are inert, and the signal appears
only when something is actually wrong or when an operator asks for it.
