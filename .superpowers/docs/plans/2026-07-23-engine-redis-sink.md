# Engine RedisSink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an engine `RedisSink` (owns pool/pipeline/flush plumbing, takes a per-row op lambda), relocate `RedisPool` into the engine, and adopt the sink in `UserEventStreamingJob` — behavior-preserving.

**Architecture:** `RedisSink` implements the existing `Sink` trait; its `write` runs `batch.foreachPartition`, borrowing a pooled `Jedis`, pipelining commands via the pure `RedisSink.foreachWithFlush`, and delegating each row's command to a `writeRow: (Pipeline, Row) => Unit` lambda. `UserEventStreamingJob` replaces its hand-rolled count-and-zincrby block with a Spark `groupBy` + `RedisSink`.

**Tech Stack:** Scala 2.12, Spark 3.5.x, Jedis, ScalaTest (`AnyFlatSpec`).

## Global Constraints

- Module: `recsys-pipeline/services/spark-streaming-job` (sbt). Scala paths below are relative to `src/main/scala` / `src/test/scala`.
- `RedisPool` moves to `com.demo.engine`; the engine must not depend on `com.demo.task`.
- `UserEventStreamingJob` behavior must be preserved: same `global:item_popularity` ZSET increments per batch, same env vars. Its existing `parseEvents`/`dedupedClicks` tests stay unchanged and green (they do not pin the aggregation mechanism).
- Serialization: inside `RedisSink.write`, copy constructor fields to local vals before `foreachPartition` so the closure does not capture `this`.
- Every commit message ends with: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.
- Run tests from the module dir: `cd recsys-pipeline/services/spark-streaming-job`.

## File Structure

- Create: `com/demo/engine/RedisPool.scala` — relocated pool (Task 1)
- Create: `com/demo/engine/RedisSink.scala` — the sink + `foreachWithFlush` (Task 2)
- Create: `com/demo/engine/RedisSinkSpec.scala` — cadence test (Task 2)
- Modify: `com/demo/task/UserEventStreamingJob.scala` — remove inline `RedisPool` (Task 1), adopt `RedisSink` (Task 3)
- Modify: `com/demo/process/MovieLensContextCollectorStreamingJob.scala` — import update (Task 1)
- Modify: `com/demo/task/UserEventStreamingJobSpec.scala` — add `itemClickCounts` test (Task 3)

---

### Task 1: Relocate RedisPool into the engine

**Files:**
- Create: `com/demo/engine/RedisPool.scala`
- Modify: `com/demo/task/UserEventStreamingJob.scala`
- Modify: `com/demo/process/MovieLensContextCollectorStreamingJob.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `object com.demo.engine.RedisPool { def get(host: String, port: Int, maxTotal: Int): JedisPool }` (public; same behavior as the current `private[demo]` object).

This is a pure relocation — verified by the existing suite, no new test.

- [ ] **Step 1: Create the relocated pool**

Create `src/main/scala/com/demo/engine/RedisPool.scala`:

```scala
package com.demo.engine

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

// One JedisPool per executor JVM — avoids a new TCP connection per partition per micro-batch.
object RedisPool {
  @volatile private var pool: JedisPool = _

  def get(host: String, port: Int, maxTotal: Int): JedisPool = {
    if (pool == null) synchronized {
      if (pool == null) {
        val cfg = new JedisPoolConfig()
        cfg.setMaxTotal(maxTotal)
        cfg.setMaxIdle(maxTotal)
        cfg.setMinIdle(1)
        pool = new JedisPool(cfg, host, port)
      }
    }
    pool
  }
}
```

- [ ] **Step 2: Remove the inline pool from UserEventStreamingJob and import the new one**

In `com/demo/task/UserEventStreamingJob.scala`:
- Delete the `import redis.clients.jedis.{JedisPool, JedisPoolConfig}` line and the entire inline `private[demo] object RedisPool { ... }` block (the comment line through its closing brace).
- Add `import com.demo.engine.RedisPool` to the import group.

The job still references `RedisPool.get(...)` in its write block (unchanged in this task) — now resolved via the import.

- [ ] **Step 3: Update MovieLensContext's import**

In `com/demo/process/MovieLensContextCollectorStreamingJob.scala`, change:
```scala
import com.demo.task.RedisPool
```
to:
```scala
import com.demo.engine.RedisPool
```
(Its two `RedisPool.get(...)` call sites are unchanged.)

- [ ] **Step 4: Verify the full suite still passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt test`
Expected: all tests PASS (pure relocation, no behavior change).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/RedisPool.scala \
        recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/MovieLensContextCollectorStreamingJob.scala
git commit -m "refactor: relocate RedisPool to com.demo.engine

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: RedisSink + foreachWithFlush

**Files:**
- Create: `com/demo/engine/RedisSink.scala`
- Test: `com/demo/engine/RedisSinkSpec.scala`

**Interfaces:**
- Consumes: `Sink` (existing), `RedisPool` (Task 1).
- Produces:
  - `RedisSink.foreachWithFlush[A](rows: Iterator[A], pipelineSize: Int)(onRow: A => Unit)(flush: () => Unit): Unit` — applies `onRow` to each element; calls `flush` after every `pipelineSize` elements and once more at the end if any remain.
  - `class RedisSink(host: String, port: Int, poolMax: Int, pipelineSize: Int, writeRow: (Pipeline, Row) => Unit) extends Sink`.

- [ ] **Step 1: Write the failing test**

Create `src/test/scala/com/demo/engine/RedisSinkSpec.scala`:

```scala
package com.demo.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RedisSinkSpec extends AnyFlatSpec with Matchers {

  "foreachWithFlush" should "apply onRow to every element and flush on the cadence" in {
    def run(n: Int, size: Int): (Int, Int) = {
      var rows = 0
      var flushes = 0
      RedisSink.foreachWithFlush((1 to n).iterator, size)(_ => rows += 1)(() => flushes += 1)
      (rows, flushes)
    }
    run(3, 2) shouldBe (3, 2)  // flush after 2, final flush for the 3rd
    run(4, 2) shouldBe (4, 2)  // exact multiple, no extra final flush
    run(1, 2) shouldBe (1, 1)  // one pending -> final flush
    run(0, 2) shouldBe (0, 0)  // nothing to do
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.RedisSinkSpec"`
Expected: FAIL — `RedisSink` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/engine/RedisSink.scala`:

```scala
package com.demo.engine

import org.apache.spark.sql.{DataFrame, Row}
import redis.clients.jedis.Pipeline

/** Persists a DataFrame to Redis: borrows a pooled Jedis per partition, pipelines
  * commands with a bounded flush, and delegates each row's command to `writeRow`. */
class RedisSink(host: String, port: Int, poolMax: Int, pipelineSize: Int,
                writeRow: (Pipeline, Row) => Unit) extends Sink {

  def write(batch: DataFrame, batchId: Long): Unit = {
    // Copy fields to locals so the partition closure doesn't capture `this`.
    val h = host; val pt = port; val mx = poolMax; val ps = pipelineSize; val wr = writeRow
    batch.foreachPartition { rows: Iterator[Row] =>
      val jedis = RedisPool.get(h, pt, mx).getResource
      try {
        val p = jedis.pipelined()
        RedisSink.foreachWithFlush(rows, ps)(r => wr(p, r))(() => p.sync())
      } finally {
        jedis.close()
      }
    }
  }
}

object RedisSink {
  /** Apply `onRow` to each element; `flush` after every `pipelineSize` elements and
    * once more at the end if any remain. Pure — the unit-test seam. */
  def foreachWithFlush[A](rows: Iterator[A], pipelineSize: Int)
                         (onRow: A => Unit)(flush: () => Unit): Unit = {
    var pending = 0
    rows.foreach { r =>
      onRow(r)
      pending += 1
      if (pending >= pipelineSize) { flush(); pending = 0 }
    }
    if (pending > 0) flush()
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.engine.RedisSinkSpec"`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/engine/RedisSink.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/engine/RedisSinkSpec.scala
git commit -m "feat: engine RedisSink + foreachWithFlush

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Adopt RedisSink in UserEventStreamingJob

**Files:**
- Modify: `com/demo/task/UserEventStreamingJob.scala`
- Modify: `com/demo/task/UserEventStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `RedisSink`, `RedisPool` (Tasks 1–2).
- Produces: `UserEventStreamingJob.itemClickCounts(batch: DataFrame): DataFrame` — `batch.groupBy("item_id").count()` (columns `item_id`, `count`).

Behavior preserved: the same per-item total is incremented into `global:item_popularity` each batch.

- [ ] **Step 1: Write the failing test**

Append to `com/demo/task/UserEventStreamingJobSpec.scala` (inside the class, after the existing tests):

```scala
  "UserEventStreamingJob.itemClickCounts" should "count clicks per item" in {
    val s = spark; import s.implicits._
    val batch = Seq("i1", "i1", "i2").toDF("item_id")
    val counts = UserEventStreamingJob.itemClickCounts(batch)
      .collect().map(r => r.getString(0) -> r.getAs[Long]("count")).toMap
    counts shouldBe Map("i1" -> 2L, "i2" -> 1L)
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.UserEventStreamingJobSpec"`
Expected: FAIL — `itemClickCounts` not found.

- [ ] **Step 3: Implement itemClickCounts and swap the write block**

In `com/demo/task/UserEventStreamingJob.scala`:

(a) Update imports: change `import com.demo.engine.RedisPool` (added in Task 1) to `import com.demo.engine.{RedisPool, RedisSink}`; delete `import org.slf4j.LoggerFactory`; change `import org.apache.spark.sql.{DataFrame, Row, SparkSession}` to `import org.apache.spark.sql.{DataFrame, SparkSession}`.

(b) Delete the logger field: remove `private val log = LoggerFactory.getLogger(getClass)`.

(c) Add the pure aggregation method (e.g. just after `parseEvents`):
```scala
  /** Per-item click counts for one micro-batch (columns: item_id, count). */
  def itemClickCounts(batch: DataFrame): DataFrame = batch.groupBy("item_id").count()
```

(d) Replace the pool local-vals + the entire `parsed.writeStream.foreachBatch { ... }` block (the `val poolHost = ...` lines through the closing `}` of `foreachBatch`, i.e. everything from the `// Executor-local pool` comment down to but not including `.option("checkpointLocation", ...)`) with:
```scala
    parsed.writeStream.foreachBatch { (batch: DataFrame, batchId: Long) =>
      val counts = itemClickCounts(batch)
      new RedisSink(redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize,
        (p, r) => p.zincrby("global:item_popularity",
                            r.getAs[Long]("count").toDouble, r.getAs[String]("item_id"))
      ).write(counts, batchId)
    }
```
Leave the trailing `.option("checkpointLocation", checkpointLocation).trigger(...).start().awaitTermination()` chain exactly as is.

- [ ] **Step 4: Run the job's spec, then the full suite**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.UserEventStreamingJobSpec"`
Expected: PASS (the 3 existing parse/dedup tests + the new `itemClickCounts` test).

Run: `sbt test`
Expected: full suite PASS, output pristine.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/UserEventStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/UserEventStreamingJobSpec.scala
git commit -m "refactor: UserEventStreamingJob writes item popularity via RedisSink

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Run the whole module suite once more:

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt test`
Expected: all specs PASS, including `RedisSinkSpec` and the unchanged `UserEventStreamingJobSpec` parse/dedup tests.

## Self-Review (completed by plan author)

- **Spec coverage:** RedisPool relocation → Task 1; RedisSink + writeRow lambda + pure flush helper → Task 2; UserEvent write-swap + `itemClickCounts` → Task 3. Out-of-scope items (MovieLensContext HSET, offline RPUSH/SET, RedisWriter fold) are not touched — matches the spec.
- **Placeholder scan:** none — every step has concrete code/commands.
- **Type consistency:** `RedisPool.get(host,port,maxTotal)`, `RedisSink(host,port,poolMax,pipelineSize,writeRow)`, `foreachWithFlush[A](rows,size)(onRow)(flush)`, `itemClickCounts(batch)` used identically across tasks.
- **Serialization:** Task 2 copies constructor fields to local vals before `foreachPartition`; the `writeRow` lambda in Task 3 captures only a string literal + column names (serializable).
- **Behavior preservation:** the ZSET end-state per batch is identical (same per-item total increment); `parseEvents`/`dedupedClicks` unchanged, so `UserEventStreamingJobSpec`'s existing tests stay green.
- **Note:** `RedisSink.write`'s live pool/Jedis/pipeline path is not unit-tested (no embedded Redis), matching the repo convention (`KafkaSink.payload` tested, `.save()` not); the pure `foreachWithFlush` covers the flush cadence.
