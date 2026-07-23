# Engine RedisSink — Design

**Date:** 2026-07-23
**Status:** Approved (design), pending implementation plan

## Problem

Redis persistence is the most fragmented write path in the repo. The shared
plumbing — get a pooled/plain Jedis, open a pipeline, flush every N commands,
sync, close — is hand-rolled three separate times ([`RedisWriter`](../../recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/sink/RedisWriter.scala)
SET, `UserEventStreamingJob` ZINCRBY, `MovieLensContextCollectorStreamingJob`
HSET), and the executor-shared `RedisPool` lives *inside* `UserEventStreamingJob`.
The engine has `KafkaSink`/`ParquetSink` but no `RedisSink`, so streaming jobs
can't route Redis writes through it.

This design adds an engine `RedisSink` that owns the pool/pipeline/flush plumbing
and delegates the actual command to a per-row lambda, and proves it by adopting it
in `UserEventStreamingJob` (write-swap only).

## Scope

**In scope:**
- Move `RedisPool` out of `UserEventStreamingJob` into `com.demo.engine.RedisPool`
  (connection infra the sink owns). Update the two references
  (`UserEventStreamingJob` defines it; `MovieLensContextCollectorStreamingJob`
  imports it).
- New `com.demo.engine.RedisSink` implementing `Sink`, parameterized by a
  `writeRow: (Pipeline, Row) => Unit` op lambda, with a pure, testable
  flush-cadence helper.
- Adopt `RedisSink` in `UserEventStreamingJob` by replacing only its hand-rolled
  `foreachPartition { Map-count → pipeline.zincrby }` block.

**Out of scope (explicit follow-ups, each a later small PR):**
- Migrating `MovieLensContextCollectorStreamingJob`'s HSET (its read-before-write
  merge is enrich+persist intertwined).
- The offline `RPUSH` (`EmbeddingCandidateGenerationJob`) and `SET`
  (`RedisWriter`) writers.
- Folding `RedisWriter` onto the shared plumbing.
- Any full engine migration of `UserEventStreamingJob` (it keeps its own
  `foreachBatch`; only the write block changes).

## Design

### 1. `com.demo.engine.RedisPool` (relocation)

Move the existing `object RedisPool { def get(host, port, maxTotal): JedisPool }`
verbatim into `com/demo/engine/RedisPool.scala`, package `com.demo.engine`, public
(so `RedisSink` and the jobs can use it). Remove the inline copy from
`UserEventStreamingJob`; change `MovieLensContextCollectorStreamingJob`'s import
from `com.demo.task.RedisPool` to `com.demo.engine.RedisPool`. No behavior change.

### 2. `com.demo.engine.RedisSink`

```scala
class RedisSink(host: String, port: Int, poolMax: Int, pipelineSize: Int,
                writeRow: (Pipeline, Row) => Unit) extends Sink {
  def write(batch: DataFrame, batchId: Long): Unit =
    batch.foreachPartition { rows: Iterator[Row] =>
      val jedis = RedisPool.get(host, port, poolMax).getResource
      try {
        val p = jedis.pipelined()
        RedisSink.foreachWithFlush(rows, pipelineSize)(writeRow(p, _))(() => p.sync())
      } finally jedis.close()
    }
}

object RedisSink {
  /** Apply `onRow` to each row; call `flush` after every `pipelineSize` rows and
    * once more at the end if any remain. Pure — the unit test target. */
  def foreachWithFlush(rows: Iterator[Row], pipelineSize: Int)
                      (onRow: Row => Unit)(flush: () => Unit): Unit = {
    var pending = 0
    rows.foreach { r =>
      onRow(r); pending += 1
      if (pending >= pipelineSize) { flush(); pending = 0 }
    }
    if (pending > 0) flush()
  }
}
```

The `writeRow` lambda carries the op (SET/ZINCRBY/HSET/RPUSH) and any TTL, keeping
`RedisSink` free of a data-structure taxonomy. `foreachWithFlush` is the pure
seam: the live pool/Jedis/pipeline in `write` is not unit-tested (same convention
as `KafkaSink.payload` tested, `.save()` not).

### 3. `UserEventStreamingJob` adoption

Replace the `foreachPartition` count-and-zincrby block (inside the existing
`writeStream.foreachBatch`) with:

```scala
val counts = itemClickCounts(batch)   // pure: batch.groupBy("item_id").count()
new RedisSink(poolHost, poolPort, poolMax, redisPipelineSize,
  (p, r) => p.zincrby("global:item_popularity",
                      r.getAs[Long]("count").toDouble, r.getAs[String]("item_id"))
).write(counts, batchId)
```

`itemClickCounts(batch: DataFrame): DataFrame = batch.groupBy("item_id").count()`
is extracted as a pure method for testing. The in-memory per-partition `Map`
aggregation becomes a Spark `groupBy`.

**Behavior preservation:** the `global:item_popularity` ZSET end-state per batch is
identical — the same per-item total is incremented. Only the number of `ZINCRBY`
calls differs (one per item vs one per item-per-partition), which does not affect
the result. `parseEvents`/`dedupedClicks` (the only methods `UserEventStreamingJobSpec`
pins) are untouched.

## Testing

- `RedisSinkSpec` — unit-tests `foreachWithFlush`: with a collecting `onRow` and a
  flush counter, assert `onRow` fires once per row and `flush` fires
  `floor(n/size)` times plus a final flush when `n % size != 0` (e.g. n=3,size=2 →
  onRow 3, flush 2; n=4,size=2 → flush 2; n=1,size=2 → flush 1). No live Redis.
- `UserEventStreamingJobSpec` — add one test for `itemClickCounts` (per-item counts
  over a small DataFrame). The three existing tests (`parseEvents` ×2,
  `dedupedClicks`) stay unchanged and green.

## Success criteria

1. `sbt test` passes, including the unchanged `UserEventStreamingJobSpec` parse/dedup
   tests and the new `RedisSinkSpec`.
2. `UserEventStreamingJob` produces the identical `global:item_popularity`
   increments (same ZSET end-state, same env vars).
3. `RedisPool` has a single home (`com.demo.engine`), used by `RedisSink`,
   `UserEventStreamingJob`, and `MovieLensContextCollectorStreamingJob`.
4. `RedisSink` is reusable by any job (streaming via `foreachBatch`, or batch via a
   direct `write` call) with a per-row op lambda.
