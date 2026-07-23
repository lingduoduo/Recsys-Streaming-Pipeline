# Shared Execution Engine — Design

**Date:** 2026-07-23
**Status:** Approved (design), pending implementation plan

## Problem

The repo's Spark Structured Streaming jobs (`UserEventStreamingJob`,
`OnlineJoinerStreamingJob`, `MovieLensContextCollectorStreamingJob`) all repeat
the same pipeline shape by hand: connect to Kafka → filter → feature-engineer →
enrich (Redis / offline join) → persist (Kafka / Parquet / Redis), wired through
env config and a `foreachBatch` loop. The shape is duplicated, and each job
re-implements config reading, the batch try/finally, and sink wiring.

This design extracts that shared shape into a reusable, config-driven **execution
engine** (`com.demo.engine`), and proves it by refactoring
`OnlineJoinerStreamingJob` onto it with no behavior change.

## Scope

**In scope:**
- A new `com.demo.engine` package: `Source`, `Stage`, `Sink`, `EngineConfig`,
  `ExecutionEngine`, with the concrete `KafkaSource`, `KafkaSink`, `ParquetSink`
  that `OnlineJoinerStreamingJob` needs.
- Refactor `OnlineJoinerStreamingJob` to run on the engine, preserving its
  external contract (entry-point class, env vars, output topics/Parquet, and its
  existing spec).

**Out of scope (YAGNI — documented extension points, not built):**
- New source/sink connectors beyond Kafka + Parquet (JDBC, log-file,
  feature-store sources; Redis sink). The traits make these easy to add later.
- Batch (non-streaming) sources.
- Refactoring the other jobs. The engine is designed so they can adopt it later.
- Any serving / Java-API changes.

## Architecture

Streaming/Kafka-focused v1. The engine models the pipeline as:

```
Source.read ──► streamingStages (fold) ──► writeStream.foreachBatch { batch, id =>
                                              records = batchStages.fold(batch)
                                              records.persist(MEMORY_AND_DISK_SER)
                                              try { sinks.foreach(withRetry(_.write(records, id))) }
                                              finally { records.unpersist() }
                                            }
                                            .option(checkpointLocation)
                                            .trigger(ProcessingTime(triggerInterval))
                                            .start().awaitTermination()
```

Two stage lists because Structured Streaming distinguishes streaming-level
transforms (which may be stateful — e.g. watermark dedup) from per-micro-batch
transforms (a non-stateful `groupBy` over one batch, as `OnlineJoiner` does):

- **`streamingStages`** run on the streaming DataFrame before `writeStream`
  (parse, watermark dedup).
- **`batchStages`** run inside `foreachBatch` on each micro-batch (the
  request-scoped label `groupBy`).

### Interfaces (each in its own small file under `com/demo/engine/`)

```scala
trait Source { def read(spark: SparkSession, cfg: EngineConfig): DataFrame }

trait Stage  { def apply(df: DataFrame): DataFrame }   // filter, feature, enrich

trait Sink   { def write(batch: DataFrame, batchId: Long): Unit }  // persist
```

- **`KafkaSource`** — `readStream.format("kafka")` with `subscribe`,
  `startingOffsets`, `kafka.group.id`, `failOnDataLoss=false`,
  `maxOffsetsPerTrigger` from `cfg`. *Connect to data sources.*
- **`Stage`** — a single pure `DataFrame => DataFrame`. Filtering, feature
  engineering, and enrichment/joins are all `Stage`s. A `Stage` may broadcast-join
  an offline table (enrich). When enrichment is sink-specific (as with
  `OnlineJoiner`'s Parquet-only catalog join) it lives in that `Sink` instead.
  *Filtering, feature engineering, enrichment.*
- **`KafkaSink`** — writes `key`/`value` (JSON) to a topic. **`ParquetSink`** —
  applies its sink-local transform (catalog enrich + `date` column), coalesces to
  `outputFiles`, writes `partitionBy("date")` append. *Persist enriched records.*

### `EngineConfig` — parse + validate (bullet 6)

Case class of the shared knobs, built from env/args via the existing
`com.demo.util.Env`, with a fail-fast validator:

```scala
final case class EngineConfig(
  bootstrapServers: String, inputTopic: String, startingOffsets: String,
  groupId: String, maxOffsetsPerTrigger: Int, triggerInterval: String,
  checkpointLocation: String, shufflePartitions: Int,
  watermarkDelay: String, sinkMaxRetries: Int
)
object EngineConfig {
  def fromEnv(...): EngineConfig
  def validate(cfg: EngineConfig): Either[List[String], EngineConfig]
}
```

`validate` accumulates ALL errors (not first-fail):
- `bootstrapServers`, `inputTopic`, `checkpointLocation`, `triggerInterval`,
  `watermarkDelay` non-blank;
- `maxOffsetsPerTrigger > 0`, `shufflePartitions > 0`, `sinkMaxRetries >= 0`.

Returns `Left(errors)` or `Right(cfg)`. Job `main` prints the errors and exits
non-zero when invalid — configuration is validated **before** the pipeline runs.

### Concurrency, retries, backpressure (bullet 5)

Lean on Spark, surface its controls through `EngineConfig`, add one thin retry:
- **Concurrency:** Spark micro-batch execution + `shufflePartitions`.
- **Backpressure:** `maxOffsetsPerTrigger` caps per-trigger intake;
  `triggerInterval` paces batches.
- **Retries / reliability:** checkpoint-based recovery + Spark task retries come
  for free. Additionally, `ExecutionEngine` wraps each `Sink.write` in a bounded
  retry (`sinkMaxRetries`, default **0** = today's behavior) with a short fixed
  backoff; on exhaustion it rethrows, failing the batch so Spark restarts it from
  the checkpoint. The per-batch `persist`/`try`/`finally`/`unpersist` (from the
  current `OnlineJoiner`) is owned by the engine.

## Proof: refactor `OnlineJoinerStreamingJob`

`main` becomes: `EngineConfig.fromEnv` → `validate` (exit on `Left`) →
load catalog once → `ExecutionEngine.run(cfg, source, streamingStages,
batchStages, sinks)` where:

| Engine element | OnlineJoiner mapping (existing pure methods reused verbatim) |
|---|---|
| `source` | `KafkaSource` (input topic `behavior_logs`/`recsys_events`) |
| `streamingStages` | one `Stage` wrapping `dedupedEvents` (`parseEvents` + watermark dedup) |
| `batchStages` | one `Stage` wrapping `buildTrainingSamples` (+ `batch_id`) |
| `sinks` | `KafkaSink(outputTopic)` writing `sample_id` + `to_json(struct(...))`; `ParquetSink(outputPath, catalog, outputFiles)` doing `withCatalog` + `date` + `writeParquet` |

The pure methods (`parseEvents`, `dedupedEvents`, `buildTrainingSamples`,
`loadCatalog`, `withCatalog`, `enrichWithCatalog`, `writeParquet`) are unchanged,
so `OnlineJoinerStreamingJobSpec` stays green. Env vars, output topic, and Parquet
layout are identical. `BatchMetricsListener.register` is still called.

## Testing

New `AnyFlatSpec` specs (matching the repo's `local[1]` SparkSession +
`MemoryStream` pattern):
- `EngineConfigSpec` — `validate` returns `Right` for a good config; `Left` with
  the expected messages for blank topic, non-positive `maxOffsetsPerTrigger`,
  negative `sinkMaxRetries` (accumulates multiple errors).
- `ExecutionEngineSpec` — a `MemoryStream` source through two `Stage`s (a filter
  and a `withColumn`) into an in-memory collecting `Sink`, asserting the sink
  receives the transformed rows; a second case asserting `sinkMaxRetries` retries
  a failing sink the configured number of times then rethrows.
- `OnlineJoinerStreamingJobSpec` — unchanged, must stay green.

## Success criteria

1. `sbt test` passes, including the unchanged `OnlineJoinerStreamingJobSpec` and
   the new engine specs.
2. `OnlineJoinerStreamingJob` produces byte-identical output (same Kafka topic
   value schema, same Parquet partitioning) and honors the same env vars.
3. `EngineConfig.validate` rejects invalid configs before any Spark query starts,
   listing every problem.
4. No other job changes; no serving changes.
