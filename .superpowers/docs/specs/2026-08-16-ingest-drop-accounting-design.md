# Ingest Drop Accounting Design

## Purpose

Make every row the streaming pipeline discards at a field-level gate visible as a per-reason,
per-micro-batch count in the job log — and replace the `corrupt` metric that has silently reported
zero since the Avro engine migration.

## Motivation

The pipeline drops rows at eight places on the basis of which fields are present. Seven are
`isNotNull` gates in job parse functions; the eighth is the Avro decode split. None of them is
counted today, so a producer that starts emitting `request_id`-less events costs training samples
with no signal anywhere.

The one counter that exists does not work.

**The `corrupt` metric is structurally disconnected.** `BatchMetricsListener` reads
`observedMetrics.get("ingest")` off `StreamingQueryProgress`, which carries only `CollectMetrics`
nodes from the *streaming* query's plan. Both jobs that call `observeIngest` use the Avro
`ExecutionEngine.run` overload:

```scala
source.read(spark, cfg).writeStream
  .foreachBatch { (batch, batchId) => processDecodedBatch(batch, ...) }
```

Zero stages are applied to the streaming DataFrame. Every stage, `observeIngest` included, runs on
the inner batch frame, so the streaming plan contains no `CollectMetrics` at all and the lookup
never resolves. `corrupt` has logged a constant `0` in both jobs since that migration.

At `4424a62`, where the metric was introduced, both jobs used the plain streaming path and it
worked. The migration broke it. `UserEventStreamingJob` then acquired a second, independent
defect on top: `dedupedClicks` now calls `observeIngest(parseEvents(raw))`, but `parseEvents`
already filters `user_id.isNotNull`, so the observation counts null `user_id` on a frame that by
construction has none. Either defect alone yields zero.

**The existing test could not have caught either.** `BatchMetricsListenerSpec` asserts on `format`,
a pure string function that was correct throughout. The number feeding it is never exercised.

## Constraints Discovered

**The seven gates sit in two different plan topologies.** `OnlineJoiner`, `UserEvent` (via the
engine) and `ExperienceCollector` gate inside `foreachBatch`. `Ranking`, `Recall`, `Relevance` and
`MovieLensContext` gate in the streaming plan — `parseSamples(raw).writeStream`. Any mechanism must
either bridge that split or erase it. Bridging it is what produced the current bug.

**`recommendation_metrics` has no consumer.** The topic `RecommendationResponseStatsJob` publishes
to is read by nothing in this repository. Routing drop counts there would leave them as unobserved
as they are now.

**Counting kept and rejected separately costs two passes and can disagree.** On an unpersisted
frame, two `count()` calls recompute the upstream parse twice and, under a non-deterministic
source, need not sum to the input.

## Design

### 1. `FieldGate`: rejection rules as data

A gate takes a frame and an ordered `Seq[(reason, rejectWhen)]`, phrased as rejection conditions to
match the existing `rejection_reason` when-chain in `UserBehaviorProfileBatchJob`:

```scala
FieldGate(events, Seq(
  "null_request_id" -> col("request_id").isNull,
  "null_user_id"    -> col("user_id").isNull,
  "null_item_id"    -> col("item_id").isNull,
  "null_event_type" -> col("event_type").isNull,
  "null_timestamp"  -> col("timestamp").isNull
))
```

It adds a `rejection_reason` column via a first-match `when` chain. Each condition is wrapped in
`coalesce(cond, lit(false))` so a null-valued predicate cannot leak an unknown into the chain — the
guard `LateFeedbackJoin` already applies to its due computation.

First-match attribution means one reason per dropped row, so per-reason counts sum exactly to the
drop total and the numbers read as a partition of the input.

### 2. Count the tagged frame, once

`FieldGate` returns `Gated`, which carries the tagged frame and the declared reason order:

```scala
final case class Gated(tagged: DataFrame, reasons: Seq[String]) {
  def kept: DataFrame                        // rejection_reason IS NULL, column dropped
  def rejected: DataFrame                    // rejection_reason IS NOT NULL
  def counts: (Long, Seq[(String, Long)])    // one groupBy: null group = kept
}
```

A single `groupBy("rejection_reason").count()` yields the kept count (the null group) and every
per-reason count in one shuffle. `Gated` is pure DataFrame algebra with no logging in it.

**Persistence is the caller's responsibility**, matching how `ExecutionEngine` already brackets
`persist`/`finally unpersist`. It is required only in `Ranking`, `Recall`, `Relevance` and
`MovieLensContext`, where the upstream is not already persisted. In the `foreachBatch` family
`tagged` is a projection over a frame the engine has already persisted, so a second persist buys
nothing.

### 3. `DropMetrics`: rendering and emission

Lives in `com.demo.util` beside `BatchMetricsListener`, split so the logic is testable without a
logger. `Reporter` is the seam; `DropMetrics` is its logging implementation and the default
everywhere:

```scala
trait Reporter {
  def report(gated: Gated, job: String, batchId: Long): DataFrame
  def reportDecode(deadLetters: DataFrame, validCount: Long, job: String, batchId: Long): Unit
}

object DropMetrics extends Reporter {
  def format(job: String, batchId: Long, kept: Long, reasons: Seq[(String, Long)]): String
}
```

`format` is pure, mirroring `BatchMetricsListener.format`. `report` counts, logs, and returns the
kept frame.

```
[drop-metrics] job=OnlineJoinerStreamingJob batch=42 kept=4931 dropped=62
               null_request_id=3 null_user_id=7 null_item_id=48 null_event_type=0 null_timestamp=4
```

**Every declared reason is emitted every batch, including zeros, in declared order, and the line is
emitted even when `dropped=0`.** A steady `dropped=0` is the positive evidence that the counter is
alive. A metric that stays silent when it has nothing to report is indistinguishable from one that
is broken, which is the failure this work exists to fix.

### 4. The engine reports decode outcomes

In `processDecodedBatch`, after `archive.writeDeadLetters` succeeds — durable write first, so a
failed archive aborts before logging a number that was never persisted:

```scala
reporter.reportDecode(deadLetters, validCount, spark.sparkContext.appName, batchId)
```

counting `groupBy("error_code")` on the already-persisted dead-letter frame. This replaces the
null-`user_id` proxy with the four real codes: `invalid_marker`, `unknown_fingerprint`,
`corrupt_payload`, `required_field`. The job name comes from `appName`, which `SparkSessions.create`
sets per job.

The decode split is the one site that costs two counts rather than one: `valid` and `deadLetters`
are separate frames, so there is no single tagged frame to group. Both are already persisted by
`processDecodedBatch`, so `validCount` is a scan of materialized data, not a re-parse.

`processDecodedBatch` takes the reporter as a defaulted parameter so the engine test can assert on
counts without capturing logs. This follows `LateFeedbackJoin.process`, which already takes `nowMs`
as a parameter so its close rule stays testable. Spark 3.5 uses log4j2, this repository has no
logging configuration at all, and programmatic appender capture would be more machinery than
anything else in the suite.

### 5. Retiring the broken path

Three deletions, all orphaned by this change:

- `BatchMetricsListener.format`'s `corrupt` parameter and the `observedMetrics.get("ingest")` read.
  The lookup can never resolve in the Avro engine; leaving it would report a hardcoded `0` beside
  real numbers.
- `EventParsing.observeIngest` and its spec — no remaining callers.
- The `corrupt=7` assertion in `BatchMetricsListenerSpec`.

`[batch-metrics]` keeps `query`, `rows`, `rps`, `batchMs` and remains what it is: a throughput line.
Rejection accounting moves entirely to `[drop-metrics]`.

### 6. Gate sites

| Site | Reasons | Family |
|---|---|---|
| `OnlineJoinerStreamingJob.parseEvents` | 5 null-field rules | foreachBatch |
| `UserEventStreamingJob.normalize` | `null_user_id`, `null_item_id` | foreachBatch |
| `ExperienceCollectorStreamingJob.parseSamples` | 3 null-field rules | foreachBatch |
| `RankingSampleStreamingJob.parseSamples` | `null_user_id`, `null_item_id` | moves inward |
| `RecallSampleStreamingJob.parseSamples` | `null_user_id`, `null_item_id` | moves inward |
| `RelevanceSampleStreamingJob.parseSamples` | `null_user_id`, `null_item_id` | moves inward |
| `MovieLensContextCollectorStreamingJob.parseEvents` | `unclassifiable_shape` | moves inward |
| `ExecutionEngine.processDecodedBatch` | 4 Avro `error_code`s | engine |

"Moves inward" means the `parseSamples`/`parseEvents` call relocates from the streaming plan into
the `foreachBatch` body, so all eight sites count identically. `Ranking` and `Relevance` already
`collect()` distinct ids off an unpersisted batch before writing it, so they perform two full passes
today; persisting there makes it one pass plus a cheap tag.

## Scope

**In scope:** the eight sites above, `FieldGate`, `DropMetrics`, the `BatchMetricsListener` and
`EventParsing` deletions, and the test inventory below.

**Explicitly not counted as drops.** `dedupedClicks`' `event_type === "click"` filter is business
selection, not rejection; counting it would report roughly 90% "drops" on a healthy stream.
`buildTrainingSamples`' `impression_ts.isNotNull` filter, which drops pure late-feedback groups, is
already instrumented by `LateFeedbackJoin.orphanCounts`.

**Not touched.** `ExecutionEngine`'s non-Avro `run` overload has no production callers — both jobs
use the Avro one. Pre-existing dead code, flagged rather than deleted.

**Rejected alternatives.** Routing rejected rows to the dead-letter archive for redrive (adds a
write per batch; only the Avro family has an archive wired). Publishing counts to
`recommendation_metrics` (no consumer). A `QueryExecutionListener` to catch observed metrics off
inner batch queries (fires once per inner action with no clean attribution to a batch or job).
A `DROP_METRICS_ENABLED` kill switch: the cost lands on frames that are persisted or tiny, two
affected jobs come out faster, and a switch on an observability feature is how it ends up disabled
on the day it is needed.

## Testing

Every test asserts on a count, not a rendering. That is the specific gap that let a dead metric pass
CI for two months.

| Spec | Change |
|---|---|
| `FieldGateSpec` | New. First-match attribution, the null-predicate coalesce guard, kept/rejected split, `counts` including declared zeros in order, empty input |
| `DropMetricsSpec` | New. `format` determinism and reason ordering |
| `ExecutionEngineSpec` | Add: `processDecodedBatch` over a mixed valid/corrupt batch reports per-`error_code` counts |
| 7 job specs | Adapt call sites to `.kept`; add one rejection-count assertion each |
| `BatchMetricsListenerSpec` | Drop the `corrupt=7` assertion |
| `EventParsingSpec` | Remove the `observeIngest` test |

The per-job assertion is the regression test for the observe-after-filter defect: feed a batch
containing one null-`user_id` row and assert `null_user_id = 1`. A gate built on an already-filtered
frame yields `0` and fails.

The cross-plan defect is designed out rather than tested away. The count is a `collect()` on the
frame in hand, logged in the same call, so no handoff between plans remains to break.

Tests run under JDK 17. The default JDK 25 aborts every Spark-session test with a misleading
`getSubject` error.

## Success Criteria

1. A batch containing rows with null `request_id`, `user_id` and `item_id` produces one
   `[drop-metrics]` line per gate whose per-reason counts sum to `dropped`, and `kept + dropped`
   equals the input row count.
2. A batch containing a corrupt Avro payload and a payload missing a required field produces a
   decode line with `corrupt_payload=1` and `required_field=1`.
3. A clean batch still produces a `[drop-metrics]` line reading `dropped=0` with every declared
   reason at zero.
4. `[batch-metrics]` no longer contains a `corrupt` field.
5. `sbt test` passes under JDK 17.

## Accepted Limitations

**No test verifies that each job's `main()` calls `report`.** Mains are not exercised by this suite
— a pre-existing property, not one this change introduces. The engine sites are covered because the
engine is testable; per-job wiring is verified by reading the diff and one local run.

**Counts are logs, not queryable series.** Detecting a slow rise in drop rate means grepping job
logs. Publishing to a metrics backend was considered and deferred; `recommendation_metrics` is the
obvious destination once anything consumes it.

**Recall and MovieLensContext pay one extra shuffle per micro-batch.** `Ranking` and `Relevance`
come out net faster; `OnlineJoiner`, `UserEvent` and `ExperienceCollector` are unchanged because
their frames are already persisted.
