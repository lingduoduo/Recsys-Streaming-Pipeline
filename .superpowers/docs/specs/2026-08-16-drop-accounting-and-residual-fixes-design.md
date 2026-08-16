# Drop Accounting and Residual Projection Fixes Design

## Purpose

Give every field-level gate a per-reason count, and close the four findings the
[filtering and projection audit](2026-08-16-filtering-and-projection-correctness-design.md)
recorded as deferred. Drop accounting comes first because one of the residual fixes is expressed
as a gate rule and needs the machinery.

## Motivation

PR #181 made the gates and projections correct. PR #182 deleted the `corrupt` metric that could
only ever report zero. The result is a pipeline whose gates are right and whose rejections are
invisible: a producer that starts emitting `request_id`-less events costs training samples with no
signal anywhere. Decode failures still land in the dead-letter archive with error codes, but the
seven null-identifier gates count nothing.

Three residual findings from the audit remain, one of which has now been verified as a live defect.

**The DST round trip is real, not speculative.** The audit flagged it as unverified. A probe
settles it — `to_timestamp(from_unixtime(ts))` in `America/New_York`:

| epoch in | epoch out | delta |
|---|---|---|
| 1793511000 (2026-11-01T05:30Z, 01:30 EDT) | 1793511000 | 0 |
| 1793514600 (2026-11-01T06:30Z, 01:30 EST) | **1793511000** | **-3600** |

Both instants format to the same local wall clock `2026-11-01 01:30:00`, and parsing back resolves
to one of them. The later event is silently moved an hour into the past and collapsed onto the
earlier one. In UTC both round-trip exactly.

PR #181 set `spark.sql.session.timeZone = UTC` by default, so this is masked today — but masked by
a config any `spark-submit` override or bare `SparkSession.builder` can undo, which is the same
fragility that argument rejected as a primary fix for the date partitions.

## Constraints Discovered

**The gates sit in two plan topologies.** `OnlineJoiner`, `UserEvent` (via the engine) and
`ExperienceCollector` gate inside `foreachBatch`. `Ranking`, `Recall`, `Relevance` and
`MovieLensContext` gate in the streaming plan. Bridging that split with `observe` is exactly what
produced the metric #182 deleted; this design erases the split instead.

**`observe` cannot work in the Avro engine.** `ExecutionEngine.run` applies zero stages to the
streaming DataFrame, so its plan holds no `CollectMetrics` nodes and
`StreamingQueryProgress.observedMetrics` is empty. Counting must be an explicit driver-side
aggregation, not an observation read back from a listener.

**The sequence-store packing format cannot change cheaply.** `SequenceEncoder` joins values with
`,` and `|`. Changing to an escaping scheme would require decoding on every read path — Scala
`SequenceCodec`, the Parquet sink, the Redis sink — plus a migration for data already written under
the old scheme, and existing values may already contain the escape character. The fix must not
change the format.

## Design

### 1. `FieldGate`: rejection rules as data

A gate takes a frame and an ordered `Seq[(reason, rejectWhen)]`, phrased as rejection conditions to
match the `rejection_reason` when-chain in `UserBehaviorProfileBatchJob`:

```scala
FieldGate(events, Seq(
  "null_request_id" -> col("request_id").isNull,
  "null_user_id"    -> col("user_id").isNull,
  "null_item_id"    -> col("item_id").isNull,
  "null_event_type" -> col("event_type").isNull,
  "null_timestamp"  -> col("timestamp").isNull
))
```

It adds a `rejection_reason` column via a first-match `when` chain, each condition wrapped in
`coalesce(cond, lit(false))` so a null-valued predicate cannot leak an unknown into the chain — the
guard `LateFeedbackJoin` already applies to its due computation.

First-match attribution means one reason per dropped row, so per-reason counts sum exactly to the
drop total and read as a partition of the input.

### 2. Count the tagged frame, once

`FieldGate` returns `Gated`, carrying the tagged frame and the declared reason order:

```scala
final case class Gated(tagged: DataFrame, reasons: Seq[String]) {
  def kept: DataFrame                        // rejection_reason IS NULL, column dropped
  def rejected: DataFrame                    // rejection_reason IS NOT NULL
  def counts: (Long, Seq[(String, Long)])    // one groupBy: the null group is `kept`
}
```

A single `groupBy("rejection_reason").count()` yields the kept count and every per-reason count in
one shuffle. Counting kept and rejected separately would cost two passes and let the two numbers
disagree across a recompute. `Gated` is pure DataFrame algebra with no logging in it.

**Persistence is the caller's responsibility**, matching how `ExecutionEngine` already brackets
`persist`/`finally unpersist`. It is needed only where the upstream is not already persisted.

### 3. `DropMetrics`: rendering and emission

In `com.demo.util` beside `BatchMetricsListener`. `Reporter` is the seam; `DropMetrics` is its
logging implementation and the default everywhere:

```scala
trait Reporter {
  def report(gated: Gated, job: String, batchId: Long): DataFrame
  def reportDecode(deadLetters: DataFrame, validCount: Long, job: String, batchId: Long): Unit
}

object DropMetrics extends Reporter {
  def format(job: String, batchId: Long, kept: Long, reasons: Seq[(String, Long)]): String
}
```

```
[drop-metrics] job=OnlineJoinerStreamingJob batch=42 kept=4931 dropped=62
               null_request_id=3 null_user_id=7 null_item_id=48 null_event_type=0 null_timestamp=4
```

**Every declared reason is emitted every batch, including zeros, and the line is emitted even when
`dropped=0`.** A steady `dropped=0` is the positive evidence the counter is alive. A metric that
stays silent when it has nothing to report is indistinguishable from one that is broken — which is
precisely the failure #182 had to delete.

### 4. The engine reports decode outcomes

In `processDecodedBatch`, after `archive.writeDeadLetters` succeeds — durable write first, so a
failed archive aborts before logging a number that was never persisted:

```scala
reporter.reportDecode(deadLetters, validCount, spark.sparkContext.appName, batchId)
```

counting `groupBy("error_code")` on the already-persisted dead-letter frame, giving the four real
codes: `invalid_marker`, `unknown_fingerprint`, `corrupt_payload`, `required_field`. This is the
honest replacement for the metric #182 removed — a real count of a real frame, not an observation
read from a plan that never carried it.

This is the one site costing two counts rather than one: `valid` and `deadLetters` are separate
frames with no common tagged frame to group. Both are already persisted, so `validCount` scans
materialized data.

`processDecodedBatch` takes the reporter as a defaulted parameter so the engine test can assert on
counts without capturing logs — the `LateFeedbackJoin.process` precedent, which already takes
`nowMs` as a parameter to stay testable.

### 5. Gate sites

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

"Moves inward" means the parse call relocates from the streaming plan into the `foreachBatch` body
so all eight sites count identically. `Ranking` and `Relevance` already `collect()` distinct ids
off an unpersisted batch before writing it, so they perform two full passes today; persisting there
makes it one pass plus a cheap tag.

Not counted as drops: `dedupedClicks`' `event_type === "click"` filter is business selection, not
rejection, and would report ~90% "drops" on a healthy stream. `buildTrainingSamples`'
`impression_ts.isNotNull` filter is already instrumented by `LateFeedbackJoin.orphanCounts`.

### 6. Remove the DST-sensitive round trip

Three sites convert epoch seconds to a timestamp by formatting to a local string and parsing it
back:

| Site | Consequence |
|---|---|
| `OnlineJoinerStreamingJob:118` | watermark event time — an event in the repeated hour is moved back and can fall outside the watermark, dropped as late |
| `OnlineJoinerStreamingJob:152` | `impression_time` published in `training_samples`, read by `EngagementReportJob.byHour` |
| `UserEventStreamingJob:48` | watermark event time for clicks |

**Fix.** Replace `to_timestamp(from_unixtime(x))` with `timestamp_seconds(x)`, which converts epoch
seconds to `TimestampType` directly with no string formatting and no zone involved. Correct in
every session time zone rather than only in UTC, so the guarantee stops depending on a config an
override can undo.

### 7. Reject separator characters in sequence identity fields

`SequenceEncoder.sanitized` strips `,` and `|` from every packed value, so an `item_id` of `a,b`
becomes `ab` and can collide with a genuine `ab`. The packing format cannot change cheaply, and for
descriptive fields (`action`, `genres`, `rating`, `release_year`) stripping is cosmetic.

For **identity** fields it is not: `user_id` is a Redis key component and `item_id` is the value a
recommendation resolves to, so a silent collision merges two distinct entities.

**Fix.** Gate sequence events on separator characters in `user_id` and `item_id` before packing,
via `FieldGate` with reason `separator_in_identifier`, so the rows are dropped and counted rather
than silently mutated. Descriptive fields keep the existing sanitization. No format change, no
migration, no new read path.

### 8. Sessionless relevance queries fall back to the request

`RelevanceSampleStreamingJob` builds `query` as `concat_ws(":", user_id, coalesce(session_id, ""))`,
so every sessionless impression for a user collapses into one relevance query — inflating that
query's candidate set and distorting nDCG. The joiner emits `coalesce(session_id, "")`, so the
value is empty rather than null and the `coalesce` never fires.

**Fix.** Fall back to `request_id`, which is one-per-slate and non-null in `TrainingSampleSchema`
because the joiner gates on it:

```scala
when(length(coalesce(col("session_id"), lit(""))) > 0,
     concat_ws(":", col("user_id"), col("session_id")))
  .otherwise(concat_ws(":", col("user_id"), col("request_id")))
```

One query per slate is the semantically right unit when no session groups them.

## Scope

**In scope:** sections 1–8 and their tests.

**Not in scope.** The sequence packing format itself — see Constraints. `EngagementReportJob.byHour`
and `byDow` stay session-local: hour-of-day is a local-time question and changing it is a product
decision, settled in the previous design.

**Rejected alternatives.** Routing rejected rows to the dead-letter archive for redrive (adds a
write per batch; only the Avro family has an archive wired). Publishing counts to
`recommendation_metrics` (nothing consumes that topic). A `QueryExecutionListener` to catch observed
metrics off inner batch queries (fires once per inner action with no clean attribution). A
`DROP_METRICS_ENABLED` kill switch — the cost lands on frames that are persisted or tiny, two
affected jobs come out faster, and a switch on an observability feature is how it ends up disabled
on the day it is needed.

## Testing

Every test asserts on a count or a value, never on a rendering. Asserting only on `format` is what
let a dead metric pass CI for two months.

| Spec | Test |
|---|---|
| `FieldGateSpec` | New. First-match attribution, the null-predicate coalesce guard, kept/rejected split, `counts` including declared zeros in order, empty input |
| `DropMetricsSpec` | New. `format` determinism and reason ordering |
| `ExecutionEngineSpec` | `processDecodedBatch` over a mixed valid/corrupt batch reports per-`error_code` counts through an injected reporter |
| 7 job specs | Adapt call sites to `.kept`; one rejection-count assertion each |
| `OnlineJoinerStreamingJobSpec` | Watermark event time and `impression_time` are identical in `UTC` and `America/New_York` for both instants either side of the 2026-11-01 fall-back |
| `UserEventStreamingJobSpec` | Same for the click watermark |
| `SequenceEncoderSpec` | An `item_id` containing `,` is dropped and counted, not silently merged with a genuine neighbour |
| `RelevanceSampleStreamingJobSpec` | Two sessionless impressions from different requests produce two distinct `query` values, not one |

The DST tests pin the property across zones rather than the expression, so a refactor back to
`from_unixtime` fails them.

Tests run under JDK 17. The default JDK 25 aborts every Spark-session test with a misleading
`getSubject` error.

## Success Criteria

1. Every gate emits a `[drop-metrics]` line whose per-reason counts sum to `dropped`, with
   `kept + dropped` equal to the input row count.
2. A batch with a corrupt payload and one missing a required field reports `corrupt_payload=1` and
   `required_field=1`.
3. A clean batch still emits a line reading `dropped=0` with every declared reason at zero.
4. Watermark event time and `impression_time` are identical in UTC and a DST zone for both instants
   either side of a fall-back.
5. A sequence event whose `item_id` contains a separator is dropped and counted.
6. Sessionless relevance impressions from distinct requests get distinct query keys.
7. `sbt test` passes under JDK 17.

## Accepted Limitations

**No test verifies that each job's `main()` calls `report`.** Mains are not exercised by this suite
— a pre-existing property. The engine sites are covered because the engine is testable; per-job
wiring is verified by reading the diff.

**Counts are logs, not queryable series.** Detecting a slow rise in drop rate means grepping job
logs. Publishing to a metrics backend is deferred until something consumes
`recommendation_metrics`.

**Sequence identity rejection is a drop, not a repair.** An `item_id` containing a separator is
discarded rather than encoded. That is correct for the collision risk but does lose the event; the
count makes it visible, which is the whole point.
