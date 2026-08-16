# Filtering and Projection Correctness Design

## Purpose

Fix the projections that silently change the meaning of a value, and the one filter whose rule
order silently discards fields. Every item here is a case where a downstream job treats a value as
exact when the pipeline has already made it approximate, machine-dependent, or ambiguous.

## Motivation

An audit of every field-level gate and projection in the streaming jobs found the gates themselves
sound: each job rejects the identifiers it cannot work without, and the null-to-neutral coalescing
of feature maps, genres and tags is deliberate and correct. The defects are all in the
*projections* — the transformations applied to fields that survive the gate.

They share a failure mode. None of them drops a row, none raises, and none is visible in any
existing test. Each one produces a value that is the right type, in the right column, and wrong.

## Constraints Discovered

**`impression_ts` is a published unit contract with six consumers.** `training_samples` carries it
in epoch seconds, and `RecallSampleStreamingJob`, `RankingSampleStreamingJob`,
`RelevanceSampleStreamingJob`, `ExperienceCollectorStreamingJob`,
`UserBehaviorProfileBatchJob` and `CtrRankingModelTrainingJob` all read it. Changing its unit would
break all six *and* leave existing Parquet holding two units in one column with nothing to
distinguish them. The unit stays; only derived values are corrected.

**`date` is a partition column, so a change of definition cannot be applied retroactively.**
Partitions already written keep their old boundaries. New ones will use the corrected definition,
and the two coexist in the same dataset with a one-time seam at the cutover.

**The simulator cannot reproduce two of these defects.** `producer.py` stamps every impression in a
slate with the same `now_ms` and every feedback at `now_ms + k*1000`, so sub-second remainders are
identical across a slate and cancel under truncation. Any test that relies on producer-shaped data
will pass against the broken code. Tests must construct the adversarial case directly.

## Design

### 1. `date` must not depend on the deploy machine's timezone

This codebase already knows this hazard and guards against it in two places.
`SequenceSchema.bucketColumn` documents the rule:

> Uses DateType arithmetic from the epoch rather than `from_unixtime`, so the result does not depend
> on the session time zone.

and `RawArchiveSink.toUtcDate` pins its date to `ZoneOffset.UTC` explicitly. Three sites still use
the session-timezone path, and `spark.sql.session.timeZone` is set nowhere in `SparkSessions`,
`scripts/`, or `docker-compose.yml`, so all three follow the JVM default:

| Site | Current | Feeds |
|---|---|---|
| `OnlineJoinerStreamingJob:152` | `to_timestamp(from_unixtime(timestamp))` as `impression_time` | the `date` partition at line 73 |
| `ExperienceCollectorStreamingJob:120` | `to_date(from_unixtime(request_ts))` | the experiences `date` partition |
| `EngagementReportJob:51` | `to_date(impression_time)` | the per-day engagement grouping |

The consequence is not cosmetic. `CtrRankingModelTrainingJob.splitByDate` uses the `date` string as
its **train/holdout key**, so the same Parquet trained under two timezones yields two different
validation sets. `ReportWindow` lookbacks shift with it. And because the raw archive partitions by
UTC while training samples partition by local time, the two datasets cannot be cross-referenced by
date at all.

**Fix.** Introduce `TimePartitions.utcDate(epochSecondsCol)` in `com.demo.util`, built on the same
epoch-arithmetic idiom `SequenceSchema` already uses, and apply it at all three sites.
`impression_time` keeps its `TimestampType` for consumers that read it, but the `date` partition is
derived from the epoch value rather than from the formatted local timestamp.

Setting `spark.sql.session.timeZone=UTC` in `SparkSessions` was considered and rejected as the
primary fix: it makes correctness depend on a session config that any `spark-submit` override or
direct `SparkSession.builder` call in a test can silently undo. The expression-level fix is correct
regardless of session state, which is why the two existing guards in this repo are written that way.
The config is still worth setting as defence in depth, and this design does set it — but the
expression is what the tests pin.

### 2. `feedback_delay_ms` must actually be milliseconds

`OnlineJoinerStreamingJob.parseEvents` truncates the millisecond event time to integer seconds and
drops the original:

```scala
.withColumn("timestamp", (col("timestamp_ms") / 1000L).cast(LongType))
.drop("timestamp_ms")
```

`feedback_delay_ms` is then computed as `(last_feedback_ts - impression_ts) * 1000`, so it is always
a multiple of 1000 despite its name. `RecommendationResponseStatsJob` publishes it as a delay
metric, and the late-feedback design reasons about clicks 1–20s after impression, where a ±999ms
error is up to a full order of magnitude of the smallest interval being measured. Feedback arriving
within the same second records as `0`.

**Fix.** Carry `timestamp_ms` through `parseEvents` instead of dropping it. Keep `timestamp`
(seconds) exactly as it is, so the `impression_ts` unit contract and all six consumers are
untouched. In `buildTrainingSamples`, aggregate millisecond-precision impression and feedback times
alongside the existing second-precision ones, and compute `feedback_delay_ms` from that pair.

The millisecond aggregates are internal to the aggregation and are **not** added to the published
projection: `training_samples` gains no column, loses none, and the only column whose *values*
change is `feedback_delay_ms`. Its values stop being multiples of 1000, which no consumer asserts —
`RecommendationResponseStatsJob` only gates it on `>= 0`.

### 3. A null `position` must not become `0`

`coalesce(position, lit(0))` maps "no position recorded" onto `0`, which is a valid slot — the top
of the slate. `position` is nullable in the Avro schema, so the two are indistinguishable in the
output, and `ExperienceCollectorStreamingJob`'s `array_sort` comparator piles every positionless
item at the front of the slate.

**Fix.** Drop the `coalesce` and let `position` stay null. `RecommendationResponseStatsJob.ItemSchema`
already declares it nullable, so no consumer schema changes. The `array_sort` comparator's
`when(left < right, -1).when(left > right, 1).otherwise(0)` already treats a null comparison as
"equal" via its `otherwise` branch, so positionless items sort as ties rather than as leaders.

### 4. Schema nullability must state what is actually true

`EventSchemas.baseFields` declares `user_id`, `item_id` and `event_type` as `nullable = false`, and
`EventSchemas.joiner` declares `request_id` as `nullable = false` while the Avro schema defines it
as `["null", "string"]`. `from_json` does not enforce these declarations, and the code proves it:
every consumer of these schemas immediately filters those exact fields for null, which would be
dead code if the declaration bound.

**Fix.** Declare them `nullable = true`, matching both the Avro contract and what the gates
demonstrate is true in practice. This is a truthfulness change with no behavioral effect; its value
is that the next person reading the schema is not misled into removing a gate.

### 5. Event classification must not be order-dependent

`MovieLensContextCollectorStreamingJob.parseEvents` assigns a single `event_kind` by first match:

```scala
when(event_type === "rating" && user_id.isNotNull && item_id.isNotNull, "rating")
  .when(user_id.isNotNull && (age|gender|occupation|zip_code).isNotNull, "user_update")
  .when(item_id.isNotNull && (title|genres|release_year), "movie_update")
```

An event carrying a rating *and* demographics classifies as `rating`, and its `age`, `gender`,
`occupation` and `zip_code` are silently discarded. No current producer emits that shape —
`movielens_segment_producer.py` sends demographics and ratings as separate records — so this is
latent, not active. It is in scope because it is a filter whose correctness depends on rule order
rather than on the data.

**Fix.** Replace the single first-match `event_kind` with three independent boolean flags —
`is_rating`, `is_user_update`, `is_movie_update` — computed from the same conditions. Each of the
three downstream aggregations filters on its own flag, and the row-level drop becomes "none of the
three", which is exactly equivalent to today's `event_kind.isNotNull` for every shape a producer
currently emits. Ordering stops mattering, and an event carrying two kinds contributes to both.

## Scope

**In scope:** the five items above, and their tests.

**Confirmed correct, deliberately unchanged.** The seven null-identifier gates: each job rejects the
identifiers it cannot work without, and re-applying them at every hop rather than trusting upstream
is right. Null-to-neutral coalescing of `user_features`, `item_features`, `context_features`,
`genres` and `tags` is deliberate and documented. The derived-sample jobs project `impression_ts`
without a null check, which is safe because `buildTrainingSamples` filters
`impression_ts.isNotNull` upstream — an invariant, not an oversight.

**Documented, deferred.** Recorded here so they are not rediscovered as new:

- **No gate counts what it rejects**, and `BatchMetricsListener`'s `corrupt` metric has reported a
  constant zero since the Avro engine migration moved every stage inside `foreachBatch` — the
  listener reads `observedMetrics` off `StreamingQueryProgress`, which carries only `CollectMetrics`
  nodes from the streaming plan, and that plan now has none. Out of scope here: this work is about
  the gates being *right*, not about counting them.
- **`SequenceEncoder.sanitized` strips `,` and `|` from every packed value**, so an id `a,b`
  becomes `ab` and can collide with a genuine `ab`. Silent value mutation, but unreachable with
  MovieLens-shaped ids and genres.
- **`RelevanceSampleStreamingJob` builds `query` as `user_id:session_id`** with a `coalesce(_, "")`,
  collapsing every sessionless impression for a user into one relevance query. Latent: all four
  producers set `session_id`, and the joiner's `request_id` gate excludes the sessionless click
  path.
- **DST and the watermark.** `to_timestamp(from_unixtime(...))` feeds the watermark at
  `OnlineJoiner:118` and `UserEvent:48`. Local timestamps are ambiguous during a fall-back hour,
  which could in principle make event time non-monotonic. Unverified — no demonstrated path to a
  dropped row.

## Testing

Every test constructs the adversarial case directly, because producer-shaped data cannot express
two of these defects.

| Spec | Test |
|---|---|
| `TimePartitionsSpec` | New. `utcDate` agrees with `SequenceSchema.bucket` for the same instant, including across a UTC midnight boundary |
| `OnlineJoinerStreamingJobSpec` | The `date` partition is identical with `spark.sql.session.timeZone` set to `UTC`, `America/New_York` and `Asia/Tokyo`, for an impression near midnight UTC |
| `OnlineJoinerStreamingJobSpec` | Impression at `t` ms, feedback at `t + 1500` ms ⇒ `feedback_delay_ms == 1500`; feedback at `t + 400` ⇒ `400`, not `0` |
| `OnlineJoinerStreamingJobSpec` | An impression with null `position` yields a null `position`, and a slate mixing null and `0` positions does not sort the null first |
| `EventSchemasSpec` | New. The four fields are declared nullable |
| `MovieLensContextCollectorStreamingJobSpec` | An event carrying both a rating and demographics produces both a rating aggregate and a user-feature update |
| `ExperienceCollectorStreamingJobSpec` | The experiences `date` partition is timezone-independent |
| `EngagementReportJobSpec` | The per-day grouping is timezone-independent |

The timezone tests are the load-bearing ones: they pin the property directly rather than pinning the
expression, so a future refactor back to `from_unixtime` fails them.

Tests run under JDK 17. The default JDK 25 aborts every Spark-session test with a misleading
`getSubject` error.

## Success Criteria

1. Every `date` partition and day grouping is byte-identical across three session timezones.
2. `feedback_delay_ms` reports sub-second and non-round delays exactly.
3. A null `position` survives to the training sample as null.
4. `EventSchemas` declares nullable exactly where the Avro contract does.
5. An event carrying two classifiable shapes contributes to both aggregates.
6. `sbt test` passes under JDK 17.

## Accepted Limitations

**The `date` correction is not retroactive.** Partitions written before the change keep their
local-timezone boundaries. For a deployment that has only ever run in UTC there is no seam; for any
other, one day at the cutover has rows split under two definitions. Reprocessing from the raw
archive is the remedy if the seam matters; this design does not do it.

**`impression_ts` stays in seconds.** Only `feedback_delay_ms` gains true millisecond precision.
Anything else wanting sub-second impression time needs the unit change this design explicitly
rejects, and that is a migration of its own.

**The `MovieLensContext` fix is hardening, not a bug fix.** No producer emits a combined-shape
event today, so nothing observable changes until one does.
