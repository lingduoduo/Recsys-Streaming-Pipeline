# Columnar, Time-Partitioned Sequence Store — Design

**Date:** 2026-07-23
**Status:** Approved (design), pending implementation plan

## Problem

User interaction sequences are stored row-oriented, with no time dimension, in
CSV blobs:

- `MovieLensContextCollectorStreamingJob` writes `recentlyRatedMovieIds` and
  `actionSequenceMovieIds` as comma-joined strings inside one
  `user:{id}:features` hash, via read-modify-write (`hget` → `mergeRecent` →
  `hset`).
- `MovieLensServingSideEffects` writes `user:{id}:served_history` the same way.
- `RedisUserMovieHistoryClient` reads `user:{id}:recent` / `:rated` lists capped
  at 50.

Three consequences:

1. **Sequences cannot grow.** Every append rewrites the whole blob, so the cap
   (50 rated, 100 served) is a cost-control measure, not a modeling choice.
   Long-sequence models are structurally impossible today.
2. **Signals are welded together.** Adding an enrichment column to a sequence
   means changing the blob format in every reader and writer, and every reader
   pays for every signal whether it uses it or not.
3. **No time locality.** "Activity in the last 7 days" requires reading the
   entire history; retention requires rewriting it.

This design introduces a **columnar, time-partitioned sequence store** with two
physical materializations from one schema: Redis for serving, Parquet for
training.

## Scope

**In scope:**

- `com.demo.sequence` (Scala): `SequenceSchema`, `SequenceEncoder`, the pure
  `merge` seam, `SequenceRedisSink`, `SequenceParquetSink`.
- `SequenceCodec` (Java): encode/decode of the packed column strings, mirroring
  the Scala encoding.
- Producers for `kind=rating` (`MovieLensContextCollectorStreamingJob`) and
  `kind=click` (`UserEventStreamingJob`), dual-writing alongside today's paths.
- `SequenceBackfillJob` — one-shot, idempotent, populates the new store from the
  existing ratings source.
- Java serving read path: `RedisSequenceClient`, `SequenceSlice`, and
  `RatingSequencesQueryHydrator` behind `recsys.sequence.mode`.

**Out of scope (YAGNI — not built):**

- The `served` producer. It is the only writer on the serving request path, it
  would need the encoder reimplemented in Java, and the existing
  `served_history` blob works at its 100-item scale. Adding it later is a
  writer-only change.
- Deleting or migrating any legacy key. Old keys keep their writers and readers.
- Compaction, tiering to cold storage, per-column TTLs, item-side (cross-user)
  sequences.

## Key finding: the four "sequence kinds" are not four sequences

`RatingSequencesQueryHydrator` derives `action` (50), `retrieval` (100), and
`scoring` (20) by **truncating one deduped `recentlyRatedMovieIds` list**. Only
`served` is written independently. The real partitioning dimension is therefore
the **producer**, not the consumer:

| kind | producer | today |
|---|---|---|
| `rating` | `MovieLensContextCollectorStreamingJob` | CSV blob in `user:{id}:features` |
| `click` | `UserEventStreamingJob` | **not persisted per-user at all** — only `global:item_popularity` |
| `served` | `MovieLensServingSideEffects` (serving-time) | CSV blob in `user:{id}:served_history` — deferred |

`action` / `retrieval` / `scoring` remain **read-time views** (length truncation)
over these. Storing them as three physical partitions would be 3× write
amplification for data that is a prefix of the same sequence.

**Pre-existing dead code, noted but not touched:** `user:{id}:recent` and
`user:{id}:rated`, read by `RedisUserMovieHistoryClient`, have no writer anywhere
in the repo. Those reads always return empty.

## Data model

**Partition key:** `seq:{userId}:{kind}:{bucket}`, where `kind ∈ {rating, click}`
(plus `served` in a later version) and `bucket` is a UTC day stamp `YYYYMMDD`.
Bucket width is configurable via `SEQ_BUCKET_WIDTH`, default `day`.

**Value:** one Redis HASH, one field per column, positionally aligned — row *i*
is element *i* of every field.

| field | encoding | example |
|---|---|---|
| `item_id` | `,`-joined | `31,1029,1061` |
| `ts` | `,`-joined epoch millis, ascending | `1690000001000,...` |
| `action` | `,`-joined | `click,rate,click` |
| `rating` | `,`-joined, empty element = null | `,4.0,` |
| `genres` | `,`-joined rows, `\|` within a row | `Drama\|Comedy,Action,` |
| `release_year` | `,`-joined, empty element = null | `1995,,1999` |
| `n` | row count | `3` |

`genres` uses `|` within a row because genre strings already contain commas
today; a naive `,`-join would silently mis-align every column to its right.

`n` is the consistency guard. A reader that finds any column whose element count
differs from `n` truncates all columns to the minimum and logs it. Writes are a
single `HSET`, so this should never fire — but a torn write must degrade, not
corrupt.

**Retention:** `EXPIRE` on each bucket key at `SEQ_LOOKBACK_DAYS` (default 90).
Old buckets vanish without a compaction job.

**Parquet mirror:** identical columns, `partitionBy("bucket", "kind")`, one row
per event (exploded — Parquet does its own columnar encoding, so no packed
strings offline).

Column names, `kind` values, and the bucket function come from one
`SequenceSchema` object. The Java reader mirrors the constants, with a test
asserting the two agree.

## Write path

```scala
object SequenceSchema {
  val Columns = Seq("item_id","ts","action","rating","genres","release_year")
  def bucket(tsMillis: Long, width: BucketWidth): String   // UTC → "20260723"
  def key(userId: String, kind: String, bucket: String): String
}

object SequenceEncoder {
  // events(user_id, kind, item_id, ts, action, rating, genres, release_year)
  //   → (user_id, kind, bucket, item_id, ts, action, rating, genres, release_year, n)
  def toColumnChunks(events: DataFrame, width: BucketWidth): DataFrame
}
```

`toColumnChunks` is one `groupBy(user_id, kind, bucket)` with
`sort_array(collect_list(struct(ts, ...)))` — the idiom
`ItemSequencePreprocessingJob` already uses, so no UDF.

### `SequenceRedisSink extends Sink`

Per partition, follows the two-phase pattern established in
`MovieLensContextCollectorStreamingJob`:

- **Phase 1** — direct, non-pipelined `HMGET` of each existing bucket, then
  `merge(existing, new, maxRows)`.
- **Phase 2** — all `HSET` + `EXPIRE` through one pipeline.

Reads must not interleave with an open pipeline on the same connection: `hget`
would read buffered pipeline replies and corrupt the protocol. That constraint is
inherited deliberately, not rediscovered.

Two decisions keep the writer cheap:

- **The writer never re-sorts.** New rows are appended batch-sorted by `ts`; a
  late event lands after newer rows within the same bucket. The **reader** sorts
  by `ts` after decoding, bounded by `maxRows`, so ordering is exact at read time
  and the write stays O(appended).
- **No dedup on write.** The store is a raw event log, which is what training
  needs. Dedup stays a read-time view, where `RatingSequencesQueryHydrator`
  already does it.

`SEQ_MAX_ROWS_PER_BUCKET` (default 500) bounds a single bucket so the
read-modify-write cannot degenerate; on overflow the oldest rows in that bucket
drop.

### `SequenceParquetSink extends Sink`

The same chunk DataFrame `explode`d back to one row per event,
`partitionBy("bucket", "kind")`, append mode.

### Producers

| kind | job | change |
|---|---|---|
| `rating` | `MovieLensContextCollectorStreamingJob` | add both sinks alongside the existing CSV write |
| `click` | `UserEventStreamingJob` | add both sinks next to the popularity sink — new capability |

### `SequenceBackfillJob`

One-shot Spark job: read the existing ratings source → `SequenceEncoder` → both
sinks. Idempotent per `(user, kind, bucket)`: `SequenceRedisSink` takes a
`mode: Append | Overwrite` parameter (streaming producers use `Append`, which
runs the phase-1 `HMGET` + `merge`; backfill uses `Overwrite`, which skips
phase 1 and `HSET`s the chunk directly), and `SequenceParquetSink` writes
`overwrite` with dynamic partition overwrite. Re-runs are therefore safe.
Required for shadow mode to be meaningful — a diff against an empty store proves
nothing.

## Read path

```java
SequenceSlice read(String userId, String kind, Set<String> columns,
                   int maxRows, Duration lookback);
```

`SequenceSlice` is **columnar in Java too** — per-column arrays plus `size()`,
not a `List<SequenceRow>`. A caller that asked for `item_id,ts` never allocates
rating or genre objects. Decoding into row objects at the boundary would reduce
column pruning to a network-bytes optimization.

**Bucket walk, newest-first, early exit:** iterate day buckets from today
backwards, pipelining `HMGET key <requested columns> n` in chunks of
`SEQ_BUCKET_FETCH_CHUNK` (default 7), stopping the moment `maxRows` is filled.
A user active this week costs one round trip of 7 `HMGET`s; `SEQ_LOOKBACK_DAYS`
is a ceiling, not the normal cost. Decode → sort by `ts` descending → truncate.

`RatingSequencesQueryHydrator` becomes
`read(userId, "rating", {item_id, ts}, 100, 90d)` → dedup → truncate to
50 / 100 / 20. Same three outputs and order semantics; the source is no longer
capped at 50.

## Rollout

`recsys.sequence.mode`, default `off`:

- **`off`** — today's path exactly.
- **`shadow`** — read both, **serve the old result**, log a structured diff
  (length, prefix agreement, first divergence index).
- **`on`** — serve the new result. On **exception**, fall back to the old client
  and increment a counter. On **empty**, do *not* fall back: an empty sequence
  for a user with no events is a correct answer, and masking it would hide
  exactly the bug shadow mode exists to catch.

Legacy keys are untouched. `user:{id}:features` keeps its CSV fields,
`served_history` keeps its blob, both writers keep writing. Nothing is deleted.

## Testing

The repo's convention is to test a pure seam rather than Redis itself
(`RedisSinkSpec` tests `foreachWithFlush` with counters and never opens a
connection). Every non-trivial decision here lives in a pure function.

**Scala** (`AnyFlatSpec`, `local[1]` session via `SparkTestSupport`):

- `SequenceSchemaSpec` — UTC bucket boundaries (23:59:59.999 and 00:00:00.000
  land in different buckets); key format.
- `SequenceEncoderSpec` — grouping by `(user, kind, bucket)`; ascending `ts`
  within a chunk; null `rating` / `release_year` encode as empty and round-trip
  as null; a genre string containing a comma survives; `n` equals every column's
  element count.
- `SequenceMergeSpec` — the pure `merge(existing, new, maxRows)` seam: concat
  preserves order; overflow drops oldest; empty existing is identity.
- `SequenceParquetSinkSpec` — the written frame carries `bucket` / `kind`
  partition columns and one row per event.
- `MovieLensContextCollectorStreamingJobSpec` and `UserEventStreamingJobSpec`
  stay green unmodified.

**Java** (JUnit + Mockito):

- `SequenceCodecTest` — decode round-trip; ragged columns truncate to the minimum
  and log rather than mis-aligning rows; `n` mismatch takes the same path.
- `RedisSequenceClientTest` — the walk stops after one chunk when `maxRows` is
  filled (asserted on the number of pipelined batches, not just the result);
  `lookback` bounds the walk; output is `ts`-descending across bucket boundaries.
- `SequenceSchemaConstantsTest` — Java constants match a checked-in schema
  fixture that `SequenceSchemaSpec` also asserts against. Cross-language drift is
  this design's most exposed failure mode, so it gets a test rather than a
  comment.
- `RatingSequencesQueryHydratorTest` — existing assertions stay green under
  `mode=off`; new cases cover `on`.

## Success criteria

1. `sbt test` and `mvn test` pass, including every existing spec unmodified.
2. With `recsys.sequence.mode=off`, serving output is unchanged — proven by the
   untouched hydrator tests.
3. A user with 200 rated items yields 100 `retrievalSequenceMovieIds`. Today that
   is structurally impossible, since the source list is capped at 50. **This is
   the criterion the design exists to satisfy.**
4. A hydrator asking for `{item_id, ts}` issues `HMGET` with exactly those fields
   plus `n`, verified on a mock. Column pruning is asserted, not assumed.
5. A user active today costs one Redis round trip regardless of
   `SEQ_LOOKBACK_DAYS`.
6. Shadow mode over a `run-movielens-segment-sim.sh` run reports prefix agreement
   with the legacy path for users under 50 items.

## Configuration

| setting | default | where |
|---|---|---|
| `SEQ_BUCKET_WIDTH` | `day` | Spark jobs |
| `SEQ_LOOKBACK_DAYS` | `90` | both |
| `SEQ_MAX_ROWS_PER_BUCKET` | `500` | Spark jobs |
| `SEQ_BUCKET_FETCH_CHUNK` | `7` | serving |
| `recsys.sequence.mode` | `off` | serving |

## Known limitations (accepted at merge, 2026-07-24)

Surfaced by the whole-branch review; accepted as-is because all three are off
the default path, and documented here rather than fixed.

1. **`SEQ_BUCKET_WIDTH=hour` is incompatible with serving.** The Scala writers
   support an `hour` bucket width (`yyyyMMddHH`), but the Java read path
   (`SequenceSchemaConstants.bucket`, `RedisSequenceClient.bucketsToWalk`) only
   understands day buckets (`yyyyMMdd`). If a producer runs with `hour` while
   `recsys.sequence.mode=on`, every serving key misses and the reader returns an
   **empty** sequence — which is deliberately never fallback-masked, so it fails
   silent, not loud. The default (`day`) is safe end to end and is what every
   test exercises. **Do not set `SEQ_BUCKET_WIDTH=hour` while serving from the
   store.** The clean fix, if hour bucketing is ever wanted, is to drop the hour
   capability entirely (YAGNI — nothing consumes it today) or thread the width
   through the Java reader. Tracked for later.

2. **Popularity double-count on micro-batch retry (click producer).** In
   `UserEventStreamingJob`, the non-idempotent `zincrby global:item_popularity`
   commits *before* the sequence write in the same `foreachBatch`. Because
   `SequenceRedisSink` deliberately fails the batch on Redis infrastructure
   errors (rethrows `JedisException` so Spark retries from the checkpoint), a
   transient sequence-store error *after* the `zincrby` commits re-runs the
   `zincrby` on retry, inflating popularity. This is an amplification of the
   pre-existing at-least-once behavior of that counter, not a new bug class.
   Window is narrow (a hard Redis outage fails the `zincrby` too, so nothing
   commits). Fix options if it ever matters: make popularity idempotent, or
   reorder so the sequence write precedes the `zincrby`.

3. **No producer-side dual-write kill switch.** `SequenceSinks.write` is called
   unconditionally in both producers; combined with fail-batch-on-`JedisException`
   (#2), a sequence-store Redis problem stalls both streaming jobs' batches with
   no runtime mitigation short of revert/redeploy. A `SEQ_WRITE_ENABLED` flag
   mirroring the serving-side `recsys.sequence.mode` is the intended fast-follow.

The serving-side sequence read path is gated by `recsys.sequence.mode` (default
`off`), so none of the above affects production until the store is deliberately
enabled.
