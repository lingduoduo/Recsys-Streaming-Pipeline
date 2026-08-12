# Late-Feedback Join Design

## Purpose

Make `OnlineJoinerStreamingJob` join feedback to its impression across micro-batch boundaries, so a
click or order that arrives after its impression's batch produces a correctly labelled training
sample instead of being discarded.

This is the fix that `2026-08-10-late-feedback-visibility-design.md` deliberately left out of scope.
That work made the loss visible and reproducible; this work removes it.

## Motivation

`buildTrainingSamples` groups a micro-batch by `(request_id, user_id, item_id)` and drops groups
with no impression in that batch:

```scala
// Drop groups that have no impression in this batch (pure late-feedback events)
.filter(col("impression_ts").isNotNull)
```

The consequence is not lost throughput. It is **a training sample labelled "not clicked" for an item
that was clicked**: the earlier batch already published the impression with `clicked = 0`,
`label = 0.0`, the click's own group has no impression and is dropped, and nothing restates the
published row. `OnlineJoinerStreamingJobSpec` pins this today.

The producers' own model of user behaviour puts clicks 1–20s and orders 21–120s after impression.
Against the default 10-second trigger, production traffic crosses that boundary continuously.

## Constraints Discovered

**Spark's stateful operators are unavailable where the join runs.** `OnlineJoinerStreamingJob` uses
the Avro `ExecutionEngine.run` overload, which applies its stages *inside* `foreachBatch` on a static
per-batch DataFrame. `flatMapGroupsWithState` and friends cannot run there, and restructuring the
engine to expose a streaming plan would conflict with the archive-before-transform guarantee that
requires `foreachBatch`. The fix must be a durable snapshot store in the style of
`RawArchiveSink.deduplicateValid`.

**`training_samples` has eleven consumers.** Four streaming jobs read the Kafka topic
(`RecallSampleStreamingJob`, `RankingSampleStreamingJob`, `RelevanceSampleStreamingJob`,
`ExperienceCollectorStreamingJob`) and seven report jobs read the Parquet sink. Every one of them
assumes one row per `sample_id`. Any design that restates a published sample forces all eleven to
dedupe by version, and an offline trainer that misses that change trains on both the wrong and the
right label for the same impression.

**Retries must recompute, not accumulate.** `deduplicateValid` reads the snapshot for batch `N-1` and
writes `N`, so a batch that fails after writing state recomputes identically on retry. Any new store
must follow the same rule or a retry will double-publish or strand rows.

## Design

### 1. One row per sample, published when its window closes

A slate's sample is published once, after its feedback window has closed, carrying whatever feedback
arrived inside the window. `sample_id` stays unique, so no consumer changes.

The cost is latency: a sample now reaches `training_samples` one `FEEDBACK_JOIN_WAIT` after its
impression rather than in the impression's own batch. The `date` partition is still
`to_date(impression_time)`, so a late-published sample lands in its impression's date partition, and
the report jobs' lookback windows (bounded in PRs #172–#174) are far larger than the delay.

### 2. `LateFeedbackJoin`, a durable pending-slate store

A new `LateFeedbackJoin` in `com.demo.process` becomes the joiner's batch stage, wrapping the
existing `buildTrainingSamples`. Per micro-batch `N`:

1. **Load** the pending snapshot written by batch `N-1`: raw event rows, each carrying the
   `first_seen_ms` stamped when it first entered the store.
2. **Union** it with this batch's staged events, stamping `first_seen_ms = batch wall clock` on rows
   entering the store for the first time.
3. **Score each slate** — group by `(request_id, user_id, item_id)` for `impression_ts`, the slate's
   earliest event time, and its minimum `first_seen_ms`.
4. **Split** slates into *due* and *open* by the close rule in section 3.
5. **Publish** `buildTrainingSamples(events of due slates)` — unchanged code, unchanged output
   columns.
6. **Persist** the events of open slates as the snapshot for batch `N`, then hand the published rows
   to the sinks.

The store holds **raw events**, not merged partial aggregates. Feeding `pending ∪ batch` into the
existing aggregation reuses `max_by` last-feedback-wins and its `(timestamp, event_id)` tiebreak
verbatim, under the tests that already cover them. Merging pre-aggregated rows would mean
re-implementing every aggregate's associativity and carrying its ordering keys explicitly — new
logic in the one place a mistake silently corrupts labels, to save bytes that are not scarce.

State lives at `_queries/<namespace>/_pending/<batchId>` under the existing archive root. Reading
`N-1` and writing `N` gives the retry semantics above, and `completeBusinessBatch`'s existing rule —
delete snapshots older than `N-1` — compacts it.

### 3. The close rule: event time or wall clock

A slate is **due** when either arm fires:

- **Event time:** `max event timestamp observed over pending ∪ batch` ≥
  `coalesce(impression_ts, min event time in the slate) + FEEDBACK_JOIN_WAIT`. An archive backfill
  that lands a week of events in one batch therefore closes everything immediately.
- **Wall clock:** `batch wall clock − min(first_seen_ms) ≥ FEEDBACK_JOIN_WAIT`. This drains the store
  when the stream goes idle. Empty micro-batches still fire on the trigger interval, evaluate the
  rule, and flush — without this arm, a sim whose producer has finished would end with samples
  stranded in the store forever.

The deadline is anchored on `impression_ts`, never on the latest feedback, so feedback cannot extend
a slate's window. That is what guarantees a slate publishes exactly once.

### 4. Orphan feedback is dropped and counted

A slate that comes due with a null `impression_ts` is feedback whose impression never arrived inside
the window — genuinely very late, or belonging to a slate published in an earlier window. It
publishes nothing, exactly as today, and its counts are logged per batch:

```
[late-feedback] batch=N orphan_slates=… orphan_events=…
```

This makes the residual loss measurable. It does not eliminate it: feedback arriving more than
`FEEDBACK_JOIN_WAIT` after its impression is still lost, by construction of the one-row contract.

### 5. Configuration

| Var | Default | Meaning |
|---|---|---|
| `FEEDBACK_JOIN_WAIT` | `3 minutes` | How long a slate's feedback window stays open. Same interval syntax as `EVENT_WATERMARK_DELAY`. |

Three minutes covers the producers' 120-second order tail with margin. `0` restores today's
behaviour exactly — every slate is due on arrival — which gives the tests a clean way to prove the
old path still works. The interval is parsed and required to be non-negative in `LateFeedbackJoin`'s
constructor, which `main` builds before the stream starts, so a bad value fails the job at startup.
It is not added to `EngineConfig`: the wait belongs to this one job, not to every engine adopter.

Steady state holds one window of raw events (`throughput × wait`), rewritten once per trigger
interval: a few thousand rows per snapshot at simulation volume.

### 6. Extracting the commit protocol

The snapshot commit protocol — attempt directory, manifest with row count and SHA-256 inventory,
atomic non-overwriting rename, validation on read — is private inside `RawArchiveSink`. It moves to a
shared helper in `com.demo.engine`, used by both `RawArchiveSink`'s dedupe state and the new pending
store. The extraction is mechanical and its regression gate is the existing 545-line
`RawArchiveSinkSpec`, which must pass unmodified.

The alternative, duplicating the protocol in the new store, leaves two copies of the durability
contract to keep in sync; folding the pending store into `RawArchiveSink` pushes join semantics into
the archive sink and grows a 448-line file that already does two jobs.

### 7. Sims

`run-movielens-segment-sim.sh` and `run-movie-category-sim.sh` compute a drain floor of
`150s × FEEDBACK_DELAY_SCALE`. The joiner now holds each slate a further `FEEDBACK_JOIN_WAIT` past
its impression, so draining on the current floor would exit with samples still pending and report
short. Both scripts export `FEEDBACK_JOIN_WAIT` scaled by `FEEDBACK_DELAY_SCALE` — the same 10-second
minimum the tail floor uses — and extend the drain floor to `tail + join wait + one trigger
interval`. A sim at `FEEDBACK_DELAY_SCALE=1.0` runs roughly three minutes longer; a compressed run
barely changes.

## Scope

In scope:

- `services/spark-streaming-job/src/main/scala/com/demo/process/LateFeedbackJoin.scala` (new)
- `services/spark-streaming-job/src/main/scala/com/demo/engine/` — extracted commit helper (new file)
  and `RawArchiveSink.scala` delegating to it
- `OnlineJoinerStreamingJob.scala` — its `batchStages` and the `FEEDBACK_JOIN_WAIT` read
- `scripts/run-movielens-segment-sim.sh`, `scripts/run-movie-category-sim.sh`
- `docs/recommendation_architecture/Data_Pipeline.md`, `README.md`
- the matching test files

Out of scope:

- the event schema, the Avro decoder, and the on-disk commit protocol format
- `ExecutionEngine` — the join is a batch stage, the engine does not learn about it
- every consumer of `training_samples`; the one-row contract is what keeps all eleven untouched
- `RawArchiveSink`'s archive and dead-letter paths, and `run_replay`

## Testing

`LateFeedbackJoinSpec`:

- a click in batch 2 joins its batch-1 impression and publishes one row with `clicked = 1`,
  `label = 1.0`
- an impression alone publishes nothing while its window is open
- a click at +5s and an order at +90s land on the same single published sample
- `FEEDBACK_JOIN_WAIT=0` reproduces today's per-batch output exactly
- a slate with feedback and no impression publishes nothing and is counted as orphan
- the wall-clock arm flushes a pending slate across empty batches
- the event-time arm closes a slate when a much newer batch arrives, without wall-clock time passing
- replaying batch `N` after its snapshot was written produces byte-identical published rows

Existing suites that must pass unmodified: `RawArchiveSinkSpec` (the extraction's regression gate)
and `ExecutionEngineSpec`. PR #175's two pinned tests stay and stay true — they assert
`buildTrainingSamples` over a single batch, which remains batch-local; a comment points at
`LateFeedbackJoin` as the layer that now spans batches so they are not misread as end-to-end
guarantees.

## Success Criteria

- A click arriving in a later micro-batch than its impression yields exactly one training sample,
  labelled `clicked = 1, label = 1.0`.
- No `sample_id` is ever published twice.
- Output columns, the Kafka key, and the Parquet `date` partition are unchanged.
- A retried micro-batch publishes the same rows it published before the retry.
- Running a sim end to end produces at least one sample whose feedback arrived in a later micro-batch
  than its impression and is labelled correctly.
- `FEEDBACK_JOIN_WAIT=0` reproduces the pre-change output.

## Accepted Limitations

- Feedback arriving more than `FEEDBACK_JOIN_WAIT` after its impression is still dropped. It is
  counted, not recovered. Recovering it would require restating published samples, which the one-row
  contract rules out.
- Every training sample is now published one window later than before. This is a deliberate trade of
  latency for label correctness.
- The pending store is rewritten in full once per trigger interval. At the volumes this pipeline
  targets that is inexpensive; at much higher throughput it would want incremental state.
