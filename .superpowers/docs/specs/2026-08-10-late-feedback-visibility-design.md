# Late-Feedback Visibility Design

## Purpose

Make `OnlineJoinerStreamingJob`'s handling of cross-batch feedback **visible and reproducible**:
asserted by tests, and actually exercised by the simulations.

This does **not** change how late feedback is handled. The current behaviour — dropping feedback
whose impression fell in an earlier micro-batch — remains exactly as it is. The goal is to stop that
behaviour from being invisible.

## Motivation

`buildTrainingSamples` groups a micro-batch by `(request_id, user_id, item_id)` and then drops
groups with no impression in that batch:

```scala
// Drop groups that have no impression in this batch (pure late-feedback events)
.filter(col("impression_ts").isNotNull)
```

When a click arrives in a later batch than its impression:

1. The earlier batch already published that impression with `clicked = 0`, `label = 0.0`.
2. The click's own group has no impression, so it is dropped.
3. Nothing corrects the earlier row.

The result is not merely lost throughput — it is **a training sample labelled "not clicked" for an
item that was clicked**. There is no compensating path: no stream-stream join, no reprocessing.
`EVENT_WATERMARK_DELAY` looks like the relevant control but governs deduplication only, not join
buffering.

Two things keep this hidden today:

- **The producers fabricate delay instead of having it.** `make_slate` returns a slate's impression,
  click, and order in one list, emitted in the same instant. The delay exists only in the
  `timestamp_ms` payload (`now_ms + 1–20s` for clicks, `now_ms + 21–120s` for orders). Note what
  those numbers assert: the producers' own model of user behaviour says orders occur 21–120 seconds
  after impression, which against a 10-second trigger would cross a batch boundary essentially every
  time in production.
- **No test covers it.** Every case in `OnlineJoinerStreamingJobSpec` puts impression and feedback in
  the same DataFrame.

## Constraints Discovered

Two facts shape this design and were confirmed before writing it.

**Spark's stateful operators are unavailable where the join runs.** `OnlineJoinerStreamingJob` uses
the Avro `ExecutionEngine.run` overload, which applies its "streaming stages" *inside*
`foreachBatch`, on a static per-batch DataFrame. `flatMapGroupsWithState` and friends cannot run
there. That is the same constraint that forced `RawArchiveSink.deduplicateValid` to be a hand-rolled
snapshot store. Restructuring the engine to expose a streaming plan would conflict with the
archive-before-transform guarantee, which requires `foreachBatch`. A real buffering fix is therefore
a durable pending-impression store, not a Spark timer — and it is explicitly out of scope here.

**Emitting with real delay is not sufficient on its own.** Both sims run the producer to completion
*first*, then start the Spark job with `KAFKA_STARTING_OFFSETS=earliest` and
`MAX_OFFSETS_PER_TRIGGER=1000000`. The job drains the whole backlog in its first micro-batch, so
impression and order land together regardless of when they were emitted. Making the sim honest
requires the job to be running *while* events arrive.

## Design

### 1. Pin the behaviour

Scala tests feeding `buildTrainingSamples` two separate batches — impression in the first, click in
the second — asserting what happens today:

- batch one emits one sample with `clicked = 0`, `label = 0.0`
- batch two emits nothing at all
- the click is discarded, and no mechanism restates the earlier sample

These tests are independently valuable and land regardless of the rest. They convert an accident
into an asserted decision, so any future buffering work has a baseline to change deliberately.

### 2. Deferred emission in the producers

A shared helper, `feedback_schedule.py`, holding events on a due-time heap:

```python
class FeedbackSchedule:
    def __init__(self, scale: float = 1.0, clock=time.monotonic)
    def schedule(self, delay_seconds: float, event: dict) -> None
    def due(self, now: float | None = None) -> list[dict]
    def pending(self) -> int
    def next_due_in(self, now: float | None = None) -> float | None
```

`FEEDBACK_DELAY_SCALE` (default `1.0`) multiplies the existing offsets, so a run can compress a
two-minute tail when iterating. Any scale above roughly 0.5 still crosses a 10-second trigger, so
the gap stays exercised.

Impressions are sent immediately. Clicks and orders are scheduled at their existing offsets and sent
when due. A feedback event's `timestamp_ms` stays its logical time (`impression_ms + offset_ms`),
which now approximately equals its emission time — so `feedback_delay_ms` in the training sample
keeps its current meaning.

Applied to the three live producers: `producer.py` (behavior mode), `movielens_segment_producer.py`,
`movie_segment_producer.py`. `backfill_producer.py` is deliberately excluded — it replays historical
windows in bulk, where "late" has no meaning and every event is already in the past.

### 3. Sim ordering and a tail-aware drain

`run_and_drain` currently couples starting a job to draining it, which forces produce-then-consume.
Split it:

- `start_job <class> <ckpt> <label> [env...]` — starts the job in the background, records its pid
- `drain <label> <probe> <target> [min_wait_seconds]` — unchanged semantics, plus a floor
- `stop_job <label>` — kills and reaps

The sims then: start the streaming jobs, run the producer to completion, wait out the feedback tail,
drain each job, stop them.

The `min_wait_seconds` floor matters. Today's stability check is three unchanged reads at six-second
intervals, so it can declare completion after ~18 seconds — well before a 120-second order arrives.
Without the floor the sim would report success while proving nothing, which is worse than the
current state because it would look like coverage.

## Scope

Includes:

- Scala tests pinning cross-batch feedback behaviour.
- `feedback_schedule.py` and its unit tests.
- Deferred emission in the three live producers.
- `start_job` / `drain` / `stop_job` split, a `min_wait_seconds` floor, and reordering in
  `run-movielens-segment-sim.sh` and `run-movie-category-sim.sh`.
- Documentation of the behaviour and the new knob.

Excludes:

- **Any change to how late feedback is handled.** The drop stays. Buffering impressions across
  batches is a separate, larger design.
- `backfill_producer.py` and `run-engagement-sim.sh`, which replay history in bulk.
- Any change to `ExecutionEngine`, `RawArchiveSink`, or the archive commit protocol.

## Testing

- **Scala:** impression and click in separate batches — first emits `label = 0.0`, second emits
  nothing. Impression and click in the *same* batch still produce `label = 1.0`, proving the tests
  discriminate rather than passing vacuously.
- **Python:** `FeedbackSchedule` releases nothing before its due time, releases in due order, honours
  `scale`, and reports `pending`/`next_due_in` correctly. An injected clock keeps these tests fast
  and deterministic — no sleeping.
- **Producers:** a slate's impression is emitted before its click, and its click before its order,
  with the scheduler driven by an injected clock rather than wall time.
- **Scripts:** `bash -n` on both sims, plus assertions that each starts its jobs before producing and
  passes a `min_wait_seconds` floor.

## Success Criteria

- A test asserts that cross-batch feedback is dropped and that the earlier sample keeps `label = 0.0`.
- Running a sim with default settings produces training samples where at least one slate's order
  arrived in a later micro-batch than its impression — the condition the sims previously could not
  produce.
- `FEEDBACK_DELAY_SCALE` compresses the tail without collapsing feedback back into the impression's
  batch at its default.
- No change in behaviour for `backfill_producer.py` or the engagement sim.
- The existing Scala and Python suites still pass.

## Accepted Limitations

- Sim runtime grows by up to the longest feedback delay (~2 minutes at scale 1.0). This is the cost
  of exercising the boundary at all; `FEEDBACK_DELAY_SCALE` is the escape hatch.
- The sims still cannot prove label *correctness* under late feedback, because the pipeline does not
  produce correct labels there. They will prove the condition occurs and that the current behaviour
  is what the tests say it is.
