# Four-area performance profile: ingestion, feature joins, embeddings, candidates

## Problem and scope

The request was to optimize streaming ingestion, feature joins, offline embedding training and
candidate pre-computation. None of the four had been profiled. In the 2026-09-14 post-training sweep,
the function each request named was never where the time went, so every area here was measured
end to end before any change was chosen.

This document records what was measured, which changes the measurements support, and which they do
not. Two changes ship with it; the larger finding (the joiner's snapshot rewrite) is a design for a
follow-up, not part of this change.

## Method

A throwaway ScalaTest harness (not merged) drove each job's real entry points in local Spark:
`ExecutionEngine.processDecodedBatch` with each job's own stages and sinks for the two streaming
jobs, and each offline job's training function. A `SparkListener` attributed every Spark job to its
call site, so a redundant pass shows up as a file and line rather than an inference.

- Streaming: synthetic Avro micro-batches of 5,000 events (the `MAX_OFFSETS_PER_TRIGGER` default),
  a throwaway Redis on its own port. Joiner batches are slates of 10 impressions with roughly 10%
  clicked, and the wall clock advances by the 10 s trigger per batch.
- Offline: 1M synthetic ratings, 6,040 users and 3,700 items with a skewed popularity distribution
  (MovieLens-1M scale). The checked-in `sampledata/ratings.csv` has 9 lines, too few to profile.
- Candidates: 20,000 users × 10,000 items × 16 dimensions, top 100.
- 8-core Apple Silicon laptop, `local[*]`.

**Measure on a native JDK.** The only JDK 17 on the profiling machine is x86_64 and runs under
Rosetta, which has no FMA instruction, so `Math.fma` in netlib's Java BLAS falls back to
`BigDecimal`. Item2Vec ran for over 55 minutes on one core without finishing, and the streaming
jobs measured 3–4× slower than native. Every number below is from the arm64 JDK 18. A Spark planner
`INTERNAL_ERROR` seen once under Rosetta, at joiner batch 21, did not reproduce natively in 34 batches.

## Findings

| Area | Measured | Where the time is | Change |
|---|---|---|---|
| Candidate pre-computation | Parquet + Redis: 4.1–6.8 s; cached: 2.5–3.9 s | `main` runs the full top-K scoring once per sink, because `candidates` is never cached | **Cache when both outputs are set (this change)** |
| Item2Vec | 13.8 s total, of which the fit is 12.0 s | Word2Vec's `numPartitions` defaults to 1, so the fit runs in a single task. 8 partitions: fit 5.1 s | **`ITEM2VEC_NUM_PARTITIONS`, default 1 (this change)** |
| ALS | 5.3 s | spread across ALS's own iterations and `StringIndexer`; no redundant pass | none |
| User embedding | 0.8–1.0 s | text write | none |
| Ingestion (`UserEventStreamingJob`) | 2.3–3.7 s per batch, 5 s trigger | 38 Spark jobs per batch; the two Redis sinks are the largest share (~0.8 s) | none (keeps up) |
| Feature joins (`OnlineJoiner` + `LateFeedbackJoin`) | 3.4 s per batch rising to **6.2–8.1 s** once the window is full, 10 s trigger | Each batch rewrites the entire pending snapshot (16 × `count` in `CommitProtocol.writeDirectory`, 4 × `first` in `commitOpenSlates`) | follow-up (below) |

### Candidate scoring: the inner loop was not the cost

Precomputing item and user norms, so the inner loop is a single dot product, was implemented and
measured, and gave **no measurable change** (3.2–3.8 s against 3.4 s). It was reverted. The
redundant work was the second scoring pass, not the arithmetic.

### Item2Vec: faster, but different embeddings

Spark's Word2Vec trains each partition independently and averages the results, so more than one
partition gives a different model, not the same model faster. The default therefore stays at 1,
which keeps current output unchanged. Raising it is a modeling decision that should be checked
against retrieval metrics before it becomes the default.

### Ingestion: per-batch cost is mostly fixed

On the emulated JDK, batch time barely moved with batch size: 9.7 s at 500 events, 10.7 s at
5,000, 14.6 s at 20,000. The cost tracks the number of Spark jobs and commits per batch, not the
number of rows. Natively, ingestion keeps up with headroom.

Replacing the per-item `EVAL` of the ~4 KB ledger script with `EVALSHA` saved about 10% of an
operation that takes ~110 ms per 1,000 items, which is within noise. It was not adopted.

### Feature joins: follow-up design

`LateFeedbackJoin.process` unions the batch with the previous snapshot of every open slate, then
commits a **new full snapshot** of everything still open. With a 4-minute window and a 10 s trigger,
each event is rewritten about 24 times before it publishes, so per-batch work grows with
`FEEDBACK_JOIN_WAIT / TRIGGER_INTERVAL`. At the defaults the joiner uses about 80% of its trigger
budget on an 8-core laptop. A longer wait or a higher event rate would push it past the trigger.

The likely fix is append-only per-batch pending segments, with the close decision made per segment,
so each event is written once while pending. That changes the snapshot recovery contract (a batch
reads N−1 and N today) and needs its own spec with the restart and retry cases worked through. It is
not attempted here.

## Validation

- `EmbeddingCandidateGenerationJobSpec` and `Item2VecTrainingJobSpec` pass, including a new
  multi-partition vocabulary test. Full Scala suite (JDK 17): 445 passed, 0 failed.
- Default behavior is unchanged: the cache applies only when both sinks are set, and
  `ITEM2VEC_NUM_PARTITIONS` defaults to 1.
