# Spark failure observability — design

Make executor memory pressure, shuffle spill, and task failures visible in every Spark job, and
stop funnelling cluster shuffles through a partition count chosen for a laptop.

## Why this document exists

An investigation was asked for: find executor OOM, severe shuffle spill, and thousands of stage
retries in the user-session pipelines. **None of it could be found, and not because the search was
shallow.** The evidence does not exist and structurally cannot:

| Check | Finding |
|---|---|
| `spark.eventLog` configured anywhere | No. Spark writes no event logs, so no history data exists |
| Any log containing `OutOfMemoryError`, `Container killed`, `ExecutorLostFailure`, `FetchFailed`, `spill` | Zero matches across every `*.log` in the repo |
| `SPARK_MASTER` default | `local[*]` — one JVM, driver and executor are the same process |
| Spark service in `docker-compose.yml` | None. Only zookeeper, kafka, redis |

In `local[*]` there are no separate executors to OOM and no cluster across which stages retry. The
reported symptoms have not occurred here and could not have been recorded if they had.

That is the actual problem, and it is worth fixing on its own terms: **this project has no way to
observe the failures it was asked to investigate.** Not "the dashboards are poor" — there is no
signal at all. Every future question of this shape gets the same non-answer until that changes.

## What this design does and does not do

**In scope.** Make spill, shuffle volume, and task failure visible in every job; make event logs
available when wanted; stop the shuffle-partition default from being wrong on a cluster.

**Deliberately out of scope,** because acting on them now would be guesswork:

- The unbounded driver `collect()` calls in `SegmentReportJob:71`, `MovieCategoryReportJob:51`, and
  `GrpoSlates:62`. These are real unbounded-memory patterns, but nobody has measured the data
  volumes they run at, and at simulation scale they may be entirely fine.
- The `localCheckpoint(eager = true)` in `LateFeedbackJoin`, whose unreplicated blocks and cut
  lineage make an executor loss unrecoverable. That behavior is deliberate and documented, and
  `LateFeedbackJoin` is load-bearing for the whole training-sample path.

Both become answerable once the signal from this work exists. Fixing them first would be reordering
the work so that the measurement comes last.

**Correctly handled already, and not a finding.** Streaming de-duplication state is bounded:
`EventParsing.dedupeWithinWatermark` branches on `isStreaming` and uses `withWatermark` plus
`dropDuplicatesWithinWatermark`. Unbounded dedup state is the classic cause of streaming executor
OOM, and it is guarded here.

## Architecture

Everything attaches at `SparkSessions.create`, the seam all ~20 jobs already go through, so no job
needs per-job wiring:

```
SparkSessions.create(appName, defaultShufflePartitions)
  |
  +-- spark.eventLog.*                        opt-in, off by default
  +-- shufflePartitionsFor(master, default)   pure, derived from the master
  +-- SpillMetricsListener.register(spark)    new SparkListener
        (BatchMetricsListener stays as it is, alongside this)
```

### Why a new listener rather than extending the existing one

`BatchMetricsListener` is a `StreamingQueryListener`. Its `QueryProgressEvent` carries input rows,
throughput, and batch duration — and no spill or shuffle-byte fields whatsoever. Those live on
`SparkListener` task and stage end events, in `TaskMetrics`.

It also only fires for streaming queries. `SessionReportJob`, `SegmentReportJob`, and every other
batch report job would remain invisible — and `SessionReportJob.perSession` runs a
`countDistinct` inside a two-key `groupBy`, among the most spill-prone shapes in the codebase.

So this is a second, complementary listener, not a replacement. The two answer different questions:
`BatchMetricsListener` says how fast a stream is moving, `SpillMetricsListener` says what it cost.

## Component 1 — SpillMetricsListener

A `SparkListener` implementing `onStageCompleted`, reading `stageCompleted.stageInfo.taskMetrics`:

| Metric | Source |
|---|---|
| `spillMem` | `memoryBytesSpilled` |
| `spillDisk` | `diskBytesSpilled` |
| `shuffleWrite` | `shuffleWriteMetrics.bytesWritten` |
| `shuffleRead` | `shuffleReadMetrics.totalBytesRead` |
| `failedTasks` | accumulated from `onTaskEnd`, see below |
| `attempt` | `stageInfo.attemptNumber()` |

One line, in the idiom `DropMetrics` established:

```
[spill-metrics] job=SessionReportJob stage=4 attempt=0 tasks=8 spillMem=1.2G spillDisk=840.0M shuffleWrite=2.1G shuffleRead=2.1G failedTasks=0
```

### Counting failures and retries

`StageInfo` does not expose a failed-task count — `failureReason` is set only when the whole stage
failed, which is not the same thing and is absent for a stage that succeeded after retrying tasks.
So failures are accumulated in `onTaskEnd`: any `taskEnd.reason` other than `Success` increments a
counter keyed by `(stageId, stageAttemptId)`, which `onStageCompleted` reads, emits, and then
**removes**. Removing it is not tidiness — a listener that accumulates per-stage state for the life
of a long-running streaming query and never releases it is itself a memory leak, which would be an
absurd way for an observability feature to fail.

`attempt` is `stageInfo.attemptNumber()`, and it is the direct answer to the original question about
stage retries: a stage attempt above 0 *is* a retry. Any non-zero attempt emits at INFO regardless
of spill, because a retrying stage is worth seeing even when it spills nothing.

### Emission policy, and why it departs from DropMetrics

`DropMetrics` emits every batch including zeros, on the stated principle that a silent counter is
indistinguishable from a broken one. **This listener deliberately does not follow that rule at
INFO.** It emits at INFO only when spill, failed tasks, or the stage attempt number are non-zero, and at
DEBUG otherwise.

The reason is cardinality, not disagreement. `DropMetrics` fires once per micro-batch per gate. This
fires once per *stage*, and a streaming query produces stages continuously and forever. Emitting
every zero at INFO would bury the non-zero lines it exists to surface — it would defeat its own
purpose. The DEBUG line preserves the property that the counter is provably alive.

### Byte formatting

Raw byte counts are unreadable at the scale that matters (`1288490188` versus `1.2G`). A pure
`humanBytes(n: Long): String` renders B/K/M/G. It is pure, so it is table-tested directly.

## Component 2 — Event logging

| Setting | Env var | Default |
|---|---|---|
| `spark.eventLog.enabled` | `SPARK_EVENT_LOG_ENABLED` | `false` |
| `spark.eventLog.dir` | `SPARK_EVENT_LOG_DIR` | `/tmp/spark-events` |

Off by default, so no existing run changes behavior.

**The gotcha that makes this more than two config lines.** Spark throws at session creation if
`spark.eventLog.dir` does not exist — it does not create the directory itself. Enabling the flag on
a machine without that path would therefore break *every job at startup*, turning an observability
feature into an outage. `create` must ensure the directory exists before setting the config, and
must do so without failing the session if creation is impossible: a logging feature must never be
the reason a job cannot start. If the directory cannot be created, log a warning and leave event
logging off.

## Component 3 — Master-derived shuffle partitions

```scala
def shufflePartitionsFor(master: String, localDefault: Int): Int
```

Pure, and pure on purpose: it is exercised across every master string without constructing a
`SparkSession`.

| Master | Partitions |
|---|---|
| `local`, `local[N]`, `local[*]` | `localDefault` (8, or 4 for `UserEventStreamingJob`) |
| `yarn`, `k8s://…`, `spark://…`, `mesos://…`, anything else | `ClusterShufflePartitions` = 200 |

`SPARK_SQL_SHUFFLE_PARTITIONS` overrides both, exactly as it does today.

**Why not simply raise the default to 200.** `SPARK_MASTER` defaults to `local[*]`, and 8 partitions
is appropriate there: on a laptop with 8–10 cores, 200 partitions means hundreds of tiny tasks and
more scheduling overhead than work. It would slow every local run and every test in this repo —
which are the only things that execute today. The 8 is only wrong on a cluster, so the fix belongs
where the difference is known: the master string.

## Testing

Pure functions carry the load, following `DropMetricsSpec`:

- `shufflePartitionsFor` — table test over `local`, `local[4]`, `local[*]`, `yarn`, `k8s://host`,
  `spark://host:7077`, and an unrecognized string, asserting cluster masters do not silently get the
  laptop default.
- `humanBytes` — boundaries at 0, sub-K, exact K/M/G, and a value large enough to catch an `Int`
  overflow in the arithmetic.
- The emission predicate — that a zero-spill, zero-failure, attempt-0 stage does not emit at INFO,
  and that each of the three non-zero cases independently does, including a retrying stage that
  spilled nothing.
- Failure accounting — that the per-stage counter is removed once its stage completes, so the
  listener does not accumulate state across a long-running query.
- `format` — asserted directly rather than through captured logs, as `DropMetrics.format` is.

Session-level, one test: enabling event logging creates a missing directory and produces a file, and
a session whose event-log directory cannot be created still starts.

**One test deliberately not written.** Forcing deterministic shuffle spill in a unit test requires
starving executor memory and is reliably flaky. The emission path is covered by testing the
predicate and the formatter directly. A test that fails randomly is worse than the coverage it buys,
and this is stated here so its absence reads as a decision rather than an oversight.

Spark-session tests require **JDK 17**; the default JDK 25 aborts them with a misleading
`getSubject` error.

## Success criteria

Running any existing job unchanged produces no new INFO output and no behavior change — the defaults
are inert. Running one with `SPARK_EVENT_LOG_ENABLED=true` produces a readable event log. A job that
spills produces a `[spill-metrics]` line naming the stage and the volume, which is the signal that
was missing when the original investigation was asked for.

## Risks

| Risk | Mitigation |
|---|---|
| Event-log directory missing breaks every job at startup | `create` ensures it exists; failure to create leaves logging off with a warning rather than failing the session |
| Listener adds overhead to every job | `onStageCompleted` fires once per stage, not per task or per row; the work is field reads and a conditional log |
| Cluster default of 200 is wrong for some cluster | `SPARK_SQL_SHUFFLE_PARTITIONS` overrides it, unchanged from today |
| Event logs grow without bound on a long-running stream | Off by default; documented as a diagnostic to enable deliberately, not to leave on |
