# Late-Feedback Visibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `OnlineJoinerStreamingJob`'s cross-batch feedback behaviour asserted by tests and actually exercised by the simulations, without changing the behaviour itself.

**Architecture:** Scala tests pin what happens today when feedback lands in a later micro-batch than its impression. A shared Python helper defers feedback emission using the delay already encoded in each event's `timestamp_ms`, so producers stop fabricating delay and start having it. The sims are reordered to start their streaming jobs *before* producing, with a drain that will not declare completion before the feedback tail has elapsed.

**Tech Stack:** Scala 2.12 + Spark 3.5.1 Structured Streaming, ScalaTest, Python 3, kafka-python, pytest, bash.

**Spec:** `.superpowers/docs/specs/2026-08-10-late-feedback-visibility-design.md`

## Global Constraints

- **Do not change how late feedback is handled.** The `.filter(col("impression_ts").isNotNull)` drop at `OnlineJoinerStreamingJob.scala:167` stays exactly as it is. This plan makes it visible, not different.
- Do not touch `ExecutionEngine`, `RawArchiveSink`, or the archive commit protocol.
- Do not modify `backfill_producer.py` or `run-engagement-sim.sh`; they replay historical windows in bulk where "late" has no meaning.
- `FEEDBACK_DELAY_SCALE` defaults to `1.0` (faithful delays). Values above ~0.5 must still cross a 10-second trigger.
- Feedback delay is derived from each event's existing `timestamp_ms`, never from new delay parameters. All three live producers already encode it.
- Never `git commit` without first confirming the branch is not `master`:
  `test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }`

## File Structure

- `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`: adds cross-batch pinning tests.
- `recsys-pipeline/services/python-modeling/feedback_schedule.py`: new. Splits a slate into immediate/deferred events and releases deferred ones when due.
- `recsys-pipeline/integration-tests/python_modeling/test_feedback_schedule.py`: new. Unit tests with an injected clock; no sleeping.
- `recsys-pipeline/services/python-modeling/producer.py`, `movielens_segment_producer.py`, `movie_segment_producer.py`: emit impressions immediately, feedback when due.
- `recsys-pipeline/scripts/run-movielens-segment-sim.sh`, `run-movie-category-sim.sh`: split `run_and_drain`, reorder, add a wait floor.
- `recsys-pipeline/integration-tests/test_service_scripts.py`: asserts the sims start jobs before producing and pass a floor.
- `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`: documents the behaviour and the knob.

---

### Task 1: Pin Cross-Batch Feedback Behaviour

Independently valuable and lands first: it converts an accident into an asserted decision.

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `OnlineJoinerStreamingJob.buildTrainingSamples(events: DataFrame): DataFrame` (existing).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Write the failing tests**

Append inside the existing class, after the `"keep unclicked impressions as negative samples"` test. Follow that test's `.toDF(...)` column list exactly — the `timestamp` column is seconds and is converted internally.

```scala
  it should "drop feedback whose impression fell in an earlier batch" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val columns = Seq("session_id", "request_id", "user_id", "item_id", "event_type",
      "timestamp", "position", "user_features", "item_features", "context_features")
    val empty = Map.empty[String, String]

    // Batch 1: the impression alone.
    val firstBatch = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "impression", 100L, 0,
        Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios"))
    ).toDF(columns: _*)

    // Batch 2: the click alone, arriving after batch 1 already published.
    val secondBatch = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "click", 105L, 0, empty, empty, empty)
    ).toDF(columns: _*)

    val first = OnlineJoinerStreamingJob.buildTrainingSamples(firstBatch)
      .select("item_id", "clicked", "ordered", "label").collect()
    val second = OnlineJoinerStreamingJob.buildTrainingSamples(secondBatch).collect()

    // The impression publishes immediately, labelled "not clicked".
    first.length shouldBe 1
    (first.head.getInt(1), first.head.getInt(2), first.head.getDouble(3)) shouldBe (0, 0, 0.0)
    // The click is discarded, and nothing restates the sample published above.
    second shouldBe Array.empty[org.apache.spark.sql.Row]
  }

  it should "still label a click that arrives in the same batch" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val columns = Seq("session_id", "request_id", "user_id", "item_id", "event_type",
      "timestamp", "position", "user_features", "item_features", "context_features")
    val empty = Map.empty[String, String]

    val batch = Seq(
      ("sess_1", "req_1", "user_1", "item_1", "impression", 100L, 0,
        Map("tier" -> "gold"), Map("genre" -> "drama"), Map("device" -> "ios")),
      ("sess_1", "req_1", "user_1", "item_1", "click", 105L, 0, empty, empty, empty)
    ).toDF(columns: _*)

    val rows = OnlineJoinerStreamingJob.buildTrainingSamples(batch)
      .select("clicked", "label").collect()

    rows.length shouldBe 1
    (rows.head.getInt(0), rows.head.getDouble(1)) shouldBe (1, 1.0)
  }
```

The second test is not redundant: without it the first could pass because `buildTrainingSamples` was
broken in some unrelated way, rather than because of the cross-batch drop specifically.

- [ ] **Step 2: Run the tests**

```bash
cd recsys-pipeline/services/spark-streaming-job
sbt -batch 'testOnly com.demo.process.OnlineJoinerStreamingJobSpec'
```

Expected: **PASS**. These are characterization tests — they assert existing behaviour, so they pass
immediately. That is correct and expected; do not "fix" anything to make them fail.

- [ ] **Step 3: Verify the tests actually discriminate**

A characterization test that would pass against a broken implementation is worthless. Temporarily
delete the `.filter(col("impression_ts").isNotNull)` line at `OnlineJoinerStreamingJob.scala:167`,
re-run, and confirm `"drop feedback whose impression fell in an earlier batch"` **fails**. Then
restore the line and confirm it passes again.

```bash
sbt -batch 'testOnly com.demo.process.OnlineJoinerStreamingJobSpec' 2>&1 | grep -E "Tests:|\*\*\*"
```

- [ ] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala
git commit -m "test: pin cross-batch feedback behaviour in the online joiner"
```

---

### Task 2: The Deferred-Emission Helper

**Files:**
- Create: `recsys-pipeline/services/python-modeling/feedback_schedule.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_feedback_schedule.py`

**Interfaces:**
- Produces, consumed by Task 3:
  - `split_slate(events: list[dict]) -> tuple[list[dict], list[tuple[float, dict]]]`
  - `FeedbackSchedule(scale: float = 1.0, clock: Callable[[], float] = time.monotonic)`
  - `FeedbackSchedule.schedule(delay_seconds: float, event: dict) -> None`
  - `FeedbackSchedule.due(now: float | None = None) -> list[dict]`
  - `FeedbackSchedule.pending() -> int`
  - `FeedbackSchedule.next_due_in(now: float | None = None) -> float | None`
  - `DELAY_SCALE: float` — module-level, read from `FEEDBACK_DELAY_SCALE`, default `1.0`

- [ ] **Step 1: Write the failing tests**

```python
import sys
from pathlib import Path

import pytest

PYTHON_MODELING = Path(__file__).resolve().parents[2] / "services/python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

from feedback_schedule import FeedbackSchedule, split_slate


def event(event_type: str, timestamp_ms: int, item: str = "item_1") -> dict:
    return {"event_type": event_type, "timestamp_ms": timestamp_ms, "item_id": item}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_split_slate_defers_feedback_by_its_own_encoded_offset():
    events = [
        event("impression", 1_000_000),
        event("impression", 1_000_000, "item_2"),
        event("click", 1_005_000),
        event("order", 1_030_000),
    ]

    immediate, deferred = split_slate(events)

    assert [e["event_type"] for e in immediate] == ["impression", "impression"]
    assert [(delay, e["event_type"]) for delay, e in deferred] == [(5.0, "click"), (30.0, "order")]


def test_split_slate_passes_through_a_slate_with_no_impression():
    events = [event("click", 1_005_000)]

    immediate, deferred = split_slate(events)

    assert immediate == events and deferred == []


def test_schedule_releases_nothing_before_its_due_time():
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    schedule.schedule(5.0, event("click", 1_005_000))

    assert schedule.due() == []
    assert schedule.pending() == 1
    assert schedule.next_due_in() == 5.0

    clock.now = 4.999
    assert schedule.due() == []

    clock.now = 5.0
    assert [e["event_type"] for e in schedule.due()] == ["click"]
    assert schedule.pending() == 0
    assert schedule.next_due_in() is None


def test_schedule_releases_in_due_order_not_insertion_order():
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    schedule.schedule(30.0, event("order", 1_030_000))
    schedule.schedule(5.0, event("click", 1_005_000))

    clock.now = 40.0

    assert [e["event_type"] for e in schedule.due()] == ["click", "order"]


def test_scale_compresses_delays():
    clock = FakeClock()
    schedule = FeedbackSchedule(scale=0.1, clock=clock)
    schedule.schedule(30.0, event("order", 1_030_000))

    clock.now = 2.9
    assert schedule.due() == []

    clock.now = 3.0
    assert len(schedule.due()) == 1


def test_non_positive_scale_is_rejected():
    with pytest.raises(ValueError, match="FEEDBACK_DELAY_SCALE"):
        FeedbackSchedule(scale=0.0)
```

- [ ] **Step 2: Run to verify RED**

```bash
cd recsys-pipeline
python -m pytest -q integration-tests/python_modeling/test_feedback_schedule.py
```

Expected: FAIL at import — `feedback_schedule` does not exist.

- [ ] **Step 3: Implement the helper**

```python
"""Emit a slate's feedback when it is due, instead of all at once.

The producers already encode realistic delay in each event's `timestamp_ms` (clicks 1-20s after
impression, orders 21-120s). They then emit the whole slate in one instant, so a consumer never
sees feedback arrive later than its impression. This releases each feedback event at the offset its
own payload already claims.
"""

from __future__ import annotations

import heapq
import itertools
import os
import time
from collections.abc import Callable

IMPRESSION_TYPES = frozenset({"impression", "exposure"})
FEEDBACK_TYPES = frozenset({"click", "order", "purchase"})

DELAY_SCALE = float(os.getenv("FEEDBACK_DELAY_SCALE", "1.0"))


def split_slate(events: list[dict]) -> tuple[list[dict], list[tuple[float, dict]]]:
    """Split into events to send now and (delay_seconds, event) pairs to send later.

    The delay is derived from the event's own `timestamp_ms` relative to the slate's earliest
    impression, so no caller has to restate the delay model.
    """
    impressions = [e for e in events if e.get("event_type") in IMPRESSION_TYPES]
    if not impressions:
        return events, []

    base_ms = min(e["timestamp_ms"] for e in impressions)
    immediate: list[dict] = []
    deferred: list[tuple[float, dict]] = []
    for candidate in events:
        if candidate.get("event_type") in FEEDBACK_TYPES:
            delay = max(0.0, (candidate["timestamp_ms"] - base_ms) / 1000.0)
            deferred.append((delay, candidate))
        else:
            immediate.append(candidate)
    return immediate, deferred


class FeedbackSchedule:
    """A due-time heap of events awaiting release."""

    def __init__(
        self, scale: float = DELAY_SCALE, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if not scale > 0:
            raise ValueError("FEEDBACK_DELAY_SCALE must be a positive number")
        self._scale = scale
        self._clock = clock
        self._heap: list[tuple[float, int, dict]] = []
        # Sequence breaks ties so heapq never compares two dicts, which would raise.
        self._sequence = itertools.count()

    def schedule(self, delay_seconds: float, event: dict) -> None:
        due_at = self._clock() + max(0.0, delay_seconds) * self._scale
        heapq.heappush(self._heap, (due_at, next(self._sequence), event))

    def due(self, now: float | None = None) -> list[dict]:
        moment = self._clock() if now is None else now
        released = []
        while self._heap and self._heap[0][0] <= moment:
            released.append(heapq.heappop(self._heap)[2])
        return released

    def pending(self) -> int:
        return len(self._heap)

    def next_due_in(self, now: float | None = None) -> float | None:
        if not self._heap:
            return None
        moment = self._clock() if now is None else now
        return max(0.0, self._heap[0][0] - moment)
```

- [ ] **Step 4: Run to verify GREEN**

```bash
cd recsys-pipeline
python -m pytest -q integration-tests/python_modeling/test_feedback_schedule.py
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/services/python-modeling/feedback_schedule.py \
        recsys-pipeline/integration-tests/python_modeling/test_feedback_schedule.py
git commit -m "feat: add a due-time schedule for slate feedback"
```

---

### Task 3: Defer Feedback in the Three Live Producers

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/producer.py`
- Modify: `recsys-pipeline/services/python-modeling/movielens_segment_producer.py`
- Modify: `recsys-pipeline/services/python-modeling/movie_segment_producer.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_feedback_schedule.py`

**Interfaces:**
- Consumes from Task 2: `split_slate`, `FeedbackSchedule`.
- Produces: no new public names. Each producer's `main()` keeps its signature.

- [ ] **Step 1: Write the failing ordering test**

Append to `test_feedback_schedule.py`. This tests the emission pattern the producers will use,
without needing Kafka.

```python
def test_producer_emission_pattern_sends_impressions_before_feedback():
    """The pattern each producer main loop follows: immediate now, deferred when due."""
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    sent: list[str] = []

    slate = [
        event("impression", 1_000_000),
        event("click", 1_005_000),
        event("order", 1_030_000),
    ]
    immediate, deferred = split_slate(slate)
    for pending in immediate:
        sent.append(pending["event_type"])
    for delay, pending in deferred:
        schedule.schedule(delay, pending)
    for pending in schedule.due():
        sent.append(pending["event_type"])

    assert sent == ["impression"]

    clock.now = 5.0
    sent.extend(e["event_type"] for e in schedule.due())
    assert sent == ["impression", "click"]

    clock.now = 30.0
    sent.extend(e["event_type"] for e in schedule.due())
    assert sent == ["impression", "click", "order"]
    assert schedule.pending() == 0
```

- [ ] **Step 2: Run it — this one is expected to PASS**

```bash
cd recsys-pipeline
python -m pytest -q integration-tests/python_modeling/test_feedback_schedule.py -k emission_pattern
```

Expected: **PASS**. It exercises only Task 2's helper, so it is a contract test pinning the emission
order the producers must follow — not a RED step. Confirm it passes before editing any producer; if
it fails, Task 2 is incomplete and Task 3 cannot proceed.

- [ ] **Step 3: Apply the pattern to `movielens_segment_producer.py`**

Add the import near the existing imports:

```python
from feedback_schedule import FeedbackSchedule, split_slate
```

Replace the behaviour loop body. The existing inner loop is:

```python
                for event in make_slate(user, demo[user], items, rng, session_id):
                    producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                    sent += 1
```

Change to, with `schedule = FeedbackSchedule()` created just before `slates = 0`:

```python
                immediate, deferred = split_slate(make_slate(user, demo[user], items, rng, session_id))
                for event in immediate:
                    producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                    sent += 1
                for delay, event in deferred:
                    schedule.schedule(delay, event)
                for event in schedule.due():
                    producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                    sent += 1
```

After the `while slates < NUM_SLATES:` loop ends, drain the tail before the `finally` block runs:

```python
        while schedule.pending():
            wait = schedule.next_due_in()
            if wait:
                time.sleep(min(wait, 1.0))
            for event in schedule.due():
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
```

Add `import time` if the module does not already import it.

- [ ] **Step 4: Apply the same pattern to `movie_segment_producer.py`**

Identical shape. The existing inner loop is:

```python
            for event in events:
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
```

where `events = make_slate(user, user_meta[user], items, movies, rng)`. Keep the
`ratings_writer.writerows(ratings_from_events(events))` call operating on the **full** `events` list
before splitting — ratings are a separate CSV export and must not be deferred.

```python
            immediate, deferred = split_slate(events)
            for event in immediate:
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
            for delay, event in deferred:
                schedule.schedule(delay, event)
            for event in schedule.due():
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
```

Create `schedule = FeedbackSchedule()` before the `for s in range(NUM_SLATES):` loop, and add the
same tail drain after it.

- [ ] **Step 5: Apply the same pattern to `producer.py` behavior mode**

`producer.py` runs an unbounded loop with its own rate limiter, so the tail drain is not needed —
but due feedback must be pumped every tick. Replace:

```python
            for event in events:
                key = event.get("request_id") or event["user_id"]
                producer.send(TOPIC, value=event, key=key).add_errback(report_delivery_error)
```

with:

```python
            immediate, deferred = split_slate(events)
            for delay, pending in deferred:
                schedule.schedule(delay, pending)
            for event in immediate + schedule.due():
                key = event.get("request_id") or event["user_id"]
                producer.send(TOPIC, value=event, key=key).add_errback(report_delivery_error)
```

Create `schedule = FeedbackSchedule()` alongside `sent = 0`. `clickstream` mode produces single
click events with no impression; `split_slate` returns those unchanged, so that path is unaffected.

- [ ] **Step 6: Verify all three still run and the suites pass**

```bash
cd recsys-pipeline
python -m pytest -q integration-tests/python_modeling/
for f in producer movielens_segment_producer movie_segment_producer; do
  python -c "import sys; sys.path.insert(0, 'services/python-modeling'); import $f" || exit 1
done
```

Expected: all tests pass; all three modules import cleanly.

- [ ] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/services/python-modeling recsys-pipeline/integration-tests/python_modeling
git commit -m "feat: emit slate feedback when it is due, not all at once"
```

---

### Task 4: Start Sim Jobs Before Producing, and Wait for the Tail

**Files:**
- Modify: `recsys-pipeline/scripts/run-movielens-segment-sim.sh`
- Modify: `recsys-pipeline/scripts/run-movie-category-sim.sh`
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py`

**Interfaces:**
- Consumes: nothing from earlier tasks at runtime.
- Produces: shell functions `start_job`, `stop_job`; `drain` gains a fifth positional parameter.

- [ ] **Step 1: Write the failing script tests**

Append to `test_service_scripts.py`:

```python
SIMS = ["run-movielens-segment-sim.sh", "run-movie-category-sim.sh"]


@pytest.mark.parametrize("sim", SIMS)
def test_sim_starts_jobs_before_producing(sim: str) -> None:
    """A job started after the producer reads the whole backlog in one micro-batch."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    assert "start_job " in script
    assert "stop_job " in script
    first_start = script.index("start_job ")
    first_produce = script.index("python services/python-modeling/")
    assert first_start < first_produce, "jobs must be running before events are produced"


@pytest.mark.parametrize("sim", SIMS)
def test_sim_drain_waits_out_the_feedback_tail(sim: str) -> None:
    """Stability alone declares completion in ~18s, well before a 120s order arrives."""
    script = (SCRIPTS_DIR / sim).read_text(encoding="utf-8")

    assert "FEEDBACK_TAIL_SECONDS" in script
    assert "min_wait" in script
```

- [ ] **Step 2: Run to verify RED**

```bash
cd recsys-pipeline
python -m pytest -q integration-tests/test_service_scripts.py -k "sim_starts or feedback_tail"
```

Expected: FAIL — neither `start_job` nor `FEEDBACK_TAIL_SECONDS` exists.

- [ ] **Step 3: Split `run_and_drain` in `run-movielens-segment-sim.sh`**

Replace the `drain` and `run_and_drain` definitions with:

```bash
FEEDBACK_TAIL_SECONDS="${FEEDBACK_TAIL_SECONDS:-150}"

# macOS ships bash 3.2, which has no associative arrays: `declare -A` fails and every
# key silently collapses to index 0, so a pid map would return the wrong process.
# Each caller keeps its own pid variable instead.
start_job() {  # $1=class $2=ckpt $3=label ; remaining: K=V env ; sets LAST_JOB_PID
  local cls="$1" ckpt="$2" label="$3"; shift 3
  echo "==> starting $cls"
  env "$@" SPARK_MAIN_CLASS="$cls" SPARK_CHECKPOINT_LOCATION="$SIM_ROOT/$ckpt" \
    KAFKA_STARTING_OFFSETS=earliest EVENT_WATERMARK_DELAY="3650 days" \
    MAX_OFFSETS_PER_TRIGGER="${MAX_OFFSETS_PER_TRIGGER:-1000000}" \
    TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-2 seconds}" \
    ./scripts/run-streaming-job.sh >"$SIM_ROOT/${label}.log" 2>&1 &
  LAST_JOB_PID=$!
}

stop_job() {  # $1=pid
  local pid="${1:-}"
  [[ -n "$pid" ]] || return 0
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

# $4=min_wait: a floor before stability may end the drain. Feedback arrives up to
# FEEDBACK_TAIL_SECONDS after its impression, and stability alone would end the drain
# after three unchanged six-second reads — long before the last order lands.
drain() {  # $1=label $2=probe $3=target $4=min_wait
  local label="$1" probe="$2" target="$3" min_wait="${4:-0}" prev=-1 stable=0 waited=0 count
  while (( waited < DRAIN_TIMEOUT )); do
    sleep 6; waited=$((waited + 6))
    count="$(eval "$probe" 2>/dev/null | tr -d ' \r' || true)"; count="${count:-0}"
    [[ "$count" =~ ^[0-9]+$ ]] || count=0
    echo "   [$label] t=${waited}s count=$count${target:+/$target}"
    if (( waited < min_wait )); then prev="$count"; continue; fi
    if (( target > 0 )); then
      (( count >= target )) && break
    elif (( count > 0 && count == prev )); then
      stable=$((stable + 1)); (( stable >= 3 )) && break
    else
      stable=0
    fi
    prev="$count"
  done
}

# Never `kill 0` — that signals the whole process group. Guard each pid.
trap 'for p in "${CTX_PID:-}" "${OJ_PID:-}"; do [[ -n "$p" ]] && kill "$p" 2>/dev/null; done' EXIT
```

- [ ] **Step 4: Reorder `run-movielens-segment-sim.sh`**

Replace the producer-then-`run_and_drain` sequence. Both jobs start first, then the producer runs to
completion, then each job drains with a floor:

```bash
start_job com.demo.process.MovieLensContextCollectorStreamingJob ctx-ckpt redis \
  "MOVIELENS_CONTEXT_INPUT_TOPIC=$CONTEXT_TOPIC"
CTX_PID="$LAST_JOB_PID"
start_job com.demo.process.OnlineJoinerStreamingJob oj-ckpt parquet \
  "ONLINE_JOINER_HDFS_OUTPUT_PATH=$OUT_DIR" "ONLINE_JOINER_INPUT_TOPIC=$RECSYS_TOPIC"
OJ_PID="$LAST_JOB_PID"

echo "==> producing demographics ($CONTEXT_TOPIC) + behavior ($RECSYS_TOPIC)"
NUM_USERS="$NUM_USERS" NUM_SLATES="$NUM_SLATES" \
RECSYS_TOPIC="$RECSYS_TOPIC" MOVIELENS_CONTEXT_TOPIC="$CONTEXT_TOPIC" \
  python services/python-modeling/movielens_segment_producer.py

drain redis "redis_cli --scan --pattern 'user:*:features' | wc -l" "$NUM_USERS" 0
stop_job "$CTX_PID"; CTX_PID=""
drain parquet "find \"$OUT_DIR\" -name '*.parquet' | wc -l" 0 "$FEEDBACK_TAIL_SECONDS"
stop_job "$OJ_PID"; OJ_PID=""
```

The collector gets no floor — demographics are emitted immediately and have no deferred tail.

- [ ] **Step 5: Apply the same split and reorder to `run-movie-category-sim.sh`**

Copy the `FEEDBACK_TAIL_SECONDS`, `start_job`, `stop_job`, `drain`, and `trap` block from
Step 3 verbatim. Then start the collector and joiner before
`python services/python-modeling/movie_segment_producer.py`, and give the joiner's drain the
`"$FEEDBACK_TAIL_SECONDS"` floor. The three later jobs in that sim (`UserEventStreamingJob`,
`ExperienceCollectorStreamingJob`, and the category report) consume topics that are already fully
written by the time they run, so they keep `start_job`, capture `LAST_JOB_PID`, and `drain … 0 0` with a zero floor.

- [ ] **Step 6: Verify**

```bash
cd recsys-pipeline
bash -n scripts/run-movielens-segment-sim.sh && bash -n scripts/run-movie-category-sim.sh
python -m pytest -q integration-tests/test_service_scripts.py
```

Expected: both scripts parse; all script tests pass.

- [ ] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/scripts recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "test: run sim jobs alongside the producer and wait for the feedback tail"
```

---

### Task 5: Document and Verify End to End

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`
- Modify: `recsys-pipeline/README.md`

- [ ] **Step 1: Document the behaviour**

In `Data_Pipeline.md`, in the `OnlineJoinerStreamingJob` section after the numbered per-micro-batch
list, add:

```markdown
#### Feedback that arrives in a later micro-batch is dropped

Step 2 drops groups with no impression in the current batch. A click or order whose impression fell
in an earlier batch therefore has nowhere to join, and is discarded — the earlier batch already
published that impression with `clicked = 0`, `label = 0.0`, and nothing restates it. There is no
compensating path: no stream-stream join and no reprocessing. `EVENT_WATERMARK_DELAY` governs
deduplication only, not join buffering.

Labels are therefore correct only for feedback landing in the same micro-batch as its impression.
With the default 10-second trigger and the producers' own model of user behaviour — clicks 1–20s
after impression, orders 21–120s — production traffic would cross that boundary regularly.
`OnlineJoinerStreamingJobSpec` pins this behaviour so it stays a deliberate choice.

Fixing it means buffering impressions across batches until a feedback deadline. Spark's stateful
operators are unavailable where this join runs — the Avro `ExecutionEngine.run` overload applies its
stages inside `foreachBatch`, on a static per-batch DataFrame — so a fix would need a durable
pending-impression store in the style of `RawArchiveSink.deduplicateValid`.
```

- [ ] **Step 2: Document the knob**

Add to the README env-var table, next to the other simulation entries:

```markdown
| `FEEDBACK_DELAY_SCALE` | `1.0` | live producers | Multiplies the click/order delays each slate already encodes, so a sim run can compress a ~2-minute feedback tail. Any value above ~0.5 still crosses a 10-second trigger, which is what makes cross-batch feedback observable |
| `FEEDBACK_TAIL_SECONDS` | `150` | segment and category sims | Floor before a drain may end, so the sim cannot declare completion before the last deferred order has arrived |
```

- [ ] **Step 3: Run the full relevant suites**

```bash
cd recsys-pipeline
python -m pytest -q integration-tests/python_modeling/ integration-tests/test_service_scripts.py
(cd services/spark-streaming-job && sbt -batch 'testOnly com.demo.process.* com.demo.report.*')
```

Expected: all pass.

- [ ] **Step 4: Confirm scope**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git diff --stat master HEAD
```

Expected: only the files in **File Structure**. Specifically **not** `OnlineJoinerStreamingJob.scala`
— if it appears, the drop was changed and the plan's central constraint was violated.

- [ ] **Step 5: Commit and open a PR**

```bash
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/docs recsys-pipeline/README.md
git commit -m "docs: document late-feedback handling and the delay knobs"
git push -u origin HEAD
gh pr create --base master --title "test: make late-feedback handling visible and reproducible"
```

Do not merge; wait for the user.

---

## Verification of the Success Criteria

The spec's criterion "running a sim produces at least one slate whose order arrived in a later
micro-batch" cannot be asserted by a unit test — it needs a real sim run. After Task 4, run:

```bash
cd recsys-pipeline
FEEDBACK_DELAY_SCALE=1.0 NUM_SLATES=200 ./scripts/run-movielens-segment-sim.sh
```

Then confirm the joiner processed more than one batch containing behaviour events:

```bash
grep -c "batchId" /tmp/spark-recsys/movielens-segment-sim/parquet.log
```

More than one batch with rows means feedback genuinely crossed a boundary. Record the observed count
in the PR description rather than asserting it in CI — the sim needs Docker and takes minutes.
