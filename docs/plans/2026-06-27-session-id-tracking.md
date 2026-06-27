# Plan: Track `session_id` Through the Data Stream

> Implementation plan for [2026-06-27-session-id-tracking.md](../specs/2026-06-27-session-id-tracking.md).
> TDD, one work item per commit. Build/test dir: `recsys-pipeline/services/spark-streaming-job`
> (`sbt test`). Additive only — existing job specs are the regression gate.

**Status:** ✅ Tasks 1–4 implemented (PR #97) — `session_id` flows into `training_samples` and
`training_experiences`, and `session_report.py` aggregates session-level engagement. `sbt test`
green (39); Python session-report integration test green. Producers still mint one session per
slate (so live `slates/session` ≈ 1); multi-slate session grouping in the producers is the only
remaining follow-up.

## Global constraints

- Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18. No new dependencies.
- `session_id` is **nullable**; never required (clickstream `make_click_event` omits it).
- Do not rename/drop existing `training_samples` columns; the change is purely additive.

---

### Task 1 — Add `session_id` to the joiner schema

**Files:** `src/main/scala/com/demo/event/EventSchemas.scala`,
`src/test/scala/com/demo/event/EventParsingSpec.scala`.

- [ ] **Step 1 — failing test.** In `EventParsingSpec`, add a case: a joiner JSON value with
  `"session_id":"sess_abc"` parses to a row exposing `session_id == "sess_abc"`, and a payload
  without it yields `null`.
- [ ] **Step 2 — run, expect fail** (`session_id` not a column):
  `sbt "testOnly com.demo.event.EventParsingSpec"`.
- [ ] **Step 3 — implement.** Add to `EventSchemas.joiner` (keep nullable, place after `event_id`):
  ```scala
  StructField("session_id", StringType, nullable = true)
  ```
- [ ] **Step 4 — run, expect pass.** `sbt "testOnly com.demo.event.EventParsingSpec"`.
- [ ] **Step 5 — commit:** `feat: add nullable session_id to EventSchemas.joiner`.

---

### Task 2 — Carry `session_id` into `training_samples`

**Files:** `src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`,
`src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`.

**Interface:** `buildTrainingSamples` output gains a `session_id` column (one session per slate).

- [ ] **Step 1 — failing test.** In `OnlineJoinerStreamingJobSpec`, extend the input rows with a
  `session_id` column (e.g. all `"sess_1"` for `req_1`) and assert
  `buildTrainingSamples(events).first().getAs[String]("session_id") == "sess_1"`. Note: the spec's
  `.toDF(...)` column list must include `session_id` (it flows in via the parsed schema in prod).
- [ ] **Step 2 — run, expect fail** (no such column).
- [ ] **Step 3 — implement.** In `buildTrainingSamples`:
  - add to the `agg(...)`: `first(col("session_id"), ignoreNulls = true).as("session_id")`
  - add to the `select(...)`: `coalesce(col("session_id"), lit("")).as("session_id")`

  `session_id` is constant across a slate's events, so `first(ignoreNulls)` is well-defined.
  It flows automatically into the Kafka value (`to_json(struct(samples.columns.map(col): _*))`)
  and the Parquet write.
- [ ] **Step 4 — run, expect pass.** `sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec"`.
- [ ] **Step 5 — Parquet check (optional).** Extend the existing file-count test (or add one) to read
  the written Parquet back and assert it has a `session_id` column.
- [ ] **Step 6 — full suite (regression gate):** `sbt test` → all green (ExperienceCollector
  unaffected: it parses with its own schema and ignores the extra JSON field).
- [ ] **Step 7 — commit:** `feat: carry session_id through OnlineJoiner into training_samples`.

---

### Task 3 — (Phase 2, optional) `session_id` into `training_experiences`

**Files:** `src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala`, its spec.

- [ ] Add `StructField("session_id", StringType, nullable = true)` to `TrainingSampleSchema`.
- [ ] In `buildSlates` (`groupBy("request_id", "user_id")`) add
  `first(col("session_id"), ignoreNulls = true).as("session_id")` and select it.
- [ ] Spec: a sample with `session_id` produces a slate carrying it. `sbt test` green.
- [ ] **Commit:** `feat: carry session_id into training_experiences slates`.

---

### Task 4 — (Phase 2, optional) Session-level engagement report

**Files:** `services/python-modeling/session_report.py` (new), a test.

- [ ] PySpark report reading the `training_samples` Parquet: per-session aggregates
  (`sessions/user`, `clicks/session`, `slates/session`, session CTR) grouped by `session_id`
  and rolled up per user. Run via `"$SPARK_HOME/bin/spark-submit"`.
- [ ] Unit-test the pure aggregation; e2e-verify via `run-data-pipeline.sh` or a sim.
- [ ] **Commit:** `feat: session-level engagement report`.

---

## Final verification

- [ ] `cd recsys-pipeline/services/spark-streaming-job && sbt test` → all green.
- [ ] `git grep -n "session_id" src/main/scala` → present in `EventSchemas.joiner` +
  `OnlineJoinerStreamingJob` (and ExperienceCollector if Phase 2).
- [ ] Confirm no existing `training_samples` column renamed/removed (additive only).
- [ ] (If e2e) run a sim and read a Parquet sample: `session_id` populated (`sess_*`), non-null.

## Notes / risks

- **Additive contract:** safe because downstream `from_json` ignores unknown fields and Parquet
  readers select by name. Still bump a fresh checkpoint only if a stateful query's schema changes
  (it doesn't here — `buildTrainingSamples` runs inside `foreachBatch`, not a stateful operator).
- **Producers** already emit `session_id` on behavior slates; no producer change needed for
  Phase 1. (Clickstream `make_click_event` omits it by design → parses to null.)
