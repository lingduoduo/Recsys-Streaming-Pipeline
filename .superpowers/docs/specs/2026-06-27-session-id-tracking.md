# Spec: Track `session_id` Through the Data Stream

## Objective

Thread `session_id` from the producers through `OnlineJoinerStreamingJob` into
`training_samples` (and, in Phase 2, into `training_experiences`) so session-level
engagement can be analyzed — **additively**, without breaking any existing contract.

## Background (current state)

Every behavior-slate event already carries a `session_id` (`sess_<hex>`), set per slate
by the producers (`producer.py`, `backfill_producer.py`, `movielens_segment_producer.py`,
`movie_segment_producer.py`, `movielens_pipeline.py`). But **no consumer tracks it**:

- It is absent from every parse schema — `EventSchemas.userEvent` / `EventSchemas.joiner`,
  `ExperienceCollectorStreamingJob.TrainingSampleSchema`,
  `MovieLensContextCollectorStreamingJob.ContextEventSchema`.
- Spark's `from_json` silently drops JSON fields not in the schema, so `session_id` is
  discarded the moment `EventParsing.fromJson(...)` runs.

Net: `session_id` is logged into Kafka (`recsys_events`) but never reaches `training_samples`,
Parquet, Redis, or `training_experiences`. Grouping is by `request_id`/`user_id` only — no
sessionization, no per-session metrics. (The Java `RecSysEvent` path does not emit it at all.)

## Why

Session is the natural unit for several engagement questions the pipeline can't answer today:
sessions per user, clicks/conversions per session, slate count per session, intra-session
position effects. Carrying `session_id` into the training samples unlocks these for the
existing reports without a new ingestion path.

## Scope

- **In:**
  - `EventSchemas.joiner` — add a nullable `session_id`.
  - `OnlineJoinerStreamingJob.buildTrainingSamples` — pass `session_id` through to the output
    (one session per `request_id`/slate).
  - **Phase 2 (optional):** `ExperienceCollectorStreamingJob` — parse `session_id` from
    `training_samples` and carry it into the slate output; a session-level report.
- **Out:** the Java `RecSysEvent` path (would need its own `session_id` field — separate
  change); the Flink user-history path; `UserEventStreamingJob` (clickstream popularity path
  doesn't need session).

## Design

1. **Schema (additive).** Add `StructField("session_id", StringType, nullable = true)` to
   `EventSchemas.joiner`. `session_id` is constant across a slate's events; nullable so older
   payloads (or the clickstream `make_click_event`, which omits it) parse to null.

2. **Joiner passthrough.** In `buildTrainingSamples`, `groupBy(request_id, user_id, item_id)`
   already collapses a slate's events; add
   `first(col("session_id"), ignoreNulls = true).as("session_id")` to the `agg`, and
   `coalesce(col("session_id"), lit("")).as("session_id")` to the `select`. This adds a
   `session_id` column to **both** the Kafka `training_samples` value (`to_json(struct(*))`)
   and the date-partitioned Parquet. Additive only — column order/values of existing fields
   unchanged.

3. **Backward compatibility.** Downstream `ExperienceCollectorStreamingJob` reads
   `training_samples` with an explicit `TrainingSampleSchema`; an extra JSON field is ignored
   until Phase 2 opts in. The Parquet gains a column; existing readers that select by name are
   unaffected.

4. **Phase 2 (optional).** Add `session_id` to `TrainingSampleSchema`; in `buildSlates`
   (`groupBy(request_id, user_id)`) add `first(session_id, ignoreNulls=true)` so slates carry
   the session; add a session-level engagement report (sessions/user, clicks/session, slates/session).

## Boundaries

- **Always:** additive only — do not rename or drop existing `training_samples` /
  `training_experiences` columns; keep `session_id` nullable; `sbt test` green before each commit.
- **Never:** rename topics/keys; make `session_id` a required field (breaks clickstream events).

## Success criteria (testable)

- [ ] `EventSchemas.joiner` includes a nullable `session_id`; `EventParsingSpec` asserts it parses.
- [ ] `OnlineJoinerStreamingJob.buildTrainingSamples` output includes `session_id`; its spec asserts
      the value is carried from the slate's events.
- [ ] `training_samples` Kafka value and Parquet both contain `session_id` (verified by the spec /
      a small Parquet read).
- [ ] Existing job specs stay green (additive change, no regressions).
- [ ] *(Phase 2)* `training_experiences` slates carry `session_id`; a session-level report runs.
