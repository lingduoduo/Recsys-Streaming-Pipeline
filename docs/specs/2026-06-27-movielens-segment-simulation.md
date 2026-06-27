# Spec: MovieLens-Aligned User-Segment Simulation

> Successor to the self-contained segment sim (PR #93). That one embedded invented
> attributes in `user_features`; this one aligns with the **canonical `UserEvent`
> demographics** (age:int, gender, occupation, zip_code) and routes them through the
> **real context path**, so segments reflect the pipeline's actual user model.

## Objective

Compare engagement across user segments where demographics flow through the genuine
MovieLens path (`movielens_context` → `MovieLensContextCollectorStreamingJob` → Redis)
and engagement flows through the behavior path (`recsys_events` → `OnlineJoinerStreamingJob`
→ Parquet). The report **joins** the two by `user_id`. No topic/schema/Redis-key change.

## Why this shape

`UserEvent` demographics are not carried on behavior events — they ride the context path and
land in Redis `user:{id}:features` (age, gender, occupation, zipCode). Engagement
(clicked/ordered) lives in the OnlineJoiner Parquet. There is no pre-existing job that joins
them, so the **report** does the join. This mirrors how the serving side enriches: behavior
events + Redis user features.

## Data flow

```
movielens_segment_producer
  ├─ per user: {user_id, age:int, gender, occupation, zip_code, timestamp}
  │     → movielens_context → MovieLensContextCollectorStreamingJob → Redis user:{id}:features
  └─ behavior slates (impression/click/order, NO demographics)
        → recsys_events → OnlineJoinerStreamingJob → training_samples Parquet (clicked, ordered, platform)

movielens_segment_report.py  (PySpark)
  platform      ← Parquet context_features[platform]            (event-level)
  age_band/gender/occupation/geo ← Redis user:{id}:features ⨝ Parquet by user_id   (user-level)
```

## Scope

- **In:** the segment producer, the e2e runner, the join report, the shared
  `segment_features` derivations, and one Scala line making the context collector's
  `startingOffsets` env-configurable (it was hardcoded `latest`; siblings already env-configure it).
- **Out:** changes to job logic/schemas; significance testing / cross-tabs (future).

## Metrics & comparison

Per segment value: `impressions`, `ctr`, `order_rate`, `clicks_per_user`, `ctr_lift_pct`.

## Ground truth (injected, validated by the run)

Additive click-prob effects keyed by the derived buckets: age 25-34 top / 55+ bottom;
F > M; student/engineer/scientist top, retired bottom; West/Northeast top; ios > android > web.

## Success criteria (testable)

- [ ] Demographics emitted as canonical `UserEvent` fields to `movielens_context`.
- [ ] Context collector populates Redis `user:{id}:features` (run with `KAFKA_STARTING_OFFSETS=earliest`).
- [ ] Report joins Parquet engagement with Redis demographics; emits per-segment CTR/order/clicks-per-user + lift.
- [ ] Report degrades to platform-only if Redis is unreachable.
- [ ] Run recovers every injected ordering.

## Boundaries

- **Always:** demographics travel only via the context path; engagement via the behavior path;
  PySpark run through the pinned Spark.
- **Never:** rename topics/keys or embed demographics back into behavior events.
