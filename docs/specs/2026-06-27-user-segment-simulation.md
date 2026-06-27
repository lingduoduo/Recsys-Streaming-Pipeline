# Spec: User-Segment Engagement Simulation

> Sibling of the engagement time-series sim (PR #92). That one studies engagement
> *over time*; this one studies how engagement differs *across user segments* —
> cohort (new vs existing), age band, sex, education, geo, and platform.

## Objective

Generate a synthetic event stream where each user carries demographics and each
event carries geo/platform, run it through the **real pipeline** (Kafka → Spark),
and produce a per-segment engagement comparison — without any schema/topic/Redis-key
change. Engagement differences are injected as a documented ground truth so the
report can be validated against what was produced.

## Key insight (why no schema change is needed)

`OnlineJoinerStreamingJob.buildTrainingSamples` already carries `user_features` and
`context_features` (MapType) into the date-partitioned Parquet. So:

- **user-level** attributes (cohort, age_band, sex, education) ride in `user_features`
- **context-level** attributes (geo, platform) ride in `context_features`

and the report simply projects them out of the maps and groups by them. No change to
`recsys_events`, `training_samples`, the Parquet schema, or any Redis key.

## Scope

- **In:** a segment-aware producer, the e2e runner, and a PySpark segment report.
- **Out:** changes to the streaming jobs; statistical significance testing and 2-D
  cross-tabs (considered, deferred); time-series breakdowns (covered by the sibling sim).

## Metrics & comparison

Per segment **value**, for each dimension:

- `impressions` — sample size
- `ctr` = avg(clicked)
- `order_rate` = avg(ordered)
- `clicks_per_user` = sum(clicked) / distinct users
- `ctr_lift_pct` = (segment CTR − overall CTR) / overall CTR × 100

## Ground truth (injected, for validation)

Additive effects on click probability (base 0.20), documented in `segment_producer.py`:

- cohort: existing > new
- age_band: 25-34 highest → 55+ lowest
- education: grad > college > hs
- geo: US > CA > GB > DE > IN
- platform: ios > android > web
- sex: F slightly > M

order-given-click is also segment-modulated (existing / ios / grad higher).

## Commands

```bash
# from recsys-pipeline
./run-segment-sim.sh                       # docker → produce → OnlineJoiner → Parquet → report
"$SPARK_HOME/bin/spark-submit" services/python-modeling/segment_report.py \
    --input /tmp/spark-recsys/segment-sim/training-samples
```

## Success criteria (testable)

- [ ] Producer assigns stable per-user demographics + per-event geo/platform.
- [ ] e2e run writes Parquet whose `user_features`/`context_features` carry the segment attrs.
- [ ] Report emits, per dimension, CTR + order_rate + clicks/user + counts + CTR lift.
- [ ] The report recovers every injected ordering (validated on a real run).
- [ ] `$SPARK_HOME/bin/spark-submit` used so PySpark matches the pinned Spark 3.5.1.

## Boundaries

- **Always:** keep topic/schema/Redis-key contracts stable; segment attrs travel only
  via the existing feature maps; run PySpark via the project's pinned Spark.
- **Never:** rename `recsys_events` / `training_samples` / `global:item_popularity`,
  or change the Parquet schema.

## Notes / caveats

- CTR here is per-impression (slate-click ÷ slate-size); the patterns are what matter.
- `clicks_per_user` is most meaningful for user-level dims; for per-event dims
  (geo/platform) it is clicks ÷ all users, so it reads lower by construction.
- Cross-sectional only — significance testing / cross-tabs are an easy future add.
