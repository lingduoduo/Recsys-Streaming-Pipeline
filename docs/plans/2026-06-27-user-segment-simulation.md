# Plan: User-Segment Engagement Simulation

Implementation plan for [2026-06-27-user-segment-simulation.md](../specs/2026-06-27-user-segment-simulation.md).
Shipped in PR #93 (branch `feat/user-segment-sim`).

**Status:** ✅ shipped. Verified e2e: 20k slates → 100k impressions; every injected
segment ordering recovered. Tests: Python 63 passed / 1 skipped.

## Decisions taken

- Metrics: CTR + order rate + clicks/user.
- Comparison: rate + lift + counts (no significance test / cross-tab this pass).
- Pipeline: full Kafka + Spark e2e (consistent with the time-series sim).
- Report: PySpark (matches the consolidated report stack), run via `$SPARK_HOME/bin/spark-submit`.

## Tasks

### 1 — segment_producer.py → verify: model unit tests
- Per-user demographics via seeded RNG (`assign_demographics`): cohort/age_band/sex/education.
- `click_prob(demo, geo, platform)` and `order_prob(demo, platform)` apply the documented
  additive ground-truth effects, clipped to [0.02, 0.95].
- Emit behavior slates: user attrs in `user_features`, geo/platform in `context_features`.
- File: `services/python-modeling/segment_producer.py`.

### 2 — run-segment-sim.sh → verify: produces Parquet + prints report
- docker up (Kafka+Redis) → wait healthy → build jar if missing → run producer →
  `OnlineJoinerStreamingJob` (wide `EVENT_WATERMARK_DELAY`, earliest) drain into Parquet via a
  `wait_stable` poll → run `segment_report.py`.
- File: `run-segment-sim.sh`.

### 3 — segment_report.py (PySpark) → verify: integration test on tiny Parquet
- `with_segment_columns`: project the 6 segment attrs out of the feature maps.
- `segment_metrics(seg, dim, overall_ctr)`: impressions, ctr, order_rate, clicks/user, ctr_lift_pct.
- One `show()` + CSV per dimension under `report-segments/by_<dim>`.
- File: `services/python-modeling/segment_report.py`.

### 4 — tests → verify: pytest green
- Model units: existing>new, ios>web, 25-34>55+, bounded, deterministic demographics, order ordering.
- PySpark integration: write a tiny Parquet (feature maps), run the report via
  `$SPARK_HOME/bin/spark-submit`, assert by_cohort CTR. Skipped if Spark absent.
- File: `integration-tests/python_modeling/test_segment_sim.py`.

## Verification (real run)

| Dimension | CTR high → low | matches injected? |
|---|---|---|
| cohort | existing 0.061 > new 0.053 | ✓ |
| age_band | 25-34 > 35-44 > 45-54 > 18-24 > 55+ | ✓ |
| education | grad > college > hs | ✓ |
| geo | US > CA > GB > DE > IN | ✓ |
| platform | ios > android > web | ✓ |
| sex | F > M | ✓ |

## Future scope

- Significance tests (z-test / chi-square per segment vs rest) and 2-D cross-tabs
  (e.g. platform × cohort) to surface interactions.
