# Plan: MovieLens-Aligned User-Segment Simulation

Implementation plan for [2026-06-27-movielens-segment-simulation.md](../specs/2026-06-27-movielens-segment-simulation.md).
Shipped in PR #94 (branch `feat/movielens-segment-sim`).

**Status:** ✅ shipped. Verified e2e (800 users, 20k slates, 100k impressions) — recovered
every injected ordering via the real Redis-demographics join. Tests: Python 66 passed /
1 skipped; Scala 38.

## Decisions

- Faithful build (per the chosen scope): canonical demographics via `movielens_context` → Redis;
  engagement via `recsys_events` → Parquet; report joins them.
- New branch off master so PR #93 (the self-contained sim) stays intact — this PR **supersedes**
  that approach; close one once a direction is picked.

## Tasks

### 1 — Scala: collector offsets env-configurable → verify: sbt test + assembly
- `MovieLensContextCollectorStreamingJob`: `startingOffsets` now
  `sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "latest")` (default unchanged). Lets the e2e read
  all demographics deterministically with `earliest`.

### 2 — segment_features.py → verify: derive unit tests
- `derive_age_band(age)` and `derive_geo(zip_code)` — pure, importable under spark-submit Python.

### 3 — movielens_segment_producer.py → verify: model + mapping unit tests
- `assign_demographics` (canonical age:int/gender/occupation/zip_code), `click_prob`/`order_prob`
  (ground-truth effects keyed by derived buckets), `demographics_event` (UserUpdated shape).
- Emits demographics → `movielens_context`, behavior → `recsys_events` (no embedded demo).

### 4 — movielens_segment_report.py (PySpark) → verify: fetch parsing + spark-submit platform test
- `fetch_demographics` (Redis → rows with derived age_band/geo; `[]` if Redis down).
- `per_user_engagement`, `platform_metrics`, `demographic_metrics`, `_finalize`.
- Joins per-user engagement with demographics; platform straight from the Parquet map.

### 5 — run-movielens-segment-sim.sh → verify: e2e run
- docker → producer → context-collector (→ Redis, drain on `user:*:features` count) →
  OnlineJoiner (→ Parquet, drain on file count) → report.

## Verification (real run)

| Dimension | source | CTR high → low | matches injected? |
|---|---|---|---|
| platform | Parquet | ios > android > web | ✓ |
| age_band | Redis | 25-34 > 35-44 > 45-54 > 18-24 > 55+ | ✓ |
| gender | Redis | F > M | ✓ |
| occupation | Redis | student/engineer top, retired bottom | ✓ |
| geo | Redis | West/Northeast top, Southeast bottom | ✓ (top recovered; near-zero-effect mid-ranks shuffle on noise) |

## Follow-up: avg-rating-per-segment (explicit feedback)

Added after the initial build:
- Producer emits segment-modulated `RatingEvent`s to `movielens_context` (`rating_mean`/`rating_value`).
- Report reads Redis `avgRating`/`ratingCount` and adds a count-weighted `avg_rating` per demographic dim.
- **Bug fixed:** `MovieLensContextCollectorStreamingJob.writeUserUpdates` interleaved `jedis.hget`
  with an open pipeline (only when ratings present) → wrote ~1 user per partition. Split read-phase
  from write-phase. Also hardened the sim runner: unique per-run topics, Redis flush, and a
  collector drain that waits for all `NUM_USERS` keys (the "stable count" heuristic killed it mid-batch).
- Verified: collector writes all 800 keys; `avg_rating` tracks CTR (age 25-34 3.84 → 55+ 3.21).

## Future scope

- Significance tests (z / chi-square per segment vs rest) and 2-D cross-tabs (e.g. platform × age_band).
