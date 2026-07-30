# Measurement Dashboard — Design

**Date:** 2026-07-30
**Status:** approved
**Depends on:** [2026-07-30-recommendation-measurements-design.md](2026-07-30-recommendation-measurements-design.md) (merged as PR #156)

## Problem

The recommendation measurements release shipped seven sections — relevance,
satisfaction, freshness, diversity, fairness, safety, latency — that render an
availability envelope. Every one of them is N/A in practice:

- No producer emits `published_at`, `new_release`, `filter_reason`,
  `unsafe_label`, `dwell_millis`, or `completion_rate`, so four sections have no
  signals to measure even after a full pipeline run.
- Slate experiences exist only on the `training_experiences` Kafka topic, never
  on disk, so relevance and diversity have no reachable input.
- No sim starts the retrieval service, so there is no `/metrics` capture and no
  latency.

The result is a dashboard that *can* report these metrics but never *does*. The
tables are also unreadable at a glance: seven sections of dense columns with no
summary and no visual encoding.

## Goal

`./run-movie-category-sim.sh` produces a committed `frontend/data/dashboard.json`
in which all seven sections are `available` with non-zero support, and the React
dashboard presents them as a scorecard plus readable detail sections.

Measurement-only still holds: nothing in this work changes candidate generation,
filtering decisions, ranking scores, or selection behavior.

## Approach

Extend the canonical movie-category sim in place, and give slate experiences a
Parquet sink symmetric with the joiner's. The rejected alternatives were dumping
the Kafka topic to JSONL inside the sim script (leaves slates unreachable for
every other consumer, and depends on a timeout heuristic to know when the dump
is complete) and building a separate measurements sim (duplicates ~150 lines of
infrastructure bootstrap and leaves the canonical dashboard path showing N/A).

## 1. Data generation

`movie_segment_producer.py` gains a documented measurement ground truth,
following the convention already in that file — `FAMILY_EFF` and `DECADE_EFF`
are recoverable from the report, so the report can be validated against what was
generated.

| Signal | Carried on | Ground truth |
|---|---|---|
| `published_at`, `new_release` | impression | Catalog *availability* instant, deliberately distinct from `release_year`: a 1985 film can enter the catalog last week. A documented share (~15%) lands inside the 30-day freshness window. |
| `unsafe_label` | impression | A documented ~2% of impressions are flagged by an independent labeler. This is the denominator `unsafe_exposure_rate` is defined over. |
| `rating` | order | Ratings accompany orders. |
| `dwell_millis`, `completion_rate` | click | Correlated with the item's click propensity so the values carry signal rather than noise. |
| `negative_feedback_reason` | click | Clicks whose `completion_rate` falls below 0.10 carry `not_interested` — a click that went badly. |
| `gender`, `age_band`, `country`, `subscription` | `user_features` on impression | Assigned per user at startup and stable across slates, with one documented CTR effect so fairness has a real, explainable gap. |

Two consequences of this design:

**Safety splits across the two paths.** The producer emits `unsafe_label` but not
`filter_reason`: nothing in the offline producer path filters candidates, so
inventing rejection reasons there would be fiction. Real filter decisions come
from the live service during the traffic burst, which applies its own expiry,
muted-genre, and muted-keyword rules. The safety section then shows an `offline`
row (unsafe exposure) beside a `live_service` row (filter decisions) — the case
the offline/live merge was built for. Offline `filter_decisions` stays `None`,
which `compute_safety` already represents correctly.

**Demographics arrive as a features map.** The pipeline carries `user_features`
into training samples, but `compute_fairness` reads columns named `gender`,
`age_band`, `country`, and `subscription`. The exporter gets a small adapter that
hoists the allowlisted demographic keys out of `user_features` into columns —
the same shape as the existing `_with_published_timestamps` adapter. No schema
change, and it works for any run whose events carry them. The allowlist is the
cardinality guard: keys outside `governance_measurements.DEFAULT_DIMENSIONS` are
never hoisted.

## 2. Pipeline wiring

**Slates to disk.** `ExperienceCollectorStreamingJob` gains an optional
`EXPERIENCE_COLLECTOR_OUTPUT_PATH`. Its existing `foreachBatch` already holds the
built slates, so it reuses the shared `ParquetSink` (`write(batch, batchId)`) —
the same class the joiner uses — partitioned on a date derived from `request_ts`.
Kafka output is unchanged. With the variable unset the job behaves exactly as it
does today.

**Sim script**, three additions to `run-movie-category-sim.sh` after the joiner
step:

1. `run_and_drain com.demo.process.ExperienceCollectorStreamingJob` with a
   parquet-file-count probe, the same shape as the joiner's step, inheriting the
   existing drain, timeout, and kill handling.
2. A live-service block: start the retrieval service in the background, poll
   until `/metrics` answers, drive a traffic burst of 50 `/recommend` calls
   across 10 users and a `/feedback` post for roughly half the returned items,
   capture `/metrics > live-metrics.json`, stop the service. The burst is sized
   to populate every latency bucket and both endpoints without extending the
   sim by more than a few seconds; `MEASUREMENT_BURST_REQUESTS` overrides it.
3. The export step gains `--experiences` and `--live-metrics`, followed by
   `npm run validate:data` so a bad snapshot fails the sim instead of landing
   silently.

**Failure behavior.** Each addition degrades to N/A rather than failing the sim.
If the service never starts, the capture is skipped, latency stays unavailable,
and the sim continues. The sim doubles as the repo's smoke test; the measurement
sections must not turn it red on a machine without a spare port.

**Artifacts** land under the existing `$SIM_ROOT`: `training-samples/` (today),
`slates/`, `live-metrics.json`, `report-dashboard/`.

## 3. Frontend

**Scorecard.** A new `Scorecard` renders seven `MetricTile`s above the detail
sections. Each tile carries the dimension name, one headline number, its label,
the sample size, and a status dot: green for available, amber for available with
`coverage` below 0.50, grey for N/A with the reason. A tile never
colors by whether a number is *good* — no targets have been set for CTR or for a
fairness gap, and a red tile would invent one. Tiles anchor-link to their section.

**Detail sections.** `MeasurementSection` grows from "coverage line plus table"
to "KPI tiles → one chart → full table". The table is unchanged — still
horizontally scrollable, still N/A for nulls.

| Section | Headline | Chart |
|---|---|---|
| Relevance | NDCG@10 | NDCG and MRR by cutoff k |
| Satisfaction | CTR | Coverage per optional signal |
| Freshness | fresh share | Fresh vs established CTR |
| Diversity | normalized genre entropy | Entropy, intra-list distance, long-tail share (one 0–1 scale) |
| Fairness | largest CTR gap | CTR per group against the overall |
| Safety | unsafe exposure rate | Filter decisions by reason |
| Latency | p95 `/recommend` | p95 per stage |

**Reuse over rewrite.** The existing `BarChart` and `DataTable` in `ui.jsx` stay.
New pieces are `Scorecard`, `MetricTile`, and a grouped-bar variant for the
two-series charts. The `dataviz` skill is consulted before any chart code is
written, so palette and mark choices are consistent and accessible in both
themes rather than invented.

**Unavailable stays first-class.** A section with no data renders the N/A card it
renders today, and its tile is grey with the reason, so a partially instrumented
run reads honestly instead of looking broken.

## 4. Testing

| Surface | Verification |
|---|---|
| Producer | Seeded unit tests in `test_movie_category_sim.py`: measurement fields appear on the correct event types, documented shares land within tolerance, demographics are stable per user across slates. No Kafka required. |
| Experience collector | `ExperienceCollectorStreamingJobSpec`: with the path set, a batch lands as readable Parquet matching the slate contract; with it unset, nothing is written and the Kafka path is untouched. |
| Exporter | The demographic-hoisting adapter promotes allowlisted keys and ignores everything else — the allowlist is a cardinality guard, so the exclusion is what needs proving. |
| Frontend | No JS test runner exists in this repo. Verification is `npm run validate:data` plus `npm run build`, with the existing JSX-column/calculator-key correspondence test extended to the new headline and KPI field references — the scorecard introduces that bug class at seven more sites. |
| End to end | With Docker running: the instrumented sim, then assert every section reports `available` with non-zero support in the snapshot, run the validator and the production build, and review the rendered page before committing it. Full Maven, SBT, and pytest suites before the PR. |

## Definition of done

`run-movie-category-sim.sh` produces a committed `dashboard.json` in which all
seven measurement sections are `available` with non-zero support, and the
dashboard renders the scorecard and every detail section from that real run.

## Out of scope

- Thresholds, targets, or alerting on measurement values.
- Instrumenting the engagement or movielens-segment sims.
- Time-series history: the dashboard reports one run, not a trend.
- Any change to serving behavior.
