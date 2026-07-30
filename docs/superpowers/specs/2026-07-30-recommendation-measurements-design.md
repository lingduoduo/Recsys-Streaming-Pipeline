# Recommendation Measurements Design

**Date:** 2026-07-30  
**Status:** Approved for implementation planning

## Objective

Add production-quality, measurement-only coverage for relevance, user satisfaction,
freshness, diversity, fairness, safety, and latency. The implementation must expose
live measures through the Java `/metrics` endpoint and present live plus offline
measures in the consolidated React dashboard. It must not change candidate
generation, filtering policy, ranking scores, or selection behavior.

## Architectural Approach

Use the existing system boundaries:

- The Java retrieval service records live operational measurements and emits a
  structured `/metrics` response.
- Kafka/Spark preserves measurement attribution and computes streaming aggregates.
- Pure Python calculators compute offline, listwise, and historical measures from
  training samples and slate experiences.
- The dashboard exporter merges live and offline results into a versioned JSON
  contract consumed by React.

This split keeps synchronous serving instrumentation lightweight while using the
existing analytical path for listwise and group-based measures.

## Measurement Contract

Each served request receives a measurement context containing:

- `requestId`
- algorithm name
- model version
- policy version
- serve timestamp
- total request latency
- selected item rank
- the catalog attributes needed for measurement

The feedback request remains backward compatible with the existing
`user`, `item`, `clicked`, and `reward` fields. It adds these optional fields:

- `requestId`
- `rating`
- `negativeFeedbackReason`
- `dwellMillis`
- `completionRate`

Spark schemas treat all new fields as nullable so previously produced events remain
readable. Offline result objects use a common envelope:

```json
{
  "status": "available",
  "headline": "NDCG@10 0.421",
  "sampleSize": 12500,
  "coverage": 0.93,
  "window": {
    "start": "2026-07-23T00:00:00Z",
    "end": "2026-07-30T00:00:00Z"
  },
  "warnings": [],
  "rows": []
}
```

When required data is absent, `status` is `unavailable`, numeric fields are omitted,
and `warnings` explains the exact missing prerequisite. The dashboard must render
this state as N/A rather than zero.

## Measures

### Relevance

Compute NDCG, MRR, Recall, and HitRate at K values 5, 10, and 20. Graded relevance
uses the pipeline's existing labels:

- impression only: gain 0
- click: gain 1
- order or purchase: gain 2

NDCG is computed per slate and averaged over evaluable slates. Recall and HitRate
retain the existing leave-one-out user protocol. Output includes evaluated slate
count, evaluated user count, positive-label count, and label coverage.

### User Satisfaction

Report:

- click-through rate
- order/conversion rate
- average observed reward
- mean explicit rating
- negative-feedback rate
- mean dwell time
- mean completion rate

Each optional signal has an independent coverage value. Missing optional feedback
must not be interpreted as a zero rating, zero dwell, or negative response.

### Freshness

Report:

- fresh-item exposure share at K
- mean and median content age
- click-through rate and reward for fresh versus established items

The catalog gains optional `publishedAt` and `availableAt` timestamps. A configurable
freshness window defaults to 30 days. Until timestamps are populated, the existing
`newRelease` flag may supply a result explicitly identified as
`boolean_new_release`; content-age measures remain unavailable.

### Diversity

Report per slate and as aggregates:

- unique genre count at K
- normalized genre entropy
- intra-list genre distance using pairwise Jaccard distance
- long-tail exposure share

Long-tail membership is defined as items below the configurable 80th percentile of
catalog popularity. Empty genre sets are reported through metadata coverage and are
not silently treated as maximally diverse.

### Fairness

When present, calculate exposure share, CTR, order rate, mean reward, and NDCG for:

- age band
- gender
- occupation
- geography
- platform
- country
- subscription

For each dimension and measure, output group support, overall value, group value,
absolute gap from overall, max-minus-min gap, and minimum-to-maximum ratio. Groups
below the configurable minimum support of 100 impressions are suppressed and
reported only as a suppressed-group count. No causal or legal fairness conclusion
is inferred from these observational disparities.

### Safety

Measure the behavior of the existing filter taxonomy:

- expired item
- muted product type
- muted genre
- muted keyword
- muted title match
- unknown/unclassified item

Output evaluated candidates, decisions and rates by reason, unknown share, and a
configured policy version. Unsafe exposure rate is only available when a catalog or
event contains an explicit versioned unsafe label. Filtered does not automatically
mean unsafe, and unfiltered does not automatically mean safe.

### Latency

The Java service records:

- `/recommend` p50, p95, and p99
- `/feedback` p50, p95, and p99
- error and timeout rates
- hydration time
- Redis/candidate-fetch time
- scoring time
- selection time
- serving-side-effect time

Micrometer timers are the source of live latency data. Histogram buckets are
configured for 5, 10, 25, 50, 100, 250, 500, 1000, and 2500 milliseconds.

Spark separately reports:

- Kafka ingest lag
- impression-to-feedback delay

Pipeline lag and feedback delay must not be presented as synchronous serving
latency.

## Live Metrics API

`GET /metrics` retains the existing top-level bandit metrics for compatibility and
adds a `measurements` object:

```json
{
  "algorithm": "ucb",
  "requests": 100,
  "ctr": 0.08,
  "measurements": {
    "schemaVersion": "2.0",
    "latency": {},
    "freshness": {},
    "safety": {},
    "feedbackCoverage": {}
  }
}
```

The response contains bounded dimensions only. User IDs, item IDs, request IDs,
free-form feedback reasons, and arbitrary demographic values must never become
metric tag names or time-series labels.

## Dashboard

The consolidated dashboard adds one section for each measurement family. Every
section uses the same presentation hierarchy:

1. availability and headline
2. sample size and coverage
3. warnings
4. summary cards or trend visualization when meaningful
5. detailed table

The exporter accepts an optional live metrics snapshot. Without that input, live-only
cards render as unavailable while offline sections still render. Older dashboard
JSON remains readable through frontend defaults.

## Configuration

Provide explicit defaults and environment overrides for:

- fairness minimum support: 100 impressions
- freshness window: 30 days
- long-tail popularity percentile: 0.80
- latency histogram buckets: 5–2500 milliseconds
- safety policy version: `catalog-filter-v1`

Invalid configuration fails at startup when it would make a measure misleading.
Missing data at analysis time produces an unavailable result instead of failing the
whole report.

## Reliability and Failure Behavior

- Measurement recording is best-effort and cannot fail a recommendation or feedback
  request.
- Measurement exceptions are logged with bounded context.
- Existing API payloads and stored events remain readable.
- Optional fields remain absent rather than receiving fabricated defaults.
- Denominators and coverage accompany every rate.
- Offline calculators are pure where practical and return deterministic results for
  identical input.

## Testing Strategy

Use test-driven development for every production change:

- Java unit and controller tests cover feedback compatibility, timers, live metric
  structure, bounded labels, and non-fatal recorder failures.
- Spark tests cover old/new event parsing, nullable-field preservation, slate
  attribution, ingest lag, and feedback delay.
- Python tests cover every formula, edge case, support threshold, missing-input path,
  and common result envelope.
- Frontend tests or build-time assertions cover available and unavailable rendering
  for all seven sections.
- An exporter integration test validates the versioned JSON contract with combined
  offline and live input.

Targeted tests run during each red-green cycle. Before completion, run the complete
Java Maven tests, Spark SBT tests, Python test suite, dashboard exporter tests, and
frontend production build.

## Non-Goals

- Changing recommendation ranking, filtering, or candidate selection
- Automatically optimizing a blended multi-objective score
- Claiming causal, legal, or ethical fairness from observational metrics
- Introducing a general content-moderation model
- Adding raw high-cardinality identifiers to monitoring labels
- Requiring all optional feedback fields from existing clients
