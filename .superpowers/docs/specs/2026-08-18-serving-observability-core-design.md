# Serving Observability Core — Design

**Date:** 2026-08-18
**Status:** approved
**Depends on:** [2026-07-30-recommendation-measurements-design.md](2026-07-30-recommendation-measurements-design.md)

## Problem

`RecommendationMeasurementService` reports latency percentiles, error rate, and
timeout rate for two endpoints. An audit of the monitoring surface found four
defects in that coverage:

- **No QPS, and none derivable.** The snapshot exposes a cumulative `count` with
  no timestamp and no window. Differencing two polls is unreliable, so request
  rate cannot be recovered by a consumer. Spark's per-batch `rps`
  (`BatchMetricsListener`) measures pipeline throughput, not serving throughput.
- **No cache hit rate, despite two live caches.** `FeatureCache` builds its
  Caffeine item-vector and reward-stats caches without `recordStats()`, so even
  Caffeine's own counters are disabled. The caches exist to collapse per-request
  Redis round-trips from O(N x features) to O(1), and nothing measures whether
  they do.
- **Percentiles never decay.** Neither timer sets `distributionStatisticExpiry`,
  so both are cumulative for the life of the JVM. A single cold-start spike pins
  p99 permanently, which makes the number useless for judging current health.
- **Three endpoints are untimed.** `/predict/*`, `/embedding/{item}`, and
  `/users/{user}/profile` record no latency and no errors. An outage confined to
  them is invisible in the snapshot.

## Goal

`GET /metrics` reports request rate, cache effectiveness, and decaying latency
percentiles across every recommendation endpoint, with each number's support
disclosed rather than implied.

Measurement-only still holds: nothing here changes candidate generation,
filtering, scoring, or selection. The caches keep their existing eviction
behavior; only their statistics recording changes.

## Non-goals

Deliberately excluded, each deserving its own spec:

| Deferred | Why separate |
|---|---|
| Prometheus scrape endpoint | Adds a dependency and an exposed ops surface; changes how metrics are consumed, not what is measured |
| Offline metric-correctness fixes (Recall@K, NDCG zero-slate bias, `coverage` naming) | Moves numbers already committed to `dashboard.json` |
| Online conversion (`ordered` on feedback) | Changes the public API contract and the Spark join |
| Kafka `Metrics` no-op stub | Belongs with the Kafka consumer, not the serving path |

## Approach

Extend `RecommendationMeasurementService` in place. It is already the single
aggregation point for the snapshot, already bounds every tag by allowlist, and
already degrades each section independently through `snapshotSection`. Two new
sections follow those same conventions.

The rejected alternative was exposing `/actuator/prometheus` and computing rate
and hit ratio in a scraper. That is the better long-run answer for rate, but it
moves the whole consumption model: the sim captures a single JSON snapshot
(`live-metrics.json`) with no scraper in the loop, so a Prometheus-only rate is
unreadable by the one consumer that exists today.

## 1. Throughput

A new `throughput` snapshot section, sibling to `latency`.

Per endpoint, a ring buffer of one-second counters covering a configurable
trailing window (`throughput-window-seconds`, default 60). On each recorded
request the bucket for the current second is advanced; buckets older than the
window are zeroed as the ring wraps.

Reported per endpoint:

| Field | Meaning |
|---|---|
| `qps` | `windowRequests / observedSeconds` |
| `windowRequests` | Requests inside the trailing window |
| `windowSeconds` | Configured window width |
| `observedSeconds` | Seconds elapsed since the first recorded request, capped at `windowSeconds` |

`observedSeconds` is the key honesty property. The canonical sim burst issues
its requests over a few seconds; dividing by a fixed 60-second window would
understate QPS by roughly an order of magnitude. Dividing by elapsed-observed
time reports the rate actually sustained, and publishing the denominator lets a
reader see how thin the support is. Before any request is recorded `qps` is
`null`, never `0.0` — the existing `rate()` convention, where an undefined ratio
is absent rather than zero.

Time comes from an injected `LongSupplier` of epoch milliseconds, defaulting to
`System::currentTimeMillis`. Tests advance a fake clock instead of sleeping.

## 2. Cache statistics

Both Caffeine caches in `FeatureCache` gain `.recordStats()`. `FeatureCache`
exposes a read-only `stats()` accessor returning Caffeine's `CacheStats` per
cache; `RecommendationMeasurementService` reads it when assembling the snapshot,
so the cache stays free of any dependency on the measurement layer.

A new `cache` section reports, for `item_vectors` and `reward_stats`:

| Field | Source |
|---|---|
| `hitCount`, `missCount` | `CacheStats` |
| `hitRate` | `hits / (hits + misses)`, `null` when no lookups |
| `evictionCount` | `CacheStats` |
| `estimatedSize` | `Cache.estimatedSize()` |

Enabling `recordStats()` requires no call-site changes: `getIfPresent` already
records a hit or a miss. `hasItemVector` also calls `getIfPresent`, so a
presence probe counts as a lookup. That is intentional and accurate — it is a
real cache read — but it means item-vector lookups exceed the number of vectors
consumed, and the spec records that so the ratio is not misread later.

`RedisCachedMoviesClient` is excluded. Despite its name it is not a cache: it
reads precomputed candidates from `user:{id}:cached_movies`, written by an
offline path. It has no miss concept to measure.

## 3. Percentile decay

Both `Timer.builder` calls gain `distributionStatisticExpiry` (from
`percentile-window-seconds`, default 300) and `distributionStatisticBufferLength(5)`,
giving five rotating 60-second buffers.

`Timer.count()` is unaffected by expiry and stays cumulative, so `errorRate` and
`timeoutRate` keep their existing denominators and remain lifetime rates. Only
p50/p95/p99 decay. This asymmetry is deliberate: a rate over all traffic is the
useful error number, while a percentile over all traffic is not.

The committed `dashboard.json` is unaffected in practice — the sim captures
`/metrics` immediately after its burst, far inside a 300-second window.

## 4. Endpoint coverage

`predict`, `embedding`, and `profile` join the `ENDPOINTS` allowlist, and the
three controller methods adopt the `try/finally` + `recordRequest` pattern that
`/recommend` and `/feedback` already use, including `isTimeout` classification.

`/predict/{user}/{item}` and `/predict/id` both record as `predict`; the
endpoint label names the operation, not the route.

Endpoints that have served no traffic report `count: 0` with `null` percentiles,
which the existing `emptyEndpointLatency` shape already covers.

## 5. Schema version

The snapshot version goes `2.0` -> `2.1`: additive sections only, no field
removed or retyped.

`MEASUREMENT_SCHEMA_VERSION` in `analysis_dashboard_report.py` is a *different*
version — it stamps the dashboard export, not the service snapshot — and stays
at `2.0`. The Python exporter reads named keys from the live snapshot and
ignores unknown ones, so no dashboard code, no dashboard contract, and no
regenerated `dashboard.json` is required.

## Error handling

Both new sections follow the established pattern exactly: every recording path
is wrapped so a measurement failure logs and returns rather than propagating
into a served request, and `snapshotSection` degrades each section to an
`availability: "unavailable"` shape with `null` values on assembly failure. A
broken cache stat never breaks latency reporting, and vice versa.

The `noOp()` instance records nothing and reports both new sections as empty,
preserving its use as a null object in tests.

## Testing

| Concern | Test |
|---|---|
| QPS over a burst | Fake clock, 30 requests across 3 seconds, assert `qps == 10.0` and `observedSeconds == 3` |
| Window eviction | Advance the clock past the window, assert stale buckets drop out and `qps` reflects only recent traffic |
| No traffic | `qps` is `null`, not `0.0` |
| Cache hit rate | Miss then put then hit, assert `hitRate == 0.5` and counts |
| Cache never used | `hitRate` is `null` |
| Percentile decay configured | Assert the timer's expiry and buffer length come from config |
| Error rate stays cumulative | Percentiles decay while `count` and `errorRate` do not |
| New endpoints | Controller tests assert timers exist for `predict`, `embedding`, `profile` |
| Bounded tags | An unknown endpoint name records nothing, matching existing allowlist behavior |
| Thread safety | Concurrent `recordRequest` across endpoints, assert exact total |
