# Recommendation Measurements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add measurement-only coverage for relevance, satisfaction, freshness, diversity, fairness, safety, and latency to the live Java metrics API and consolidated dashboard.

**Architecture:** The Java service records bounded live operational measurements, Spark preserves attribution and calculates streaming delays, and pure Python calculators produce offline listwise and group metrics. The existing exporter merges all results into a versioned dashboard contract; React renders a consistent section for each measurement family without changing recommendation behavior.

**Tech Stack:** Java 17, Spring Boot 3.3, Micrometer, Redis, Scala 2.12, Spark 3.5, Python 3/pandas/pytest, Next.js 15/React 19.

## Global Constraints

- This release is measurement-only: do not change candidate generation, filtering decisions, ranking scores, or selection behavior.
- Existing feedback payloads containing only `user`, `item`, `clicked`, and `reward` remain valid.
- Existing Kafka events and dashboard JSON remain readable.
- Missing optional signals are unavailable, never zero-filled.
- Every rate includes a denominator or sample size and every optional measure includes coverage.
- No user ID, item ID, request ID, free-form reason, or arbitrary demographic value may become a time-series label.
- Fairness groups below 100 impressions are suppressed by default.
- Freshness defaults to 30 days; long-tail defaults to the bottom 80 percent of popularity; safety policy defaults to `catalog-filter-v1`.
- Latency buckets are 5, 10, 25, 50, 100, 250, 500, 1000, and 2500 milliseconds.

---

### Task 1: Common Offline Measurement Contract

**Files:**
- Create: `recsys-pipeline/services/python-modeling/measurement_contract.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_measurement_contract.py`

**Interfaces:**
- Produces: `available(headline, rows, sample_size, coverage, window=None, warnings=()) -> dict`
- Produces: `unavailable(reason, warnings=()) -> dict`
- Produces: `safe_ratio(numerator, denominator) -> float | None`
- Contract keys: `status`, `headline`, `sampleSize`, `coverage`, `window`, `warnings`, `rows`

- [ ] **Step 1: Write failing contract tests**

```python
from measurement_contract import available, safe_ratio, unavailable


def test_available_keeps_support_and_coverage():
    result = available("CTR 25.0%", [{"ctr": 0.25}], 8, 0.8)
    assert result == {
        "status": "available",
        "headline": "CTR 25.0%",
        "sampleSize": 8,
        "coverage": 0.8,
        "window": None,
        "warnings": [],
        "rows": [{"ctr": 0.25}],
    }


def test_unavailable_does_not_fabricate_numeric_values():
    result = unavailable("missing rating")
    assert result["status"] == "unavailable"
    assert result["warnings"] == ["missing rating"]
    assert "sampleSize" not in result
    assert "coverage" not in result


def test_safe_ratio_rejects_empty_denominator():
    assert safe_ratio(3, 0) is None
    assert safe_ratio(3, 4) == 0.75
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_measurement_contract.py
```

Expected: collection fails because `measurement_contract` does not exist.

- [ ] **Step 3: Implement the minimal contract**

```python
def safe_ratio(numerator, denominator):
    return None if denominator <= 0 else numerator / denominator


def available(headline, rows, sample_size, coverage, window=None, warnings=()):
    return {
        "status": "available",
        "headline": headline,
        "sampleSize": int(sample_size),
        "coverage": round(float(coverage), 4),
        "window": window,
        "warnings": list(warnings),
        "rows": list(rows),
    }


def unavailable(reason, warnings=()):
    return {
        "status": "unavailable",
        "headline": "N/A",
        "warnings": [reason, *warnings],
        "rows": [],
    }
```

- [ ] **Step 4: Run tests and verify GREEN**

Run the command from Step 2. Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/python-modeling/measurement_contract.py \
  recsys-pipeline/integration-tests/python_modeling/test_measurement_contract.py
git commit -m "feat: add offline measurement contract"
```

---

### Task 2: Relevance, Satisfaction, Freshness, and Diversity Calculators

**Files:**
- Create: `recsys-pipeline/services/python-modeling/quality_measurements.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py`

**Interfaces:**
- Consumes: `measurement_contract.available`, `measurement_contract.unavailable`
- Produces: `compute_relevance(slates, ks=(5, 10, 20)) -> dict`
- Produces: `compute_satisfaction(samples) -> dict`
- Produces: `compute_freshness(samples, now, window_days=30) -> dict`
- Produces: `compute_diversity(slates, long_tail_percentile=0.80) -> dict`
- Input slates are pandas rows containing ordered `items`; item entries contain `label`, `genres`, `popularity`, `published_at`, and `new_release` where available.

- [ ] **Step 1: Write failing formula tests**

```python
from datetime import datetime, timezone
import pandas as pd
from quality_measurements import (
    compute_diversity,
    compute_freshness,
    compute_relevance,
    compute_satisfaction,
)


def test_relevance_uses_graded_gain_and_rank():
    slates = pd.DataFrame([{
        "request_id": "r1",
        "items": [
            {"label": 2.0},
            {"label": 0.0},
            {"label": 1.0},
        ],
    }])
    result = compute_relevance(slates, ks=(3,))
    assert result["status"] == "available"
    assert result["rows"][0]["ndcg_at_k"] == 1.0
    assert result["rows"][0]["mrr_at_k"] == 1.0


def test_satisfaction_reports_optional_signal_coverage():
    samples = pd.DataFrame([
        {"clicked": 1, "ordered": 0, "reward": 1.0, "rating": 5.0,
         "negative_feedback_reason": None, "dwell_millis": 1000, "completion_rate": 0.8},
        {"clicked": 0, "ordered": 0, "reward": 0.0, "rating": None,
         "negative_feedback_reason": "not_interested", "dwell_millis": None, "completion_rate": None},
    ])
    result = compute_satisfaction(samples)
    row = result["rows"][0]
    assert row["ctr"] == 0.5
    assert row["mean_rating"] == 5.0
    assert row["rating_coverage"] == 0.5
    assert row["negative_feedback_rate"] == 0.5


def test_freshness_uses_timestamp_and_labels_boolean_fallback():
    now = datetime(2026, 7, 30, tzinfo=timezone.utc)
    timestamped = pd.DataFrame([
        {"published_at": "2026-07-20T00:00:00Z", "new_release": False, "clicked": 1, "reward": 1.0},
        {"published_at": "2026-05-01T00:00:00Z", "new_release": True, "clicked": 0, "reward": 0.0},
    ])
    result = compute_freshness(timestamped, now, window_days=30)
    assert result["rows"][0]["freshness_source"] == "published_at"
    assert result["rows"][0]["fresh_share"] == 0.5

    fallback = compute_freshness(
        pd.DataFrame([{"new_release": True, "clicked": 1, "reward": 1.0}]),
        now,
        window_days=30,
    )
    assert fallback["rows"][0]["freshness_source"] == "boolean_new_release"
    assert fallback["rows"][0]["mean_content_age_days"] is None


def test_diversity_reports_entropy_jaccard_and_long_tail():
    slates = pd.DataFrame([{
        "items": [
            {"genres": ["drama"], "popularity": 100.0},
            {"genres": ["comedy"], "popularity": 5.0},
        ]
    }])
    row = compute_diversity(slates, long_tail_percentile=0.80)["rows"][0]
    assert row["unique_genres_at_k"] == 2.0
    assert row["normalized_genre_entropy"] == 1.0
    assert row["intra_list_genre_distance"] == 1.0
    assert row["long_tail_exposure_share"] == 0.5
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_quality_measurements.py
```

Expected: collection fails because `quality_measurements` does not exist.

- [ ] **Step 3: Implement pure helpers and calculators**

Implement:

```python
def dcg(labels, k):
    return sum((2.0 ** max(0.0, float(label)) - 1.0) / math.log2(rank + 2)
               for rank, label in enumerate(labels[:k]))


def ndcg(labels, k):
    ideal = dcg(sorted(labels, reverse=True), k)
    return None if ideal == 0 else dcg(labels, k) / ideal


def reciprocal_rank(labels, k):
    return next((1.0 / (i + 1) for i, value in enumerate(labels[:k]) if value > 0), 0.0)


def jaccard_distance(left, right):
    union = set(left) | set(right)
    return None if not union else 1.0 - len(set(left) & set(right)) / len(union)
```

Use `pandas.notna` to distinguish missing optional values. Aggregate only over
non-missing observations, calculate coverage as observed rows divided by total rows,
and round published values to four decimals. For diversity, calculate the popularity
cutoff across all measured items and average slate-level measures.

- [ ] **Step 4: Run tests and verify GREEN**

Run the command from Step 2. Expected: 4 passed.

- [ ] **Step 5: Add edge-case tests**

Add tests proving:

- all-zero labels produce NDCG `None` without failing other relevance rows;
- no optional satisfaction signals return those values as `None` with zero coverage;
- missing timestamps and missing `new_release` return unavailable freshness;
- empty genre arrays reduce genre coverage and do not count as distance 1;
- a one-item slate has intra-list distance `None`.

- [ ] **Step 6: Run the expanded tests**

Run the command from Step 2. Expected: all quality-measurement tests pass.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/python-modeling/quality_measurements.py \
  recsys-pipeline/integration-tests/python_modeling/test_quality_measurements.py
git commit -m "feat: calculate recommendation quality measures"
```

---

### Task 3: Fairness and Safety Offline Calculators

**Files:**
- Create: `recsys-pipeline/services/python-modeling/governance_measurements.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_governance_measurements.py`

**Interfaces:**
- Consumes: the common measurement contract
- Produces: `compute_fairness(samples, min_support=100, dimensions=DEFAULT_DIMENSIONS) -> dict`
- Produces: `compute_safety(samples, policy_version="catalog-filter-v1") -> dict`
- Fairness dimensions: `age_band`, `gender`, `occupation`, `geo`, `platform`, `country`, `subscription`
- Safety reasons: `expired`, `muted_product_type`, `muted_genre`, `muted_keyword`, `muted_title`, `unknown`

- [ ] **Step 1: Write failing governance tests**

```python
import pandas as pd
from governance_measurements import compute_fairness, compute_safety


def test_fairness_suppresses_small_groups_and_reports_gaps():
    rows = (
        [{"gender": "a", "clicked": 1, "ordered": 0, "reward": 1.0, "label": 1.0}] * 2
        + [{"gender": "b", "clicked": 0, "ordered": 0, "reward": 0.0, "label": 0.0}] * 2
        + [{"gender": "tiny", "clicked": 1, "ordered": 1, "reward": 1.0, "label": 2.0}]
    )
    result = compute_fairness(pd.DataFrame(rows), min_support=2, dimensions=("gender",))
    groups = result["rows"][0]["groups"]
    assert [group["group"] for group in groups] == ["a", "b"]
    assert result["rows"][0]["suppressed_group_count"] == 1
    assert result["rows"][0]["ctr_max_min_gap"] == 1.0
    assert result["rows"][0]["ctr_disparity_ratio"] == 0.0


def test_safety_distinguishes_filtering_from_unsafe_labels():
    samples = pd.DataFrame([
        {"filter_reason": "muted_genre", "unsafe_label": None},
        {"filter_reason": None, "unsafe_label": True},
        {"filter_reason": None, "unsafe_label": False},
    ])
    result = compute_safety(samples, policy_version="catalog-filter-v2")
    row = result["rows"][0]
    assert row["policy_version"] == "catalog-filter-v2"
    assert row["filter_decision_rate"] == 1 / 3
    assert row["unsafe_exposure_rate"] == 0.5
    assert row["unsafe_label_coverage"] == 2 / 3
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_governance_measurements.py
```

Expected: collection fails because `governance_measurements` does not exist.

- [ ] **Step 3: Implement bounded fairness and safety aggregation**

Implement group metrics with explicit support and overall comparisons. Use only the
fixed dimension and reason allowlists. Normalize unknown dimension values to
`unknown`; do not expose arbitrary field names as dimensions.

For group NDCG, group rows by `request_id`, preserve `position`, compute NDCG from
labels, and average evaluable slates. If those columns are missing, keep group NDCG
`None` while retaining exposure/CTR/order/reward measures.

For safety, report:

```python
{
    "policy_version": policy_version,
    "evaluated_candidates": total,
    "filter_decisions": filtered,
    "filter_decision_rate": safe_ratio(filtered, total),
    "reason_counts": {reason: count for reason in SAFETY_REASONS},
    "unknown_share": safe_ratio(unknown, total),
    "unsafe_exposure_rate": safe_ratio(unsafe_exposed, labeled_exposed),
    "unsafe_label_coverage": safe_ratio(labeled, total),
}
```

- [ ] **Step 4: Run tests and verify GREEN**

Run the command from Step 2. Expected: 2 passed.

- [ ] **Step 5: Add missing-input and bounded-dimension tests**

Prove that absent demographics return unavailable fairness, absent unsafe labels set
unsafe exposure to `None`, and a column not in `DEFAULT_DIMENSIONS` is never emitted.

- [ ] **Step 6: Run expanded governance tests and commit**

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_governance_measurements.py
git add services/python-modeling/governance_measurements.py \
  integration-tests/python_modeling/test_governance_measurements.py
git commit -m "feat: add fairness and safety measurements"
```

---

### Task 4: Java Measurement Configuration and Feedback Contract

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/pom.xml`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/FeedbackRequest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/config/MeasurementPropertiesTest.java`

**Interfaces:**
- Produces: optional feedback accessors `requestId()`, `rating()`, `negativeFeedbackReason()`, `dwellMillis()`, `completionRate()`
- Produces: `RecommendationProperties.Measurements`
- Adds `spring-boot-starter-actuator` and `micrometer-core`; no external monitoring backend is required.

- [ ] **Step 1: Write failing feedback compatibility tests**

Add MockMvc cases proving the legacy body returns 200 and the enriched body accepts:

```json
{
  "user": "u1",
  "item": "item1",
  "clicked": true,
  "reward": 1.0,
  "requestId": "req-1",
  "rating": 4.5,
  "negativeFeedbackReason": null,
  "dwellMillis": 12000,
  "completionRate": 0.75
}
```

Add invalid cases for rating outside 0–5, negative dwell, and completion outside
0–1.

- [ ] **Step 2: Run controller tests and verify RED**

Run:

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn -Dtest=RecommendationControllerTest test
```

Expected: enriched contract assertions fail because the fields do not exist.

- [ ] **Step 3: Extend `FeedbackRequest` compatibly**

Use boxed nullable types:

```java
public record FeedbackRequest(
    @NotBlank String user,
    @NotBlank String item,
    boolean clicked,
    @DecimalMin("0.0") @DecimalMax("1.0") double reward,
    @Pattern(regexp = "[a-zA-Z0-9_:-]{1,128}") String requestId,
    @DecimalMin("0.0") @DecimalMax("5.0") Double rating,
    @Pattern(regexp = "[a-zA-Z0-9_-]{1,64}") String negativeFeedbackReason,
    @PositiveOrZero Long dwellMillis,
    @DecimalMin("0.0") @DecimalMax("1.0") Double completionRate
) {}
```

Update direct constructor calls in tests to pass trailing nulls.

- [ ] **Step 4: Run controller tests and verify GREEN**

Run the command from Step 2. Expected: all controller tests pass.

- [ ] **Step 5: Write failing measurement-property binding test**

Assert defaults: support 100, freshness 30 days, percentile 0.80, policy version
`catalog-filter-v1`, and exact latency buckets.

- [ ] **Step 6: Run property test and verify RED**

Run:

```bash
mvn -Dtest=MeasurementPropertiesTest test
```

Expected: compilation fails because `Measurements` does not exist.

- [ ] **Step 7: Add dependencies and configuration**

Add:

```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
```

Add `Measurements` with validated fields and bind these YAML values:

```yaml
recsys:
  measurements:
    fairness-min-support: ${RECSYS_FAIRNESS_MIN_SUPPORT:100}
    freshness-window-days: ${RECSYS_FRESHNESS_WINDOW_DAYS:30}
    long-tail-percentile: ${RECSYS_LONG_TAIL_PERCENTILE:0.80}
    safety-policy-version: ${RECSYS_SAFETY_POLICY_VERSION:catalog-filter-v1}
    latency-buckets-ms: ${RECSYS_LATENCY_BUCKETS_MS:5,10,25,50,100,250,500,1000,2500}
```

- [ ] **Step 8: Run Java tests and commit**

```bash
mvn -Dtest=MeasurementPropertiesTest,RecommendationControllerTest test
git add pom.xml src/main src/test
git commit -m "feat: add measurement configuration and feedback fields"
```

---

### Task 5: Java Live Latency, Freshness, Safety, and Feedback Measurements

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/measurement/RecommendationMeasurementService.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/measurement/FilterDecision.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/measurement/MeasurementSnapshot.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/controller/RecommendationController.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/retrieval/ContentCandidateRetriever.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/retrieval/RetrievalOutcome.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/measurement/RecommendationMeasurementServiceTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/HybridRecommendationServiceTest.java`

**Interfaces:**
- `recordRequest(endpoint, duration, error)` records bounded endpoint timers/counters.
- `timeStage(stage, Supplier<T>)` measures only fixed stages.
- `recordFreshness(selectedProfiles)` records timestamp or boolean-fallback exposure.
- `recordFilterDecisions(Collection<FilterDecision>)` records fixed reason counts.
- `recordFeedbackCoverage(FeedbackRequest)` records presence counters, never values or reasons as labels.
- `snapshot() -> MeasurementSnapshot` returns schema version `2.0`.
- `RetrievalOutcome` gains `List<FilterDecision> filterDecisions`.

- [ ] **Step 1: Write failing measurement-service tests**

Using `SimpleMeterRegistry`, prove endpoint timers have counts, fixed stage names work,
free-form filter strings collapse to `unknown`, feedback coverage increments only
presence counters, and `snapshot()` contains p50/p95/p99 plus schema version.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn -Dtest=RecommendationMeasurementServiceTest test
```

Expected: compilation fails because the measurement classes do not exist.

- [ ] **Step 3: Implement the measurement service**

Use `Timer.builder(...).serviceLevelObjectives(...)` with the configured buckets and
fixed tag allowlists:

```java
private static final Set<String> ENDPOINTS = Set.of("recommend", "feedback");
private static final Set<String> STAGES =
    Set.of("hydration", "redis_fetch", "scoring", "selection", "side_effects");
private static final Set<String> FILTER_REASONS =
    Set.of("expired", "muted_product_type", "muted_genre", "muted_keyword", "muted_title", "unknown");
```

Wrap every recorder method in a local `try/catch (RuntimeException)` that logs and
returns without changing the serving result.

- [ ] **Step 4: Run measurement-service tests and verify GREEN**

Run the command from Step 2. Expected: all tests pass.

- [ ] **Step 5: Write failing controller and retrieval-observation tests**

Prove:

- `/recommend` and `/feedback` call `recordRequest`;
- `/metrics` retains existing keys and adds `measurements`;
- each existing eligibility rejection produces the corresponding `FilterDecision`;
- the final recommendation list is identical with a no-op measurement service.

- [ ] **Step 6: Run focused tests and verify RED**

Run:

```bash
mvn -Dtest=RecommendationControllerTest,HybridRecommendationServiceTest test
```

Expected: assertions for measurement calls and filter decisions fail.

- [ ] **Step 7: Integrate observation without behavior changes**

Time controller requests with `System.nanoTime()`, use `finally` to record completion,
and stage-time the existing blocks without moving or changing their logic. Return:

```java
Map<String, Object> aggregate = recommendationService.getAggregateMetrics();
aggregate.put("measurements", measurementService.snapshot().asMap());
```

In candidate eligibility, preserve the current boolean result and attach exactly one
reason when rejected. Measure selected freshness after selection. Record optional
feedback coverage before the existing Redis write pipeline.

- [ ] **Step 8: Run focused and full Java tests**

```bash
mvn -Dtest=RecommendationMeasurementServiceTest,RecommendationControllerTest,HybridRecommendationServiceTest test
mvn test
```

Expected: both commands succeed with zero failures.

- [ ] **Step 9: Commit**

```bash
git add src/main src/test
git commit -m "feat: instrument live recommendation measurements"
```

---

### Task 6: Spark Attribution, Feedback Delay, and Ingest Lag

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/event/EventSchemas.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/OnlineJoinerStreamingJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/RecommendationResponseStatsJob.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/OnlineJoinerStreamingJobSpec.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/ExperienceCollectorStreamingJobSpec.scala`
- Modify: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/process/RecommendationResponseStatsJobSpec.scala`

**Interfaces:**
- Nullable event fields: model/policy/algorithm versions, rating, negative reason, dwell, completion, published timestamp, new-release flag, filter reason, unsafe label.
- Training samples preserve `last_feedback_ts` and calculate `feedback_delay_ms`.
- Slate experiences preserve ordered measurement fields in `items`.
- Response metrics add fixed names `feedback_delay_ms` and `kafka_ingest_lag_ms`.

- [ ] **Step 1: Write failing schema-preservation tests**

Create one legacy event and one enriched event. Assert both parse; enriched fields
survive `buildTrainingSamples` and `buildSlates`; legacy fields are null.

- [ ] **Step 2: Run Spark tests and verify RED**

Run:

```bash
cd recsys-pipeline/services/spark-streaming-job
sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec com.demo.process.ExperienceCollectorStreamingJobSpec"
```

Expected: enriched-field assertions fail.

- [ ] **Step 3: Extend nullable schemas and transformations**

Add nullable fields to `EventSchemas.joiner`, aggregate them with `first(...,
ignoreNulls = true)`, and calculate:

```scala
when(col("last_feedback_ts").isNotNull,
  (col("last_feedback_ts") - col("impression_ts")) * 1000L
).as("feedback_delay_ms")
```

Preserve fields inside each sorted slate item.

- [ ] **Step 4: Run preservation tests and verify GREEN**

Run the command from Step 2. Expected: tests pass.

- [ ] **Step 5: Write failing response-stat delay tests**

Assert fixed metric events contain feedback delay and ingest lag, exclude negative
delays, and never place request/user IDs in tags.

- [ ] **Step 6: Run response-stat tests and verify RED**

```bash
sbt "testOnly com.demo.process.RecommendationResponseStatsJobSpec"
```

Expected: delay metric assertions fail.

- [ ] **Step 7: Implement delay metric events**

Calculate ingest lag from Kafka timestamp versus event timestamp at parse time.
Emit delay values with fixed metric names and existing bounded subscription/country
tags. Do not filter out zero-valued delay measurements.

- [ ] **Step 8: Run focused Spark tests and commit**

```bash
sbt "testOnly com.demo.process.OnlineJoinerStreamingJobSpec com.demo.process.ExperienceCollectorStreamingJobSpec com.demo.process.RecommendationResponseStatsJobSpec"
git add src/main src/test
git commit -m "feat: preserve recommendation measurement attribution"
```

---

### Task 7: Consolidated Exporter Contract

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/analysis_dashboard_report.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_analysis_dashboard.py`
- Modify: `frontend/export_dashboard_json.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py`

**Interfaces:**
- Consumes the calculators from Tasks 2 and 3.
- Adds CLI options `--experiences`, `--live-metrics`, `--fairness-min-support`, `--freshness-window-days`, `--long-tail-percentile`, and `--safety-policy-version`.
- Produces dashboard `schemaVersion: "2.0"` plus keys `relevance`, `satisfaction`, `freshness`, `diversity`, `fairness`, `safety`, `latency`.
- Merges live latency/freshness/safety/feedback coverage without overwriting offline rows.

- [ ] **Step 1: Write failing exporter-contract test**

Construct a small Parquet sample, slate JSON/Parquet input, and live metrics JSON.
Run exporter entry points and assert:

```python
assert output["schemaVersion"] == "2.0"
assert set(output) >= {
    "relevance", "satisfaction", "freshness", "diversity",
    "fairness", "safety", "latency",
}
assert output["latency"]["status"] == "available"
assert output["fairness"]["status"] in {"available", "unavailable"}
```

Add a second test with no optional inputs and assert all seven keys remain present,
with exact unavailable warnings for missing prerequisites.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_dashboard_measurement_contract.py
```

Expected: schema/version and new-section assertions fail.

- [ ] **Step 3: Integrate calculators and live snapshot**

Add a `build_measurement_dashboard(samples, slates, live, config) -> dict` pure
function. Keep current dashboard keys for compatibility and populate new keys with
the common result envelope. Validate configured percentiles/ranges before compute.

- [ ] **Step 4: Run contract tests and verify GREEN**

Run the command from Step 2. Expected: tests pass.

- [ ] **Step 5: Update existing exporter tests**

Adjust existing expected top-level keys without weakening prior relevance, recall,
ranking, OPE, or MDP assertions. Add a JSON serialization round-trip assertion that
rejects NaN and Infinity:

```python
json.dumps(output, allow_nan=False)
```

- [ ] **Step 6: Run Python dashboard tests**

```bash
pytest -q integration-tests/python_modeling/test_analysis_dashboard.py \
  integration-tests/python_modeling/test_dashboard_measurement_contract.py
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add services/python-modeling/analysis_dashboard_report.py \
  integration-tests/python_modeling/test_analysis_dashboard.py \
  integration-tests/python_modeling/test_dashboard_measurement_contract.py \
  ../frontend/export_dashboard_json.py
git commit -m "feat: export unified recommendation measurements"
```

---

### Task 8: React Dashboard Sections

**Files:**
- Modify: `frontend/components/sections.jsx`
- Modify: `frontend/app/page.jsx`
- Modify: `frontend/app/globals.css`
- Modify: `frontend/data/dashboard.json`
- Modify: `frontend/README.md`
- Create: `frontend/validate_measurements.mjs`
- Modify: `frontend/package.json`

**Interfaces:**
- Produces React exports: `SatisfactionSection`, `FreshnessSection`, `DiversitySection`, `FairnessSection`, `SafetySection`, `LatencySection`.
- Reuses the existing relevance section but renders the new common envelope.
- Adds `npm run validate:data`; `npm run build` invokes data validation first.

- [ ] **Step 1: Add failing data-contract validator**

Create `validate_measurements.mjs` that reads `data/dashboard.json`, requires all seven
measurement keys, validates `status`, and rejects available sections without
`sampleSize`/`coverage`. Add:

```json
"validate:data": "node validate_measurements.mjs",
"build": "npm run validate:data && next build"
```

Run:

```bash
cd frontend
npm run validate:data
```

Expected: failure because the current snapshot lacks the new contract.

- [ ] **Step 2: Update the fixture and verify validator GREEN**

Regenerate or hand-update the checked-in snapshot through
`export_dashboard_json.py`, ensuring unavailable sections have explicit warnings.
Run `npm run validate:data`; expected: success.

- [ ] **Step 3: Add the seven consistent sections**

Use one shared `MeasurementSection` component:

```jsx
function MeasurementSection({ title, data, columns }) {
  if (!data || data.status !== "available") {
    return <NaCard title={title} reason={data?.warnings?.[0] || "measurement unavailable"} />;
  }
  return (
    <section className="panel measurement-panel">
      <SectionHeader title={title} headline={data.headline} />
      <Coverage sampleSize={data.sampleSize} coverage={data.coverage} />
      <Warnings warnings={data.warnings} />
      <DataTable rows={data.rows || []} columns={columns} />
    </section>
  );
}
```

Keep tables horizontally scrollable and render null values as N/A. Do not add a chart
where a headline and compact table communicate the relationship more clearly.

- [ ] **Step 4: Wire sections into the page**

Order: relevance, satisfaction, freshness, diversity, fairness, safety, latency,
then retain recall/ranking/OPE/MDP diagnostic sections.

- [ ] **Step 5: Run frontend validation and build**

```bash
npm run validate:data
npm run build
```

Expected: both succeed without React or Next.js warnings.

- [ ] **Step 6: Commit**

```bash
git add app components data package.json validate_measurements.mjs README.md
git commit -m "feat: present recommendation measurements dashboard"
```

---

### Task 9: Documentation and Full Verification

**Files:**
- Modify: `README.md`
- Modify: `recsys-pipeline/README.md`
- Modify: `frontend/README.md`
- Modify: `recsys-pipeline/sampledata/catalog.json`

**Interfaces:**
- Documents metric definitions, availability rules, commands, configurations, and measurement-only limitations.
- Sample catalog adds `publishedAt` to enough items to demonstrate timestamp freshness while retaining `newRelease`.

- [ ] **Step 1: Add documentation assertions**

Extend `recsys-pipeline/integration-tests/test_application_config.py` to assert every
new environment variable is documented and that the sample catalog includes valid
ISO-8601 `publishedAt` values.

- [ ] **Step 2: Run assertions and verify RED**

```bash
cd recsys-pipeline
pytest -q integration-tests/test_application_config.py
```

Expected: documentation/config assertions fail.

- [ ] **Step 3: Document operation and interpretation**

Document:

- how to capture `curl http://localhost:8080/metrics` into a live snapshot;
- how to run the dashboard exporter with training samples and slate experiences;
- definitions and denominators for all seven dimensions;
- N/A behavior and optional-signal coverage;
- fairness suppression and observational-only caveat;
- safety taxonomy scope;
- latency versus stream-lag distinction;
- all configuration variables and defaults.

Add timestamped sample catalog entries using UTC instants.

- [ ] **Step 4: Run documentation assertions and verify GREEN**

Run the command from Step 2. Expected: pass.

- [ ] **Step 5: Run full verification**

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn test

cd ../spark-streaming-job
sbt test

cd ../../..
pytest -q

cd ../frontend
npm run validate:data
npm run build

cd ..
git diff --check
git status --short
```

Expected:

- Maven: zero failures/errors.
- SBT: all suites pass.
- Pytest: all tests pass.
- Frontend validation and production build succeed.
- `git diff --check` emits no output.
- `git status --short` contains only intentional implementation and planning files.

- [ ] **Step 6: Commit documentation**

```bash
git add README.md recsys-pipeline/README.md frontend/README.md \
  recsys-pipeline/sampledata/catalog.json \
  recsys-pipeline/integration-tests/test_application_config.py
git commit -m "docs: explain recommendation measurements"
```

- [ ] **Step 7: Request final code review**

Invoke `superpowers:requesting-code-review` against the complete implementation diff.
Address correctness, compatibility, privacy/cardinality, and measurement-semantics
findings before declaring completion.
