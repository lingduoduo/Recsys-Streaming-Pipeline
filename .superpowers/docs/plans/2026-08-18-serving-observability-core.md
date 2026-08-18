# Serving Observability Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `GET /metrics` reports request rate, cache hit rate, and decaying latency percentiles across every recommendation endpoint.

**Architecture:** Extend `RecommendationMeasurementService`, the existing single aggregation point for the snapshot. Rate tracking lives in a new `ThroughputWindow` value class (a per-endpoint ring buffer of one-second counters) so the 486-line service does not absorb it. Cache statistics come from Caffeine's own counters, surfaced through a `FeatureCache.stats()` accessor that keeps Caffeine types out of the measurement layer.

**Tech Stack:** Java 17, Spring Boot, Micrometer, Caffeine, JUnit 5, Mockito, Maven.

**Spec:** [.superpowers/docs/specs/2026-08-18-serving-observability-core-design.md](../specs/2026-08-18-serving-observability-core-design.md)

## Global Constraints

- Java 17 (`<java.version>17</java.version>`); no new Maven dependencies in this plan.
- Run every Maven command from `recsys-pipeline/services/java-retrieval-service`.
- **JDK 17 is required.** The default JDK on this machine is 25 and aborts tests with a misleading `getSubject` error. Prefix commands with `JAVA_HOME=$(/usr/libexec/java_home -v 17)`.
- Measurement-only: no change to candidate generation, filtering, scoring, or selection.
- Every recording path is wrapped so a measurement failure logs and returns instead of propagating into a served request. Every snapshot section degrades independently through `snapshotSection`.
- Tag values stay bounded by allowlist. Never tag a meter with a user id, item id, or request id.
- An undefined ratio is `null`, never `0.0`. Follow the existing `rate(long, long)` helper.
- Snapshot schema version becomes `2.1`. `MEASUREMENT_SCHEMA_VERSION` in `analysis_dashboard_report.py` is a different version and stays `2.0`.

---

### Task 1: ThroughputWindow ring buffer

**Files:**
- Create: `src/main/java/com/demo/retrieval/measurement/ThroughputWindow.java`
- Test: `src/test/java/com/demo/retrieval/measurement/ThroughputWindowTest.java`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `ThroughputWindow(int windowSeconds, LongSupplier nowMillis)`, `void record()`, `Map<String, Object> snapshot()` returning keys `qps` (`Double`, nullable), `windowRequests` (`Long`), `windowSeconds` (`Integer`), `observedSeconds` (`Long`). Task 5 calls all three.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/demo/retrieval/measurement/ThroughputWindowTest.java`:

```java
package com.demo.retrieval.measurement;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThroughputWindowTest {

    @Test
    void reportsNullRateBeforeAnyRequestIsRecorded() {
        Map<String, Object> snapshot = new ThroughputWindow(60, () -> 0L).snapshot();

        assertNull(snapshot.get("qps"));
        assertEquals(0L, snapshot.get("windowRequests"));
        assertEquals(0L, snapshot.get("observedSeconds"));
        assertEquals(60, snapshot.get("windowSeconds"));
    }

    @Test
    void dividesBurstByObservedSecondsRatherThanWindowWidth() {
        AtomicLong clock = new AtomicLong(0L);
        ThroughputWindow window = new ThroughputWindow(60, clock::get);

        for (int second = 0; second < 3; second++) {
            clock.set(second * 1_000L);
            for (int i = 0; i < 10; i++) {
                window.record();
            }
        }

        Map<String, Object> snapshot = window.snapshot();
        assertEquals(30L, snapshot.get("windowRequests"));
        assertEquals(3L, snapshot.get("observedSeconds"));
        assertEquals(10.0, snapshot.get("qps"));
    }

    @Test
    void dropsBucketsThatFallOutOfTheTrailingWindow() {
        AtomicLong clock = new AtomicLong(0L);
        ThroughputWindow window = new ThroughputWindow(60, clock::get);

        for (int i = 0; i < 10; i++) {
            window.record();
        }
        clock.set(120_000L);

        Map<String, Object> snapshot = window.snapshot();
        assertEquals(0L, snapshot.get("windowRequests"));
        assertEquals(60L, snapshot.get("observedSeconds"));
        assertEquals(0.0, snapshot.get("qps"));
    }

    @Test
    void countsEveryRequestUnderConcurrentRecording() throws Exception {
        ThroughputWindow window = new ThroughputWindow(60, () -> 0L);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);

        for (int t = 0; t < 4; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                for (int i = 0; i < 1_000; i++) {
                    window.record();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(4_000L, window.snapshot().get("windowRequests"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=ThroughputWindowTest`
Expected: compilation failure — `cannot find symbol: class ThroughputWindow`.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/java/com/demo/retrieval/measurement/ThroughputWindow.java`:

```java
package com.demo.retrieval.measurement;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Request rate over a trailing window, as a ring of one-second counters.
 *
 * Rate is reported over the time actually observed rather than the full window
 * width: a three-second burst divided by a sixty-second window would understate
 * throughput by twenty times. {@code observedSeconds} publishes that denominator
 * so a reader can see how thin the support is.
 */
final class ThroughputWindow {

    private static final long NO_REQUESTS = Long.MIN_VALUE;

    private final int windowSeconds;
    private final LongSupplier nowMillis;
    private final long[] counts;
    private final long[] seconds;
    private long firstSecond = NO_REQUESTS;

    ThroughputWindow(int windowSeconds, LongSupplier nowMillis) {
        this.windowSeconds = windowSeconds;
        this.nowMillis = nowMillis;
        this.counts = new long[windowSeconds];
        this.seconds = new long[windowSeconds];
        java.util.Arrays.fill(this.seconds, NO_REQUESTS);
    }

    synchronized void record() {
        long second = currentSecond();
        int slot = slotFor(second);
        if (seconds[slot] != second) {
            seconds[slot] = second;
            counts[slot] = 0L;
        }
        counts[slot]++;
        if (firstSecond == NO_REQUESTS) {
            firstSecond = second;
        }
    }

    synchronized Map<String, Object> snapshot() {
        long second = currentSecond();
        long cutoff = second - windowSeconds + 1;
        long requests = 0L;
        for (int slot = 0; slot < windowSeconds; slot++) {
            if (seconds[slot] >= cutoff && seconds[slot] <= second) {
                requests += counts[slot];
            }
        }
        long observed = firstSecond == NO_REQUESTS
            ? 0L
            : Math.min(windowSeconds, second - firstSecond + 1);

        Map<String, Object> values = new LinkedHashMap<>();
        values.put("qps", observed <= 0 ? null : round((double) requests / observed));
        values.put("windowRequests", requests);
        values.put("windowSeconds", windowSeconds);
        values.put("observedSeconds", observed);
        return values;
    }

    private long currentSecond() {
        return Math.floorDiv(nowMillis.getAsLong(), 1_000L);
    }

    private int slotFor(long second) {
        return (int) Math.floorMod(second, (long) windowSeconds);
    }

    private static Double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=ThroughputWindowTest`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/measurement/ThroughputWindow.java \
        src/test/java/com/demo/retrieval/measurement/ThroughputWindowTest.java
git commit -m "feat: add trailing-window throughput counter"
```

---

### Task 2: Measurement window configuration

**Files:**
- Modify: `src/main/java/com/demo/retrieval/config/RecommendationProperties.java` (imports at line 2-9; `Measurements` class at line 463)
- Modify: `src/main/resources/application.yml` (the `measurements:` block, lines 17-22)
- Test: `src/test/java/com/demo/retrieval/config/MeasurementWindowPropertiesTest.java` (create)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `RecommendationProperties.Measurements#getThroughputWindowSeconds()` and `#getPercentileWindowSeconds()`, both `int`, defaults 60 and 300. Tasks 3 and 5 read them.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/demo/retrieval/config/MeasurementWindowPropertiesTest.java`:

```java
package com.demo.retrieval.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MeasurementWindowPropertiesTest {

    @Test
    void defaultsThroughputAndPercentileWindows() {
        RecommendationProperties.Measurements measurements =
            new RecommendationProperties().getMeasurements();

        assertEquals(60, measurements.getThroughputWindowSeconds());
        assertEquals(300, measurements.getPercentileWindowSeconds());
    }

    @Test
    void acceptsOverriddenWindows() {
        RecommendationProperties.Measurements measurements =
            new RecommendationProperties().getMeasurements();

        measurements.setThroughputWindowSeconds(15);
        measurements.setPercentileWindowSeconds(120);

        assertEquals(15, measurements.getThroughputWindowSeconds());
        assertEquals(120, measurements.getPercentileWindowSeconds());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=MeasurementWindowPropertiesTest`
Expected: compilation failure — `cannot find symbol: method getThroughputWindowSeconds()`.

- [ ] **Step 3: Write minimal implementation**

Add the `Max` import alongside the existing constraint imports at the top of `RecommendationProperties.java`:

```java
import jakarta.validation.constraints.Max;
```

Inside `public static class Measurements`, after the `latencyBucketsMs` field declaration, add:

```java
        @Min(1)
        @Max(3600)
        private int throughputWindowSeconds = 60;
        @Min(1)
        @Max(3600)
        private int percentileWindowSeconds = 300;

        public int getThroughputWindowSeconds() {
            return throughputWindowSeconds;
        }

        public void setThroughputWindowSeconds(int throughputWindowSeconds) {
            this.throughputWindowSeconds = throughputWindowSeconds;
        }

        public int getPercentileWindowSeconds() {
            return percentileWindowSeconds;
        }

        public void setPercentileWindowSeconds(int percentileWindowSeconds) {
            this.percentileWindowSeconds = percentileWindowSeconds;
        }
```

In `src/main/resources/application.yml`, add two keys to the existing `measurements:` block, matching the surrounding `${VAR:default}` style:

```yaml
    throughput-window-seconds: ${RECSYS_THROUGHPUT_WINDOW_SECONDS:60}
    percentile-window-seconds: ${RECSYS_PERCENTILE_WINDOW_SECONDS:300}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=MeasurementWindowPropertiesTest`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/config/RecommendationProperties.java \
        src/main/resources/application.yml \
        src/test/java/com/demo/retrieval/config/MeasurementWindowPropertiesTest.java
git commit -m "feat: configure throughput and percentile windows"
```

---

### Task 3: Decaying latency percentiles

**Files:**
- Modify: `src/main/java/com/demo/retrieval/measurement/RecommendationMeasurementService.java` (fields at line 37-51; constructors at line 53-69; `recordStage` at line 196; `requestTimer` at line 209)
- Test: `src/test/java/com/demo/retrieval/measurement/RecommendationMeasurementServiceTest.java` (add one test)

**Interfaces:**
- Consumes: `Measurements#getPercentileWindowSeconds()` from Task 2.
- Produces: a `private final Duration percentileWindow` field on the service, read by both timer builders. Task 5 does not touch it.

- [ ] **Step 1: Write the failing test**

Add to `RecommendationMeasurementServiceTest`. Micrometer does not expose a
timer's configured expiry, so this asserts the observable consequence — an old
sample leaving the window — rather than the setting:

```java
    @Test
    void decaysLatencyPercentilesWhileKeepingCountsCumulative() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationProperties properties = new RecommendationProperties();
        properties.getMeasurements().setPercentileWindowSeconds(1);
        RecommendationMeasurementService measurements =
            new RecommendationMeasurementService(registry, properties);

        measurements.recordRequest("recommend", Duration.ofMillis(900), false);
        assertTrue(((Number) endpoint(measurements, "recommend").get("p99")).doubleValue() > 100.0);

        await(2_000);
        measurements.recordRequest("recommend", Duration.ofMillis(1), false);
        Map<?, ?> after = endpoint(measurements, "recommend");

        assertTrue(((Number) after.get("p99")).doubleValue() < 100.0,
            "percentiles must decay out of the configured window");
        assertEquals(2L, after.get("count"), "counts stay cumulative");
        assertEquals(0.0, after.get("errorRate"), "error rate stays cumulative");
    }

    private Map<?, ?> endpoint(RecommendationMeasurementService measurements, String name) {
        Map<?, ?> latency = (Map<?, ?>) measurements.snapshot().asMap().get("latency");
        return (Map<?, ?>) ((Map<?, ?>) latency.get("endpoints")).get(name);
    }

    private void await(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
```

This uses the two-argument `@Autowired` constructor, which is still the only one
at this point. Task 5 adds a third parameter and updates this call site.

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=RecommendationMeasurementServiceTest#decaysLatencyPercentilesWhileKeepingCountsCumulative`
Expected: FAIL — the p99 after the window still reflects the 900ms sample, because no expiry is configured.

- [ ] **Step 3: Write minimal implementation**

In `RecommendationMeasurementService`, add a field beside `latencyBuckets`:

```java
    private final Duration percentileWindow;
```

Set it in the private constructor (add a `Duration percentileWindow` parameter), pass `percentileWindow(properties)` from the `@Autowired` constructor, and pass `Duration.ofSeconds(300)` from `noOp()`.

Add the resolver beside `latencyBuckets(RecommendationProperties)`:

```java
    private static Duration percentileWindow(RecommendationProperties properties) {
        return properties == null || properties.getMeasurements() == null
            ? Duration.ofSeconds(300)
            : Duration.ofSeconds(properties.getMeasurements().getPercentileWindowSeconds());
    }
```

Add these two lines to **both** `Timer.builder` chains — in `recordStage` and in `requestTimer` — immediately after `.publishPercentiles(0.50, 0.95, 0.99)`:

```java
            .distributionStatisticExpiry(percentileWindow)
            .distributionStatisticBufferLength(5)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=RecommendationMeasurementServiceTest`
Expected: PASS, all tests in the class.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/measurement/RecommendationMeasurementService.java \
        src/test/java/com/demo/retrieval/measurement/RecommendationMeasurementServiceTest.java
git commit -m "fix: decay latency percentiles out of a bounded window"
```

---

### Task 4: Cache statistics on FeatureCache

**Files:**
- Modify: `src/main/java/com/demo/retrieval/model/FeatureCache.java`
- Test: `src/test/java/com/demo/retrieval/model/FeatureCacheStatsTest.java` (create)

**Interfaces:**
- Consumes: `RecommendationProperties` (already a constructor parameter).
- Produces: `FeatureCache.CacheStatsView(long hitCount, long missCount, long evictionCount, long estimatedSize)` and `Map<String, CacheStatsView> FeatureCache#stats()`, keyed `"item_vectors"` and `"reward_stats"` in that order. Task 5 consumes both.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/demo/retrieval/model/FeatureCacheStatsTest.java`:

```java
package com.demo.retrieval.model;

import com.demo.retrieval.config.RecommendationProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FeatureCacheStatsTest {

    @Test
    void reportsZeroedStatsBeforeAnyLookup() {
        Map<String, FeatureCache.CacheStatsView> stats = new FeatureCache(new RecommendationProperties()).stats();

        assertEquals(Map.of("item_vectors", 0L, "reward_stats", 0L).size(), stats.size());
        assertEquals(0L, stats.get("item_vectors").hitCount());
        assertEquals(0L, stats.get("item_vectors").missCount());
        assertEquals(0L, stats.get("reward_stats").hitCount());
    }

    @Test
    void countsItemVectorHitsAndMisses() {
        FeatureCache cache = new FeatureCache(new RecommendationProperties());

        cache.getItemVector("missing");
        cache.putItemVector("present", new double[] {1.0, 2.0});
        cache.getItemVector("present");

        FeatureCache.CacheStatsView vectors = cache.stats().get("item_vectors");
        assertEquals(1L, vectors.hitCount());
        assertEquals(1L, vectors.missCount());
        assertEquals(1L, vectors.estimatedSize());
    }

    @Test
    void countsRewardStatLookupsSeparately() {
        FeatureCache cache = new FeatureCache(new RecommendationProperties());

        cache.getRewardStats("absent");
        cache.putRewardStats("key", new FeatureCache.RewardModelStats(3L, 1.5));
        cache.getRewardStats("key");

        FeatureCache.CacheStatsView rewards = cache.stats().get("reward_stats");
        assertEquals(1L, rewards.hitCount());
        assertEquals(1L, rewards.missCount());
        assertEquals(0L, cache.stats().get("item_vectors").hitCount());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=FeatureCacheStatsTest`
Expected: compilation failure — `cannot find symbol: class CacheStatsView`.

- [ ] **Step 3: Write minimal implementation**

In `FeatureCache.java`, add `.recordStats()` to both builders:

```java
        this.itemVectors = Caffeine.newBuilder()
            .maximumSize(cfg.getItemVectorMaxSize())
            .expireAfterWrite(cfg.getItemVectorTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
        this.rewardStats = Caffeine.newBuilder()
            .maximumSize(cfg.getRewardMaxSize())
            .expireAfterWrite(cfg.getRewardTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
```

Add the view type and accessor, plus the imports `java.util.LinkedHashMap` and `java.util.Map`:

```java
    /**
     * Caffeine's counters, copied into a plain value so the measurement layer
     * never depends on the cache library.
     *
     * A presence probe through {@code hasItemVector} is a real cache read and
     * counts as a lookup, so item-vector lookups exceed the number of vectors
     * actually consumed.
     */
    public record CacheStatsView(long hitCount, long missCount, long evictionCount, long estimatedSize) {}

    public Map<String, CacheStatsView> stats() {
        Map<String, CacheStatsView> values = new LinkedHashMap<>();
        values.put("item_vectors", view(itemVectors));
        values.put("reward_stats", view(rewardStats));
        return values;
    }

    private static CacheStatsView view(Cache<String, ?> cache) {
        com.github.benmanes.caffeine.cache.stats.CacheStats stats = cache.stats();
        return new CacheStatsView(
            stats.hitCount(), stats.missCount(), stats.evictionCount(), cache.estimatedSize());
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=FeatureCacheStatsTest`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/model/FeatureCache.java \
        src/test/java/com/demo/retrieval/model/FeatureCacheStatsTest.java
git commit -m "feat: record Caffeine cache statistics"
```

---

### Task 5: Throughput and cache snapshot sections

**Files:**
- Modify: `src/main/java/com/demo/retrieval/measurement/RecommendationMeasurementService.java`
- Modify: `src/main/java/com/demo/retrieval/measurement/MeasurementSnapshot.java`
- Modify: `src/test/java/com/demo/retrieval/measurement/RecommendationMeasurementServiceTest.java` (the `measurements(registry)` helper and the `2.0` assertion at line 76)
- Modify: `src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java:343` (the `2.0` assertion)
- Modify: `../../integration-tests/python_modeling/test_dashboard_measurement_contract.py:70` (the live-snapshot fixture's `schemaVersion`)

**Interfaces:**
- Consumes: `ThroughputWindow` (Task 1), `getThroughputWindowSeconds()` (Task 2), `FeatureCache#stats()` and `CacheStatsView` (Task 4).
- Produces: `MeasurementSnapshot(String schemaVersion, Map latency, Map throughput, Map cache, Map freshness, Map safety, Map feedbackCoverage)` with `asMap()` keys in that order. Nothing later consumes it.

- [ ] **Step 1: Write the failing test**

Add to `RecommendationMeasurementServiceTest`:

```java
    @Test
    void snapshotReportsThroughputAndCacheSectionsAtSchemaTwoOne() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache cache = new FeatureCache(properties);
        RecommendationMeasurementService measurements =
            new RecommendationMeasurementService(registry, properties, cache);

        measurements.recordRequest("recommend", Duration.ofMillis(5), false);
        measurements.recordRequest("recommend", Duration.ofMillis(5), false);
        cache.getItemVector("missing");
        cache.putItemVector("present", new double[] {1.0});
        cache.getItemVector("present");

        Map<String, Object> values = measurements.snapshot().asMap();
        assertEquals("2.1", measurements.snapshot().schemaVersion());

        Map<?, ?> throughput = (Map<?, ?>) values.get("throughput");
        assertEquals("available", throughput.get("availability"));
        Map<?, ?> recommend = (Map<?, ?>) ((Map<?, ?>) throughput.get("endpoints")).get("recommend");
        assertEquals(2L, recommend.get("windowRequests"));
        assertEquals(60, recommend.get("windowSeconds"));
        Map<?, ?> feedback = (Map<?, ?>) ((Map<?, ?>) throughput.get("endpoints")).get("feedback");
        assertNull(feedback.get("qps"), "an endpoint with no traffic has an undefined rate");

        Map<?, ?> caches = (Map<?, ?>) values.get("cache");
        assertEquals("available", caches.get("availability"));
        Map<?, ?> vectors = (Map<?, ?>) ((Map<?, ?>) caches.get("caches")).get("item_vectors");
        assertEquals(1L, vectors.get("hitCount"));
        assertEquals(1L, vectors.get("missCount"));
        assertEquals(0.5, vectors.get("hitRate"));
        Map<?, ?> rewards = (Map<?, ?>) ((Map<?, ?>) caches.get("caches")).get("reward_stats");
        assertNull(rewards.get("hitRate"), "a cache with no lookups has an undefined hit rate");
    }

    @Test
    void noOpMeasurementsReportEmptyThroughputAndCacheSections() {
        Map<String, Object> values = RecommendationMeasurementService.noOp().snapshot().asMap();

        Map<?, ?> throughput = (Map<?, ?>) values.get("throughput");
        Map<?, ?> recommend = (Map<?, ?>) ((Map<?, ?>) throughput.get("endpoints")).get("recommend");
        assertNull(recommend.get("qps"));
        assertTrue(((Map<?, ?>) ((Map<?, ?>) values.get("cache")).get("caches")).isEmpty());
    }
```

Update the existing `measurements(SimpleMeterRegistry)` helper so every other test still compiles:

```java
    private RecommendationMeasurementService measurements(SimpleMeterRegistry registry) {
        RecommendationProperties properties = new RecommendationProperties();
        return new RecommendationMeasurementService(registry, properties, new FeatureCache(properties));
    }
```

Change the existing assertion at line 76 from `assertEquals("2.0", ...)` to `assertEquals("2.1", ...)`. Update the two-argument construction inside `decaysLatencyPercentilesWhileKeepingCountsCumulative` (Task 3) to `new RecommendationMeasurementService(registry, properties, new FeatureCache(properties))`. Add the imports `com.demo.retrieval.model.FeatureCache` and `java.util.Map` if absent.

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=RecommendationMeasurementServiceTest`
Expected: FAIL — no three-argument constructor, no `throughput` key, schema still `2.0`.

- [ ] **Step 3: Write minimal implementation**

In `MeasurementSnapshot`, add `throughput` and `cache` as the second and third components, copy them in the compact constructor exactly like the others, and put them into `asMap()` after `latency`.

In `RecommendationMeasurementService`:

Add imports `com.demo.retrieval.model.FeatureCache` and `java.util.function.LongSupplier`, and fields:

```java
    private final FeatureCache featureCache;
    private final Map<String, ThroughputWindow> throughputByEndpoint = new LinkedHashMap<>();
```

Add `FeatureCache featureCache`, `int throughputWindowSeconds`, and `LongSupplier nowMillis` parameters to the private constructor. The `@Autowired` constructor gains `FeatureCache featureCache` as a third parameter and delegates with `properties.getMeasurements().getThroughputWindowSeconds()` (guarding a null `getMeasurements()` with the default `60`, exactly as `latencyBuckets` and `percentileWindow` already do) and `System::currentTimeMillis`. `noOp()` passes `null`, `60`, and `System::currentTimeMillis`.

In the private constructor, populate the window map once so it is never mutated afterwards:

```java
        ENDPOINTS.stream().sorted().forEach(endpoint -> throughputByEndpoint.put(
            endpoint, new ThroughputWindow(throughputWindowSeconds, nowMillis)));
```

In `recordRequest`, inside the existing `synchronized (requestLock)` block and after the timer record, add:

```java
                throughputByEndpoint.get(endpoint).record();
```

Add both sections to `snapshot()`:

```java
            snapshotSection("throughput", this::throughputSnapshot, this::unavailableThroughput),
            snapshotSection("cache", this::cacheSnapshot, this::unavailableCache),
```

placed after the `latency` section, and add the four suppliers:

```java
    private Map<String, Object> throughputSnapshot() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        throughputByEndpoint.forEach((endpoint, window) -> endpoints.put(endpoint, window.snapshot()));
        Map<String, Object> throughput = new LinkedHashMap<>();
        throughput.put("availability", "available");
        throughput.put("unit", "requests_per_second");
        throughput.put("endpoints", endpoints);
        return throughput;
    }

    private Map<String, Object> unavailableThroughput() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        ENDPOINTS.stream().sorted().forEach(endpoint -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("qps", null);
            values.put("windowRequests", null);
            values.put("windowSeconds", null);
            values.put("observedSeconds", null);
            endpoints.put(endpoint, values);
        });
        Map<String, Object> throughput = new LinkedHashMap<>();
        throughput.put("availability", "unavailable");
        throughput.put("unit", "requests_per_second");
        throughput.put("endpoints", endpoints);
        return throughput;
    }

    private Map<String, Object> cacheSnapshot() {
        Map<String, Object> caches = new LinkedHashMap<>();
        if (featureCache != null) {
            featureCache.stats().forEach((name, stats) -> {
                Map<String, Object> values = new LinkedHashMap<>();
                values.put("hitCount", stats.hitCount());
                values.put("missCount", stats.missCount());
                values.put("hitRate", rate(stats.hitCount(), stats.hitCount() + stats.missCount()));
                values.put("evictionCount", stats.evictionCount());
                values.put("estimatedSize", stats.estimatedSize());
                caches.put(name, values);
            });
        }
        Map<String, Object> cache = new LinkedHashMap<>();
        cache.put("availability", "available");
        cache.put("caches", caches);
        return cache;
    }

    private Map<String, Object> unavailableCache() {
        Map<String, Object> cache = new LinkedHashMap<>();
        cache.put("availability", "unavailable");
        cache.put("caches", new LinkedHashMap<String, Object>());
        return cache;
    }
```

Change the snapshot version string from `"2.0"` to `"2.1"`.

Update the two remaining `2.0` assertions: `RecommendationControllerTest.java:343` and the `schemaVersion` value in the fixture at `integration-tests/python_modeling/test_dashboard_measurement_contract.py:70`.

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test`
Expected: PASS, whole Java suite.

Run: `cd ../../.. && python3 -m pytest recsys-pipeline/integration-tests/python_modeling/test_dashboard_measurement_contract.py -q`
Expected: PASS — the exporter reads named keys and ignores the new sections.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/measurement/ \
        src/test/java/com/demo/retrieval/measurement/ \
        src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java \
        ../../integration-tests/python_modeling/test_dashboard_measurement_contract.py
git commit -m "feat: report request rate and cache hit rate at schema 2.1"
```

---

### Task 6: Endpoint coverage

**Files:**
- Modify: `src/main/java/com/demo/retrieval/measurement/RecommendationMeasurementService.java:29` (the `ENDPOINTS` set)
- Modify: `src/main/java/com/demo/retrieval/controller/RecommendationController.java` (`/embedding/{item}` at line 72, `/predict/{user}/{item}` at line 125, `/predict/id` at line 140, `/users/{user}/profile` at line 153)
- Test: `src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java`

**Interfaces:**
- Consumes: `recordRequest(String, Duration, boolean, boolean)` — unchanged.
- Produces: nothing later consumes.

- [ ] **Step 1: Write the failing test**

Add to `RecommendationControllerTest` (it already declares `measurementService` as a `@MockBean` and already imports `ValueOperations`, `mock`, `verify`, and `ArgumentMatchers.*`, so no new imports are needed):

```java
    @Test
    void recordsLatencyForPredictEmbeddingAndProfileEndpoints() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:i1")).thenReturn("0.1 0.2");
        when(predictionService.predict("u1", "i1")).thenReturn(Optional.empty());
        when(predictionService.metadata()).thenReturn(Map.of("model", "test"));
        when(userProfileClient.getProfile("u1")).thenReturn(Optional.of(profile("u1")));

        mockMvc.perform(get("/predict/u1/i1")).andExpect(status().isOk());
        mockMvc.perform(get("/users/u1/profile")).andExpect(status().isOk());
        mockMvc.perform(get("/embedding/i1")).andExpect(status().isOk());

        verify(measurementService).recordRequest(eq("predict"), any(Duration.class), eq(false), eq(false));
        verify(measurementService).recordRequest(eq("profile"), any(Duration.class), eq(false), eq(false));
        verify(measurementService).recordRequest(eq("embedding"), any(Duration.class), eq(false), eq(false));
    }

    @Test
    void treatsAnUnknownProfileAsAbsentRatherThanAServerError() throws Exception {
        when(userProfileClient.getProfile("nobody")).thenReturn(Optional.empty());

        mockMvc.perform(get("/users/nobody/profile")).andExpect(status().isNotFound());

        verify(measurementService).recordRequest(eq("profile"), any(Duration.class), eq(false), eq(false));
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=RecommendationControllerTest`
Expected: FAIL — `Wanted but not invoked: measurementService.recordRequest("predict", ...)`.

- [ ] **Step 3: Write minimal implementation**

Widen the allowlist in `RecommendationMeasurementService`:

```java
    private static final Set<String> ENDPOINTS =
        Set.of("recommend", "feedback", "predict", "embedding", "profile");
```

Wrap each of the four controller methods in the pattern `/recommend` already uses. For `/embedding/{item}`, `/predict/{user}/{item}`, and `/predict/id`, that is exactly:

```java
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            /* existing body, returning as before */
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("<name>", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
```

with `error = false;` set immediately before each `return`, and `<name>` being `embedding`, `predict`, and `predict` respectively. `/predict/metadata` is a static metadata read, not a prediction, and stays uninstrumented.

`/users/{user}/profile` needs one extra clause, because a missing profile is a 404 and not a failure:

```java
    @GetMapping("/users/{user}/profile")
    public ResponseEntity<UserBehaviorProfile> profile(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user
    ) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            ResponseEntity<UserBehaviorProfile> response = userProfileClient.getProfile(user)
                .map(ResponseEntity::ok)
                .orElseThrow(() -> new ProfileNotFoundException(user));
            error = false;
            return response;
        } catch (ProfileNotFoundException e) {
            error = false;
            throw e;
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("profile", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test`
Expected: PASS, whole Java suite.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/controller/RecommendationController.java \
        src/main/java/com/demo/retrieval/measurement/RecommendationMeasurementService.java \
        src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java
git commit -m "feat: time the predict, embedding, and profile endpoints"
```

---

### Task 7: Document the metrics surface

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_flows/9_Track_Metrics.md`

**Interfaces:**
- Consumes: the snapshot shape from Tasks 5 and 6.
- Produces: nothing.

- [ ] **Step 1: Extend the metrics documentation**

`9_Track_Metrics.md` currently documents only the bandit aggregate fields. Append a section describing the `measurements` block, since `GET /metrics` now returns two more sections under it:

```markdown
## `measurements` (schema 2.1)

`GET /metrics` nests a `measurements` object beside the bandit aggregates.

| Section | Contents |
|---|---|
| `latency` | p50/p95/p99, count, error rate, and timeout rate per endpoint (`recommend`, `feedback`, `predict`, `embedding`, `profile`) and per stage |
| `throughput` | `qps`, `windowRequests`, `windowSeconds`, and `observedSeconds` per endpoint |
| `cache` | `hitCount`, `missCount`, `hitRate`, `evictionCount`, and `estimatedSize` for `item_vectors` and `reward_stats` |
| `freshness` | Fresh-item exposure share |
| `safety` | Candidate-filter decisions by reason |
| `feedbackCoverage` | Presence rate of each optional feedback signal |

Percentiles decay over `RECSYS_PERCENTILE_WINDOW_SECONDS` (default 300), so they
describe recent traffic. Counts, error rate, and timeout rate stay cumulative for
the life of the process.

`qps` divides by `observedSeconds` — the time actually observed, capped at the
window — rather than the full window width, so a short burst is not understated.
An endpoint that has served nothing reports `qps: null`, not zero.
```

- [ ] **Step 2: Verify the documented fields match the code**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=RecommendationMeasurementServiceTest`
Expected: PASS. Cross-check each documented key against the assertions in `snapshotReportsThroughputAndCacheSectionsAtSchemaTwoOne`.

- [ ] **Step 3: Commit**

```bash
git add ../../docs/recommendation_flows/9_Track_Metrics.md
git commit -m "docs: document the throughput and cache measurement sections"
```

---

## Verification

- [ ] `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test` passes from `recsys-pipeline/services/java-retrieval-service`.
- [ ] `python3 -m pytest recsys-pipeline/integration-tests/python_modeling -q` passes from the repository root.
- [ ] `GET /metrics` returns `measurements.schemaVersion == "2.1"` with non-empty `throughput` and `cache` sections.
- [ ] `git diff master --stat` shows no change under `services/python-modeling/` or `frontend/`, confirming the dashboard contract is untouched.
