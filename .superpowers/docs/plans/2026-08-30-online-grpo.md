# Online GRPO Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Train a GRPO policy continuously in Scala off the live slate stream, and serve its score behind a shadow flag.

**Architecture:** The Java serving path starts publishing its own impression events to Kafka, carrying the serving `requestId`, a per-candidate `prediction_score` (the behavior policy π_old) and a versioned feature vector. Those flow through the existing joiner and experience collector, which already group them into slates. A new Spark Structured Streaming job treats each slate as a GRPO group, computes group-relative advantages, applies a clipped surrogate gradient to a linear-softmax policy, and writes the weights to Redis. A new Java scorer reads those weights and publishes `grpoScore`, weighted 0.0 until an operator flips the mode.

**Tech Stack:** Java 17 / Spring Boot (retrieval service, Maven, JUnit 5 + Mockito), Scala 2.12.18 / Spark 3.5.1 (streaming job, sbt, ScalaTest 3.2.18), Jedis 5.1.5, Kafka, Redis, Python 3 (OPE report only).

**Spec:** `.superpowers/docs/specs/2026-08-30-online-grpo-design.md`

## Global Constraints

- **sbt tests require JDK 17.** The default JDK 25 aborts every Spark-session test with a misleading `getSubject` error. Run `JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test` from `recsys-pipeline/services/spark-streaming-job`.
- **Scala 2.12.18, Spark 3.5.1** — both `Provided` at compile time. No new library dependencies: the policy gradient is analytic, so no Breeze, no DL4J, no autodiff.
- **Java tests** run with `mvn -q test` from `recsys-pipeline/services/java-retrieval-service`. JUnit 5 (`org.junit.jupiter`) with Mockito; follow `MovieLensServingSideEffectsTest` for the Redis-mock pattern.
- **Every new switch defaults to off.** `RECSYS_GRPO_EMIT_EVENTS=false`, `RECSYS_GRPO_MODE=off`. Nothing in this plan changes serving behavior until an operator sets a variable.
- **No existing exploitation weight may change.** `MovieLensOutcomeScorer` documents that its weights sum to 0.85, not 1.0, and that the remainder is deliberately not renormalized. `grpoScore` claims part of the unclaimed 0.15; it never redistributes an existing weight.
- **The feature vector is versioned on the wire** (`v1:` prefix). A consumer reading an unknown version drops the row and counts it — never silently misaligns weights against features.
- **Feature dimension is 10**, fixed by `GrpoFeatures.DIM`, and both the Java producer and the Scala consumer read that one constant's value.

---

### Task 1: GrpoFeatures — the versioned feature vector

The one definition of what `x` is, shared by the emitter (Task 3) and the scorer (Task 9). Pure, no Spring, no Redis.

The spec's prose named `weightedOutcome`, `qValue` and `posteriorMean` among the features. Those are not fields on `ServedMovie` — they live inside its untyped `modelPredictions` map. This task uses the typed record fields instead, which is the same information reached without depending on map keys. `predictionScore` still comes from the map, because that is the only place it exists.

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoFeatures.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoFeaturesTest.java`

**Interfaces:**
- Consumes: `MovieLensServingSideEffects.ServedMovie` (record: `movieId`, `estimatedReward`, `onlineScore`, `explorationBonus`, `banditScore`, `coldStart`, `impressions`, `clicks`, `modelPredictions`).
- Produces:
  - `public static final int DIM = 10`
  - `public static final String VERSION = "v1"`
  - `public static double[] of(ServedMovie movie, int position, int slateSize)`
  - `public static String pack(double[] x)` → `"v1:0.1,0.2,..."`
  - `public static double predictionScore(ServedMovie movie)`

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoFeaturesTest {

    private ServedMovie movie(double banditScore, long impressions, long clicks, Map<String, Object> predictions) {
        return new ServedMovie("m1", 0.4, 0.3, 0.05, banditScore, false, impressions, clicks, predictions);
    }

    @Test
    void producesExactlyDimValues() {
        double[] x = GrpoFeatures.of(movie(0.7, 10, 2, Map.of()), 0, 5);
        assertEquals(GrpoFeatures.DIM, x.length);
    }

    @Test
    void firstDimensionIsTheBiasTerm() {
        double[] x = GrpoFeatures.of(movie(0.7, 10, 2, Map.of()), 3, 5);
        assertEquals(1.0, x[0]);
    }

    @Test
    void countsEnterLogarithmicallySoAPopularItemDoesNotDominate() {
        double[] few = GrpoFeatures.of(movie(0.5, 10, 0, Map.of()), 0, 5);
        double[] many = GrpoFeatures.of(movie(0.5, 100_000, 0, Map.of()), 0, 5);
        // log1p keeps a 10,000x impression gap inside one order of magnitude.
        assertTrue(many[6] < 4.0 * few[6], "impressions must be compressed, got " + many[6] + " vs " + few[6]);
    }

    @Test
    void positionIsNormalizedBySlateSize() {
        double[] x = GrpoFeatures.of(movie(0.5, 0, 0, Map.of()), 2, 4);
        assertEquals(0.5, x[8]);
    }

    @Test
    void aSingletonSlateDoesNotDivideByZero() {
        double[] x = GrpoFeatures.of(movie(0.5, 0, 0, Map.of()), 0, 0);
        assertEquals(0.0, x[8]);
    }

    @Test
    void packCarriesTheVersionPrefix() {
        String packed = GrpoFeatures.pack(new double[] {1.0, 0.5});
        assertTrue(packed.startsWith(GrpoFeatures.VERSION + ":"), packed);
        assertEquals("v1:1.0,0.5", packed);
    }

    @Test
    void predictionScoreComesFromTheModelPredictionsMap() {
        assertEquals(0.83, GrpoFeatures.predictionScore(movie(0.5, 0, 0, Map.of("predictionScore", 0.83))));
    }

    @Test
    void predictionScoreFallsBackToBanditScoreWhenAbsent() {
        // An absent key must not silently become 0.0: a zero logit is a real, wrong policy claim,
        // whereas banditScore is the score predictionScore is derived from.
        assertEquals(0.61, GrpoFeatures.predictionScore(movie(0.61, 0, 0, Map.of())));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoFeaturesTest test`
Expected: FAIL — `GrpoFeatures` does not exist (compilation error).

- [ ] **Step 3: Write minimal implementation**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;

import java.util.StringJoiner;

/**
 * The GRPO policy's feature vector — one definition, read by the serving-side emitter and by the
 * shadow scorer, so a weight vector fit in Scala is never applied to a different layout here.
 *
 * Every value is already computed during scoring; building this vector adds no model call to the
 * request path. The version prefix travels on the wire because weights and features must agree:
 * a consumer that reads an unknown version drops the row rather than misaligning them.
 */
public final class GrpoFeatures {

    public static final int DIM = 10;
    public static final String VERSION = "v1";

    private GrpoFeatures() {}

    public static double[] of(ServedMovie movie, int position, int slateSize) {
        return new double[] {
            1.0,                                              // 0 bias
            movie.banditScore(),                              // 1
            movie.estimatedReward(),                          // 2
            movie.onlineScore(),                              // 3
            movie.explorationBonus(),                         // 4
            movie.coldStart() ? 1.0 : 0.0,                    // 5
            Math.log1p(movie.impressions()),                  // 6
            Math.log1p(movie.clicks()),                       // 7
            slateSize <= 0 ? 0.0 : (double) position / slateSize, // 8
            movie.clicks() / (double) (movie.impressions() + 1) // 9 smoothed CTR
        };
    }

    public static String pack(double[] x) {
        StringJoiner joiner = new StringJoiner(",");
        for (double value : x) {
            joiner.add(Double.toString(value));
        }
        return VERSION + ":" + joiner;
    }

    /** The behavior policy's logit. Falls back to banditScore, which predictionScore derives from. */
    public static double predictionScore(ServedMovie movie) {
        Object raw = movie.modelPredictions().get("predictionScore");
        return raw instanceof Number number ? number.doubleValue() : movie.banditScore();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoFeaturesTest test`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoFeatures.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoFeaturesTest.java
git commit -m "feat: define the versioned GRPO policy feature vector"
```

---

### Task 2: GrpoImpressionEvents — build the impression events

Pure event construction, separated from the Kafka send so the schema contract is testable without a broker. The event shape must match what the Python producers emit, because `OnlineJoinerStreamingJob.parseEvents` and every downstream schema are built on it.

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoImpressionEvents.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoImpressionEventsTest.java`

**Interfaces:**
- Consumes: `GrpoFeatures.of`, `GrpoFeatures.pack`, `GrpoFeatures.predictionScore` (Task 1); `MovieLensServingSideEffects.ServingSideEffectRequest`.
- Produces: `public static List<Map<String, Object>> build(ServingSideEffectRequest request, long nowMs)`

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoImpressionEventsTest {

    private ServedMovie movie(String id, double banditScore) {
        return new ServedMovie(id, 0.4, 0.3, 0.05, banditScore, false, 10, 2,
            Map.of("predictionScore", banditScore));
    }

    private ServingSideEffectRequest request(List<ServedMovie> selected) {
        return new ServingSideEffectRequest(
            "req-1", "u1", "hybrid", Map.of(), selected, selected,
            List.of(), List.of(), 0L, 0L, selected.size(), 0.0, 0.0, 0.0);
    }

    @Test
    void emitsOneEventPerSelectedItem() {
        List<Map<String, Object>> events =
            GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7), movie("m2", 0.4))), 1000L);
        assertEquals(2, events.size());
    }

    @Test
    void carriesTheFieldsTheJoinerRequiresNonNull() {
        Map<String, Object> event = GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7))), 1000L).get(0);
        // OnlineJoinerStreamingJob.parseEvents gates on exactly these three.
        assertEquals("req-1", event.get("request_id"));
        assertEquals("u1", event.get("user_id"));
        assertEquals("m1", event.get("item_id"));
        assertEquals("impression", event.get("event_type"));
        assertEquals(1000L, event.get("timestamp_ms"));
        assertNotNull(event.get("event_id"));
    }

    @Test
    void positionMatchesSlateOrder() {
        List<Map<String, Object>> events =
            GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7), movie("m2", 0.4))), 1000L);
        assertEquals(0, events.get(0).get("position"));
        assertEquals(1, events.get(1).get("position"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void itemFeaturesCarryPredictionScoreAndPackedVector() {
        Map<String, Object> event = GrpoImpressionEvents.build(request(List.of(movie("m1", 0.73))), 1000L).get(0);
        Map<String, String> itemFeatures = (Map<String, String>) event.get("item_features");
        assertEquals("0.73", itemFeatures.get("prediction_score"));
        assertTrue(itemFeatures.get("grpo_x").startsWith("v1:"), itemFeatures.get("grpo_x"));
    }

    @Test
    void anEmptySlateEmitsNothing() {
        assertTrue(GrpoImpressionEvents.build(request(List.of()), 1000L).isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void everyEventInOneSlateSharesOneSessionId() {
        List<Map<String, Object>> events =
            GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7), movie("m2", 0.4))), 1000L);
        assertEquals(events.get(0).get("session_id"), events.get(1).get("session_id"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoImpressionEventsTest test`
Expected: FAIL — `GrpoImpressionEvents` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serving's own impression events, shaped exactly like the ones the Python producers emit.
 *
 * The shape is not a choice. OnlineJoinerStreamingJob.parseEvents gates on non-null request_id,
 * user_id and item_id, and every schema downstream of it is built on these field names. Two keys
 * are added inside item_features: prediction_score, the behavior policy's logit, and grpo_x, the
 * versioned feature vector. Nothing else about the event differs from a producer's.
 *
 * The requestId here is serving's own, which is the point: it is what makes the Kafka stream and
 * the Redis replay buffer share an id namespace for the first time.
 */
public final class GrpoImpressionEvents {

    private GrpoImpressionEvents() {}

    public static List<Map<String, Object>> build(ServingSideEffectRequest request, long nowMs) {
        List<ServedMovie> selected = request.selected();
        if (selected.isEmpty()) {
            return List.of();
        }
        String sessionId = "sess_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        List<Map<String, Object>> events = new ArrayList<>(selected.size());
        for (int position = 0; position < selected.size(); position++) {
            ServedMovie movie = selected.get(position);
            Map<String, String> itemFeatures = new LinkedHashMap<>();
            itemFeatures.put("prediction_score", Double.toString(GrpoFeatures.predictionScore(movie)));
            itemFeatures.put("grpo_x", GrpoFeatures.pack(GrpoFeatures.of(movie, position, selected.size())));

            Map<String, Object> event = new LinkedHashMap<>();
            event.put("event_id", UUID.randomUUID().toString());
            event.put("request_id", request.requestId());
            event.put("session_id", sessionId);
            event.put("user_id", request.userId());
            event.put("item_id", movie.movieId());
            event.put("event_type", "impression");
            event.put("timestamp_ms", nowMs);
            event.put("position", position);
            event.put("user_features", Map.of("algorithm", request.algorithm()));
            event.put("item_features", itemFeatures);
            event.put("context_features", Map.of());
            events.add(event);
        }
        return List.copyOf(events);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoImpressionEventsTest test`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoImpressionEvents.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoImpressionEventsTest.java
git commit -m "feat: build serving-side impression events carrying pi_old and features"
```

---

### Task 3: Wire the emitter into serving behind a flag

Adds the config property, the publisher, and the call site. After this task serving can publish to Kafka; the flag keeps it off.

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoImpressionPublisher.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java` (add the `Grpo` nested class and its getter, following the existing `Sequence` class)
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml` (add the `grpo:` block under `recsys:`)
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffects.java` (constructor gains a publisher; `recordServed` calls it)
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java:123` (construction site)
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoImpressionPublisherTest.java`

**Interfaces:**
- Consumes: `GrpoImpressionEvents.build` (Task 2).
- Produces:
  - `GrpoImpressionPublisher(ObjectMapper objectMapper, GrpoSender sender, boolean enabled)`
  - `public void publish(ServingSideEffectRequest request, long nowMs)`
  - `public interface GrpoSender { void send(String key, byte[] payload); }` — a seam so tests need no broker; production wires it to `new KafkaProducer(KafkaUtils.producerConfig(base))::send` against `KafkaTopics.BEHAVIOR_LOGS`.
  - `RecommendationProperties.Grpo` with `getMode()/setMode(String)` (default `"off"`) and `isEmitEvents()/setEmitEvents(boolean)` (default `false`), reached via `properties.getGrpo()`.

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoImpressionPublisherTest {

    private final List<String> sent = new ArrayList<>();
    private final GrpoImpressionPublisher.GrpoSender recorder =
        (key, payload) -> sent.add(new String(payload));

    private ServingSideEffectRequest request() {
        ServedMovie movie = new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2,
            Map.of("predictionScore", 0.7));
        return new ServingSideEffectRequest(
            "req-1", "u1", "hybrid", Map.of(), List.of(movie), List.of(movie),
            List.of(), List.of(), 0L, 0L, 1L, 0.0, 0.0, 0.0);
    }

    @Test
    void sendsNothingWhenDisabled() {
        new GrpoImpressionPublisher(new ObjectMapper(), recorder, false).publish(request(), 1000L);
        assertTrue(sent.isEmpty());
    }

    @Test
    void sendsOneMessagePerItemWhenEnabled() {
        new GrpoImpressionPublisher(new ObjectMapper(), recorder, true).publish(request(), 1000L);
        assertEquals(1, sent.size());
        assertTrue(sent.get(0).contains("\"item_id\":\"m1\""), sent.get(0));
        assertTrue(sent.get(0).contains("\"grpo_x\":\"v1:"), sent.get(0));
    }

    @Test
    void aSenderFailureNeverPropagates() {
        GrpoImpressionPublisher.GrpoSender broken = (key, payload) -> {
            throw new IllegalStateException("broker down");
        };
        // A Kafka outage must not fail a recommendation request. This is a logging side effect.
        new GrpoImpressionPublisher(new ObjectMapper(), broken, true).publish(request(), 1000L);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoImpressionPublisherTest test`
Expected: FAIL — `GrpoImpressionPublisher` does not exist.

- [ ] **Step 3: Write the publisher**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Publishes serving's impression events to Kafka, off by default.
 *
 * Every failure is swallowed and logged. This is a logging side effect on the serving path, not a
 * serving dependency: a broker outage must degrade training data, never a recommendation response.
 */
public class GrpoImpressionPublisher {

    private static final Logger log = LoggerFactory.getLogger(GrpoImpressionPublisher.class);

    /** Seam over the Kafka producer, so tests need no broker. */
    public interface GrpoSender {
        void send(String key, byte[] payload);
    }

    private final ObjectMapper objectMapper;
    private final GrpoSender sender;
    private final boolean enabled;

    public GrpoImpressionPublisher(ObjectMapper objectMapper, GrpoSender sender, boolean enabled) {
        this.objectMapper = objectMapper;
        this.sender = sender;
        this.enabled = enabled;
    }

    public void publish(ServingSideEffectRequest request, long nowMs) {
        if (!enabled || sender == null) {
            return;
        }
        try {
            for (Map<String, Object> event : GrpoImpressionEvents.build(request, nowMs)) {
                sender.send(request.requestId(),
                    objectMapper.writeValueAsBytes(event));
            }
        } catch (Exception e) {
            log.warn("Failed to publish GRPO impression events for request {}", request.requestId(), e);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoImpressionPublisherTest test`
Expected: PASS, 3 tests.

- [ ] **Step 5: Add the configuration property**

In `RecommendationProperties.java`, add alongside the existing `Sequence` nested class, plus a `private Grpo grpo = new Grpo();` field with `getGrpo()`/`setGrpo()`:

```java
    public static class Grpo {
        /** off | shadow | on — the GRPO policy rollout, mirroring the sequence switches. */
        private String mode = "off";
        /**
         * Whether serving publishes its own impression events to Kafka. Off by default: it is the
         * first time this service writes to the event stream, and it is a training-data feature,
         * not a serving one.
         */
        private boolean emitEvents = false;

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public boolean isEmitEvents() {
            return emitEvents;
        }

        public void setEmitEvents(boolean emitEvents) {
            this.emitEvents = emitEvents;
        }
    }
```

In `application.yml`, under `recsys:`, after the `sequence:` block:

```yaml
  grpo:
    # off = not computed; shadow = computed and logged at weight 0.0; on = enters the blend
    mode: ${RECSYS_GRPO_MODE:off}
    # Serving publishes its own impressions to behavior_logs, carrying pi_old and the feature
    # vector. This is what gives the Kafka stream and the Redis replay buffer one requestId
    # namespace; offline DPO cannot join without it.
    emit-events: ${RECSYS_GRPO_EMIT_EVENTS:false}
```

- [ ] **Step 6: Wire the call site**

In `MovieLensServingSideEffects`, add a `private final GrpoImpressionPublisher grpoPublisher;` field, accept it as a fourth constructor parameter, and call it at the end of `recordServed` — after the Redis pipeline, so a publish problem cannot affect the Redis write:

```java
        grpoPublisher.publish(request, now);
```

In `HybridRecommendationService.java:123`, build the publisher and pass it:

```java
        this.servingSideEffects = new MovieLensServingSideEffects(
            redis, objectMapper, properties.getReplayBuffer().getPendingTtl(),
            new GrpoImpressionPublisher(objectMapper, grpoSender, properties.getGrpo().isEmitEvents()));
```

where `grpoSender` sends to `KafkaTopics.BEHAVIOR_LOGS` through a `KafkaProducer` built with `KafkaUtils.producerConfig(...)`, or is `null` when `emit-events` is false so no producer connection is opened at all.

- [ ] **Step 7: Run the full Java suite**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q test`
Expected: PASS. `MovieLensServingSideEffectsTest` needs its constructor call updated to the new four-argument form; pass `new GrpoImpressionPublisher(new ObjectMapper(), null, false)`.

- [ ] **Step 8: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/ \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/ \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java \
        recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffects.java \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java
git commit -m "feat: serving publishes its own impressions to Kafka behind a flag"
```

---

### Task 4: GrpoMath — advantages, softmax, exact KL, clipped gradient

The whole learning rule, as pure functions over `Array[Double]`. No Spark, no Redis, no Kafka — this file is where the algorithm is right or wrong, and it must be testable in milliseconds.

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala`

**Interfaces:**
- Produces:
  - `def softmax(logits: Array[Double], temperature: Double): Array[Double]`
  - `def advantages(rewards: Array[Double]): Option[Array[Double]]` — `None` for a degenerate group
  - `def kl(p: Array[Double], q: Array[Double]): Double` — exact, over the enumerated slate
  - `def loss(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double], w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Double`
  - `def gradient(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double], w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double]`

The two references are **separate parameters and must stay separate**. The ratio is measured
against `snapshotLogits`; the KL is measured against `loggedLogits`. A single-reference signature
would make the spec's central correctness argument inexpressible, and a caller composing two
single-reference calls silently gets a spurious KL against the snapshot.
  - `final case class GrpoHyperParams(temperature: Double, clipEpsilon: Double, klBeta: Double, learningRate: Double, innerEpochs: Int)`
  - `val AdvantageFloor: Double = 1e-8`

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoMathSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoHyperParams(
    temperature = 1.0, clipEpsilon = 0.2, klBeta = 0.02, learningRate = 0.01, innerEpochs = 4)

  "softmax" should "produce a distribution summing to one" in {
    GrpoMath.softmax(Array(1.0, 2.0, 3.0), 1.0).sum shouldBe 1.0 +- 1e-12
  }

  it should "not overflow on large logits" in {
    // Naive exp() overflows here; the max must be subtracted first.
    val p = GrpoMath.softmax(Array(1000.0, 1001.0), 1.0)
    p.forall(v => !v.isNaN) shouldBe true
    p.sum shouldBe 1.0 +- 1e-12
  }

  "advantages" should "centre on the group mean and scale by its deviation" in {
    val adv = GrpoMath.advantages(Array(1.0, 0.0, 0.0, 0.0)).get
    adv.sum shouldBe 0.0 +- 1e-9
    adv(0) should be > 0.0
    adv(1) should be < 0.0
  }

  it should "reject a group whose rewards are all identical" in {
    // The no-click slate: zero variance, undefined advantage. Dividing by a floor would not
    // produce a small gradient, it would produce noise amplified by 1/floor.
    GrpoMath.advantages(Array(0.0, 0.0, 0.0)) shouldBe None
  }

  it should "reject a group smaller than two" in {
    GrpoMath.advantages(Array(1.0)) shouldBe None
    GrpoMath.advantages(Array.empty[Double]) shouldBe None
  }

  "kl" should "be zero for identical distributions" in {
    val p = GrpoMath.softmax(Array(0.5, 1.5, 2.0), 1.0)
    GrpoMath.kl(p, p) shouldBe 0.0 +- 1e-12
  }

  it should "be positive and finite for differing distributions" in {
    val p = GrpoMath.softmax(Array(0.0, 3.0), 1.0)
    val q = GrpoMath.softmax(Array(3.0, 0.0), 1.0)
    GrpoMath.kl(p, q) should be > 0.0
    GrpoMath.kl(p, q).isInfinite shouldBe false
  }

  "gradient" should "match a finite-difference approximation of the loss" in {
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9), Array(1.0, 0.4))
    val logged = Array(0.3, 0.6, 0.1)
    val w = Array(0.05, -0.3)
    val adv = GrpoMath.advantages(Array(1.0, 0.0, 0.0)).get

    val snapshot = Array(0.2, 0.5, 0.1)
    val analytic = GrpoMath.gradient(x, snapshot, logged, w, adv, cfg)
    val h = 1e-6
    val numeric = w.indices.map { i =>
      val up = w.clone(); up(i) += h
      val down = w.clone(); down(i) -= h
      (GrpoMath.loss(x, snapshot, logged, up, adv, cfg) -
        GrpoMath.loss(x, snapshot, logged, down, adv, cfg)) / (2 * h)
    }.toArray

    analytic.zip(numeric).foreach { case (a, n) => a shouldBe n +- 1e-4 }
  }

  it should "be inert when the policy equals the snapshot and the advantage is zero" in {
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9))
    val g = GrpoMath.gradient(x, Array(0.1, 0.1), Array(0.1, 0.1), Array(0.0, 0.0), Array(0.0, 0.0), cfg)
    g.foreach(_ shouldBe 0.0 +- 1e-12)
  }

  it should "keep the ratio and the KL on separate references" in {
    // The whole shadow-mode correctness argument: moving the KL anchor alone must change the
    // gradient. If it does not, the two references have been collapsed into one somewhere.
    val x = Array(Array(1.0, 0.2), Array(1.0, 0.9))
    val w = Array(0.05, -0.3)
    val adv = GrpoMath.advantages(Array(1.0, 0.0)).get
    val snapshot = Array(0.3, 0.3)
    val a = GrpoMath.gradient(x, snapshot, Array(0.3, 0.3), w, adv, cfg)
    val b = GrpoMath.gradient(x, snapshot, Array(2.0, -1.0), w, adv, cfg)
    a.toSeq should not be b.toSeq
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoMathSpec"`
Expected: FAIL — `GrpoMath` is not a member of `com.demo.grpo`.

- [ ] **Step 3: Write the implementation**

> **The committed source is authoritative, not this block.**
> The `surrogateScale` line below was wrong when this plan was written: it read
> `-adv(i) * ratio / pi.length`, which carries a spurious extra factor of `pi_i` because the
> accumulator multiplies by `pi(i) / temperature` further down. The finite-difference test in
> Step 1 caught it during implementation (0.0469 computed against 0.1301 correct — a 3x error
> that would have trained a subtly wrong objective while every metric looked healthy), and a
> reviewer re-derived the correction independently. It is fixed inline below, but read
> `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala`
> rather than transcribing from here.

```scala
package com.demo.grpo

/** Hyperparameters of the surrogate objective. See GrpoJobConfig for how they are read. */
final case class GrpoHyperParams(
    temperature: Double,
    clipEpsilon: Double,
    klBeta: Double,
    learningRate: Double,
    innerEpochs: Int)

/** The GRPO learning rule as pure functions.
  *
  * The action space of a group is the slate: finite, small, and fully enumerated in the logged
  * event. So the softmax partition function is computable and the KL term is EXACT, rather than
  * the k3 estimator language-model implementations are forced into.
  *
  * The policy is linear in the features, so the gradient is analytic and no autodiff library is
  * needed:  d log pi_i / dw = x_i - sum_j pi_j x_j.
  */
object GrpoMath {

  /** Guards the advantage denominator. Deliberately not configurable: a group that needs a larger
    * floor is a degenerate group, which `advantages` rejects outright instead. */
  val AdvantageFloor: Double = 1e-8

  def softmax(logits: Array[Double], temperature: Double): Array[Double] = {
    val scaled = logits.map(_ / temperature)
    val max = scaled.max                 // subtract before exp, or large logits overflow
    val exp = scaled.map(v => math.exp(v - max))
    val total = exp.sum
    exp.map(_ / total)
  }

  /** Group-relative advantage, or None when the group cannot produce one.
    *
    * A slate with fewer than two items has no group to be relative to. A slate whose rewards are
    * all identical -- the ordinary no-click case -- has zero variance, and normalizing it would
    * amplify floating-point noise by 1/AdvantageFloor rather than yield a small gradient.
    */
  def advantages(rewards: Array[Double]): Option[Array[Double]] = {
    if (rewards.length < 2) return None
    val mean = rewards.sum / rewards.length
    val variance = rewards.map(r => (r - mean) * (r - mean)).sum / rewards.length
    val std = math.sqrt(variance)
    if (std < AdvantageFloor) None
    else Some(rewards.map(r => (r - mean) / std))
  }

  /** Exact KL(p || q) over the enumerated slate. */
  def kl(p: Array[Double], q: Array[Double]): Double =
    p.indices.foldLeft(0.0) { (acc, i) =>
      if (p(i) <= 0.0) acc
      else acc + p(i) * math.log(p(i) / math.max(q(i), AdvantageFloor))
    }

  private def logits(x: Array[Array[Double]], w: Array[Double]): Array[Double] =
    x.map(row => row.indices.foldLeft(0.0)((acc, i) => acc + row(i) * w(i)))

  /** The clipped surrogate plus the KL penalty, averaged over the group.
    *
    * TWO references, deliberately not one:
    *
    *   `snapshotLogits` — the policy frozen at the start of the micro-batch. The ratio is measured
    *     against this, so inner epochs see a ratio that departs from 1 and clipping engages.
    *   `loggedLogits`   — the behavior policy that actually served the slate. The KL anchors here,
    *     bounding how far the policy drifts from what is live.
    *
    * Collapsing them would be a silent failure in shadow mode, where serving never changes: the
    * ratio would grow without bound, clipping would latch permanently active, and the gradient
    * would go to zero while every batch still looked healthy.
    */
  def loss(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
           w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Double = {
    val piSnap = softmax(snapshotLogits, cfg.temperature)
    val piOld = softmax(loggedLogits, cfg.temperature)
    val pi = softmax(logits(x, w), cfg.temperature)
    val surrogate = pi.indices.map { i =>
      val ratio = pi(i) / math.max(piSnap(i), AdvantageFloor)
      val clipped = math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio))
      math.min(ratio * adv(i), clipped * adv(i))
    }.sum / pi.length
    -surrogate + cfg.klBeta * kl(pi, piOld)
  }

  /** Analytic gradient of `loss` with respect to w. */
  def gradient(x: Array[Array[Double]], snapshotLogits: Array[Double], loggedLogits: Array[Double],
               w: Array[Double], adv: Array[Double], cfg: GrpoHyperParams): Array[Double] = {
    val dim = w.length
    val piSnap = softmax(snapshotLogits, cfg.temperature)
    val piOld = softmax(loggedLogits, cfg.temperature)
    val pi = softmax(logits(x, w), cfg.temperature)

    // Expected feature vector under pi -- the term that makes d log pi_i / dw a centred difference.
    val expected = Array.fill(dim)(0.0)
    pi.indices.foreach(i => (0 until dim).foreach(d => expected(d) += pi(i) * x(i)(d)))

    val grad = Array.fill(dim)(0.0)
    pi.indices.foreach { i =>
      val ratio = pi(i) / math.max(piSnap(i), AdvantageFloor)   // ratio: snapshot reference
      val clippedActive =
        ratio < 1.0 - cfg.clipEpsilon || ratio > 1.0 + cfg.clipEpsilon
      // Outside the clip range the surrogate is flat in w, so it contributes no gradient --
      // unless the unclipped branch is the smaller one, which is when min() selects it.
      val unclippedSelected = !clippedActive ||
        (ratio * adv(i)) < (math.max(1.0 - cfg.clipEpsilon, math.min(1.0 + cfg.clipEpsilon, ratio)) * adv(i))
      // d(-1/N * ratio_i * adv_i)/dpi_i = -(1/N) * adv_i / piSnap_i -- NOT -(1/N) * adv_i * ratio_i,
      // which would carry a spurious extra factor of pi_i once multiplied through below.
      val surrogateScale =
        if (unclippedSelected) -adv(i) / (math.max(piSnap(i), AdvantageFloor) * pi.length) else 0.0
      val klScale = cfg.klBeta * (math.log(math.max(pi(i), AdvantageFloor) /
        math.max(piOld(i), AdvantageFloor)) + 1.0)
      val scale = (surrogateScale + klScale) * pi(i) / cfg.temperature
      (0 until dim).foreach(d => grad(d) += scale * (x(i)(d) - expected(d)))
    }
    grad
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoMathSpec"`
Expected: PASS, 10 tests. The finite-difference check is the one that matters: if it fails, the analytic gradient is wrong and every later task is building on sand.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoMath.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoMathSpec.scala
git commit -m "feat: GRPO learning rule as pure functions with an analytic gradient"
```

---

### Task 5: GrpoJobConfig — env-driven hyperparameters

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoJobConfig.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoJobConfigSpec.scala`

**Interfaces:**
- Consumes: `GrpoHyperParams` (Task 4).
- Produces:
  - `final case class GrpoJobConfig(hyper: GrpoHyperParams, featureVersion: String, dim: Int, redisHost: String, redisPort: Int, weightsKey: String)`
  - `object GrpoJobConfig { def from(env: Map[String, String]): GrpoJobConfig; def fromEnv(): GrpoJobConfig }`

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoJobConfigSpec extends AnyFlatSpec with Matchers {

  "from" should "supply the documented defaults on an empty environment" in {
    val cfg = GrpoJobConfig.from(Map.empty)
    cfg.hyper.temperature shouldBe 1.0
    cfg.hyper.clipEpsilon shouldBe 0.2
    cfg.hyper.klBeta shouldBe 0.02
    cfg.hyper.learningRate shouldBe 0.01
    cfg.hyper.innerEpochs shouldBe 4
    cfg.featureVersion shouldBe "v1"
    cfg.dim shouldBe 10
    cfg.weightsKey shouldBe "grpo:policy:weights"
  }

  it should "read every hyperparameter from the environment" in {
    val cfg = GrpoJobConfig.from(Map(
      "GRPO_TEMPERATURE" -> "0.5", "GRPO_CLIP_EPSILON" -> "0.1",
      "GRPO_KL_BETA" -> "0.05", "GRPO_LEARNING_RATE" -> "0.003",
      "GRPO_INNER_EPOCHS" -> "8"))
    cfg.hyper.temperature shouldBe 0.5
    cfg.hyper.innerEpochs shouldBe 8
  }

  it should "fall back to the default on an unparseable value" in {
    // Mirrors SequenceJobConfig: a typo must not silently change the objective.
    GrpoJobConfig.from(Map("GRPO_KL_BETA" -> "yes")).hyper.klBeta shouldBe 0.02
  }

  it should "force inner epochs above one" in {
    // At one step per batch pi still equals the snapshot when the ratio is formed, so r = 1
    // everywhere and the clip branch is unreachable. Clipping would be decorative.
    GrpoJobConfig.from(Map("GRPO_INNER_EPOCHS" -> "1")).hyper.innerEpochs shouldBe 2
    GrpoJobConfig.from(Map("GRPO_INNER_EPOCHS" -> "0")).hyper.innerEpochs shouldBe 2
  }

  it should "reject a non-positive temperature, which would divide by zero" in {
    GrpoJobConfig.from(Map("GRPO_TEMPERATURE" -> "0")).hyper.temperature shouldBe 1.0
    GrpoJobConfig.from(Map("GRPO_TEMPERATURE" -> "-1")).hyper.temperature shouldBe 1.0
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoJobConfigSpec"`
Expected: FAIL — `GrpoJobConfig` not found.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.grpo

/** Job knobs, read once at start.
  *
  * Follows SequenceJobConfig: only an explicitly parseable value counts, and anything else falls
  * back to the default. A typo in a hyperparameter must not silently change the objective.
  */
final case class GrpoJobConfig(
    hyper: GrpoHyperParams,
    featureVersion: String,
    dim: Int,
    redisHost: String,
    redisPort: Int,
    weightsKey: String)

object GrpoJobConfig {

  /** Must match GrpoFeatures.DIM and GrpoFeatures.VERSION on the Java side. */
  val FeatureVersion = "v1"
  val Dim = 10
  val WeightsKey = "grpo:policy:weights"

  private def doubleFrom(env: Map[String, String], key: String, default: Double): Double =
    env.get(key).flatMap(v => try Some(v.toDouble) catch { case _: NumberFormatException => None })
      .getOrElse(default)

  private def intFrom(env: Map[String, String], key: String, default: Int): Int =
    env.get(key).flatMap(v => try Some(v.toInt) catch { case _: NumberFormatException => None })
      .getOrElse(default)

  def from(env: Map[String, String]): GrpoJobConfig = {
    val temperature = doubleFrom(env, "GRPO_TEMPERATURE", 1.0)
    GrpoJobConfig(
      hyper = GrpoHyperParams(
        temperature  = if (temperature > 0.0) temperature else 1.0,
        clipEpsilon  = doubleFrom(env, "GRPO_CLIP_EPSILON", 0.2),
        klBeta       = doubleFrom(env, "GRPO_KL_BETA", 0.02),
        learningRate = doubleFrom(env, "GRPO_LEARNING_RATE", 0.01),
        // Below two, the ratio is identically 1 on every step and clipping never engages.
        innerEpochs  = math.max(2, intFrom(env, "GRPO_INNER_EPOCHS", 4))),
      featureVersion = FeatureVersion,
      dim            = Dim,
      redisHost      = env.getOrElse("REDIS_HOST", "localhost"),
      redisPort      = intFrom(env, "REDIS_PORT", 6379),
      weightsKey     = WeightsKey)
  }

  def fromEnv(): GrpoJobConfig = from(sys.env)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoJobConfigSpec"`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoJobConfig.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoJobConfigSpec.scala
git commit -m "feat: GRPO job configuration with defaults that keep clipping meaningful"
```

---

### Task 6: GrpoSlates — parse and gate the slate stream

Turns a `training_experiences` micro-batch into typed groups, dropping the ones GRPO cannot learn from and counting why.

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoSlatesSpec.scala`

**Interfaces:**
- Consumes: `RecommendationResponseStatsJob.SlateSchema` (existing, the shape `ExperienceCollectorStreamingJob` publishes); `GrpoMath.advantages` (Task 4); `GrpoJobConfig` (Task 5).
- Produces:
  - `final case class GrpoGroup(slateId: String, x: Array[Array[Double]], logged: Array[Double], rewards: Array[Double])`
  - `final case class GateCounts(kept: Long, tooSmall: Long, zeroVariance: Long, badFeatureVersion: Long)`
  - `def parseFeatureVector(packed: String, expectedVersion: String, dim: Int): Option[Array[Double]]`
  - `def toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts)`

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.grpo

import com.demo.SparkTestSupport
import com.demo.process.RecommendationResponseStatsJob
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoSlatesSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val cfg = GrpoJobConfig.from(Map.empty)
  private def vec(v: Double): String = "v1:" + Array.fill(10)(v).mkString(",")

  "parseFeatureVector" should "accept a correctly versioned vector of the right width" in {
    GrpoSlates.parseFeatureVector(vec(0.5), "v1", 10).map(_.length) shouldBe Some(10)
  }

  it should "reject an unknown version rather than misalign weights against features" in {
    GrpoSlates.parseFeatureVector("v2:" + Array.fill(10)(0.5).mkString(","), "v1", 10) shouldBe None
  }

  it should "reject a vector of the wrong width" in {
    GrpoSlates.parseFeatureVector("v1:0.5,0.5", "v1", 10) shouldBe None
  }

  it should "reject an unparseable vector" in {
    GrpoSlates.parseFeatureVector("v1:a,b,c", "v1", 10) shouldBe None
  }

  "toGroups" should "keep a slate with reward variance and count it" in {
    val (groups, counts) = GrpoSlates.toGroups(slateFrame(Seq(("m1", 1.0), ("m2", 0.0))), cfg)
    groups should have size 1
    groups.head.rewards shouldBe Array(1.0, 0.0)
    counts.kept shouldBe 1L
  }

  it should "drop a slate where every reward is identical" in {
    val (groups, counts) = GrpoSlates.toGroups(slateFrame(Seq(("m1", 0.0), ("m2", 0.0))), cfg)
    groups shouldBe empty
    counts.zeroVariance shouldBe 1L
  }

  it should "drop a single-item slate" in {
    val (groups, counts) = GrpoSlates.toGroups(slateFrame(Seq(("m1", 1.0))), cfg)
    groups shouldBe empty
    counts.tooSmall shouldBe 1L
  }

  it should "drop a slate whose feature version it does not recognise" in {
    val (groups, counts) = GrpoSlates.toGroups(
      slateFrame(Seq(("m1", 1.0), ("m2", 0.0)), featureVersion = "v9"), cfg)
    groups shouldBe empty
    counts.badFeatureVersion shouldBe 1L
  }

  /** One slate carrying the given (item, label) pairs, shaped like ExperienceCollector publishes. */
  private def slateFrame(items: Seq[(String, Double)], featureVersion: String = "v1") = {
    val s = spark
    import s.implicits._
    val rows = items.zipWithIndex.map { case ((item, label), position) =>
      (position, item, if (label > 0) 1 else 0, 0, label,
        Map("grpo_x" -> (featureVersion + ":" + Array.fill(10)(0.5).mkString(",")),
            "prediction_score" -> "0.4"))
    }
    Seq(("req-1:u1", "req-1", "u1", 1000L, items.map(_._2).sum, items.size, rows))
      .toDF("slate_id", "request_id", "user_id", "request_ts", "slate_reward", "slate_size", "items")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoSlatesSpec"`
Expected: FAIL — `GrpoSlates` not found.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.grpo

import org.apache.spark.sql.{DataFrame, Row}

/** One GRPO group: the candidates of a single slate, with their features, logged logits, rewards. */
final case class GrpoGroup(
    slateId: String,
    x: Array[Array[Double]],
    logged: Array[Double],
    rewards: Array[Double])

/** Why slates were kept or dropped in one micro-batch.
  *
  * At low click-through almost every slate is zero-variance, and that is the expected steady
  * state, not a fault. The surviving fraction has to be visible or an operator cannot tell
  * "learning from few slates" from "learning from none".
  */
final case class GateCounts(kept: Long, tooSmall: Long, zeroVariance: Long, badFeatureVersion: Long) {
  def total: Long = kept + tooSmall + zeroVariance + badFeatureVersion
  def reasons: Seq[(String, Long)] =
    Seq("slate_too_small" -> tooSmall, "zero_reward_variance" -> zeroVariance,
      "bad_feature_version" -> badFeatureVersion)
}

object GrpoSlates {

  /** Parse a packed feature vector, or None if it cannot be trusted to align with the weights. */
  def parseFeatureVector(packed: String, expectedVersion: String, dim: Int): Option[Array[Double]] = {
    if (packed == null) return None
    val separator = packed.indexOf(':')
    if (separator < 0) return None
    if (packed.substring(0, separator) != expectedVersion) return None
    val parts = packed.substring(separator + 1).split(",")
    if (parts.length != dim) return None
    try Some(parts.map(_.toDouble)) catch { case _: NumberFormatException => None }
  }

  def toGroups(slates: DataFrame, cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts) = {
    var tooSmall = 0L
    var zeroVariance = 0L
    var badVersion = 0L
    val kept = scala.collection.mutable.ArrayBuffer.empty[GrpoGroup]

    slates.select("slate_id", "items").collect().foreach { row =>
      val slateId = row.getString(0)
      val items = row.getSeq[Row](1)
      if (items.size < 2) {
        tooSmall += 1L
      } else {
        val parsed = items.map { item =>
          val features = item.getAs[Map[String, String]]("item_features")
          val x = parseFeatureVector(features.getOrElse("grpo_x", null), cfg.featureVersion, cfg.dim)
          val logged = try features.getOrElse("prediction_score", "0.0").toDouble
                       catch { case _: NumberFormatException => 0.0 }
          val label = if (item.isNullAt(item.fieldIndex("label"))) 0.0
                      else item.getAs[Double]("label")
          (x, logged, label)
        }
        if (parsed.exists(_._1.isEmpty)) {
          badVersion += 1L
        } else {
          val rewards = parsed.map(_._3).toArray
          GrpoMath.advantages(rewards) match {
            case None => zeroVariance += 1L
            case Some(_) =>
              kept += GrpoGroup(slateId, parsed.map(_._1.get).toArray, parsed.map(_._2).toArray, rewards)
          }
        }
      }
    }
    (kept.toSeq, GateCounts(kept.size.toLong, tooSmall, zeroVariance, badVersion))
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoSlatesSpec"`
Expected: PASS, 8 tests.

- [ ] **Step 5: Add the cross-language contract test**

This is the guard against the synthetic-producer world and the serving world silently diverging
again. It asserts that what `GrpoImpressionEvents` (Task 2) emits parses under the schema the
joiner and collector actually read. The JSON literal below is copied from that class's output —
if the two ever drift apart, this test is what notices.

Append to `GrpoSlatesSpec.scala`:

```scala
  "the Java emitter's event shape" should "parse under TrainingSampleSchema with the gated fields set" in {
    val s = spark; import s.implicits._
    // Verbatim shape of one GrpoImpressionEvents.build(...) element.
    val emitted =
      """{"event_id":"e1","request_id":"req-1","session_id":"sess_abcd1234","user_id":"u1",
        |"item_id":"m1","event_type":"impression","timestamp_ms":1000,"position":0,
        |"user_features":{"algorithm":"hybrid"},
        |"item_features":{"prediction_score":"0.73","grpo_x":"v1:1.0,0.7,0.4,0.3,0.05,0.0,2.4,1.1,0.0,0.18"},
        |"context_features":{}}""".stripMargin.replaceAll("\n", "")

    val parsed = spark.read.schema(com.demo.process.OnlineJoinerStreamingJob.EventSchema)
      .json(Seq(emitted).toDS())
    val row = parsed.collect().head

    // OnlineJoinerStreamingJob.parseEvents drops rows with any of these null.
    row.getAs[String]("request_id") shouldBe "req-1"
    row.getAs[String]("user_id") shouldBe "u1"
    row.getAs[String]("item_id") shouldBe "m1"
    // And the two keys GRPO depends on survived the round trip.
    val itemFeatures = row.getAs[Map[String, String]]("item_features")
    itemFeatures("prediction_score") shouldBe "0.73"
    GrpoSlates.parseFeatureVector(itemFeatures("grpo_x"), "v1", 10).map(_.length) shouldBe Some(10)
  }
```

If `OnlineJoinerStreamingJob` exposes its event schema under a different name than `EventSchema`,
use that name — read the object before writing this test rather than guessing.

- [ ] **Step 6: Run the contract test**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoSlatesSpec"`
Expected: PASS, 9 tests.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoSlates.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoSlatesSpec.scala
git commit -m "feat: parse slates into GRPO groups, dropping ones with no learnable signal"
```

---

### Task 7: GrpoWeightStore — Redis persistence with a feature-version guard

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoWeightStore.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoWeightStoreSpec.scala`

**Interfaces:**
- Consumes: `GrpoJobConfig` (Task 5).
- Produces:
  - `final case class GrpoWeights(weights: Array[Double], featureVersion: String, batchId: Long, slatesApplied: Long)`
  - `def encode(w: GrpoWeights, nowMs: Long): java.util.Map[String, String]`
  - `def decode(fields: Map[String, String], cfg: GrpoJobConfig): Either[String, GrpoWeights]`
  - `def initial(cfg: GrpoJobConfig): GrpoWeights` — a zero vector

Redis I/O itself goes through `RedisPool.get(host, port, maxTotal)` in the job (Task 8); this task keeps encode/decode pure so the version guard is testable without a server.

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoWeightStoreSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoJobConfig.from(Map.empty)

  "initial" should "be a zero vector of the configured width" in {
    val w = GrpoWeightStore.initial(cfg)
    w.weights.length shouldBe cfg.dim
    w.weights.forall(_ == 0.0) shouldBe true
    w.batchId shouldBe -1L
  }

  "encode then decode" should "round-trip the weights exactly" in {
    val original = GrpoWeights(Array(0.1, -0.2, 0.3, 0.0, 1.5, -0.7, 0.05, 0.0, 2.0, -1.0), "v1", 42L, 900L)
    val fields = GrpoWeightStore.encode(original, 1000L)
    val decoded = GrpoWeightStore.decode(
      scala.jdk.CollectionConverters.MapHasAsScala(fields).asScala.toMap, cfg)
    decoded.map(_.weights.toSeq) shouldBe Right(original.weights.toSeq)
    decoded.map(_.batchId) shouldBe Right(42L)
    decoded.map(_.slatesApplied) shouldBe Right(900L)
  }

  "decode" should "refuse weights fit against a different feature version" in {
    // Applying v1 weights to a v2 feature layout is silent, total nonsense. Refuse loudly.
    val fields = Map("weights" -> Array.fill(10)(0.1).mkString(","),
      "feature_version" -> "v0", "dim" -> "10", "batch_id" -> "1", "slates_applied" -> "1")
    GrpoWeightStore.decode(fields, cfg).left.map(_.contains("feature_version")) shouldBe Left(true)
  }

  it should "refuse a weight vector of the wrong width" in {
    val fields = Map("weights" -> "0.1,0.2", "feature_version" -> "v1", "dim" -> "2",
      "batch_id" -> "1", "slates_applied" -> "1")
    GrpoWeightStore.decode(fields, cfg).isLeft shouldBe true
  }

  it should "treat an empty hash as a cold start rather than an error" in {
    GrpoWeightStore.decode(Map.empty, cfg).map(_.weights.forall(_ == 0.0)) shouldBe Right(true)
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoWeightStoreSpec"`
Expected: FAIL — `GrpoWeightStore` not found.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.grpo

import scala.jdk.CollectionConverters._

final case class GrpoWeights(
    weights: Array[Double],
    featureVersion: String,
    batchId: Long,
    slatesApplied: Long)

/** The policy's durable form.
  *
  * A restart resumes training rather than resetting the policy, so the weights live in Redis with
  * no TTL. The feature version travels with them: weights fit against one feature layout applied
  * to another produce a plausible-looking score that means nothing, and nothing downstream could
  * detect it. Decode refuses instead.
  */
object GrpoWeightStore {

  def initial(cfg: GrpoJobConfig): GrpoWeights =
    GrpoWeights(Array.fill(cfg.dim)(0.0), cfg.featureVersion, -1L, 0L)

  def encode(w: GrpoWeights, nowMs: Long): java.util.Map[String, String] =
    Map(
      "weights" -> w.weights.mkString(","),
      "dim" -> w.weights.length.toString,
      "feature_version" -> w.featureVersion,
      "updated_at" -> nowMs.toString,
      "batch_id" -> w.batchId.toString,
      "slates_applied" -> w.slatesApplied.toString
    ).asJava

  def decode(fields: Map[String, String], cfg: GrpoJobConfig): Either[String, GrpoWeights] = {
    if (fields.isEmpty) return Right(initial(cfg))
    val version = fields.getOrElse("feature_version", "")
    if (version != cfg.featureVersion)
      return Left(s"stored feature_version '$version' does not match job's '${cfg.featureVersion}'")
    val parts = fields.getOrElse("weights", "").split(",").filter(_.nonEmpty)
    if (parts.length != cfg.dim)
      return Left(s"stored weight width ${parts.length} does not match dim ${cfg.dim}")
    try Right(GrpoWeights(
      parts.map(_.toDouble), version,
      fields.getOrElse("batch_id", "-1").toLong,
      fields.getOrElse("slates_applied", "0").toLong))
    catch { case e: NumberFormatException => Left(s"unparseable stored weights: ${e.getMessage}") }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoWeightStoreSpec"`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoWeightStore.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoWeightStoreSpec.scala
git commit -m "feat: persist GRPO weights with a feature-version guard"
```

---

### Task 8: GrpoPolicyStreamingJob — assemble the loop

The update rule with inner epochs against a per-batch snapshot, plus the streaming `main`.

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala`

**Interfaces:**
- Consumes: `GrpoMath.gradient` (Task 4), `GrpoJobConfig` (Task 5), `GrpoSlates.toGroups` / `GrpoGroup` / `GateCounts` (Task 6), `GrpoWeights` / `GrpoWeightStore` (Task 7), `SparkSessions.create`, `DropMetrics.format`.
- Produces:
  - `def applyBatch(current: GrpoWeights, groups: Seq[GrpoGroup], cfg: GrpoJobConfig, batchId: Long): GrpoWeights`
  - `def main(args: Array[String]): Unit`

- [ ] **Step 1: Write the failing test**

```scala
package com.demo.grpo

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GrpoPolicyStreamingJobSpec extends AnyFlatSpec with Matchers {

  private val cfg = GrpoJobConfig.from(Map.empty)

  private def group(rewards: Array[Double]): GrpoGroup = GrpoGroup(
    slateId = "s1",
    x = rewards.indices.map(i => Array.fill(cfg.dim)(0.1 * (i + 1))).toArray,
    logged = rewards.indices.map(_ => 0.5).toArray,
    rewards = rewards)

  "applyBatch" should "leave the weights untouched when no group survived the gate" in {
    val before = GrpoWeightStore.initial(cfg)
    val after = GrpoPolicyStreamingJob.applyBatch(before, Seq.empty, cfg, 7L)
    after.weights.toSeq shouldBe before.weights.toSeq
    // The batch id still advances: the job ran, it simply had nothing to learn from.
    after.batchId shouldBe 7L
    after.slatesApplied shouldBe 0L
  }

  it should "move the weights when a group carries reward variance" in {
    val after = GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0, 0.0))), cfg, 1L)
    after.weights.exists(_ != 0.0) shouldBe true
    after.slatesApplied shouldBe 1L
  }

  it should "accumulate slatesApplied across batches" in {
    val first = GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0))), cfg, 1L)
    val second = GrpoPolicyStreamingJob.applyBatch(first, Seq(group(Array(1.0, 0.0))), cfg, 2L)
    second.slatesApplied shouldBe 2L
  }

  it should "produce finite weights after many batches" in {
    // A diverging policy that silently becomes NaN would be served as a real score.
    val trained = (1 to 200).foldLeft(GrpoWeightStore.initial(cfg)) { (w, batch) =>
      GrpoPolicyStreamingJob.applyBatch(w, Seq(group(Array(1.0, 0.0, 0.0))), cfg, batch.toLong)
    }
    trained.weights.forall(v => !v.isNaN && !v.isInfinite) shouldBe true
  }

  it should "preserve the feature version it was fit under" in {
    GrpoPolicyStreamingJob.applyBatch(
      GrpoWeightStore.initial(cfg), Seq(group(Array(1.0, 0.0))), cfg, 1L
    ).featureVersion shouldBe cfg.featureVersion
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec"`
Expected: FAIL — `GrpoPolicyStreamingJob` not found.

- [ ] **Step 3: Write the implementation**

```scala
package com.demo.grpo

import com.demo.process.RecommendationResponseStatsJob
import com.demo.engine.RedisPool
import com.demo.event.EventParsing
import com.demo.util.{DropMetrics, SparkSessions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/** GRPO trained continuously off the slate stream.
  *
  * Each micro-batch is a minibatch. Within it the ratio is measured against a snapshot of the
  * weights taken at the batch's start, NOT against the logged serving policy -- see applyBatch.
  */
object GrpoPolicyStreamingJob {

  private val JobName = "GrpoPolicyStreamingJob"
  private val log = LoggerFactory.getLogger(getClass)

  /** One micro-batch of learning.
    *
    * The inner-epoch loop is what makes clipping meaningful. The ratio is pi_theta / pi_snapshot,
    * where the snapshot is `current` frozen before the first step. Anchoring the ratio to the
    * LOGGED policy instead would be a silent failure in shadow mode: serving never changes there,
    * so the ratio would grow without bound as training proceeds, clipping would latch permanently
    * active, and the gradient would go to zero -- while every batch still looked healthy. The KL
    * term still anchors to the logged policy, which is what keeps the drift bounded.
    */
  def applyBatch(current: GrpoWeights, groups: Seq[GrpoGroup], cfg: GrpoJobConfig,
                 batchId: Long): GrpoWeights = {
    if (groups.isEmpty) return current.copy(batchId = batchId)

    val snapshot = current.weights.clone()
    var w = current.weights.clone()
    (1 to cfg.hyper.innerEpochs).foreach { _ =>
      val total = Array.fill(cfg.dim)(0.0)
      groups.foreach { g =>
        GrpoMath.advantages(g.rewards).foreach { adv =>
          // Ratio against the snapshot; KL against what actually served. GrpoMath.gradient takes
          // both references, so the two cannot be conflated here.
          val snapshotLogits =
            g.x.map(row => row.indices.foldLeft(0.0)((a, i) => a + row(i) * snapshot(i)))
          val grad = GrpoMath.gradient(g.x, snapshotLogits, g.logged, w, adv, cfg.hyper)
          (0 until cfg.dim).foreach(d => total(d) += grad(d))
        }
      }
      (0 until cfg.dim).foreach(d => w(d) -= cfg.hyper.learningRate * total(d) / groups.size)
    }
    GrpoWeights(w, cfg.featureVersion, batchId, current.slatesApplied + groups.size)
  }

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("GRPO_INPUT_TOPIC", "training_experiences")
    val checkpointLocation =
      sys.env.getOrElse("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-recsys/grpo-policy")
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")
    val cfg = GrpoJobConfig.fromEnv()

    val spark = SparkSessions.create(JobName)
    val pool = RedisPool.get(cfg.redisHost, cfg.redisPort, 8)

    // Refuse to start on a layout mismatch rather than applying stale weights to new features.
    var weights: GrpoWeights = {
      val jedis = pool.getResource
      try GrpoWeightStore.decode(jedis.hgetAll(cfg.weightsKey).asScala.toMap, cfg) match {
        case Right(w) => w
        case Left(reason) => throw new IllegalStateException(s"$JobName refusing to start: $reason")
      } finally jedis.close()
    }

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val slates = EventParsing.fromJson(batch, RecommendationResponseStatsJob.SlateSchema)
        val (groups, counts) = GrpoSlates.toGroups(slates, cfg)
        log.info(DropMetrics.format(JobName, batchId, counts.kept, counts.reasons))

        weights = applyBatch(weights, groups, cfg, batchId)
        val jedis = pool.getResource
        try jedis.hset(cfg.weightsKey, GrpoWeightStore.encode(weights, System.currentTimeMillis()))
        finally jedis.close()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt "testOnly com.demo.grpo.GrpoPolicyStreamingJobSpec"`
Expected: PASS, 5 tests.

- [ ] **Step 5: Run the whole Scala suite for regressions**

Run: `cd recsys-pipeline/services/spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test`
Expected: PASS, no existing spec broken.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/grpo/GrpoPolicyStreamingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/grpo/GrpoPolicyStreamingJobSpec.scala
git commit -m "feat: GrpoPolicyStreamingJob trains the policy off the live slate stream"
```

---

### Task 9: GrpoPolicyScorer — shadow scoring

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java`

**Interfaces:**
- Consumes: `GrpoFeatures.DIM` / `GrpoFeatures.of` (Task 1); `RecommendationProperties.Grpo.getMode()` (Task 3); the Redis hash `grpo:policy:weights` written by Task 8; `StringRedisTemplate`.
- Produces:
  - `public static final String MODE_OFF/MODE_SHADOW/MODE_ON = "off"/"shadow"/"on"`
  - `public static final String WEIGHTS_KEY = "grpo:policy:weights"`
  - `public static final double ON_BLEND_WEIGHT = 0.10`
  - `public boolean enabled()`
  - `public double score(ServedMovie movie, int position, int slateSize)`
  - `public double blendWeight()` — `0.0` unless mode is `on`

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class GrpoPolicyScorerTest {

    private final ServedMovie movie =
        new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2, Map.of("predictionScore", 0.7));

    private GrpoPolicyScorer scorer(String mode, String weights, String version) {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode(mode);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(anyString())).thenReturn(weights == null ? Map.of()
            : Map.of("weights", weights, "feature_version", version, "dim", "10"));
        return new GrpoPolicyScorer(redis, properties);
    }

    private String ones() {
        return String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM, "1.0"));
    }

    @Test
    void offModeIsDisabledAndScoresZero() {
        GrpoPolicyScorer s = scorer("off", ones(), "v1");
        assertFalse(s.enabled());
        assertEquals(0.0, s.score(movie, 0, 5));
    }

    @Test
    void shadowModeScoresButClaimsNoBlendWeight() {
        GrpoPolicyScorer s = scorer("shadow", ones(), "v1");
        assertTrue(s.enabled());
        assertEquals(0.0, s.blendWeight(), "shadow must never move a recommendation");
        double expected = java.util.Arrays.stream(GrpoFeatures.of(movie, 0, 5)).sum();
        assertEquals(expected, s.score(movie, 0, 5), 1e-9);
    }

    @Test
    void onModeClaimsOnlyTheUnclaimedBlendWeight() {
        GrpoPolicyScorer s = scorer("on", ones(), "v1");
        assertEquals(GrpoPolicyScorer.ON_BLEND_WEIGHT, s.blendWeight());
        // MovieLensOutcomeScorer's weights sum to 0.85; taking more than 0.15 would silently
        // change every existing score.
        assertTrue(s.blendWeight() <= 0.15);
    }

    @Test
    void anUnrecognisedModeIsTreatedAsOff() {
        assertFalse(scorer("enabled", ones(), "v1").enabled());
    }

    @Test
    void weightsOfADifferentFeatureVersionAreIgnored() {
        assertEquals(0.0, scorer("shadow", ones(), "v0").score(movie, 0, 5));
    }

    @Test
    void aMissingWeightVectorScoresZeroRatherThanFailing() {
        assertEquals(0.0, scorer("shadow", null, "v1").score(movie, 0, 5));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoPolicyScorerTest test`
Expected: FAIL — `GrpoPolicyScorer` does not exist.

- [ ] **Step 3: Write the implementation**

```java
package com.demo.retrieval.service.grpo;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Serves the GRPO policy score, off by default.
 *
 *   off    — not computed, no Redis read
 *   shadow — computed and logged at blend weight 0.0
 *   on     — claims part of the blend weight MovieLensOutcomeScorer leaves unclaimed
 *
 * Mirrors recsys.sequence.behavior-mode so operators meet a rollout shape they already know.
 * Every failure path scores 0.0: a missing, stale or wrongly versioned weight vector must leave
 * recommendations exactly as they were, never fail a request.
 */
@Service
public class GrpoPolicyScorer {

    private static final Logger log = LoggerFactory.getLogger(GrpoPolicyScorer.class);

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";
    public static final String WEIGHTS_KEY = "grpo:policy:weights";

    /**
     * MovieLensOutcomeScorer's exploitation weights sum to 0.85, not 1.0, because a fourth term
     * was always 0.0 at runtime, and it documents that the remainder is deliberately NOT
     * renormalized. Taking 0.10 of the unclaimed 0.15 means no existing weight changes and no
     * existing score moves when GRPO is switched on.
     */
    public static final double ON_BLEND_WEIGHT = 0.10;

    private final StringRedisTemplate redis;
    private final String mode;

    public GrpoPolicyScorer(StringRedisTemplate redis, RecommendationProperties properties) {
        this.redis = redis;
        String configured = properties.getGrpo().getMode();
        String normalized = configured == null ? MODE_OFF : configured.trim().toLowerCase(Locale.ROOT);
        if (!MODE_OFF.equals(normalized) && !MODE_SHADOW.equals(normalized) && !MODE_ON.equals(normalized)) {
            log.warn("Unrecognized recsys.grpo.mode '{}', treating as '{}'", configured, MODE_OFF);
            normalized = MODE_OFF;
        }
        this.mode = normalized;
    }

    public boolean enabled() {
        return !MODE_OFF.equals(mode);
    }

    public double blendWeight() {
        return MODE_ON.equals(mode) ? ON_BLEND_WEIGHT : 0.0;
    }

    public double score(ServedMovie movie, int position, int slateSize) {
        if (!enabled()) {
            return 0.0;
        }
        Optional<double[]> weights = readWeights();
        if (weights.isEmpty()) {
            return 0.0;
        }
        double[] w = weights.get();
        double[] x = GrpoFeatures.of(movie, position, slateSize);
        double sum = 0.0;
        for (int i = 0; i < GrpoFeatures.DIM; i++) {
            sum += w[i] * x[i];
        }
        return sum;
    }

    private Optional<double[]> readWeights() {
        Map<Object, Object> raw = redis.opsForHash().entries(WEIGHTS_KEY);
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        Object version = raw.get("feature_version");
        if (version == null || !GrpoFeatures.VERSION.equals(version.toString())) {
            log.warn("GRPO weights carry feature_version '{}', expected '{}' — ignoring",
                version, GrpoFeatures.VERSION);
            return Optional.empty();
        }
        Object packed = raw.get("weights");
        if (packed == null) {
            return Optional.empty();
        }
        String[] parts = packed.toString().split(",");
        if (parts.length != GrpoFeatures.DIM) {
            return Optional.empty();
        }
        double[] w = new double[GrpoFeatures.DIM];
        try {
            for (int i = 0; i < parts.length; i++) {
                w[i] = Double.parseDouble(parts[i]);
            }
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return Optional.of(w);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q -Dtest=GrpoPolicyScorerTest test`
Expected: PASS, 6 tests.

- [ ] **Step 5: Run the whole Java suite**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -q test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java
git commit -m "feat: serve the GRPO policy score in shadow mode at zero blend weight"
```

---

### Task 10: Register grpoScore in the OPE guard and document the rollout

The one-line omission that would let the policy grade itself, plus the operator documentation.

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/ope_eval_report.py:45` (add `GRPO_PRED_KEY` to `POLICY_ONLY_PRED_KEYS`)
- Modify: `recsys-pipeline/README.md` (the measurement/environment-variable section)
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md` (the jobs list and the rollout table)
- Test: `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`

**Interfaces:**
- Consumes: nothing new. `GRPO_PRED_KEY = "grpoScore"` must match the key the scorer publishes in Task 9.

- [ ] **Step 1: Write the failing test**

Append to `recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py`:

```python
def test_grpo_score_is_excluded_from_reward_model_features():
    """A model:* score fed to the reward model that grades it makes the policy grade itself.

    This is not hypothetical here: feature_names() builds the reward model's inputs from every
    modelPredictions key that is not registered as policy-only.
    """
    import ope_eval_report

    assert ope_eval_report.GRPO_PRED_KEY in ope_eval_report.POLICY_ONLY_PRED_KEYS

    event = {"modelPredictions": {"predictionScore": 0.4, ope_eval_report.GRPO_PRED_KEY: 0.9}}
    assert ope_eval_report.GRPO_PRED_KEY not in ope_eval_report.feature_names([event])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py::test_grpo_score_is_excluded_from_reward_model_features -v`
Expected: FAIL — `AttributeError: module 'ope_eval_report' has no attribute 'GRPO_PRED_KEY'`.

- [ ] **Step 3: Register the key**

In `ope_eval_report.py`, beside the existing `TABULAR_Q_PRED_KEY` / `FQI_Q_PRED_KEY` / `DPO_PRED_KEY` definitions:

```python
#: The online GRPO policy's score, written by GrpoPolicyScorer in the serving path.
GRPO_PRED_KEY = "grpoScore"

POLICY_ONLY_PRED_KEYS = (TABULAR_Q_PRED_KEY, FQI_Q_PRED_KEY, DPO_PRED_KEY, GRPO_PRED_KEY)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests/python_modeling/test_ope_eval.py -v`
Expected: PASS, including the pre-existing tests in that file.

- [ ] **Step 5: Document the rollout**

In `recsys-pipeline/README.md`, add the new environment variables to the table that
`test_readme_documents_every_measurement_environment_variable` checks:

| Variable | Default | Meaning |
|---|---|---|
| `RECSYS_GRPO_EMIT_EVENTS` | `false` | Serving publishes its own impressions to `behavior_logs`. |
| `RECSYS_GRPO_MODE` | `off` | `off` / `shadow` / `on` — the GRPO policy rollout. |
| `GRPO_TEMPERATURE` | `1.0` | Softmax temperature. |
| `GRPO_CLIP_EPSILON` | `0.2` | PPO clip range. |
| `GRPO_KL_BETA` | `0.02` | Weight on the KL to the logged serving policy. |
| `GRPO_LEARNING_RATE` | `0.01` | SGD step size. |
| `GRPO_INNER_EPOCHS` | `4` | Gradient steps per micro-batch. Below 2, clipping never engages. |
| `GRPO_INPUT_TOPIC` | `training_experiences` | Slate stream the job consumes. |

In `Data_Pipeline.md`, add `GrpoPolicyStreamingJob` to the streaming-jobs list, and a rollout note
recording two things an operator cannot infer from the code: that `RECSYS_GRPO_EMIT_EVENTS` is the
prerequisite for the whole chain and also unblocks offline DPO, and that `shadow` is safe to leave
on indefinitely because its blend weight is 0.0.

- [ ] **Step 6: Run the documentation tests**

Run: `cd recsys-pipeline && python3 -m pytest integration-tests -k "readme or documentation" -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/python-modeling/ope_eval_report.py \
        recsys-pipeline/integration-tests/python_modeling/test_ope_eval.py \
        recsys-pipeline/README.md \
        recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "feat: keep grpoScore out of its own reward model and document the rollout"
```

---

## Verification

After Task 10, the full check:

```bash
cd recsys-pipeline/services/java-retrieval-service && mvn -q test
cd ../spark-streaming-job && JAVA_HOME=$(/usr/libexec/java_home -v 17) sbt test
cd ../../ && python3 -m pytest integration-tests -q
```

All three suites pass, and with both flags at their defaults nothing about serving has changed —
which is the point: the rollout is a decision an operator makes after shadow data exists, not
something this plan performs.
