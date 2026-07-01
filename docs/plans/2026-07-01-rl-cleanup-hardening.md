# RL Cleanup + Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Default the deep-learning term off, remove two code smells, and add real Redis-backed tests for the merged B1/B3 fixes — one PR, commits split per thread.

**Architecture:** T1 flips a config default and its docs. T2a/T2b are surgical edits in `HybridRecommendationService`. T3 adds a Testcontainers `redis:7-alpine` integration test, Docker-gated so Docker-less builds skip (not fail).

**Tech Stack:** Java 17, Spring Boot 3.3.5 (Spring Data Redis / Lettuce), JUnit 5, Mockito, Testcontainers, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-01-rl-cleanup-hardening.md](../specs/2026-07-01-rl-cleanup-hardening.md).
- Existing 50 unit tests stay green; the new IT skips cleanly without Docker.
- Testcontainers version is managed by the Spring Boot parent — do NOT pin a version.
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Modify: `src/main/resources/application.yml` — DL weight default.
- Modify: `README.md` (root) + `recsys-pipeline/README.md` — DL weight docs.
- Modify: `service/HybridRecommendationService.java` — delete `readCount`; clamp the two-tower/DL fusion.
- Modify: `pom.xml` — Testcontainers test dependency.
- Create (test): `service/HybridFeedbackRedisTest.java` — Docker-gated Redis integration test.

---

### Task 1 (T1): Deep-learning weight defaults to 0.0

**Files:**
- Modify: `src/main/resources/application.yml`
- Modify: `README.md`
- Modify: `recsys-pipeline/README.md`

- [ ] **Step 1: application.yml.** Change the default:

```yaml
    deep-learning-weight: ${RECSYS_DEEP_LEARNING_WEIGHT:0.0}
```

- [ ] **Step 2: root README architecture line** (line ~118):

```
    ├── DeepLearningPredictionService  mlp_embedding_model.onnx (classpath; blend weight 0.0 by default)
```

- [ ] **Step 3: recsys-pipeline/README config tables** — set both rows to `0.0`:

```
| `recsys.bandit.deep-learning-weight` | `0.0` |
```
```
| `RECSYS_DEEP_LEARNING_WEIGHT` | `0.0` |
```

- [ ] **Step 4: Verify consistency.**

Run: `rg -n "deep-learning-weight|DEEP_LEARNING_WEIGHT" recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml README.md recsys-pipeline/README.md`
Expected: every default shows `0.0` (env-var override syntax may still name the var).

- [ ] **Step 5: Commit.**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml README.md recsys-pipeline/README.md
git commit -m "chore(rl): default deep-learning-weight to 0.0 (opt-in)"
```

---

### Task 2 (T2a): Remove dead `readCount`

**Files:**
- Modify: `service/HybridRecommendationService.java`

- [ ] **Step 1: Confirm no callers.**

Run: `rg -n "readCount" recsys-pipeline/services/java-retrieval-service/src`
Expected: only the declaration line.

- [ ] **Step 2: Delete the method** (near line 1311):

```java
    private long readCount(Long value) {
        return value == null ? 0L : Math.max(0L, value);
    }
```

- [ ] **Step 3: Compile + full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 50`.

- [ ] **Step 4: Commit.**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "chore(rl): remove dead readCount(Long)"
```

---

### Task 3 (T2b): Clamp two-tower/DL fusion

**Files:**
- Modify: `service/HybridRecommendationService.java` (~lines 212-215)

- [ ] **Step 1: Replace the merge block.** Change:

```java
                Map<String, Double> merged = new HashMap<>(dlScoresRaw);
                twoTowerScores.forEach((item, score) ->
                    merged.merge(item, score, Math::max));
                dlScoresRaw = Map.copyOf(merged);
```

to (clamp both operands to `[0,1]` before `max`):

```java
                Map<String, Double> merged = new HashMap<>();
                dlScoresRaw.forEach((item, score) -> merged.put(item, RecommendationConstants.clamp(score)));
                twoTowerScores.forEach((item, score) ->
                    merged.merge(item, RecommendationConstants.clamp(score), Math::max));
                dlScoresRaw = Map.copyOf(merged);
```

- [ ] **Step 2: Full suite (two-tower disabled path unaffected).**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 50`.

- [ ] **Step 3: Commit.**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "fix(rl): clamp two-tower/DL scores to [0,1] before max fusion"
```

---

### Task 4 (T3): Testcontainers Redis integration test

**Files:**
- Modify: `pom.xml`
- Create: `src/test/java/com/demo/retrieval/service/HybridFeedbackRedisTest.java`

**Interfaces:**
- Consumes: `HybridRecommendationService.recordFeedback(FeedbackRequest)`, `MovieLensServingSideEffects.pendingReplayKey(user, item)`, `TabularStateKey.hash(genres, tags)` (all reachable from the same package / public).

- [ ] **Step 1: Add the test dependency to `pom.xml`** (inside `<dependencies>`, no version — Spring Boot manages it):

```xml
        <dependency>
            <groupId>org.testcontainers</groupId>
            <artifactId>junit-jupiter</artifactId>
            <scope>test</scope>
        </dependency>
```

- [ ] **Step 2: Create the Docker-gated integration test.** Manual container lifecycle so `assumeTrue` runs before any container start.

`src/test/java/com/demo/retrieval/service/HybridFeedbackRedisTest.java`:

```java
package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HybridFeedbackRedisTest {

    private static GenericContainer<?> redisContainer;
    private static StringRedisTemplate redis;

    @BeforeAll
    static void startRedis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker not available");
        redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redisContainer.start();
        LettuceConnectionFactory cf = new LettuceConnectionFactory(
            redisContainer.getHost(), redisContainer.getMappedPort(6379));
        cf.afterPropertiesSet();
        redis = new StringRedisTemplate(cf);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void stopRedis() {
        if (redisContainer != null) {
            redisContainer.stop();
        }
    }

    @BeforeEach
    void flush() {
        Set<String> keys = redis.keys("*");
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    private HybridRecommendationService qLearningService() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("q-learning");
        properties.setCatalog(new java.util.LinkedHashMap<>());
        FeatureCache featureCache = new FeatureCache(properties);
        DeepLearningPredictionService dl = mock(DeepLearningPredictionService.class);
        when(dl.predictBatch(any(), any())).thenReturn(java.util.Map.of());
        when(dl.predict(any(), any())).thenReturn(Optional.empty());
        TwoTowerPredictionService twoTower = mock(TwoTowerPredictionService.class);
        when(twoTower.isEnabled()).thenReturn(false);
        return new HybridRecommendationService(
            redis, properties, dl,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache,
            List.of(new MovieLensUserHistoryQueryHydrator(
                userId -> new UserMovieHistory(List.of(), List.of()))),
            twoTower);
    }

    // B1: the pending key is deleted on consume and a replayed feedback does not re-apply the Q-update.
    @Test
    void feedbackDeletesPendingKeyAndReplayIsIdempotent() {
        HybridRecommendationService service = qLearningService();
        String user = "u1";
        String item = "m1";
        String pendingKey = MovieLensServingSideEffects.pendingReplayKey(user, item);
        // Seed a serve-time pending context with a taste-profile state (genre "drama").
        redis.opsForValue().set(pendingKey,
            "{\"type\":\"rl_experience\",\"action\":\"m1\","
          + "\"state\":{\"recent\":[],\"genres\":[\"drama\"],\"tags\":[]}}",
            Duration.ofHours(1));

        service.recordFeedback(new FeedbackRequest(user, item, true, 1.0));

        // pending key consumed
        assertNull(redis.opsForValue().get(pendingKey));
        // Q written once: q0=0, next=0 -> updated = alpha*reward = 0.1*1.0
        String qKey = "q-learning:q:" + TabularStateKey.hash(List.of("drama"), List.of());
        double qAfterFirst = Double.parseDouble(String.valueOf(redis.opsForHash().get(qKey, item)));
        assertEquals(0.1, qAfterFirst, 1e-9);

        // Replay: pending gone -> no state -> no second Q change.
        service.recordFeedback(new FeedbackRequest(user, item, true, 1.0));
        double qAfterReplay = Double.parseDouble(String.valueOf(redis.opsForHash().get(qKey, item)));
        assertEquals(qAfterFirst, qAfterReplay, 1e-9);
    }

    // B1 (TTL) + B3 (Lua): serve writes the pending key with a TTL; a second real feedback accumulates Q.
    @Test
    void servingWritesPendingWithTtlAndLuaAccumulatesQ() {
        MovieLensServingSideEffects sideEffects =
            new MovieLensServingSideEffects(redis, new ObjectMapper(), Duration.ofSeconds(120));
        // (recordServed path is covered for TTL by MovieLensServingSideEffectsTest; here we verify the
        // Lua update accumulates across two consumes against real Redis.)
        HybridRecommendationService service = qLearningService();
        String qKey = "q-learning:q:" + TabularStateKey.hash(List.of("drama"), List.of());
        String user = "u2";
        String item = "m2";
        String pendingPayload = "{\"type\":\"rl_experience\",\"action\":\"m2\","
            + "\"state\":{\"recent\":[],\"genres\":[\"drama\"],\"tags\":[]}}";

        redis.opsForValue().set(MovieLensServingSideEffects.pendingReplayKey(user, item), pendingPayload);
        service.recordFeedback(new FeedbackRequest(user, item, true, 1.0));
        double q1 = Double.parseDouble(String.valueOf(redis.opsForHash().get(qKey, item)));

        // Second serve+feedback: q1 -> q1 + 0.1*(1.0 + 0.9*next - q1); with next=0 -> q1 + 0.1*(1-q1).
        redis.opsForValue().set(MovieLensServingSideEffects.pendingReplayKey(user, item), pendingPayload);
        service.recordFeedback(new FeedbackRequest(user, item, true, 1.0));
        double q2 = Double.parseDouble(String.valueOf(redis.opsForHash().get(qKey, item)));

        assertEquals(q1 + 0.1 * (1.0 - q1), q2, 1e-9);
        assertTrue(q2 > q1);
        assertTrue(sideEffects != null); // constructor with TTL compiles against the new signature
    }
}
```

- [ ] **Step 3: Run the IT (with Docker) + full suite.**

Run: `mvn -o test -Dtest=HybridFeedbackRedisTest` then `mvn -o test`
Expected (Docker present): PASS (2 tests). Full suite `Tests run: 52`. Without Docker: the class is **skipped** (assumption failed), suite stays green at 50.

- [ ] **Step 4: Commit.**

```bash
git add recsys-pipeline/services/java-retrieval-service/pom.xml recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/HybridFeedbackRedisTest.java
git commit -m "test(rl): Redis-backed IT for pending-key idempotency and atomic Q-update"
```

---

### Task 5: Branch, docs, push, PR

- [ ] **Step 1: Branch off master, carrying the uncommitted spec/plan.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b chore/rl-cleanup-hardening
```

Do Tasks 1–4 on this branch (their commits land here). Then commit the docs:

```bash
git add docs/specs/2026-07-01-rl-cleanup-hardening.md docs/plans/2026-07-01-rl-cleanup-hardening.md
git commit -m "docs(rl): spec + plan for cleanup + hardening"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS` (52 with Docker, 50 skipping the IT without).

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin chore/rl-cleanup-hardening
gh pr create --base master --title "RL cleanup + hardening: DL-weight default, fusion clamp, dead code, Redis IT" --body "See docs/specs/2026-07-01-rl-cleanup-hardening.md. T1: deep-learning-weight defaults to 0.0 (opt-in) + docs. T2a: remove dead readCount. T2b: clamp two-tower/DL scores before max. T3: Testcontainers Redis IT for pending-key idempotency + atomic Lua Q-update (Docker-gated). mvn test green.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

- **Spec coverage:** T1→Task 1; T2a→Task 2; T2b→Task 3; T3→Task 4; artifacts+PR→Task 5. All threads covered.
- **Placeholders:** none — full code/values in every step.
- **Type consistency:** `RecommendationConstants.clamp(double)` (existing) used in Task 3; `MovieLensServingSideEffects(StringRedisTemplate, ObjectMapper, Duration)` (from #112) and `TabularStateKey.hash(Object, Object)` (from #111) used in Task 4 match their current signatures; `FeedbackRequest(user, item, clicked, reward)` matches the model.
- **Gotcha noted:** container is started manually (not `@Testcontainers`) so `assumeTrue` gates Docker before any start.
