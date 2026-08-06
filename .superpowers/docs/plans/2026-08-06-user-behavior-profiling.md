# User Behavioral Profiling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate deterministic batch user-preference and persona profiles from behavioral samples, publish completed snapshots to Parquet and Redis, expose profiles through the retrieval API, and use weighted preferences in recommendation scoring.

**Architecture:** A Spark batch job validates and deduplicates `training_samples`, derives decayed behavioral evidence and fixed-taxonomy personas, writes a Parquet snapshot, then activates a run-scoped Redis snapshot. The Java service reads only the active run, hydrates weighted genre/tag preferences into existing user features, uses them in content scoring, and exposes the full explainable profile through a read-only endpoint.

**Tech Stack:** Scala 2.12, Spark SQL 3.5.1, ScalaTest 3.2.18, Jedis 5.1.5, Java 17, Spring Boot 3.3.5, Spring Data Redis, Jackson, JUnit 5, Mockito, MockMvc.

## Global Constraints

- Profile schema version is exactly `1`.
- Redis keys are `user-profile:v1:{runId}:{userId}` and the activation pointer is `user-profile:v1:active-run`.
- Input is the existing enriched `training_samples` Parquet dataset.
- Defaults are impression `-0.1`, click `1.0`, order `3.0`; rating is centered on a configurable neutral midpoint.
- Recency uses `base_weight * 0.5 ^ (age / half_life)`.
- Preference scores are shrinkage-adjusted, bounded to `[-1, 1]`, and deterministically ordered.
- Only positive preferences influence serving; negative values remain explanatory and never filter candidates.
- `new_or_unknown` is exclusive; all other personas are deterministic and multi-label.
- Optional metrics are nullable Parquet fields and explicit JSON `null` fields.
- A failed Redis publication must not change the active-run pointer.
- Missing, malformed, expired, incomplete, and unsupported profiles preserve baseline recommendation behavior.
- Do not add an LLM, real-time update path, profile mutation endpoint, or UI.

---

### Task 1: Pure Behavioral Evidence and Persona Rules

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile/UserProfileConfig.scala`
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile/UserProfileDerivations.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/profile/UserProfileDerivationsSpec.scala`

**Interfaces:**
- Produces: `ProfileConfig`, `EvidenceInput`, `Preference`, `BehavioralFeatures`, `Persona`, `UserProfileDerivations.eventWeight`, `normalizePreferences`, and `classifyPersonas`.
- Consumes: no production code beyond the Scala standard library.

- [ ] **Step 1: Write failing evidence-weight tests**

Create table-driven ScalaTest cases that assert signal precedence and decay:

```scala
val cfg = ProfileConfig(referenceEpochSeconds = 1_000L, halfLifeSeconds = 100L)
eventWeight(EvidenceInput(900L, clicked = true, ordered = true, rating = None), cfg) shouldBe 1.5 +- 1e-9
eventWeight(EvidenceInput(900L, clicked = false, ordered = false, rating = None), cfg) shouldBe -0.05 +- 1e-9
eventWeight(EvidenceInput(1_000L, clicked = true, ordered = false, rating = Some(5.0)), cfg) shouldBe 1.0 +- 1e-9
```

The first assertion proves order wins over click and then decays; the third proves rating wins over click and maps a maximum rating to `1.0`.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.profile.UserProfileDerivationsSpec"`

Expected: compilation fails because `ProfileConfig` and `UserProfileDerivations` do not exist.

- [ ] **Step 3: Implement configuration and evidence weighting**

Define immutable defaults and validate positive half-life, rating range, and shrinkage:

```scala
case class ProfileConfig(
  referenceEpochSeconds: Long,
  halfLifeSeconds: Long,
  impressionWeight: Double = -0.1,
  clickWeight: Double = 1.0,
  orderWeight: Double = 3.0,
  ratingMin: Double = 1.0,
  ratingMidpoint: Double = 3.0,
  ratingMax: Double = 5.0,
  shrinkage: Double = 5.0,
  minimumEvidence: Long = 5L,
  maxGenres: Int = 10,
  maxTags: Int = 20
)
```

Implement rating-first, then order, click, impression precedence; clamp negative ages to zero; apply exponential decay.

- [ ] **Step 4: Write failing normalization and persona-boundary tests**

Assert shrinkage (`raw / (count + shrinkage)`), `[-1,1]` clamping, score/name ordering, positive-only serving selection, exclusive `new_or_unknown`, and each taxonomy boundary. Include the exact expected order:

```scala
classifyPersonas(lowEvidence, cfg).map(_.personaType) shouldBe Seq("new_or_unknown")
classifyPersonas(explorerAndRecent, cfg).map(_.personaType) shouldBe
  Seq("genre_explorer", "recent_release_seeker")
```

- [ ] **Step 5: Run tests, implement derivations, and verify GREEN**

Implement named rules in fixed order: `genre_enthusiast`, `genre_explorer`, `focused_viewer`, `recent_release_seeker`, `high_intent_engager`, `casual_browser`. Confidence is the normalized distance beyond a rule threshold, clamped to `[0,1]`; every persona stores its evidence map.

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.profile.UserProfileDerivationsSpec"`

Expected: PASS.

- [ ] **Step 6: Commit the pure profile domain**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/profile
git commit -m "feat: add behavioral profile derivations"
```

---

### Task 2: Spark Validation, Deduplication, Aggregation, and Parquet Snapshot

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile/UserBehaviorProfileBatchJob.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/profile/UserBehaviorProfileBatchJobSpec.scala`
- Create: `recsys-pipeline/integration-tests/fixtures/user_profile_v1.json`

**Interfaces:**
- Consumes: Task 1 `ProfileConfig` and `UserProfileDerivations`.
- Produces: `validateAndDeduplicate(input, config): ProfileInput`, `buildProfiles(valid, config): DataFrame`, `run(spark, inputPath, outputPath, config): ProfileRunResult`.
- `ProfileInput` contains `valid: DataFrame`, `rejected: DataFrame`, and count metrics; `ProfileRunResult` contains `runId`, `profiles`, `outputPath`, and metrics.

- [ ] **Step 1: Write failing validation and identity tests**

Create Spark fixtures containing a duplicate `sample_id`, a null user, an invalid timestamp, and two rows without `sample_id`. Assert explicit IDs deduplicate first and fallback identity is `sha2(concat_ws("\u001f", user_id, request_id, item_id, impression_ts, clicked, ordered, rating), 256)`.

```scala
val prepared = UserBehaviorProfileBatchJob.validateAndDeduplicate(input, cfg)
prepared.valid.count() shouldBe 2L
prepared.rejected.groupBy("rejection_reason").count().as[(String, Long)].collect().toMap shouldBe
  Map("missing_user" -> 1L, "invalid_timestamp" -> 1L)
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.profile.UserBehaviorProfileBatchJobSpec"`

Expected: compilation fails because the batch job is absent.

- [ ] **Step 3: Implement source preparation**

Normalize `genres` and `tags` to lowercase trimmed distinct arrays, accept absent `tags`/`rating`/`new_release` columns by adding typed null or empty defaults, filter to `[sourceWindowStart, sourceWindowEnd)`, assign one rejection reason per invalid row, and use `dropDuplicates("dedupe_id")` for valid rows.

- [ ] **Step 4: Write failing per-user aggregation tests**

Build a fixed-clock fixture with recent Sci-Fi clicks/orders, old Drama clicks, and unengaged Comedy impressions. Assert exact counts/rates, Sci-Fi above Drama, Comedy below zero, stable preference ordering, `recent_release_affinity`, persona evidence, and explicit null `average_rating` when no ratings exist.

- [ ] **Step 5: Implement aggregation and JSON contract**

Use Spark aggregations for counts and exploded genre/tag evidence. Join aggregate tables by `user_id`, then use a typed UDF only for the small deterministic persona-rule registry. Produce columns matching the spec, including nested `source_window`, preference arrays, behavioral feature struct, persona array, `profile_json`, `profile_version`, `run_id`, and `generated_at`.

- [ ] **Step 6: Write and pass the snapshot test**

Test `run` with temporary input/output directories. Read the resulting Parquet, compare two reruns after dropping run metadata, and parse `profile_json` with Jackson to confirm optional metrics are explicit JSON nulls. Serialize one fixed user and assert exact equality with `integration-tests/fixtures/user_profile_v1.json`; Task 7 consumes this cross-language contract fixture.

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.profile.UserBehaviorProfileBatchJobSpec"`

Expected: PASS.

- [ ] **Step 7: Commit the batch profiler**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile/UserBehaviorProfileBatchJob.scala recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/profile/UserBehaviorProfileBatchJobSpec.scala recsys-pipeline/integration-tests/fixtures/user_profile_v1.json
git commit -m "feat: build batch user profiles"
```

---

### Task 3: Run-Scoped Redis Publication and Operator Script

**Files:**
- Create: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile/UserProfileRedisPublisher.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/profile/UserProfileRedisPublisherSpec.scala`
- Create: `recsys-pipeline/scripts/run-user-profile-pipeline.sh`
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py`

**Interfaces:**
- Consumes: Task 2 profile rows containing `user_id`, `run_id`, and `profile_json`.
- Produces: `UserProfileRedisPublisher.publish(rows, RedisProfileConfig): Unit`; activation occurs only after all partition writes return successfully.

- [ ] **Step 1: Write failing publisher tests**

Extract a small `RedisProfileStore` interface so tests use a recording fake, not a Redis mock:

```scala
trait RedisProfileStore {
  def writeProfiles(runId: String, values: Iterator[(String, String)], ttlSeconds: Int): Unit
  def activate(runId: String): Unit
}
```

Assert keys are run-scoped, TTL is forwarded, activation is last, and an injected write failure leaves `activeRun` unchanged.

- [ ] **Step 2: Run publisher tests and verify RED**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.profile.UserProfileRedisPublisherSpec"`

Expected: compilation fails because publisher types do not exist.

- [ ] **Step 3: Implement publication and wire it into `main`**

Use Jedis pipelines within partitions for `user-profile:v1:{runId}:{userId}`. Collect partition success at the driver; only then set `user-profile:v1:active-run` without an expiry. Configure host, port, TTL, prefix, lookback, half-life, weights, thresholds, and maximum preferences from environment variables. Print structured run metrics after completion.

- [ ] **Step 4: Add the operator script and its contract tests**

The script must require `USER_PROFILE_INPUT_PATH`, default `USER_PROFILE_OUTPUT_PATH` to `sampledata/user_profiles`, show effective non-secret configuration, locate `spark-submit` like `run-user-embedding-pipeline.sh`, and invoke `com.demo.profile.UserBehaviorProfileBatchJob`.

Add Python assertions for missing input, missing jar, correct Spark class, positional paths, and environment forwarding.

Run: `cd recsys-pipeline && pytest integration-tests/test_service_scripts.py -q`

Expected: PASS.

- [ ] **Step 5: Run all Spark profile tests and commit**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.profile.*"`

Expected: PASS.

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/profile/UserProfileRedisPublisher.scala recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/profile/UserProfileRedisPublisherSpec.scala recsys-pipeline/scripts/run-user-profile-pipeline.sh recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "feat: publish versioned user profiles"
```

---

### Task 4: Java Profile Contract, Active-Run Client, and Profile API

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/UserBehaviorProfile.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/clients/UserProfileClient.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/clients/RedisUserProfileClient.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/clients/RedisUserProfileClientTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/controller/RecommendationController.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml`

**Interfaces:**
- Produces: `Optional<UserBehaviorProfile> UserProfileClient.getProfile(String userId)`.
- `UserBehaviorProfile` contains nested Jackson-compatible records for source window, preference, behavioral features, persona, and evidence.

- [ ] **Step 1: Write failing active-run client tests**

Assert that the client reads `user-profile:v1:active-run`, then `user-profile:v1:{runId}:{userId}`; parses explicit null metrics; returns empty for missing keys, invalid JSON, user mismatch, version other than 1, and a Redis exception; and increments Micrometer counters for each fallback category.

- [ ] **Step 2: Run the focused Maven test and verify RED**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=RedisUserProfileClientTest test`

Expected: compilation fails because client/model types do not exist.

- [ ] **Step 3: Implement the model and client**

Use `ObjectMapper.readValue`, validate `profileVersion == 1` and `userId.equals(requestedUserId)`, normalize preference names, and defensively copy all lists/maps. Catch Redis and Jackson failures, log without profile payloads, record `profile.lookup` timer plus tagged fallback counters, and return `Optional.empty()`.

- [ ] **Step 4: Write failing profile endpoint tests**

Add `@MockBean UserProfileClient`. Assert `GET /users/u1/profile` returns the complete nested contract and `application/json`; absent profile returns HTTP 404 with `{"error":"profile_not_found","user_id":"u1"}`; invalid user ID returns 400.

- [ ] **Step 5: Implement the endpoint and configuration**

Inject `UserProfileClient` into `RecommendationController`, add the validated path, return `ResponseEntity<UserBehaviorProfile>` for success, and throw a controller-local `ProfileNotFoundException` handled as the specified JSON 404. Add `recsys.user-profile.key-prefix: ${RECSYS_USER_PROFILE_KEY_PREFIX:user-profile:v1}`.

- [ ] **Step 6: Run Java tests and commit**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=RedisUserProfileClientTest,RecommendationControllerTest test`

Expected: PASS.

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/clients/RedisUserProfileClientTest.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/controller/RecommendationControllerTest.java
git commit -m "feat: expose explainable user profiles"
```

---

### Task 5: Hydrate and Preserve Weighted Preferences

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/MovieLensUserFeatures.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/query_hydrators/UserBehaviorProfileQueryHydrator.java`
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/query_hydrators/UserBehaviorProfileQueryHydratorTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/MovieLensUserFeaturesTest.java`

**Interfaces:**
- Consumes: Task 4 `UserProfileClient` and `UserBehaviorProfile`.
- Produces: `MovieLensUserFeatures.genrePreferences(): Map<String,Double>`, `tagPreferences()`, and `withBehaviorPreferences(Map<String,Double>, Map<String,Double>)`.

- [ ] **Step 1: Write failing feature-copy and hydrator tests**

Assert the hydrator keeps only positive finite scores, normalizes names, and places them in descending deterministic order. Run a profile-enriched feature object through every existing `with...` method and assert the maps survive. Assert a missing profile produces unchanged features.

- [ ] **Step 2: Run tests and verify RED**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=MovieLensUserFeaturesTest,UserBehaviorProfileQueryHydratorTest test`

Expected: compilation fails because weighted preference accessors are absent.

- [ ] **Step 3: Extend the record compatibly**

Append the two maps to the canonical record and provide an overload matching the prior canonical signature so existing call sites continue compiling. Update the private `copy` method to preserve maps, implement `withBehaviorPreferences`, reject null/non-finite/non-positive entries, and store unmodifiable insertion-ordered maps.

- [ ] **Step 4: Implement the hydrator and verify GREEN**

`hydrate` calls `profileClient.getProfile(query.userId())`; `update` merges the hydrated preference maps into `query.userFeatures()` without replacing any other hydrated fields. Map only positive profile preferences and cap values at `1.0`.

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=MovieLensUserFeaturesTest,UserBehaviorProfileQueryHydratorTest test`

Expected: PASS.

- [ ] **Step 5: Commit hydration**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/MovieLensUserFeatures.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/query_hydrators/UserBehaviorProfileQueryHydrator.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/MovieLensUserFeaturesTest.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/query_hydrators/UserBehaviorProfileQueryHydratorTest.java
git commit -m "feat: hydrate behavioral preferences"
```

---

### Task 6: Weighted Preferences in Content Retrieval

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/content/CatalogContentScoring.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/retrieval/ContentCandidateRetriever.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/content/CatalogContentScoringTest.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/HybridRecommendationServiceTest.java`

**Interfaces:**
- Consumes: Task 5 weighted preference maps.
- Produces: `contentScore(NormalizedProfile, Map<String,Double>, Map<String,Double>): double`.

- [ ] **Step 1: Write failing weighted-scoring tests**

Assert a catalog item matching genre score `0.9` outranks one matching `0.3`, tag weight contributes independently, unknown/negative values contribute zero, and the previous set overload delegates through unit weights to preserve old tests.

```java
assertTrue(scoring.contentScore(sciFi, Map.of("sci-fi", .9), Map.of())
    > scoring.contentScore(drama, Map.of("drama", .3), Map.of()));
```

- [ ] **Step 2: Run tests and verify RED**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=CatalogContentScoringTest,HybridRecommendationServiceTest test`

Expected: compilation fails because the map overload is absent.

- [ ] **Step 3: Implement weighted overlap**

For each catalog genre/tag match, sum the user's bounded positive preference weights and divide by the total positive weight in that preference category. Combine genre/tag ratios with existing `CONTENT_GENRE_WEIGHT` and `CONTENT_TAG_WEIGHT`, then clamp with `RecommendationConstants.clamp`.

- [ ] **Step 4: Thread maps through retrieval and merge precedence**

Change `TasteProfile` to hold maps. Seed-derived and explicit `favoriteGenres` receive weight `1.0`; then supplement with behavioral genre/tag preferences using `putIfAbsent`, so explicit/current-session evidence wins. Update `ContentCandidateRetriever` and scoring calls to accept maps. Keep sorted key lists in diagnostic state.

- [ ] **Step 5: Add service-level regression tests and verify GREEN**

Assert a profiled user selects the stronger matching genre when popularity is tied, explicit favorite genre retains weight `1.0` over a weaker inferred duplicate, and `UserProfileClient.empty()` yields the exact baseline ordering.

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=CatalogContentScoringTest,HybridRecommendationServiceTest test`

Expected: PASS.

- [ ] **Step 6: Commit serving integration**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/content/CatalogContentScoring.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/retrieval/ContentCandidateRetriever.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/content/CatalogContentScoringTest.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/HybridRecommendationServiceTest.java
git commit -m "feat: rank with weighted user preferences"
```

---

### Task 7: Cross-Language Integration, Documentation, and Full Verification

**Files:**
- Create: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/UserProfileIntegrationTest.java`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`
- Modify: `recsys-pipeline/docs/recommendation_architecture/API.md`
- Modify: `recsys-pipeline/README.md`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: executable cross-language contract fixture and operator documentation.

- [ ] **Step 1: Write the failing cross-language contract test**

Load Task 2's `integration-tests/fixtures/user_profile_v1.json` through a Testcontainers Redis instance and a Spring Boot test context. Set the active-run pointer, call the profile endpoint, and assert version, nulls, preference order, persona evidence, and run metadata. Call the recommendation endpoint with tied-popularity Sci-Fi and Drama items and assert Sci-Fi wins; delete the active pointer and assert the recorded baseline ordering returns.

- [ ] **Step 2: Run the integration test and verify RED**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -Dtest=UserProfileIntegrationTest test`

Expected: FAIL because the Redis container and dynamic Spring Redis properties are not configured yet.

- [ ] **Step 3: Complete the minimal test harness and documentation**

Implement the test with `@Testcontainers`, `@Container GenericContainer<>("redis:7-alpine").withExposedPorts(6379)`, and `@DynamicPropertySource` for the Spring Redis host and port. Seed the two catalog entries and tied popularity data required by the recommendation assertions. Document input fields, evidence precedence, decay, persona taxonomy, environment variables, Parquet layout, Redis activation protocol, API response/404 behavior, safe fallback, metrics, and the `run-user-profile-pipeline.sh` command.

- [ ] **Step 4: Run focused and full verification**

Run:

```bash
cd recsys-pipeline/services/spark-streaming-job && sbt test
cd recsys-pipeline/services/java-retrieval-service && mvn test
cd recsys-pipeline && pytest integration-tests/test_service_scripts.py -q
git diff --check
```

Expected: all commands exit 0 with no failed tests and `git diff --check` prints nothing.

- [ ] **Step 5: Commit documentation and integration coverage**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/UserProfileIntegrationTest.java recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md recsys-pipeline/docs/recommendation_architecture/API.md recsys-pipeline/README.md
git commit -m "docs: document behavioral user profiles"
```
