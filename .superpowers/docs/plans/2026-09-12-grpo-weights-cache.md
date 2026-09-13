# GRPO Weights Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Read the GRPO weight vector through `FeatureCache` with its own TTL, as the online GRPO design specified, instead of once per request.

**Architecture:** A third Caffeine cache in `FeatureCache`, a TTL property, and a cache-first `readWeights` in the scorer that stores parsed weights (or an empty array for "nothing usable"). The scorer gains a `FeatureCache` constructor argument wired from `HybridRecommendationService`.

**Tech Stack:** Java 17, Spring Boot, Caffeine, Maven, JUnit 5, Mockito.

**Spec:** [GRPO weights through the feature cache](../specs/2026-09-12-grpo-weights-cache-design.md)

## Global Constraints

- Java 17 with Maven; add no dependencies (Caffeine is already used).
- Keep `GrpoPolicyScorer`'s modes, `blendWeight`, `reRankOrder`, `recordShadowSlate`, the version, width, and finite guards, and the failure contract (any exception degrades to no re-rank and no log line) unchanged.
- Keep the existing `item_vectors` and `reward_stats` caches and their configuration unchanged.
- Keep `off` mode free of Redis reads.

## Execution notes

All paths are repository-relative. Maven runs from `recsys-pipeline/services/java-retrieval-service/`
with `JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home`.
The work is on `feat/grpo-weights-cache`, based on `origin/master`. Publish a PR against `master`.

Baseline (2026-09-12): `mvn test -Dtest='GrpoPolicyScorerTest,FeatureCacheStatsTest,MovieLensServingSideEffectsTest,RecommendationMeasurementServiceTest'`
→ recorded in Task 1 Step 2 below.

---

### Task 1: The cache and its configuration

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/FeatureCache.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java:147-160`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml:25-29`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/model/FeatureCacheStatsTest.java`

**Interfaces:**
- Produces: `FeatureCache.getGrpoWeights(String key): double[]` (null when not cached), `FeatureCache.putGrpoWeights(String key, double[] weights)`, stats entry `grpo_weights`, `RecommendationProperties.Cache.getGrpoWeightsTtlSeconds(): long` (default 10).

- [ ] **Step 1: Write the failing tests**

In `FeatureCacheStatsTest`, change `assertEquals(2, stats.size());` to `assertEquals(3, stats.size());` and add after `reportsZeroedStatsBeforeAnyLookup`:

```java
    @Test
    void countsGrpoWeightLookupsSeparately() {
        FeatureCache cache = new FeatureCache(new RecommendationProperties());

        cache.getGrpoWeights("absent");
        cache.putGrpoWeights("key", new double[] {0.5});
        cache.getGrpoWeights("key");

        FeatureCache.CacheStatsView weights = cache.stats().get("grpo_weights");
        assertEquals(1L, weights.hitCount());
        assertEquals(1L, weights.missCount());
        assertEquals(0L, cache.stats().get("reward_stats").missCount());
    }
```

- [ ] **Step 2: Run the baseline and confirm the new test fails to compile**

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn test -Dtest='GrpoPolicyScorerTest,FeatureCacheStatsTest,MovieLensServingSideEffectsTest,RecommendationMeasurementServiceTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `COMPILATION ERROR` naming `getGrpoWeights`. (The pre-edit baseline for these four classes is run before Step 1 and recorded in the verification record.)

- [ ] **Step 3: Add the property, the YAML line, and the cache**

In `RecommendationProperties.Cache`, add after `private long rewardTtlSeconds = 30;`:

```java
        private long grpoWeightsTtlSeconds = 10;
```

and after the `setRewardTtlSeconds` line:

```java
        public long getGrpoWeightsTtlSeconds() { return grpoWeightsTtlSeconds; }
        public void setGrpoWeightsTtlSeconds(long grpoWeightsTtlSeconds) { this.grpoWeightsTtlSeconds = grpoWeightsTtlSeconds; }
```

In `application.yml`, after `reward-ttl-seconds: ${RECSYS_REWARD_TTL:30}` add:

```yaml
    grpo-weights-ttl-seconds: ${RECSYS_GRPO_WEIGHTS_TTL:10}
```

In `FeatureCache`, extend the class doc with one line after the reward-stats line:

```java
 * GRPO policy weights (grpo:policy:weights): rewritten once per training micro-batch; the TTL matches that trigger.
```

add the field `private final Cache<String, double[]> grpoWeights;`, build it in the constructor after `rewardStats`:

```java
        this.grpoWeights = Caffeine.newBuilder()
            .maximumSize(1)   // one fixed key: GrpoPolicyScorer.WEIGHTS_KEY
            .expireAfterWrite(cfg.getGrpoWeightsTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
```

add after `invalidateRewardStats`:

```java
    // --- GRPO policy weights ---

    /** The parsed weight vector, an empty array when Redis held nothing usable, or null when not cached. */
    public double[] getGrpoWeights(String key) {
        return grpoWeights.getIfPresent(key);
    }

    public void putGrpoWeights(String key, double[] weights) {
        grpoWeights.put(key, weights != null ? weights : new double[0]);
    }
```

and in `stats()` add `values.put("grpo_weights", view(grpoWeights));` after the `reward_stats` line.

- [ ] **Step 4: Run the stats test**

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=... mvn test -Dtest=FeatureCacheStatsTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 4, Failures: 0`.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/FeatureCache.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/model/FeatureCacheStatsTest.java
git commit -m "feat(cache): a TTL cache for the GRPO policy weight vector"
```

---

### Task 2: The scorer reads through the cache

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java`
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java:134`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java`
- Test: `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java`

**Interfaces:**
- Consumes: `FeatureCache.getGrpoWeights` / `putGrpoWeights` from Task 1.
- Produces: `GrpoPolicyScorer(StringRedisTemplate, RecommendationProperties, FeatureCache)`.

- [ ] **Step 1: Update the harnesses and write the two failing tests**

In `GrpoPolicyScorerTest`, add `import com.demo.retrieval.model.FeatureCache;` and `import static org.mockito.Mockito.times;`, change the harness line `return new GrpoPolicyScorer(redis, properties);` to `return new GrpoPolicyScorer(redis, properties, new FeatureCache(properties));`, change the two direct constructions in `reRankOrderNeverThrowsWhenRedisFails` and `aFailingSlateNeverThrowsIntoTheServingPath` the same way, and add after `aNonFiniteWeightIsUnusable`:

```java
    @Test
    void weightsAreReadFromRedisOncePerTtlWindow() {
        // The vector changes once per training micro-batch; re-reading it on every slate is what
        // the feature cache exists to stop.
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v2");
        s.recordShadowSlate(twoItemSlate());
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, times(1)).opsForHash();
    }

    @Test
    void anAbsentVectorIsNotReReadWithinTheTtl() {
        GrpoPolicyScorer s = scorer("shadow", null, "v2");
        s.recordShadowSlate(twoItemSlate());
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, times(1)).opsForHash();
    }
```

In `MovieLensServingSideEffectsTest`, add `import com.demo.retrieval.model.FeatureCache;` and change `offScorer()` to:

```java
    private static GrpoPolicyScorer offScorer() {
        RecommendationProperties properties = new RecommendationProperties();
        return new GrpoPolicyScorer(mock(StringRedisTemplate.class), properties, new FeatureCache(properties));
    }
```

- [ ] **Step 2: Run to verify compilation fails on the new constructor**

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=... mvn test -Dtest=GrpoPolicyScorerTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `COMPILATION ERROR` on the three-argument constructor.

- [ ] **Step 3: Implement the cache-first read and wire it**

In `GrpoPolicyScorer`, add `import com.demo.retrieval.model.FeatureCache;`, the field `private final FeatureCache featureCache;`, and change the constructor to:

```java
    public GrpoPolicyScorer(StringRedisTemplate redis, RecommendationProperties properties, FeatureCache featureCache) {
        this.redis = redis;
        this.featureCache = featureCache;
```

(keep the rest of the constructor body). Replace `readWeights` with:

```java
    /**
     * The usable weight vector, read through the feature cache: the vector changes once per
     * training micro-batch, so it is re-read from Redis at most once per
     * recsys.cache.grpo-weights-ttl-seconds. An absent or rejected vector is cached as an empty
     * array for the same window rather than re-read on every request.
     */
    private Optional<double[]> readWeights() {
        double[] cached = featureCache.getGrpoWeights(WEIGHTS_KEY);
        if (cached == null) {
            cached = parseWeights(redis.opsForHash().entries(WEIGHTS_KEY)).orElseGet(() -> new double[0]);
            featureCache.putGrpoWeights(WEIGHTS_KEY, cached);
        }
        return cached.length == 0 ? Optional.empty() : Optional.of(cached);
    }

    private static Optional<double[]> parseWeights(Map<Object, Object> raw) {
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
```

followed by the existing body from `Object version = raw.get("feature_version");` through `return Optional.of(w);` and the closing brace, unchanged.

In `HybridRecommendationService`, change `this.grpoPolicyScorer = new GrpoPolicyScorer(redis, properties);` to `this.grpoPolicyScorer = new GrpoPolicyScorer(redis, properties, featureCache);`.

- [ ] **Step 4: Run the four affected classes**

Run: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=... mvn test -Dtest='GrpoPolicyScorerTest,FeatureCacheStatsTest,MovieLensServingSideEffectsTest,RecommendationMeasurementServiceTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: all pass; `GrpoPolicyScorerTest` reports 31 (29 + 2).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java
git commit -m "feat(grpo): read the policy weights through the feature cache"
```

---

### Task 3: Docs, full suite, records, and PR

**Files:**
- Modify: `recsys-pipeline/README.md:91,344`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md:64,1011`
- Modify: `.superpowers/docs/specs/2026-09-12-grpo-weights-cache-design.md` (status and verification record)
- Modify: this plan (check boxes, record observations)

- [ ] **Step 1: Documentation**

In `README.md` after the `recsys.cache.reward-ttl-seconds` table row add:

```
| `recsys.cache.grpo-weights-ttl-seconds` | `RECSYS_GRPO_WEIGHTS_TTL` | `10` | GRPO policy weight vector TTL; matches the training job's trigger interval |
```

In both `README.md` and `Data_Pipeline.md`, in the in-memory (Caffeine) storage row, change `reward model stats (`reward-model:*`)` to `reward model stats (`reward-model:*`), GRPO policy weights (`grpo:policy:weights`)`.

In `Data_Pipeline.md`, after the sentence ending `and `GrpoPolicyScorer` ignores\none it reads back.` add a paragraph:

```
`GrpoPolicyScorer` reads the vector through `FeatureCache` with a
`recsys.cache.grpo-weights-ttl-seconds` TTL (`RECSYS_GRPO_WEIGHTS_TTL`, default `10`, the training
job's trigger interval), so a new batch's weights reach serving within one TTL rather than on the
next request. An absent or rejected vector is cached the same way and is not re-read until the TTL
expires. `off` mode still reads nothing.
```

- [ ] **Step 2: Run the full retrieval Maven suite**

Run in the background: `cd recsys-pipeline/services/java-retrieval-service && JAVA_HOME=... mvn test`
Expected: `BUILD SUCCESS`; `UserProfileIntegrationTest` skipped as always. If unrelated tests fail, confirm they fail on `origin/master` too before reporting.

- [ ] **Step 3: Update the spec status and verification record, tick this plan, and commit**

Set the spec's status line to `Implemented and verified; PR pending` and add a `## Verification record` with the baseline and final counts.

```bash
git add recsys-pipeline/README.md recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md .superpowers/docs/specs/2026-09-12-grpo-weights-cache-design.md .superpowers/docs/plans/2026-09-12-grpo-weights-cache.md
git commit -m "docs: GRPO weights cache TTL and verification record"
```

- [ ] **Step 4: Open the PR**

```bash
git push -u origin feat/grpo-weights-cache
gh pr create --base master --title "feat(grpo): read the policy weights through the feature cache" --body "$(cat <<'EOF'
## Summary
- The online GRPO design specified that the serving scorer reads the weight vector through `FeatureCache` on the reward-model TTL pattern; the shipped scorer read Redis on every shadow slate and every `on`-mode request.
- Add a third Caffeine cache, `grpo_weights`, with `recsys.cache.grpo-weights-ttl-seconds` (`RECSYS_GRPO_WEIGHTS_TTL`, default 10 s, the training job's trigger interval). The scorer reads through it; an absent or rejected vector is cached as empty for the same window.
- Behavior change: a new batch's weights reach serving within one TTL instead of on the next request. `off` mode still reads nothing. Guards and the failure contract are unchanged.

## Test plan
- [x] New: two slates within the TTL cause one Redis read; an absent vector is not re-read; GRPO weight lookups count separately in `FeatureCache.stats()`.
- [x] `GrpoPolicyScorerTest` 31, `FeatureCacheStatsTest` 4, `MovieLensServingSideEffectsTest` 3, `RecommendationMeasurementServiceTest` unchanged.
- [x] Full retrieval Maven suite: <fill from Step 2>.

Spec: `.superpowers/docs/specs/2026-09-12-grpo-weights-cache-design.md`
Plan: `.superpowers/docs/plans/2026-09-12-grpo-weights-cache.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Then record the PR link in the spec's status line and commit it with `docs: link GRPO weights cache PR`.

---

## Self-review

- Spec coverage: cache and config (Task 1), scorer and wiring (Task 2), docs, suite, PR (Task 3). Acceptance criteria 1-5 map to Task 2 Step 4, Task 1 Step 4, Task 2 Step 4, the existing `offModeReadsNothingFromRedis` test in Task 2 Step 4, Task 3 Step 2.
- Placeholders: one `<fill from Step 2>` in the PR body supplied by Task 3 Step 2; `JAVA_HOME=...` abbreviates the path stated in the execution notes.
- Names: `getGrpoWeights`, `putGrpoWeights`, `grpo_weights`, `getGrpoWeightsTtlSeconds`, `parseWeights`, `twoItemSlate`, `weightsOnBanditScoreOnly` are consistent with the spec and the existing test harness.
