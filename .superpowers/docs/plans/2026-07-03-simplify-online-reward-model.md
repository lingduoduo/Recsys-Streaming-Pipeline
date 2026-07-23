# Simplify Online Reward Model Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the repetition in `OnlineLearningService` (four blend blocks, duplicated stats decode, local clamp) and add its missing unit test, without changing `onlineScore`.

**Architecture:** `score()` folds a `(configWeight, estimate)` list instead of four copy-pasted blocks; one `parseStats` helper replaces two inline decodes; `RecommendationConstants.clamp` replaces the local one. A new `OnlineLearningServiceTest` pins the confidence-weighted blend.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Mockito, Maven, Caffeine.

## Global Constraints

- Spec: [docs/specs/2026-07-03-simplify-online-reward-model.md](../specs/2026-07-03-simplify-online-reward-model.md).
- Behavior-preserving: `score()` output byte-identical; `reward-model:*` keys and `normalize` untouched (cross-language contract with the Python pipeline).
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Modify: `service/OnlineLearningService.java` — S1 blend loop, S2 `parseStats`, S3 clamp reuse.
- Create (test): `service/OnlineLearningServiceTest.java` — blend + fallback.

---

### Task 1: DRY OnlineLearningService (S1 + S2 + S3)

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/OnlineLearningService.java`

**Interfaces:**
- Produces (private): `record WeightedEstimate(double configWeight, RewardEstimate estimate)`; `FeatureCache.RewardModelStats parseStats(Map<Object,Object> raw)`.

- [ ] **Step 1: Add the `java.util.ArrayList` import** (needed by the new `score()`). Add near the other `java.util` imports:

```java
import java.util.ArrayList;
```

- [ ] **Step 2: Replace `score()` (S1)** with the folded version + the `WeightedEstimate` record. Replace the whole method:

```java
    public double score(String itemId, MovieProfile profile, double fallback) {
        double weightedReward = 0.0;
        double totalWeight = 0.0;

        RewardEstimate global = readRewardEstimate(GLOBAL_KEY);
        if (global.count() > 0) {
            double weight = properties.getRewardModel().getGlobalWeight() * confidence(global.count());
            weightedReward += global.mean() * weight;
            totalWeight += weight;
        }

        RewardEstimate item = readRewardEstimate(ITEM_PREFIX + itemId);
        if (item.count() > 0) {
            double weight = properties.getRewardModel().getItemWeight() * confidence(item.count());
            weightedReward += item.mean() * weight;
            totalWeight += weight;
        }

        if (profile != null) {
            RewardEstimate genre = aggregateFeatureEstimates(GENRE_PREFIX, normalize(profile.getGenres()));
            if (genre.count() > 0) {
                double weight = properties.getRewardModel().getGenreWeight() * confidence(genre.count());
                weightedReward += genre.mean() * weight;
                totalWeight += weight;
            }

            RewardEstimate tag = aggregateFeatureEstimates(TAG_PREFIX, normalize(profile.getTags()));
            if (tag.count() > 0) {
                double weight = properties.getRewardModel().getTagWeight() * confidence(tag.count());
                weightedReward += tag.mean() * weight;
                totalWeight += weight;
            }
        }

        return totalWeight == 0.0 ? clamp(fallback) : clamp(weightedReward / totalWeight);
    }
```

with:

```java
    public double score(String itemId, MovieProfile profile, double fallback) {
        RecommendationProperties.RewardModel cfg = properties.getRewardModel();
        List<WeightedEstimate> contributions = new ArrayList<>();
        contributions.add(new WeightedEstimate(cfg.getGlobalWeight(), readRewardEstimate(GLOBAL_KEY)));
        contributions.add(new WeightedEstimate(cfg.getItemWeight(), readRewardEstimate(ITEM_PREFIX + itemId)));
        if (profile != null) {
            contributions.add(new WeightedEstimate(cfg.getGenreWeight(),
                aggregateFeatureEstimates(GENRE_PREFIX, normalize(profile.getGenres()))));
            contributions.add(new WeightedEstimate(cfg.getTagWeight(),
                aggregateFeatureEstimates(TAG_PREFIX, normalize(profile.getTags()))));
        }

        double weightedReward = 0.0;
        double totalWeight = 0.0;
        for (WeightedEstimate c : contributions) {
            if (c.estimate().count() > 0) {
                double weight = c.configWeight() * confidence(c.estimate().count());
                weightedReward += c.estimate().mean() * weight;
                totalWeight += weight;
            }
        }
        return totalWeight == 0.0 ? RecommendationConstants.clamp(fallback)
            : RecommendationConstants.clamp(weightedReward / totalWeight);
    }

    private record WeightedEstimate(double configWeight, RewardEstimate estimate) {
    }
```

- [ ] **Step 3: Add `parseStats` (S2)** and use it in `batchWarmRewardStats`. Add the helper (near `readRewardEstimate`):

```java
    private FeatureCache.RewardModelStats parseStats(Map<Object, Object> raw) {
        return new FeatureCache.RewardModelStats(
            raw == null ? 0L : readLong(raw.get("count")),
            raw == null ? 0.0 : readDouble(raw.get("reward_total")));
    }
```

In `batchWarmRewardStats`, replace the decode loop body:

```java
        for (int i = 0; i < Math.min(cold.size(), results.size()); i++) {
            Map<Object, Object> raw = (Map<Object, Object>) results.get(i);
            long count = raw == null ? 0L : readLong(raw.get("count"));
            double rewardTotal = raw == null ? 0.0 : readDouble(raw.get("reward_total"));
            featureCache.putRewardStats(cold.get(i), new FeatureCache.RewardModelStats(count, rewardTotal));
        }
```

with:

```java
        for (int i = 0; i < Math.min(cold.size(), results.size()); i++) {
            featureCache.putRewardStats(cold.get(i), parseStats((Map<Object, Object>) results.get(i)));
        }
```

- [ ] **Step 4: Use `parseStats` in `readRewardEstimate`.** Replace its Redis-miss body:

```java
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(key)).orElseGet(Map::of);
        long count = readLong(raw.get("count"));
        double rewardTotal = readDouble(raw.get("reward_total"));
        featureCache.putRewardStats(key, new FeatureCache.RewardModelStats(count, rewardTotal));
        return count == 0L ? new RewardEstimate(0L, 0.0) : new RewardEstimate(count, clamp(rewardTotal / count));
```

with:

```java
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(key)).orElseGet(Map::of);
        FeatureCache.RewardModelStats stats = parseStats(raw);
        featureCache.putRewardStats(key, stats);
        return stats.count() == 0L
            ? new RewardEstimate(0L, 0.0)
            : new RewardEstimate(stats.count(), RecommendationConstants.clamp(stats.rewardTotal() / stats.count()));
```

Also update the cache-hit branch of `readRewardEstimate` to use `RecommendationConstants.clamp` (replacing local `clamp`):

```java
        if (cached != null) {
            return cached.count() == 0L
                ? new RewardEstimate(0L, 0.0)
                : new RewardEstimate(cached.count(), RecommendationConstants.clamp(cached.rewardTotal() / cached.count()));
        }
```

- [ ] **Step 5: Delete the local `clamp` (S3).** Remove:

```java
    private double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }
```

Run: `rg -n "\bclamp\(" src/main/java/com/demo/retrieval/service/OnlineLearningService.java`
Expected: only `RecommendationConstants.clamp(` occurrences remain (no bare `clamp(`).

- [ ] **Step 6: Compile.**

Run: `mvn -o test-compile`
Expected: `BUILD SUCCESS`.

- [ ] **Step 7: Full suite (behavior unchanged via the oracle).**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`; `RecommendationControllerTest` + `HybridRecommendationServiceTest` green.

- [ ] **Step 8: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/OnlineLearningService.java
git commit -m "refactor(online): fold reward blend loop, share stats parsing + clamp"
```

---

### Task 2: Add `OnlineLearningServiceTest`

**Files:**
- Create: `src/test/java/com/demo/retrieval/service/OnlineLearningServiceTest.java`

- [ ] **Step 1: Write the test.**

```java
package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.model.FeatureCache.RewardModelStats;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class OnlineLearningServiceTest {

    private OnlineLearningService serviceWith(FeatureCache cache, RecommendationProperties properties) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(any())).thenReturn(null);
        return new OnlineLearningService(redis, properties, cache);
    }

    @Test
    void emptyStatsFallBackToClampedFallback() {
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache cache = new FeatureCache(properties);
        OnlineLearningService service = serviceWith(cache, properties);

        assertEquals(0.7, service.score("m1", null, 0.7), 1e-9);
    }

    @Test
    void blendsGlobalAndItemRewardMeansByConfigWeights() {
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache cache = new FeatureCache(properties);
        // global: mean 0.8, weight 0.15 ; item: mean 0.5, weight 0.45 ; confidence(10)=1.0 (minFeatureCount 3)
        cache.putRewardStats("reward-model:global", new RewardModelStats(10, 8.0));
        cache.putRewardStats("reward-model:item:m1", new RewardModelStats(10, 5.0));
        OnlineLearningService service = serviceWith(cache, properties);

        // (0.8*0.15 + 0.5*0.45) / (0.15 + 0.45) = 0.345 / 0.6 = 0.575
        assertEquals(0.575, service.score("m1", null, 0.0), 1e-9);
    }
}
```

- [ ] **Step 2: Run the test.**

Run: `mvn -o test -Dtest=OnlineLearningServiceTest`
Expected: PASS (2 tests).

- [ ] **Step 3: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, all green (52 tests: prior 51 + 1 net new class of 2, minus none removed → confirm the printed total).

- [ ] **Step 4: Commit.**

```bash
git add src/test/java/com/demo/retrieval/service/OnlineLearningServiceTest.java
git commit -m "test(online): pin confidence-weighted reward blend + fallback"
```

---

### Task 3: Branch, docs, push, PR

- [ ] **Step 1: Branch off master (do Tasks 1–2 on it), then commit the docs.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/simplify-online-reward-model
git add docs/specs/2026-07-03-simplify-online-reward-model.md docs/plans/2026-07-03-simplify-online-reward-model.md
git commit -m "docs(online): spec + plan for reward-model simplification"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/simplify-online-reward-model
gh pr create --base master --title "Simplify OnlineLearningService: fold reward blend, share helpers, add test" --body "See docs/specs/2026-07-03-simplify-online-reward-model.md. Folds score()'s four identical blend blocks into one loop, extracts a shared parseStats helper (batchWarm + readRewardEstimate), reuses RecommendationConstants.clamp, and adds the missing OnlineLearningServiceTest. Behavior-preserving; reward-model:* keys and normalization untouched. mvn test green."
```

---

## Self-Review

- **Spec coverage:** S1→Task 1 Step 2; S2→Task 1 Steps 3–4; S3→Task 1 Steps 4–5; S4→Task 2; artifacts+PR→Task 3. Covered.
- **Placeholders:** none — full before/after code shown.
- **Type consistency:** `WeightedEstimate(double, RewardEstimate)` matches its use in `score()`; `parseStats(Map<Object,Object>) → FeatureCache.RewardModelStats` matches both call sites; `RecommendationConstants.clamp(double)` is the existing shared static (same package, no import needed); `RewardModelStats(long, double)` with `count()`/`rewardTotal()` matches the test.
- **Oracle:** `RecommendationControllerTest` + `HybridRecommendationServiceTest` exercise `score()` via `learnedPrior`, guarding the no-behavior-change requirement.
