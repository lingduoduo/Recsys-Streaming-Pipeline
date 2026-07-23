# Tidy Bandit Exploration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `computeBanditArmScore` readable (named UCB/Thompson helpers, one `clamp`, a cold-start comment) and add a pin test, with no exploration-behavior change.

**Architecture:** Extract `ucbExplorationBonus` and `thompsonArmScore` from `computeBanditArmScore` (verbatim logic); document the cold-start double-boost; make the method + `BanditArmScore` record package-private and add `BanditExplorationTest`.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Mockito, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-03-simplify-bandit-exploration.md](../specs/2026-07-03-simplify-bandit-exploration.md).
- Behavior-preserving: UCB/Thompson output identical for the same inputs.
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Modify: `service/HybridRecommendationService.java` — refactor `computeBanditArmScore`, add two helpers, cold-start comment, package-private visibility.
- Create (test): `service/BanditExplorationTest.java`.

---

### Task 1: Refactor `computeBanditArmScore` (B1 + B2 + visibility)

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`

**Interfaces:**
- Produces (package-private): `BanditArmScore computeBanditArmScore(double, long, long, long, boolean)`; `record BanditArmScore(double posteriorMean, double explorationBonus, double rankingScore)`. New private helpers `ucbExplorationBonus(long, long, double, boolean)` and `thompsonArmScore(double, double, double)`.

- [ ] **Step 1: Replace the method** (drop `private`, extract helpers, add the comment, one `clamp`). Replace:

```java
    private BanditArmScore computeBanditArmScore(
        double baseScore,
        long itemImpressions,
        long clicks,
        long totalImpressions,
        boolean coldStart
    ) {
        String algorithm = currentAlgorithm();
        long failures = Math.max(itemImpressions - clicks, 0L);
        double priorStrength = coldStart
            ? Math.max(WARM_PRIOR_STRENGTH,
                properties.getBandit().getColdStartBoost() * RecommendationConstants.COLD_START_PRIOR_STRENGTH_MULTIPLIER)
            : WARM_PRIOR_STRENGTH;
        double priorAlpha = 1.0 + (clamp(baseScore) * priorStrength);
        double priorBeta = 1.0 + ((1.0 - clamp(baseScore)) * priorStrength);
        double posteriorAlpha = priorAlpha + clicks;
        double posteriorBeta = priorBeta + failures;
        double posteriorMean = clamp(posteriorAlpha / (posteriorAlpha + posteriorBeta));

        // Thompson: the ranking score IS the posterior sample drawn below, not
        // posteriorMean + bonus. The explorationMagnitude (|sample - mean|) is a
        // reported diagnostic only. The additive "mean + explorationBonus" form is
        // literal for UCB (below), not for Thompson.
        if ("thompson".equals(algorithm)) {
            double sampledPosterior = clamp(sampleBeta(posteriorAlpha, posteriorBeta));
            double explorationMagnitude = Math.min(
                Math.abs(sampledPosterior - posteriorMean),
                properties.getBandit().getMaxExplorationBonus()
            );
            return new BanditArmScore(posteriorMean, explorationMagnitude, sampledPosterior);
        }

        double effectivePulls = itemImpressions + priorStrength;
        double confidence = Math.sqrt(Math.log(totalImpressions + 2.0) / (2.0 * (effectivePulls + 1.0)));
        double bonus = properties.getBandit().getExplorationAlpha() * confidence;
        if (coldStart) {
            bonus *= properties.getBandit().getColdStartBoost();
        }
        bonus = Math.min(bonus, properties.getBandit().getMaxExplorationBonus());
        return new BanditArmScore(posteriorMean, bonus, posteriorMean + bonus);
    }
```

with:

```java
    BanditArmScore computeBanditArmScore(
        double baseScore,
        long itemImpressions,
        long clicks,
        long totalImpressions,
        boolean coldStart
    ) {
        long failures = Math.max(itemImpressions - clicks, 0L);
        // Cold-start raises priorStrength, which also raises effectivePulls in ucbExplorationBonus and thus
        // SHRINKS the UCB confidence bonus; coldStartBoost then multiplies that bonus back up, so the two
        // effects nearly cancel — cold-start's net UCB boost is small. Left as-is (tuning it is behavior-changing).
        double priorStrength = coldStart
            ? Math.max(WARM_PRIOR_STRENGTH,
                properties.getBandit().getColdStartBoost() * RecommendationConstants.COLD_START_PRIOR_STRENGTH_MULTIPLIER)
            : WARM_PRIOR_STRENGTH;
        double base = clamp(baseScore);
        double priorAlpha = 1.0 + (base * priorStrength);
        double priorBeta = 1.0 + ((1.0 - base) * priorStrength);
        double posteriorAlpha = priorAlpha + clicks;
        double posteriorBeta = priorBeta + failures;
        double posteriorMean = clamp(posteriorAlpha / (posteriorAlpha + posteriorBeta));

        if ("thompson".equals(currentAlgorithm())) {
            return thompsonArmScore(posteriorAlpha, posteriorBeta, posteriorMean);
        }
        double bonus = ucbExplorationBonus(itemImpressions, totalImpressions, priorStrength, coldStart);
        return new BanditArmScore(posteriorMean, bonus, posteriorMean + bonus);
    }

    // Thompson: the ranking score IS the posterior sample; the reported explorationBonus
    // (|sample - mean|, capped) is a diagnostic only. "mean + bonus" is literal for UCB, not Thompson.
    private BanditArmScore thompsonArmScore(double posteriorAlpha, double posteriorBeta, double posteriorMean) {
        double sampledPosterior = clamp(sampleBeta(posteriorAlpha, posteriorBeta));
        double explorationMagnitude = Math.min(
            Math.abs(sampledPosterior - posteriorMean),
            properties.getBandit().getMaxExplorationBonus()
        );
        return new BanditArmScore(posteriorMean, explorationMagnitude, sampledPosterior);
    }

    private double ucbExplorationBonus(long itemImpressions, long totalImpressions, double priorStrength, boolean coldStart) {
        double effectivePulls = itemImpressions + priorStrength;
        double confidence = Math.sqrt(Math.log(totalImpressions + 2.0) / (2.0 * (effectivePulls + 1.0)));
        double bonus = properties.getBandit().getExplorationAlpha() * confidence;
        if (coldStart) {
            bonus *= properties.getBandit().getColdStartBoost();
        }
        return Math.min(bonus, properties.getBandit().getMaxExplorationBonus());
    }
```

- [ ] **Step 2: Make the `BanditArmScore` record package-private.** Change `private record BanditArmScore(` to `record BanditArmScore(`.

- [ ] **Step 3: Compile + full suite (behavior unchanged via oracle).**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`; `RecommendationControllerTest` + `HybridRecommendationServiceTest` green.

- [ ] **Step 4: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "refactor(bandit): extract ucbExplorationBonus + thompsonArmScore, document cold-start boost"
```

---

### Task 2: Add `BanditExplorationTest` (B3)

**Files:**
- Create: `src/test/java/com/demo/retrieval/service/BanditExplorationTest.java`

**Interfaces:**
- Consumes: package-private `computeBanditArmScore` + `BanditArmScore` from Task 1.

- [ ] **Step 1: Write the test.**

```java
package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "null"})
class BanditExplorationTest {

    private HybridRecommendationService service(RecommendationProperties properties) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        FeatureCache featureCache = new FeatureCache(properties);
        DeepLearningPredictionService dl = mock(DeepLearningPredictionService.class);
        TwoTowerPredictionService twoTower = mock(TwoTowerPredictionService.class);
        when(twoTower.isEnabled()).thenReturn(false);
        return new HybridRecommendationService(
            redis, properties, dl,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of(), twoTower);
    }

    @Test
    void ucbBonusFollowsConfidenceFormulaWhenUncapped() {
        HybridRecommendationService svc = service(new RecommendationProperties());
        // base 0.5, 100 impressions, 50 clicks -> posteriorMean 0.5; effectivePulls 102 (priorStrength 2)
        var score = svc.computeBanditArmScore(0.5, 100, 50, 1000, false);
        double expectedBonus = 0.75 * Math.sqrt(Math.log(1002.0) / (2.0 * 103.0)); // ~0.1373596, < cap 0.25
        assertEquals(0.5, score.posteriorMean(), 1e-9);
        assertEquals(expectedBonus, score.explorationBonus(), 1e-9);
        assertEquals(0.5 + expectedBonus, score.rankingScore(), 1e-9);
    }

    @Test
    void ucbBonusIsCappedAtMaxExplorationBonus() {
        HybridRecommendationService svc = service(new RecommendationProperties());
        // base 0.5, 1 impression, 0 clicks -> posteriorMean 0.4; raw bonus > 0.25 -> capped
        var score = svc.computeBanditArmScore(0.5, 1, 0, 2, false);
        assertEquals(0.4, score.posteriorMean(), 1e-9);
        assertEquals(0.25, score.explorationBonus(), 1e-9);
        assertEquals(0.65, score.rankingScore(), 1e-9);
    }

    @Test
    void thompsonReturnsPosteriorSampleWithinBounds() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("thompson");
        HybridRecommendationService svc = service(properties);
        var score = svc.computeBanditArmScore(0.5, 10, 5, 100, false);
        assertEquals(0.5, score.posteriorMean(), 1e-9);
        assertTrue(score.rankingScore() >= 0.0 && score.rankingScore() <= 1.0);
        assertTrue(score.explorationBonus() <= 0.25 + 1e-9);
    }
}
```

- [ ] **Step 2: Run the test.**

Run: `mvn -o test -Dtest=BanditExplorationTest`
Expected: PASS (3 tests).

- [ ] **Step 3: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, all green (56 tests: 53 + 3).

- [ ] **Step 4: Commit.**

```bash
git add src/test/java/com/demo/retrieval/service/BanditExplorationTest.java
git commit -m "test(bandit): pin UCB confidence bonus + cap and Thompson sample bounds"
```

---

### Task 3: Branch, docs, push, PR

- [ ] **Step 1: Branch off master (do Tasks 1–2 on it), then commit the docs.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/simplify-bandit-exploration
git add docs/specs/2026-07-03-simplify-bandit-exploration.md docs/plans/2026-07-03-simplify-bandit-exploration.md
git commit -m "docs(bandit): spec + plan for exploration tidy"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/simplify-bandit-exploration
gh pr create --base master --title "Tidy UCB/Thompson bandit exploration + pin test" --body "See docs/specs/2026-07-03-simplify-bandit-exploration.md. Extracts ucbExplorationBonus + thompsonArmScore from computeBanditArmScore, de-dupes clamp(baseScore), and documents the cold-start double-boost near-cancellation (left in place). Adds BanditExplorationTest pinning UCB (uncapped + capped) and Thompson bounds. Behavior-preserving; oracle green. mvn test: 56 passing."
```

---

## Self-Review

- **Spec coverage:** B1→Task 1 Step 1; B2→Task 1 Step 1 (comment); B3→Task 1 Step 2 (visibility) + Task 2; artifacts+PR→Task 3. Covered.
- **Placeholders:** none — full before/after code + concrete test values.
- **Type consistency:** `BanditArmScore(posteriorMean, explorationBonus, rankingScore)` accessors used exactly in the test; `computeBanditArmScore(double, long, long, long, boolean)` signature unchanged (only visibility); helpers `ucbExplorationBonus(long, long, double, boolean)` / `thompsonArmScore(double, double, double)` match their call sites.
- **Determinism note:** the Thompson test asserts only `posteriorMean` (exact) and bounds — the sample is random (`ThreadLocalRandom`), so no exact value is asserted.
- **Test math:** UCB uncapped `0.75·√(ln1002/206) ≈ 0.1373596` (< 0.25); capped case raw bonus `0.75·√(ln4/8) ≈ 0.3122` → `0.25`, mean `2/5 = 0.4`, ranking `0.65`.
