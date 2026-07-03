# Simplify DL MLP Score Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the ONNX score decoder and skip the DL model run when `deep-learning-weight` is 0, without changing ranking.

**Architecture:** D1 makes `readScore` delegate to the existing `normalizeBatchScores` (one decoder). D2 guards `predictBatch` + the two-tower fusion in `HybridRecommendationService` on a positive `deep-learning-weight`, so the ONNX model doesn't run for a 0-weighted result by default.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Mockito, Maven, ONNX Runtime.

## Global Constraints

- Spec: [docs/specs/2026-07-02-simplify-dl-score.md](../specs/2026-07-02-simplify-dl-score.md).
- No ranking change; the end-to-end tests are the oracle.
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Modify: `service/DeepLearningPredictionService.java` — `readScore` delegates to `normalizeBatchScores`.
- Modify: `service/HybridRecommendationService.java` — guard the DL block on `deep-learning-weight > 0`.
- Modify (test): `service/HybridRecommendationServiceTest.java` — assert `predictBatch` is skipped at the default weight.

---

### Task 1 (D1): Unify the ONNX score decoder

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/DeepLearningPredictionService.java`
- Test (oracle, unchanged): `src/test/java/com/demo/retrieval/service/DeepLearningPredictionServiceTest.java`

- [ ] **Step 1: Baseline green.**

Run: `mvn -o test -Dtest=DeepLearningPredictionServiceTest`
Expected: PASS (the classpath-model `predict` tests exercise `readScore`; `normalizeBatchScoresPadsAndTruncatesToN` covers the decoder).

- [ ] **Step 2: Replace `readScore` with a delegate.** Replace the whole method:

```java
    private double readScore(OnnxValue value) throws OrtException {
        Object raw = value.getValue();
        if (raw instanceof float[][] scores && scores.length > 0 && scores[0].length > 0) {
            return scores[0][0];
        }
        if (raw instanceof float[] scores && scores.length > 0) {
            return scores[0];
        }
        if (raw instanceof double[][] scores && scores.length > 0 && scores[0].length > 0) {
            return scores[0][0];
        }
        if (raw instanceof double[] scores && scores.length > 0) {
            return scores[0];
        }
        throw new IllegalStateException("Unsupported prediction output shape: " + raw.getClass().getName());
    }
```

with:

```java
    private double readScore(OnnxValue value) throws OrtException {
        return normalizeBatchScores(value.getValue(), 1)[0];
    }
```

- [ ] **Step 3: Run — expect PASS** (single decoder now serves both paths).

Run: `mvn -o test -Dtest=DeepLearningPredictionServiceTest`
Expected: PASS. `predict("user_employee_01", "action_benefits")` still returns a score via `normalizeBatchScores(raw, 1)[0]`.

- [ ] **Step 4: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/DeepLearningPredictionService.java
git commit -m "refactor(dl): unify readScore into normalizeBatchScores decoder"
```

---

### Task 2 (D2): Skip DL inference when the weight is 0

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
- Test: `src/test/java/com/demo/retrieval/service/HybridRecommendationServiceTest.java`

- [ ] **Step 1: Guard the DL block.** Replace the current block:

```java
        Map<String, Double> dlScoresRaw = predictionService.predictBatch(user, eligibleList);
        if (twoTowerPredictionService.isEnabled()) {
            Map<String, Double> twoTowerScores = twoTowerPredictionService.predictBatch(user, eligibleList);
            if (!twoTowerScores.isEmpty()) {
                Map<String, Double> merged = new HashMap<>();
                dlScoresRaw.forEach((item, score) -> merged.put(item, RecommendationConstants.clamp(score)));
                twoTowerScores.forEach((item, score) ->
                    merged.merge(item, RecommendationConstants.clamp(score), Math::max));
                dlScoresRaw = Map.copyOf(merged);
            }
        }
```

with (guard both the model run and the fusion on a positive weight):

```java
        boolean deepLearningEnabled = properties.getBandit().getDeepLearningWeight() > 0.0;
        Map<String, Double> dlScoresRaw = deepLearningEnabled
            ? predictionService.predictBatch(user, eligibleList)
            : Map.of();
        if (deepLearningEnabled && twoTowerPredictionService.isEnabled()) {
            Map<String, Double> twoTowerScores = twoTowerPredictionService.predictBatch(user, eligibleList);
            if (!twoTowerScores.isEmpty()) {
                Map<String, Double> merged = new HashMap<>();
                dlScoresRaw.forEach((item, score) -> merged.put(item, RecommendationConstants.clamp(score)));
                twoTowerScores.forEach((item, score) ->
                    merged.merge(item, RecommendationConstants.clamp(score), Math::max));
                dlScoresRaw = Map.copyOf(merged);
            }
        }
```

- [ ] **Step 2: Add the skip assertion to `HybridRecommendationServiceTest`.** Ensure these static imports exist (add any missing):

```java
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
```

At the end of `recommendsFreshMoviesFromMovieLensBehaviorSignals` (after the existing assertions; the properties there use the default `deep-learning-weight` of `0.0`), add:

```java
        verify(predictionService, never()).predictBatch(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
```

- [ ] **Step 3: Run the test.**

Run: `mvn -o test -Dtest=HybridRecommendationServiceTest`
Expected: PASS. The `["fresh"]` recommendation assertion proves ranking is unchanged; the new `never()` verify proves `predictBatch` is skipped at weight `0.0`.

- [ ] **Step 4: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, all tests green (`RecommendationControllerTest` is the end-to-end oracle).

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java src/test/java/com/demo/retrieval/service/HybridRecommendationServiceTest.java
git commit -m "perf(dl): skip ONNX inference when deep-learning-weight is 0"
```

---

### Task 3: Branch, docs, push, PR

- [ ] **Step 1: Branch off master (do Tasks 1–2 on it), then commit the docs.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/simplify-dl-score
git add docs/specs/2026-07-02-simplify-dl-score.md docs/plans/2026-07-02-simplify-dl-score.md
git commit -m "docs(dl): spec + plan for DL-score simplification"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/simplify-dl-score
gh pr create --base master --title "Simplify DL MLP score: unify ONNX decoder + skip inference when disabled" --body "See docs/specs/2026-07-02-simplify-dl-score.md. D1: readScore delegates to normalizeBatchScores (one ONNX decoder). D2: skip predictBatch + two-tower fusion when deep-learning-weight is 0 (the default) so the model does not run for a 0-weighted result. Ranking unchanged; disabled-path diagnostic deepLearningScore reads 0.0. mvn test green.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

- **Spec coverage:** D1→Task 1; D2→Task 2; artifacts+PR→Task 3. Covered.
- **Placeholders:** none — full before/after code shown.
- **Type consistency:** `normalizeBatchScores(Object, int)` (existing static) called with `(value.getValue(), 1)`; `properties.getBandit().getDeepLearningWeight()` returns `double`; `verify(predictionService, never()).predictBatch(...)` matches the mocked `DeepLearningPredictionService`.
- **Oracle:** `recommendsFreshMoviesFromMovieLensBehaviorSignals` + `RecommendationControllerTest` guard the no-ranking-change requirement.
