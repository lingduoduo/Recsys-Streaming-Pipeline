# Consolidate Ranking Scores Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the redundant `outcomeProbability`/`overall()` outcome blend and name the `1.0` diversity default, leaving ranking unchanged.

**Architecture:** `weightedOutcome` is the one outcome blend that ranks; `overall()`/`outcomeProbability` is a dead sibling. Delete it from the scorer, the `ScoredCandidate` record, and the response surfacings. Replace the hardcoded `1.0` diversity slot with a named constant.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-03-consolidate-scores.md](../specs/2026-07-03-consolidate-scores.md).
- Ranking output unchanged: `predictionScore` and `finalScore` are identical for the same inputs (they use `weightedOutcome`, never `outcomeProbability`).
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Modify: `service/scorers/MovieLensOutcomeScorer.java` — drop `overall()` + `ScoringResult.outcomeProbability`; name the diversity default.
- Modify (test): `service/scorers/MovieLensOutcomeScorerTest.java` — drop the 2 dead-blend assertions.
- Modify: `service/HybridRecommendationService.java` — drop `ScoredCandidate.outcomeProbability` + its 2 constructions + 2 response surfacings.

---

### Task 1: Remove the dead blend from the scorer + name the diversity default

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorer.java`
- Modify: `src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java`

- [ ] **Step 1: Add the named diversity constant.** After the `DIVERSITY_FLOOR` field, add:

```java
    private static final double NO_DIVERSITY_MULTIPLIER = 1.0;
```

- [ ] **Step 2: Update `score()`** — drop `probabilities.overall()` and use the constant. Replace:

```java
        return new ScoringResult(
            estimatedReward,
            weightedOutcome,
            probabilities.overall(),
            predictionScore,
            1.0,
            predictionScore
        );
```

with:

```java
        return new ScoringResult(
            estimatedReward,
            weightedOutcome,
            predictionScore,
            NO_DIVERSITY_MULTIPLIER,
            predictionScore
        );
```

- [ ] **Step 3: Delete `overall()`** from `MovieLensOutcomeProbabilities`. Remove:

```java
        public double overall() {
            return clamp((positiveRating + preference + click + watch + novelDiscovery) / 5.0
                - 0.25 * negativeFeedback);
        }
```

(Leaving the record body empty: `) {\n    }`.)

- [ ] **Step 4: Remove the `outcomeProbability` field** from `ScoringResult`. Change:

```java
    public record ScoringResult(
        double estimatedReward,
        double weightedOutcomeScore,
        double outcomeProbability,
        double predictionScore,
        double diversityScore,
        double finalScore
    ) {
    }
```

to:

```java
    public record ScoringResult(
        double estimatedReward,
        double weightedOutcomeScore,
        double predictionScore,
        double diversityScore,
        double finalScore
    ) {
    }
```

- [ ] **Step 5: Drop the dead-blend assertions in `MovieLensOutcomeScorerTest`.** Remove the line asserting `strong.outcomeProbability() > weak.outcomeProbability()` (in `strongerMovieLensSignalsProduceHigherOutcomeScore`) and the line asserting `outcomes.overall() > 0.0` (in `outcomeProbabilitiesUseMovieLensSpecificNames`). Keep every other assertion (`weightedOutcomeScore`, `finalScore`, `positiveRating`/`watch`/`click` name checks).

- [ ] **Step 6: Compile the scorer + its test.**

Run: `mvn -o test -Dtest=MovieLensOutcomeScorerTest`
Expected: this alone won't compile until Task 2 removes the `ScoredCandidate` uses — so instead run `mvn -o test-compile` and expect errors ONLY in `HybridRecommendationService.java` (the `scoring.outcomeProbability()` / `candidate.outcomeProbability()` references), which Task 2 fixes. Proceed to Task 2 before running tests.

---

### Task 2: Remove `outcomeProbability` from the service + surfacings

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`

- [ ] **Step 1: Remove the `ScoredCandidate` record field.** In the `ScoredCandidate` record, delete the line:

```java
        double outcomeProbability,
```

- [ ] **Step 2: Remove the value from the `scoreCandidate` construction.** Where `ScoredCandidate` is built from the `ScoringResult`, delete the argument line:

```java
            scoring.outcomeProbability(),
```

- [ ] **Step 3: Remove the value from the `withDiversity` reconstruction.** In `ScoredCandidate.withDiversity(...)`, delete the argument line:

```java
                outcomeProbability,
```

- [ ] **Step 4: Remove the two response surfacings.** Delete the diagnostics line:

```java
                row.put("outcomeProbability", round(candidate.outcomeProbability()));
```

and the modelPredictions line:

```java
        predictions.put("outcomeProbability", round(candidate.outcomeProbability()));
```

- [ ] **Step 5: Confirm no references remain.**

Run: `rg -n "outcomeProbability|\.overall\(\)" src/main/java/com/demo/retrieval/service`
Expected: no output.

- [ ] **Step 6: Full suite — ranking unchanged.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, all green. `RecommendationControllerTest` (13) + `HybridRecommendationServiceTest` prove ranking is identical (neither asserts `outcomeProbability`); `MovieLensOutcomeScorerTest` passes with the reduced assertions.

- [ ] **Step 7: Commit.**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorer.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "refactor(scoring): drop redundant outcomeProbability blend; name diversity default"
```

---

### Task 3: Branch, docs, push, PR

- [ ] **Step 1: Branch off master (do Tasks 1–2 on it), then commit the docs.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/consolidate-scores
git add docs/specs/2026-07-03-consolidate-scores.md docs/plans/2026-07-03-consolidate-scores.md
git commit -m "docs(scoring): spec + plan for ranking-score consolidation"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/consolidate-scores
gh pr create --base master --title "Consolidate ranking scores: remove redundant outcomeProbability blend" --body "See docs/specs/2026-07-03-consolidate-scores.md. Removes the outcomeProbability (overall()) blend — a redundant sibling of weightedOutcome that was computed and surfaced but never ranked — from the scorer, ScoredCandidate, and the response. Names the 1.0 diversity default. Ranking-preserving (predictionScore/finalScore use weightedOutcome); the outcomeProbability response field is removed. mvn test green.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

- **Spec coverage:** C1→Task 1 Steps 2–4 + Task 2; C2→Task 1 Step 1–2; test updates→Task 1 Step 5; artifacts+PR→Task 3. Covered.
- **Placeholders:** none — full before/after code shown.
- **Type consistency:** `ScoringResult` drops one field, so both its constructor call (Task 1 Step 2) and every consumer (`ScoredCandidate` build in Task 2 Step 2) drop the matching argument; `ScoredCandidate` record + its two constructions (`scoreCandidate`, `withDiversity`) all drop the field together.
- **Ranking guard:** `RecommendationControllerTest` + `HybridRecommendationServiceTest` assert ranking output and neither references `outcomeProbability`, so they stay green unchanged.
