# Scoring Cutoff Centralization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the tunable ranking-blend weights in `MovieLensOutcomeScorer` documented names and a drift-detecting invariant test, without changing any numerical behavior.

**Architecture:** Pure refactor. Replace inline magic-number weights in `score()` and `weightedOutcome()` with named `static final` constants (following the existing `DIVERSITY_DECAY`/`DIVERSITY_FLOOR` pattern in the same class), then add a test asserting the three convex blends each sum to `1.0`. The existing `MovieLensOutcomeScorerTest` is the behavior oracle — it must pass unchanged.

**Tech Stack:** Java 17, Spring Boot 3.3, JUnit 5, Maven (surefire).

## Global Constraints

- No behavior change: existing `MovieLensOutcomeScorerTest` passes unmodified.
- Spec: [docs/specs/2026-07-01-scoring-cutoff-centralization.md](../specs/2026-07-01-scoring-cutoff-centralization.md).
- New weight constants are **package-private** `static final` (no access modifier) so the same-package test can assert the sum-to-1 invariant.
- Logit coefficients, novelty formula, and UCB internals stay inline (out of scope).
- Module dir for all commands: `recsys-pipeline/services/java-retrieval-service`.

---

## File Structure

- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorer.java` — add named weight constants; use them in `score()` and `weightedOutcome()`.
- Modify (test): `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java` — add the sum-to-1 invariant test.

---

### Task 1: Name the blend weights (B1–B3)

Pure rename covering all three inline blends at once — they share one file and one oracle test, so a reviewer would accept or reject them together.

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorer.java`
- Test (oracle, unchanged): `src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java`

**Interfaces:**
- Consumes: nothing new.
- Produces: package-private `static final` fields on `MovieLensOutcomeScorer` used by Task 2:
  `EXPLOITATION_BANDIT_WEIGHT`, `EXPLOITATION_OUTCOME_WEIGHT`, `EXPLOITATION_DL_WEIGHT`, `EXPLOITATION_Q_WEIGHT`, `ESTIMATED_REWARD_POSTERIOR_WEIGHT`, `ESTIMATED_REWARD_OUTCOME_WEIGHT`, `OUTCOME_POSITIVE_RATING_WEIGHT`, `OUTCOME_PREFERENCE_WEIGHT`, `OUTCOME_CLICK_WEIGHT`, `OUTCOME_WATCH_WEIGHT`, `OUTCOME_NOVEL_DISCOVERY_WEIGHT`, `OUTCOME_NEGATIVE_FEEDBACK_PENALTY` (all `double`).

- [ ] **Step 1: Establish the oracle baseline (test currently green)**

Run: `mvn -o test -Dtest=MovieLensOutcomeScorerTest`
Expected: PASS (2 tests). This is the behavior we must preserve.

- [ ] **Step 2: Add the constant declarations**

In `MovieLensOutcomeScorer.java`, directly below the existing
`DIVERSITY_DECAY`/`DIVERSITY_FLOOR` fields, add:

```java
    // Exploitation blend applied to the ranking score in score() (weights sum to 1.0).
    static final double EXPLOITATION_BANDIT_WEIGHT = 0.55;
    static final double EXPLOITATION_OUTCOME_WEIGHT = 0.25;
    static final double EXPLOITATION_DL_WEIGHT = 0.15;
    static final double EXPLOITATION_Q_WEIGHT = 0.05;

    // Estimated-reward blend in score() (weights sum to 1.0).
    static final double ESTIMATED_REWARD_POSTERIOR_WEIGHT = 0.65;
    static final double ESTIMATED_REWARD_OUTCOME_WEIGHT = 0.35;

    // Weighted-outcome blend in weightedOutcome(): positive weights sum to 1.0;
    // negative feedback is a subtractive penalty, not part of the convex sum.
    static final double OUTCOME_POSITIVE_RATING_WEIGHT = 0.30;
    static final double OUTCOME_PREFERENCE_WEIGHT = 0.22;
    static final double OUTCOME_CLICK_WEIGHT = 0.18;
    static final double OUTCOME_WATCH_WEIGHT = 0.22;
    static final double OUTCOME_NOVEL_DISCOVERY_WEIGHT = 0.08;
    static final double OUTCOME_NEGATIVE_FEEDBACK_PENALTY = 0.35;
```

- [ ] **Step 3: Use the constants in `score()`**

Replace the exploitation/estimated-reward block:

```java
        double exploitation = EXPLOITATION_BANDIT_WEIGHT * input.banditRankingScore()
            + EXPLOITATION_OUTCOME_WEIGHT * weightedOutcome
            + EXPLOITATION_DL_WEIGHT * clamp(input.dlScore())
            + EXPLOITATION_Q_WEIGHT * clamp(input.qValue());
        double predictionScore = clamp(exploitation + input.explorationBonus());
        double estimatedReward = clamp(ESTIMATED_REWARD_POSTERIOR_WEIGHT * input.posteriorMean()
            + ESTIMATED_REWARD_OUTCOME_WEIGHT * weightedOutcome);
```

- [ ] **Step 4: Use the constants in `weightedOutcome()`**

Replace the positive/penalty block:

```java
        double positive = OUTCOME_POSITIVE_RATING_WEIGHT * p.positiveRating()
            + OUTCOME_PREFERENCE_WEIGHT * p.preference()
            + OUTCOME_CLICK_WEIGHT * p.click()
            + OUTCOME_WATCH_WEIGHT * p.watch()
            + OUTCOME_NOVEL_DISCOVERY_WEIGHT * p.novelDiscovery();
        return clamp(positive - OUTCOME_NEGATIVE_FEEDBACK_PENALTY * p.negativeFeedback());
```

- [ ] **Step 5: Verify behavior preserved**

Run: `mvn -o test -Dtest=MovieLensOutcomeScorerTest`
Expected: PASS (2 tests) — identical outcome, proving the rename changed nothing.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorer.java
git commit -m "refactor(scorer): name MovieLensOutcomeScorer blend weights"
```

---

### Task 2: Sum-to-1 invariant test (B4)

**Files:**
- Modify (test): `src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java`

**Interfaces:**
- Consumes: the twelve package-private constants produced by Task 1.
- Produces: nothing.

- [ ] **Step 1: Add the `assertEquals` import**

At the top of `MovieLensOutcomeScorerTest.java`, alongside the existing
`import static org.junit.jupiter.api.Assertions.assertTrue;`, add:

```java
import static org.junit.jupiter.api.Assertions.assertEquals;
```

- [ ] **Step 2: Write the invariant test**

Add this method to `MovieLensOutcomeScorerTest`:

```java
    @Test
    void convexBlendWeightsSumToOne() {
        assertEquals(1.0,
            MovieLensOutcomeScorer.EXPLOITATION_BANDIT_WEIGHT
            + MovieLensOutcomeScorer.EXPLOITATION_OUTCOME_WEIGHT
            + MovieLensOutcomeScorer.EXPLOITATION_DL_WEIGHT
            + MovieLensOutcomeScorer.EXPLOITATION_Q_WEIGHT, 1e-9,
            "exploitation blend must stay convex");
        assertEquals(1.0,
            MovieLensOutcomeScorer.ESTIMATED_REWARD_POSTERIOR_WEIGHT
            + MovieLensOutcomeScorer.ESTIMATED_REWARD_OUTCOME_WEIGHT, 1e-9,
            "estimated-reward blend must stay convex");
        assertEquals(1.0,
            MovieLensOutcomeScorer.OUTCOME_POSITIVE_RATING_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_PREFERENCE_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_CLICK_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_WATCH_WEIGHT
            + MovieLensOutcomeScorer.OUTCOME_NOVEL_DISCOVERY_WEIGHT, 1e-9,
            "positive-outcome weights must stay convex");
    }
```

- [ ] **Step 3: Run the new test**

Run: `mvn -o test -Dtest=MovieLensOutcomeScorerTest`
Expected: PASS (3 tests).

- [ ] **Step 4: Run the full module suite**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 44, Failures: 0, Errors: 0`.

- [ ] **Step 5: Commit**

```bash
git add src/test/java/com/demo/retrieval/service/scorers/MovieLensOutcomeScorerTest.java
git commit -m "test(scorer): assert ranking blend weights stay convex"
```

---

### Task 3: Branch, push, open PR

Bundles Phase A (already-edited but uncommitted `RecommendationConstants` work + README fix) and Phase B (Tasks 1–2) plus the spec and this plan into one "scoring cutoff centralization" PR against `master`.

**Files:** none (git/gh operations only).

- [ ] **Step 1: Create a feature branch off latest master carrying the working-tree changes**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git stash push --include-untracked -m scoring-cutoffs
git fetch origin
git checkout -b feature/scoring-cutoff-centralization origin/master
git stash pop
```
Expected: working-tree changes reapply onto a fresh branch based on `origin/master`. If `git stash pop` reports a README.md conflict, keep both changes (the line-118 DL-weight edit is independent) and `git add README.md`.

- [ ] **Step 2: Stage and commit the remaining (Phase A + docs) changes**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/RecommendationConstants.java \
        recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/RecommendationConstantsTest.java \
        recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java \
        README.md \
        docs/specs/2026-07-01-scoring-cutoff-centralization.md \
        docs/plans/2026-07-01-scoring-cutoff-centralization.md
git commit -m "feat(scoring): centralize offline blend cutoffs and fix [0,1] invariants"
```

- [ ] **Step 3: Confirm the full suite is green on the branch**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 44, Failures: 0`.

- [ ] **Step 4: Push and open the PR**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin feature/scoring-cutoff-centralization
gh pr create --base master --title "Centralize scoring cutoffs and fix [0,1] invariants" \
  --body "$(cat <<'EOF'
## Summary
Centralizes the in-code scoring cutoffs of the Java retrieval service and fixes the [0,1]-range bugs found in the scoring-formula audit.

- **Phase A (correctness):** `RecommendationConstants` with a normalized `blendOfflineScore` (weights no longer silently sum to 1.15), dlScore clamped into range, Q-value clamped on the tabular path, `WARM_PRIOR_STRENGTH`/cold-start/content weights named. Root README DL-weight default corrected to 0.15.
- **Phase B (naming):** `MovieLensOutcomeScorer` exploitation / estimated-reward / weighted-outcome blend weights given documented names, plus a test asserting the convex blends stay sum-to-1 (drift detector for the finding-1 class of bug).

Spec: `docs/specs/2026-07-01-scoring-cutoff-centralization.md`
Plan: `docs/plans/2026-07-01-scoring-cutoff-centralization.md`

## Testing
`mvn test` in `java-retrieval-service` → BUILD SUCCESS, 44 tests, 0 failures. Pure refactor for Phase B (existing `MovieLensOutcomeScorerTest` unchanged).
EOF
)"
```
Expected: `gh` prints the new PR URL.

---

## Self-Review

- **Spec coverage:** B1→Task 1 Steps 2–3; B2→Task 1 Step 3; B3→Task 1 Steps 2,4; B4→Task 2; Phase A + artifacts shipped via Task 3. All spec work items covered.
- **Placeholders:** none — every code step shows the full replacement code.
- **Type consistency:** the twelve constant names in Task 1's Produces block match exactly those referenced in Task 2's test. All `double`.
