# Q-Learning State Recurrence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Key the tabular Q-table on the recurring genre/tag taste profile instead of raw recent item-IDs, so the Bellman bootstrap becomes meaningful.

**Architecture:** Extract a pure `TabularStateKey` helper (SHA-256 of an order-independent genre/tag signature) and make `HybridRecommendationService.stateKey` a thin delegate. `recent` stays in the state payload for `nextActionSpace`. Bundle a comment-only clarification of Thompson's ranking-score semantics (finding 6).

**Tech Stack:** Java 17, Spring Boot 3.3, JUnit 5, Maven (surefire).

## Global Constraints

- Spec: [docs/specs/2026-07-01-qlearning-state-recurrence.md](../specs/2026-07-01-qlearning-state-recurrence.md).
- No change to Bellman/TD math, reward signal, action space, or `application.yml`.
- No Redis migration — old `q-learning:q:*` / `sarsa:q:*` hashes orphan harmlessly.
- Base branch: `feature/scoring-cutoff-centralization` (stacked on PR #110); PR targets `master`.
- Module dir for all `mvn` commands: `recsys-pipeline/services/java-retrieval-service`.
- Existing suite (44 tests) stays green.

---

## File Structure

- Create: `.../service/TabularStateKey.java` — pure state-key derivation from the genre/tag signature. One responsibility.
- Create (test): `.../service/TabularStateKeyTest.java` — unit tests for the helper.
- Modify: `.../service/HybridRecommendationService.java` — delegate `stateKey`; remove the now-unused `SHA256_DIGEST` field and four imports; add the Thompson comment.

(Full path prefix: `recsys-pipeline/services/java-retrieval-service/src/{main,test}/java/com/demo/retrieval`.)

---

### Task 1: `TabularStateKey` helper + unit tests (F5.1, recurrence lock)

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/TabularStateKey.java`
- Test: `src/test/java/com/demo/retrieval/service/TabularStateKeyTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: `static String TabularStateKey.hash(Object genres, Object tags)` — base64url(SHA-256) of the canonical genre/tag signature; order-independent, de-duplicated, null/non-collection args normalize to empty.

- [ ] **Step 1: Write the failing tests**

Create `TabularStateKeyTest.java`:

```java
package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TabularStateKeyTest {

    @Test
    void orderAndDuplicatesDoNotChangeKey() {
        String a = TabularStateKey.hash(List.of("drama", "comedy"), List.of("dark"));
        String b = TabularStateKey.hash(List.of("comedy", "drama", "drama"), List.of("dark", "dark"));
        assertEquals(a, b);
    }

    @Test
    void differentProfilesProduceDifferentKeys() {
        assertNotEquals(
            TabularStateKey.hash(List.of("drama"), List.of("dark")),
            TabularStateKey.hash(List.of("comedy"), List.of("dark")));
    }

    @Test
    void nullAndEmptyNormalizeEqually() {
        assertEquals(
            TabularStateKey.hash(null, null),
            TabularStateKey.hash(List.of(), List.of()));
    }

    // Recurrence lock: two states with the same taste profile but different recent-watch
    // history must key identically (this mirrors what the service's stateKey extracts).
    @Test
    void keyIgnoresRecentItemHistory() {
        Map<String, Object> stateA = Map.of(
            "recent", List.of("m1", "m2"), "genres", List.of("drama"), "tags", List.of("dark"));
        Map<String, Object> stateB = Map.of(
            "recent", List.of("m9"), "genres", List.of("drama"), "tags", List.of("dark"));
        assertEquals(
            TabularStateKey.hash(stateA.get("genres"), stateA.get("tags")),
            TabularStateKey.hash(stateB.get("genres"), stateB.get("tags")));
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `mvn -o test -Dtest=TabularStateKeyTest`
Expected: FAIL — compilation error, `TabularStateKey` does not exist.

- [ ] **Step 3: Implement the helper**

Create `TabularStateKey.java`:

```java
package com.demo.retrieval.service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Derives the tabular Q-learning/SARSA state key from the user's coarse taste
 * profile (genre + tag sets) only. Raw recent item-IDs are deliberately excluded:
 * they are high-cardinality, so including them makes almost every state unique and
 * the Bellman next-state bootstrap degenerates to a running average of immediate
 * reward. Hashing the order-independent genre/tag signature lets states recur across
 * users and sessions, restoring temporal credit.
 */
final class TabularStateKey {

    private TabularStateKey() {
    }

    /** base64url(SHA-256) of the canonical, order-independent genre/tag signature. */
    static String hash(Object genres, Object tags) {
        String canonical = "g:" + normalize(genres) + "|t:" + normalize(tags);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String normalize(Object raw) {
        if (!(raw instanceof Collection<?> values)) {
            return "";
        }
        return values.stream()
            .filter(v -> v != null)
            .map(String::valueOf)
            .sorted()
            .distinct()
            .collect(Collectors.joining(","));
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `mvn -o test -Dtest=TabularStateKeyTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/TabularStateKey.java \
        src/test/java/com/demo/retrieval/service/TabularStateKeyTest.java
git commit -m "feat(rl): add TabularStateKey signature helper"
```

---

### Task 2: Delegate `stateKey` and remove the old hashing path (F5.2, F5.3)

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`

**Interfaces:**
- Consumes: `TabularStateKey.hash(Object, Object)` from Task 1.
- Produces: nothing new (internal wiring).

- [ ] **Step 1: Replace the `stateKey` body**

Replace the existing method (currently at ~L1156):

```java
    private String stateKey(Object state) {
        try {
            String canonical = objectMapper.writeValueAsString(state);
            byte[] hash = SHA256_DIGEST.get().digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
        } catch (JsonProcessingException e) {
            log.warn("Failed to build Q-learning state key; falling back to hashCode", e);
            return Integer.toHexString(String.valueOf(state).hashCode());
        }
    }
```

with:

```java
    // Q-table identity keys on the coarse genre/tag signature only (see TabularStateKey);
    // raw recent item-IDs stay in the state payload for nextActionSpace but are excluded here.
    private String stateKey(Object state) {
        if (state instanceof Map<?, ?> map) {
            return TabularStateKey.hash(map.get("genres"), map.get("tags"));
        }
        return TabularStateKey.hash(null, null);
    }
```

- [ ] **Step 2: Remove the now-unused `SHA256_DIGEST` field**

Delete this block (currently at ~L73):

```java
    private static final ThreadLocal<MessageDigest> SHA256_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    });
```

- [ ] **Step 3: Remove the four now-unused imports**

Delete these import lines (keep `com.fasterxml.jackson.core.JsonProcessingException` — still used at other call sites):

```java
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
```

- [ ] **Step 4: Confirm no dangling references remain**

Run: `rg -n "SHA256_DIGEST|MessageDigest|NoSuchAlgorithmException|\bBase64\b|StandardCharsets" src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
Expected: no output.

- [ ] **Step 5: Run the full module suite**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 48, Failures: 0, Errors: 0` (44 existing + 4 new from Task 1).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "fix(rl): key Q-table on genre/tag signature so states recur"
```

---

### Task 3: Thompson semantics clarification (F6.1)

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`

**Interfaces:** none.

- [ ] **Step 1: Add the clarifying comment**

In `computeBanditArmScore`, replace the Thompson branch opener:

```java
        if ("thompson".equals(algorithm)) {
```

with:

```java
        // Thompson: the ranking score IS the posterior sample drawn below, not
        // posteriorMean + bonus. The explorationMagnitude (|sample - mean|) is a
        // reported diagnostic only. The additive "mean + explorationBonus" form is
        // literal for UCB (below), not for Thompson.
        if ("thompson".equals(algorithm)) {
```

- [ ] **Step 2: Verify still green**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 48, Failures: 0`.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "docs(rl): clarify Thompson ranking-score semantics"
```

---

### Task 4: Branch, push, open the stacked PR

Bundles Tasks 1–3 plus the spec and this plan (currently uncommitted on the base branch) into a PR stacked on `feature/scoring-cutoff-centralization`, targeting `master`.

**Files:** none (git/gh only).

- [ ] **Step 1: Create the stacked branch (carrying the uncommitted spec)**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
# currently on feature/scoring-cutoff-centralization
git checkout -b feature/qlearning-state-recurrence
```

Do Tasks 1–3 on this branch (their commits land here). Then commit the docs:

```bash
git add docs/specs/2026-07-01-qlearning-state-recurrence.md \
        docs/plans/2026-07-01-qlearning-state-recurrence.md
git commit -m "docs(rl): spec + plan for Q-state recurrence"
```

- [ ] **Step 2: Confirm suite green on the branch**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 48, Failures: 0`.

- [ ] **Step 3: Push and open the PR**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin feature/qlearning-state-recurrence
gh pr create --base master --title "Recurring Q-learning state keys (finding 5) + Thompson clarity (finding 6)" \
  --body "$(cat <<'EOF'
## Summary
Keys the tabular Q-learning/SARSA table on the recurring genre/tag taste profile instead of raw recent item-IDs, so the Bellman next-state bootstrap is meaningful (audit finding 5). Bundles a comment-only clarification that Thompson's ranking score is the posterior sample, not `mean + bonus` (finding 6).

- New pure `TabularStateKey.hash(genres, tags)` — order-independent SHA-256 signature.
- `HybridRecommendationService.stateKey` delegates to it; `recent` stays in the payload for `nextActionSpace`. Old `SHA256_DIGEST` field + four imports removed.
- Old `q-learning:q:*` / `sarsa:q:*` Redis hashes orphan harmlessly (no real learned data; no migration).

Spec: `docs/specs/2026-07-01-qlearning-state-recurrence.md`
Plan: `docs/plans/2026-07-01-qlearning-state-recurrence.md`

## Testing
`mvn test` in `java-retrieval-service` → BUILD SUCCESS, 48 tests, 0 failures. Structural validation only (no dataset): equivalent taste profiles key identically; distinct profiles differ.

> Stacked on #110 — until that merges this diff also shows the scoring-cutoff changes.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
Expected: `gh` prints the new PR URL.

---

## Self-Review

- **Spec coverage:** F5.1→Task 1; F5.2→Task 2 Step 1 + Task 1 recurrence-lock test; F5.3→Task 2 Steps 2–4; F6.1→Task 3; docs+PR→Task 4. All covered.
- **Placeholders:** none — every code step shows full content.
- **Type consistency:** `TabularStateKey.hash(Object, Object)` defined in Task 1 is used with exactly that signature in Task 2. `stateKey(Object)` keeps its existing signature. Test count 44→48 consistent across Tasks 2–4.
