# Consolidate Replay-Buffer Event Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define the RL replay-event schema in one `ReplayEvent` class used by both the serve-time and feedback-time construction, and drop the duplicate `context` field — no other stored-format change.

**Architecture:** `ReplayEvent` holds the field-name constants + type/version and the `applyFeedback` completion helper. `serializeReplayContext` (serve) uses the constants and omits `context`; `buildReplayPayload` (feedback) delegates completion to `ReplayEvent.applyFeedback`.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Mockito, Jackson, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-03-consolidate-replay-buffer.md](../specs/2026-07-03-consolidate-replay-buffer.md).
- Wire-preserving except the removal of the duplicate `context` field (no consumer reads it).
- Cross-language `user`/`action` ↔ Python `userId`/`itemId` mismatch is OUT of scope.
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Create: `service/replay/ReplayEvent.java` — schema constants + `applyFeedback`.
- Create (test): `service/replay/ReplayEventTest.java`.
- Modify: `service/side_effects/MovieLensServingSideEffects.java` — `serializeReplayContext` uses constants, drops `context`.
- Modify (test): `service/side_effects/MovieLensServingSideEffectsTest.java` — assert `state` present, `context` absent.
- Modify: `service/HybridRecommendationService.java` — `buildReplayPayload` delegates.

---

### Task 1: `ReplayEvent` schema class + test

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/replay/ReplayEvent.java`
- Test: `src/test/java/com/demo/retrieval/service/replay/ReplayEventTest.java`

**Interfaces:**
- Produces: `ReplayEvent` field constants (all `public static final String`) + `EVENT_TYPE` (String), `SCHEMA_VERSION_VALUE` (int); `static void applyFeedback(Map<String,Object>, String, String, boolean, double, long, Map<String,Object>)`.

- [ ] **Step 1: Write the failing test** — `ReplayEventTest.java`:

```java
package com.demo.retrieval.service.replay;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplayEventTest {

    @Test
    void applyFeedbackFillsAllFieldsOnEmptyEvent() {
        Map<String, Object> event = new LinkedHashMap<>();
        Map<String, Object> nextState = Map.of("genres", java.util.List.of("drama"));
        ReplayEvent.applyFeedback(event, "u1", "m1", true, 0.8, 1234L, nextState);

        assertEquals(ReplayEvent.EVENT_TYPE, event.get(ReplayEvent.TYPE));
        assertEquals(ReplayEvent.SCHEMA_VERSION_VALUE, event.get(ReplayEvent.SCHEMA_VERSION));
        assertEquals("u1", event.get(ReplayEvent.USER));
        assertEquals("m1", event.get(ReplayEvent.ACTION));
        assertEquals(true, event.get(ReplayEvent.CLICKED));
        assertEquals(0.8, event.get(ReplayEvent.REWARD));
        assertEquals(1234L, event.get(ReplayEvent.FEEDBACK_TIMESTAMP));
        assertEquals(nextState, event.get(ReplayEvent.NEXT_STATE));
    }

    @Test
    void applyFeedbackPreservesExistingServeTimeFields() {
        Map<String, Object> event = new HashMap<>();
        event.put(ReplayEvent.TYPE, ReplayEvent.EVENT_TYPE);
        event.put(ReplayEvent.USER, "serve-user");
        event.put(ReplayEvent.ACTION, "serve-item");
        ReplayEvent.applyFeedback(event, "feedback-user", "feedback-item", false, 0.0, 9L, Map.of());

        // putIfAbsent leaves the serve-time identity intact
        assertEquals("serve-user", event.get(ReplayEvent.USER));
        assertEquals("serve-item", event.get(ReplayEvent.ACTION));
        // feedback fields are applied
        assertEquals(false, event.get(ReplayEvent.CLICKED));
        assertEquals(0.0, event.get(ReplayEvent.REWARD));
        assertEquals(9L, event.get(ReplayEvent.FEEDBACK_TIMESTAMP));
    }
}
```

- [ ] **Step 2: Run — expect FAIL** (class missing).

Run: `mvn -o test -Dtest=ReplayEventTest`
Expected: FAIL (compile).

- [ ] **Step 3: Create `ReplayEvent.java`:**

```java
package com.demo.retrieval.service.replay;

import java.util.Map;

/** Single source of truth for the RL replay-event JSON schema (keys + type/version) and the
 *  feedback-completion logic. Serve-time ({@code MovieLensServingSideEffects}) and feedback-time
 *  ({@code HybridRecommendationService}) both build the event through these names. */
public final class ReplayEvent {

    private ReplayEvent() {
    }

    public static final String EVENT_TYPE = "rl_experience";
    public static final int SCHEMA_VERSION_VALUE = 1;

    public static final String TYPE = "type";
    public static final String SCHEMA_VERSION = "schemaVersion";
    public static final String USER = "user";
    public static final String REQUEST_ID = "requestId";
    public static final String STATE = "state";
    public static final String CANDIDATES = "candidates";
    public static final String ACTION_SPACE = "actionSpace";
    public static final String ACTION = "action";
    public static final String ACTION_POSITION = "actionPosition";
    public static final String SLATE_SIZE = "slateSize";
    public static final String POLICY = "policy";
    public static final String MODEL_PREDICTIONS = "modelPredictions";
    public static final String ESTIMATED_REWARD = "estimatedReward";
    public static final String ONLINE_SCORE = "onlineScore";
    public static final String BANDIT_SCORE = "banditScore";
    public static final String COLD_START = "coldStart";
    public static final String TIMESTAMP = "timestamp";
    public static final String CLICKED = "clicked";
    public static final String REWARD = "reward";
    public static final String FEEDBACK_TIMESTAMP = "feedbackTimestamp";
    public static final String NEXT_STATE = "nextState";

    /** Complete a (possibly serve-time pre-populated, possibly empty) event with feedback fields.
     *  The identity fields use putIfAbsent so a serve-time context is preserved, and an empty event
     *  (missing/expired pending context) still gets the minimum type/version/user/action. */
    public static void applyFeedback(Map<String, Object> event, String user, String item,
                                     boolean clicked, double reward, long feedbackTimestamp,
                                     Map<String, Object> nextState) {
        event.putIfAbsent(TYPE, EVENT_TYPE);
        event.putIfAbsent(SCHEMA_VERSION, SCHEMA_VERSION_VALUE);
        event.putIfAbsent(USER, user);
        event.putIfAbsent(ACTION, item);
        event.put(CLICKED, clicked);
        event.put(REWARD, reward);
        event.put(FEEDBACK_TIMESTAMP, feedbackTimestamp);
        event.put(NEXT_STATE, nextState);
    }
}
```

- [ ] **Step 4: Run — expect PASS.**

Run: `mvn -o test -Dtest=ReplayEventTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/replay/ src/test/java/com/demo/retrieval/service/replay/
git commit -m "feat(replay): add ReplayEvent schema + feedback-completion helper"
```

---

### Task 2: Serve-time uses the constants + drops `context`

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffects.java`
- Modify (test): `src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java`

- [ ] **Step 1: Add the import** to `MovieLensServingSideEffects.java`:

```java
import com.demo.retrieval.service.replay.ReplayEvent;
```

- [ ] **Step 2: Rewrite the event-building block in `serializeReplayContext`.** Replace:

```java
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "rl_experience");
        event.put("schemaVersion", 1);
        event.put("requestId", request.requestId());
        event.put("user", request.userId());
        event.put("state", request.state());
        event.put("context", request.state());
        event.put("actionSpace", request.candidateSnapshot().stream().map(this::candidateFeatures).toList());
        event.put("candidates", request.candidateSnapshot().stream().map(ServedMovie::movieId).toList());
        event.put("action", selected.movieId());
        event.put("actionPosition", actionPosition);
        event.put("slateSize", slateSize);
        event.put("policy", Map.of(
            "name", request.algorithm(),
            "rankingScore", selected.banditScore(),
            "explorationBonus", selected.explorationBonus(),
            "propensity", slateSize <= 0 ? 0.0 : 1.0 / slateSize
        ));
        event.put("modelPredictions", selected.modelPredictions());
        event.put("estimatedReward", selected.estimatedReward());
        event.put("onlineScore", selected.onlineScore());
        event.put("banditScore", selected.banditScore());
        event.put("coldStart", selected.coldStart());
        event.put("timestamp", timestamp);
```

with (constants; `context` removed):

```java
        Map<String, Object> event = new LinkedHashMap<>();
        event.put(ReplayEvent.TYPE, ReplayEvent.EVENT_TYPE);
        event.put(ReplayEvent.SCHEMA_VERSION, ReplayEvent.SCHEMA_VERSION_VALUE);
        event.put(ReplayEvent.REQUEST_ID, request.requestId());
        event.put(ReplayEvent.USER, request.userId());
        event.put(ReplayEvent.STATE, request.state());
        event.put(ReplayEvent.ACTION_SPACE, request.candidateSnapshot().stream().map(this::candidateFeatures).toList());
        event.put(ReplayEvent.CANDIDATES, request.candidateSnapshot().stream().map(ServedMovie::movieId).toList());
        event.put(ReplayEvent.ACTION, selected.movieId());
        event.put(ReplayEvent.ACTION_POSITION, actionPosition);
        event.put(ReplayEvent.SLATE_SIZE, slateSize);
        event.put(ReplayEvent.POLICY, Map.of(
            "name", request.algorithm(),
            "rankingScore", selected.banditScore(),
            "explorationBonus", selected.explorationBonus(),
            "propensity", slateSize <= 0 ? 0.0 : 1.0 / slateSize
        ));
        event.put(ReplayEvent.MODEL_PREDICTIONS, selected.modelPredictions());
        event.put(ReplayEvent.ESTIMATED_REWARD, selected.estimatedReward());
        event.put(ReplayEvent.ONLINE_SCORE, selected.onlineScore());
        event.put(ReplayEvent.BANDIT_SCORE, selected.banditScore());
        event.put(ReplayEvent.COLD_START, selected.coldStart());
        event.put(ReplayEvent.TIMESTAMP, timestamp);
```

- [ ] **Step 3: Assert `state` present, `context` gone** in `MovieLensServingSideEffectsTest`. In `recordServedWritesMovieLensServingSideEffectsInOnePipeline`, replace the pending-set verification with a captor-based check:

Add imports:
```java
import org.mockito.ArgumentCaptor;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
```

Replace:
```java
        verify(valueOps).set(eq(MovieLensServingSideEffects.pendingReplayKey("u1", "m1")), org.mockito.ArgumentMatchers.contains("\"action\":\"m1\""), eq(Duration.ofHours(1)));
```
with:
```java
        ArgumentCaptor<String> pendingPayload = ArgumentCaptor.forClass(String.class);
        verify(valueOps).set(eq(MovieLensServingSideEffects.pendingReplayKey("u1", "m1")), pendingPayload.capture(), eq(Duration.ofHours(1)));
        String payload = pendingPayload.getValue();
        assertTrue(payload.contains("\"action\":\"m1\""));
        assertTrue(payload.contains("\"state\""));
        assertFalse(payload.contains("\"context\""), "context is a removed duplicate of state");
```

- [ ] **Step 4: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`; `MovieLensServingSideEffectsTest` green with the new assertions.

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffects.java src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java
git commit -m "refactor(replay): serve-time uses ReplayEvent constants, drop duplicate context"
```

---

### Task 3: Feedback-time delegates completion

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`

- [ ] **Step 1: Add the import.**

```java
import com.demo.retrieval.service.replay.ReplayEvent;
```

- [ ] **Step 2: Rewrite `buildReplayPayload`.** Replace:

```java
    private String buildReplayPayload(FeedbackRequest request, Map<String, Object> event) {
        event.putIfAbsent("type", "rl_experience");
        event.putIfAbsent("schemaVersion", 1);
        event.putIfAbsent("user", request.user());
        event.putIfAbsent("action", request.item());
        event.put("clicked", request.clicked());
        event.put("reward", request.reward());
        event.put("feedbackTimestamp", System.currentTimeMillis());
        event.put("nextState", buildCurrentState(request.user()));
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay event for user {} item {}", request.user(), request.item(), e);
            return null;
        }
    }
```

with:

```java
    private String buildReplayPayload(FeedbackRequest request, Map<String, Object> event) {
        ReplayEvent.applyFeedback(event, request.user(), request.item(), request.clicked(),
            request.reward(), System.currentTimeMillis(), buildCurrentState(request.user()));
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay event for user {} item {}", request.user(), request.item(), e);
            return null;
        }
    }
```

- [ ] **Step 3: Confirm no replay field literals remain in the two classes.**

Run: `rg -n '"type"|"schemaVersion"|"feedbackTimestamp"|"nextState"|"context"|"candidates"|"actionSpace"' src/main/java/com/demo/retrieval/service/HybridRecommendationService.java src/main/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffects.java`
Expected: no output (all via `ReplayEvent.*`; `context` gone).

- [ ] **Step 4: Full suite — replay push unchanged.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, all green (`RecommendationControllerTest` is the feedback-path oracle; `HybridFeedbackRedisTest` skips without Docker).

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "refactor(replay): feedback-time delegates completion to ReplayEvent.applyFeedback"
```

---

### Task 4: Branch, docs, push, PR

- [ ] **Step 1: Branch off master (do Tasks 1–3 on it), then commit the docs.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/consolidate-replay-buffer
git add docs/specs/2026-07-03-consolidate-replay-buffer.md docs/plans/2026-07-03-consolidate-replay-buffer.md
git commit -m "docs(replay): spec + plan for replay-event schema consolidation"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/consolidate-replay-buffer
gh pr create --base master --title "Consolidate replay-buffer event schema (ReplayEvent)" --body "See docs/specs/2026-07-03-consolidate-replay-buffer.md. Defines the replay-event field names + type/version once in ReplayEvent, centralizes the feedback completion (buildReplayPayload -> ReplayEvent.applyFeedback), and has serve-time use the constants + drop the duplicate context field. Wire-preserving except the removed context (unread by any consumer). Adds ReplayEventTest. The Java user/action vs Python userId/itemId mismatch is documented as a separate contract fix. mvn test green."
```

---

## Self-Review

- **Spec coverage:** R1→Task 1; R2→Task 3; R3→Task 2; tests→Task 1 + Task 2 Step 3; artifacts+PR→Task 4. Covered.
- **Placeholders:** none — full class + before/after code shown.
- **Type consistency:** `ReplayEvent.applyFeedback(Map, String, String, boolean, double, long, Map)` matches the `buildReplayPayload` call; every `ReplayEvent.*` constant used in `serializeReplayContext` is declared in Task 1; `EVENT_TYPE`/`SCHEMA_VERSION_VALUE` types (String/int) match the serve-time and test usage.
- **Wire check:** field *names* are byte-identical to the current literals (the constants equal the old strings); only `context` is removed. Task 3 Step 3 greps to confirm.
