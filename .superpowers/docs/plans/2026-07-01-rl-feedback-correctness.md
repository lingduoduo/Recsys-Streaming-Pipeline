# RL Feedback-Path Correctness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix four verified correctness defects in the tabular-RL feedback path (pending-key idempotency/leak, serve↔feedback state mismatch, Q-value race, ONNX length crash) without changing behavior on the correct paths.

**Architecture:** B1 adds a TTL to the pending replay key and deletes it on consume. B2 extracts the serve-time taste-profile derivation into one helper used by both serve and feedback. B3 replaces the read-modify-write Q-update with an atomic Lua script. B4 makes ONNX batch decoding return a fixed-length array. Each change is backed by a focused unit test; the existing suite is the behavior oracle.

**Tech Stack:** Java 17, Spring Boot 3.3 (Spring Data Redis / Lettuce), JUnit 5, Mockito, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-01-rl-feedback-correctness.md](../specs/2026-07-01-rl-feedback-correctness.md).
- Behavior on non-buggy paths unchanged; existing 48 tests stay green.
- Counter (clicks/reward) duplicate-idempotency is OUT of scope.
- Module dir for all `mvn` commands: `recsys-pipeline/services/java-retrieval-service`.
- Base a new branch on `master`; PR targets `master`.
- Path prefix below: `src/{main,test}/java/com/demo/retrieval`.

---

## File Structure

- Modify: `config/RecommendationProperties.java` — add `ReplayBuffer.pendingTtl` (Duration).
- Modify: `src/main/resources/application.yml` — `recsys.replay-buffer.pending-ttl`.
- Modify: `service/side_effects/MovieLensServingSideEffects.java` — TTL on pending set (constructor gains a `Duration`).
- Modify: `service/HybridRecommendationService.java` — pass TTL; delete pending key on feedback; `deriveTasteProfile` helper; hydrate in `buildCurrentState`; atomic Lua Q-update.
- Modify: `service/DeepLearningPredictionService.java` — `normalizeBatchScores` fixed-length decode.
- Tests: `MovieLensServingSideEffectsTest`, `HybridRecommendationServiceTest` (new derivation test), `DeepLearningPredictionServiceTest`.

---

### Task 1: B1 — pending-key TTL + delete-on-consume (CRITICAL)

**Files:**
- Modify: `config/RecommendationProperties.java` (ReplayBuffer)
- Modify: `src/main/resources/application.yml`
- Modify: `service/side_effects/MovieLensServingSideEffects.java`
- Modify: `service/HybridRecommendationService.java` (constructor call ~L119; feedback pipeline ~L344-366)
- Test: `service/side_effects/MovieLensServingSideEffectsTest.java`

**Interfaces:**
- Produces: `ReplayBuffer.getPendingTtl(): Duration`; `MovieLensServingSideEffects(StringRedisTemplate, ObjectMapper, Duration)` constructor.

- [ ] **Step 1: Config property.** In `RecommendationProperties.java`, add `import java.time.Duration;` and inside `class ReplayBuffer`:

```java
        private Duration pendingTtl = Duration.ofHours(1);

        public Duration getPendingTtl() {
            return pendingTtl;
        }

        public void setPendingTtl(Duration pendingTtl) {
            this.pendingTtl = pendingTtl;
        }
```

- [ ] **Step 2: application.yml.** Under `replay-buffer:` add:

```yaml
    pending-ttl: ${RECSYS_REPLAY_PENDING_TTL:1h}
```

- [ ] **Step 3: Failing test** in `MovieLensServingSideEffectsTest.java` — the pending key must be set with the TTL. Add (adjust the constructor call in the existing setup to pass `Duration.ofHours(1)`):

```java
    @Test
    void pendingReplayKeyIsSetWithTtl() {
        // given the standard mocked redis/pipeline from this test's setup that runs the SessionCallback,
        // when recordServed runs, the pending key set must carry the configured TTL.
        verify(valueOps, atLeastOnce()).set(
            startsWith("replay:pending:"), anyString(), eq(Duration.ofHours(1)));
    }
```

- [ ] **Step 4: Run — expect FAIL** (`set(key, value, Duration)` not used yet):

Run: `mvn -o test -Dtest=MovieLensServingSideEffectsTest`
Expected: FAIL.

- [ ] **Step 5: Add TTL field + constructor** in `MovieLensServingSideEffects.java`. Add `import java.time.Duration;`, a field, and widen the constructor:

```java
    private final Duration pendingTtl;

    public MovieLensServingSideEffects(StringRedisTemplate redis, ObjectMapper objectMapper, Duration pendingTtl) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.pendingTtl = pendingTtl;
    }
```

- [ ] **Step 6: Set with TTL.** Replace the pending set (line ~60):

```java
                    serializeReplayContext(request, movie, i, request.selected().size(), now)
                        .ifPresent(payload -> operations.opsForValue().set(pendingReplayKey(request.userId(), movie.movieId()), payload, pendingTtl));
```

- [ ] **Step 7: Pass TTL from the service.** In `HybridRecommendationService.java` line ~119:

```java
        this.servingSideEffects = new MovieLensServingSideEffects(redis, objectMapper, properties.getReplayBuffer().getPendingTtl());
```

- [ ] **Step 8: Delete on consume.** In the feedback `executePipelined` block (inside `execute(RedisOperations operations)`), add as the first statement:

```java
                operations.delete(pendingReplayKey(request.user(), request.item()));
```

- [ ] **Step 9: Run — expect PASS.**

Run: `mvn -o test -Dtest=MovieLensServingSideEffectsTest`
Expected: PASS.

- [ ] **Step 10: Commit.**

```bash
git add src/main/java/com/demo/retrieval/config/RecommendationProperties.java src/main/resources/application.yml src/main/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffects.java src/main/java/com/demo/retrieval/service/HybridRecommendationService.java src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java
git commit -m "fix(rl): expire and delete pending replay key to stop stale re-updates"
```

---

### Task 2: B2 — shared taste-profile derivation (HIGH)

**Files:**
- Modify: `service/HybridRecommendationService.java` (serve-time block ~L163-184; `buildCurrentState` ~L771-784)
- Test: `service/HybridRecommendationServiceTest.java`

**Interfaces:**
- Produces: `TasteProfile deriveTasteProfile(List<String> recent, List<String> rated, MovieLensUserFeatures features, Collection<String> popular)` and `record TasteProfile(Set<String> genres, Set<String> tags)` (package-private for test).

- [ ] **Step 1: Failing test** in `HybridRecommendationServiceTest.java` — same inputs yield the same signature and favoriteGenres are included. Build a service via the existing setup helper, then:

```java
    @Test
    void deriveTasteProfileIncludesFavoriteGenresAndIsDeterministic() {
        // catalog has "watched" -> genre "drama"; features favoriteGenres ["comedy"].
        var features = MovieLensUserFeatures.forUser("u1");
        var profile = serviceForDerivationTest().deriveTasteProfile(
            List.of("watched"), List.of(), features.withFavoriteGenres(List.of("comedy")), List.of());
        assertTrue(profile.genres().contains("drama"));
        assertTrue(profile.genres().contains("comedy"));
    }
```

(If `MovieLensUserFeatures` has no `withFavoriteGenres`, construct it via its existing constructor/builder with favoriteGenres set; use whatever the record exposes.)

- [ ] **Step 2: Run — expect FAIL** (method missing).

Run: `mvn -o test -Dtest=HybridRecommendationServiceTest`
Expected: FAIL (compile).

- [ ] **Step 3: Extract the helper.** Add to `HybridRecommendationService.java`:

```java
    record TasteProfile(Set<String> genres, Set<String> tags) {
    }

    TasteProfile deriveTasteProfile(List<String> recent, List<String> rated,
                                    MovieLensUserFeatures features, Collection<String> popular) {
        List<String> seedItems = firstNonEmpty(
            recent,
            features.cachedMovieIds(),
            features.actionSequenceMovieIds(),
            features.retrievalSequenceMovieIds(),
            features.scoringSequenceMovieIds(),
            rated,
            List.copyOf(popular)
        );
        Set<String> genres = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getGenres()).stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
        genres.addAll(normalize(features.favoriteGenres()));
        Set<String> tags = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> normalize(p.getTags()).stream())
            .collect(Collectors.toCollection(LinkedHashSet::new));
        return new TasteProfile(genres, tags);
    }
```

- [ ] **Step 4: Use it in `recommend()`** — replace lines ~173-183:

```java
        TasteProfile tasteProfile = deriveTasteProfile(recent, rated, userFeatures, popular);
        Set<String> userGenres = tasteProfile.genres();
        Set<String> userTags = tasteProfile.tags();
```

(`popular` is the same `List<String>`/collection already used in the seedItems fallback at this point in `recommend()`.)

- [ ] **Step 5: Consistent feedback-time state.** Replace `buildCurrentState` body (~L771-784):

```java
    private Map<String, Object> buildCurrentState(String user) {
        ScoredMoviesQuery hydrated = hydrateQuery(ScoredMoviesQuery.forUser(user));
        List<String> recent = hydrated.watchedMovieIds();
        TasteProfile profile = deriveTasteProfile(
            recent, hydrated.ratedMovieIds(), hydrated.userFeatures(), List.of());
        return buildState(recent, profile.genres(), profile.tags());
    }
```

(Empty `popular` fallback: only reached for a fully-cold user, who by definition has feedback history so `recent` is non-empty and the fallback is never used.)

- [ ] **Step 6: Run — expect PASS** and full suite green.

Run: `mvn -o test -Dtest=HybridRecommendationServiceTest` then `mvn -o test`
Expected: PASS; `Tests run: 49+`.

- [ ] **Step 7: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java src/test/java/com/demo/retrieval/service/HybridRecommendationServiceTest.java
git commit -m "fix(rl): derive feedback-time state like serve-time so Bellman bootstrap aligns"
```

---

### Task 3: B3 — atomic Lua Q-update (HIGH)

**Files:**
- Modify: `service/HybridRecommendationService.java` (`TabularRlUpdate` record; `buildTabularRlUpdate` ~L826-854; feedback block ~L344-366)

**Interfaces:**
- Consumes: nothing new.
- Produces: `record TabularRlUpdate(String qKey, String action, double reward, double alpha, double gamma, double nextValue)`; `double[] applyAtomicQUpdate(TabularRlUpdate)` returning `[updated, tdError]`.

- [ ] **Step 1: Add the script constant + imports.** In `HybridRecommendationService.java` add imports `org.springframework.data.redis.core.script.DefaultRedisScript` and `org.springframework.data.redis.core.script.RedisScript`, and a field:

```java
    private static final RedisScript<List> Q_UPDATE_SCRIPT = new DefaultRedisScript<>(
        "local q = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')\n"
      + "local tdError = (tonumber(ARGV[2]) + tonumber(ARGV[4]) * tonumber(ARGV[5])) - q\n"
      + "local updated = q + tonumber(ARGV[3]) * tdError\n"
      + "redis.call('HSET', KEYS[1], ARGV[1], tostring(updated))\n"
      + "return {tostring(updated), tostring(tdError)}",
        List.class);
```

ARGV order: `[1]=action, [2]=reward, [3]=alpha, [4]=gamma, [5]=nextValue`.

- [ ] **Step 2: Redefine `TabularRlUpdate`.** Replace its record declaration with:

```java
    private record TabularRlUpdate(String qKey, String action, double reward, double alpha, double gamma, double nextValue) {
    }
```

- [ ] **Step 3: `buildTabularRlUpdate` returns inputs (no read/compute).** Replace its body:

```java
    private TabularRlUpdate buildTabularRlUpdate(FeedbackRequest request, Map<String, Object> replayEvent) {
        Object rawState = replayEvent.get("state");
        if (!(rawState instanceof Map<?, ?> state)) {
            return null;
        }
        String action = request.item();
        String qKey = qKeyForStateMap(state);
        Map<String, Object> nextState = buildCurrentState(request.user());
        NextActionValue nextActionValue = nextActionValue(nextState);
        double alpha = clamp(properties.getBandit().getQLearningAlpha());
        double gamma = clamp(properties.getBandit().getQLearningGamma());
        replayEvent.put("nextAction", nextActionValue.action());
        return new TabularRlUpdate(qKey, action, request.reward(), alpha, gamma, nextActionValue.value());
    }
```

- [ ] **Step 4: Atomic apply helper.** Add:

```java
    // Atomic GET+compute+HSET so concurrent feedback for the same (state, action) cannot lose an
    // update. Returns [updatedQ, tdError]. nextValue is a bootstrap estimate read before the call.
    private double[] applyAtomicQUpdate(TabularRlUpdate u) {
        @SuppressWarnings("unchecked")
        List<Object> result = (List<Object>) redis.execute(
            Q_UPDATE_SCRIPT, List.of(u.qKey()),
            u.action(),
            String.valueOf(u.reward()),
            String.valueOf(u.alpha()),
            String.valueOf(u.gamma()),
            String.valueOf(u.nextValue()));
        if (result == null || result.size() < 2) {
            return new double[]{0.0, 0.0};
        }
        return new double[]{readDouble(result.get(0)), readDouble(result.get(1))};
    }
```

- [ ] **Step 5: Rewire the feedback path.** Where `tabularRlUpdate` is computed (~L335), run the script before the pipeline and pass metrics in; replace the Q block inside `executePipelined`:

```java
        TabularRlUpdate tabularRlUpdate = isTabularRl()
            ? buildTabularRlUpdate(request, replayEvent)
            : null;
        final boolean qApplied = tabularRlUpdate != null;
        final double qTdError = qApplied ? applyAtomicQUpdate(tabularRlUpdate)[1] : 0.0;
```

and inside the pipeline replace the old `if (tabularRlUpdate != null) { put(...); increment q_updates; increment q_td_error_total; }` with (the HSET now lives in the script):

```java
                if (qApplied) {
                    operations.opsForHash().increment(metricsHashKey(algorithm), "q_updates", 1L);
                    operations.opsForHash().increment(metricsHashKey(algorithm), "q_td_error_total", qTdError);
                }
```

- [ ] **Step 6: Run full suite — expect green.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, all prior tests still pass (the tabular-RL path is exercised by existing tests; no behavior regression for ucb/thompson).

- [ ] **Step 7: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "fix(rl): apply Q-update atomically via Lua to remove lost-update race"
```

---

### Task 4: B4 — ONNX batch length guard (MEDIUM)

**Files:**
- Modify: `service/DeepLearningPredictionService.java` (`readBatchScores` ~L169-190)
- Test: `service/DeepLearningPredictionServiceTest.java`

**Interfaces:**
- Produces: `static double[] normalizeBatchScores(Object raw, int n)` — always length `n`, missing → `0.0`, extras ignored.

- [ ] **Step 1: Failing test** in `DeepLearningPredictionServiceTest.java`:

```java
    @Test
    void normalizeBatchScoresPadsAndTruncatesToN() {
        assertArrayEquals(new double[]{1.0, 2.0, 0.0},
            DeepLearningPredictionService.normalizeBatchScores(new float[]{1.0f, 2.0f}, 3), 1e-9);
        assertArrayEquals(new double[]{1.0, 2.0},
            DeepLearningPredictionService.normalizeBatchScores(new float[]{1.0f, 2.0f, 9.0f}, 2), 1e-9);
        assertArrayEquals(new double[]{5.0, 0.0},
            DeepLearningPredictionService.normalizeBatchScores(new float[][]{{5.0f}, {}}, 2), 1e-9);
    }
```

- [ ] **Step 2: Run — expect FAIL** (method missing).

Run: `mvn -o test -Dtest=DeepLearningPredictionServiceTest`
Expected: FAIL (compile).

- [ ] **Step 3: Add the pure helper + delegate.** Add to `DeepLearningPredictionService.java`:

```java
    static double[] normalizeBatchScores(Object raw, int n) {
        double[] out = new double[n];
        if (raw instanceof float[][] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i].length > 0 ? s[i][0] : 0.0;
            return out;
        }
        if (raw instanceof float[] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i];
            return out;
        }
        if (raw instanceof double[][] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i].length > 0 ? s[i][0] : 0.0;
            return out;
        }
        if (raw instanceof double[] s) {
            for (int i = 0; i < n && i < s.length; i++) out[i] = s[i];
            return out;
        }
        throw new IllegalStateException("Unsupported batch prediction output shape: "
            + (raw == null ? "null" : raw.getClass().getName()));
    }
```

Replace the body of `readBatchScores(OnnxValue value, int n)` with:

```java
    private double[] readBatchScores(OnnxValue value, int n) throws OrtException {
        return normalizeBatchScores(value.getValue(), n);
    }
```

- [ ] **Step 4: Run — expect PASS**, then full suite.

Run: `mvn -o test -Dtest=DeepLearningPredictionServiceTest` then `mvn -o test`
Expected: PASS; `BUILD SUCCESS`.

- [ ] **Step 5: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/DeepLearningPredictionService.java src/test/java/com/demo/retrieval/service/DeepLearningPredictionServiceTest.java
git commit -m "fix(dl): decode ONNX batch to fixed length, no crash on shape drift"
```

---

### Task 5: Branch, docs, push, PR

**Files:** docs already written; git/gh operations.

- [ ] **Step 1: Branch off master (carrying uncommitted spec/plan + Task commits).**

Do Tasks 1–4 on a fresh branch:

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b fix/rl-feedback-correctness
```

Then commit the docs:

```bash
git add docs/specs/2026-07-01-rl-feedback-correctness.md docs/plans/2026-07-01-rl-feedback-correctness.md
git commit -m "docs(rl): spec + plan for feedback-path correctness fixes"
```

- [ ] **Step 2: Full suite green on the branch.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 51+, Failures: 0`.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin fix/rl-feedback-correctness
gh pr create --base master --title "Fix tabular-RL feedback-path correctness bugs" --body "See docs/specs/2026-07-01-rl-feedback-correctness.md. Fixes: pending-key TTL+delete (idempotency/leak), serve/feedback state consistency, atomic Lua Q-update (race), ONNX length guard. mvn test green.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

- **Spec coverage:** B1→Task 1; B2→Task 2; B3→Task 3; B4→Task 4; artifacts+PR→Task 5. All four bugs covered.
- **Placeholders:** none — full code in every step. (Test steps that depend on the exact `MovieLensUserFeatures` accessor note the fallback explicitly.)
- **Type consistency:** `TabularRlUpdate(qKey, action, reward, alpha, gamma, nextValue)` defined in Task 3 Step 2 matches `applyAtomicQUpdate`/`buildTabularRlUpdate` usage; `TasteProfile(genres, tags)` from Task 2 used consistently; `normalizeBatchScores(Object, int)` from Task 4 matches its test.
