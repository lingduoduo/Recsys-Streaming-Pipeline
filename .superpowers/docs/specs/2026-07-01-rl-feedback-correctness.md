# Spec: Tabular-RL Feedback-Path Correctness Fixes

> Third follow-up to the scoring-formula audit (after
> [scoring-cutoff-centralization.md](2026-07-01-scoring-cutoff-centralization.md)
> and [qlearning-state-recurrence.md](2026-07-01-qlearning-state-recurrence.md)).
> A deeper review of the serve → feedback → Bellman-update path found four **new**,
> code-verified correctness defects concentrated in the tabular-RL feedback path.
> This spec fixes all four. Behavior on the non-buggy paths is unchanged.

## Objective

Make the tabular Q-learning/SARSA feedback path correct under real conditions:
idempotent to duplicate/late feedback, consistent between the state used to serve
and the state used to bootstrap, race-free under concurrency, and crash-free on
model-output drift.

## Scope

- **In:** `HybridRecommendationService` (feedback path, `buildTabularRlUpdate`,
  `buildCurrentState`, serve-time state derivation), `MovieLensServingSideEffects`
  (pending-key write), `DeepLearningPredictionService` (`readBatchScores`),
  `RecommendationProperties.ReplayBuffer` (new TTL), a Lua script resource.
- **Out:** full feedback idempotency for the click/reward counters (needs an
  event-id dedup guard — noted as a known gap, not fixed here); the LOW findings
  (`readCount` dead code, `Math::max` two-tower/DL scale-mixing); any change to
  UCB/Thompson/offline-blend paths (verified correct after the prior fixes).

## Bugs & fixes

### B1 — pending replay key never expired or deleted (CRITICAL)

`replay:pending:{user}:{item}` is `set` with no TTL at serve time
(`MovieLensServingSideEffects` line ~60) and read at feedback
(`HybridRecommendationService` line ~334) but never removed. Duplicate/late/retried
feedback re-reads the same serve-time state and re-applies the Bellman update,
double-counting `reward_total`/`q_updates` and corrupting Q; the keys also
accumulate forever (memory leak).

**Fix:** (a) write the pending key **with a TTL** — new
`recsys.replay-buffer.pending-ttl` (default `1h`); (b) **delete** the pending key
inside the feedback write pipeline after it is read. On a duplicate,
`readPendingReplayContext` returns empty → `buildTabularRlUpdate` returns `null`
(no stored `state`) → no Q re-application. The counter double-count on duplicates
is out of scope (see Scope).

### B2 — serve-time vs feedback-time state derivation mismatch (HIGH)

Serve-time genres/tags derive from `seedItems`
(`firstNonEmpty(recent, cachedMovieIds, actionSequence, retrieval, scoring, rated,
popular)`) **plus `favoriteGenres`** (lines ~163-184). Feedback-time
`buildCurrentState` (lines ~781-795) derives them from `user:{user}:recent` only.
Since the Q-key hashes the genre/tag signature ([TabularStateKey]), the Bellman
`nextState` (s′) lands in a different/empty bucket than the serving path writes, so
the `γ·maxQ(s′)` bootstrap is ~0 — partially defeating the finding-5 fix.

**Fix:** extract the serve-time taste-profile derivation into one helper (returning
sorted genre/tag sets from the same inputs, `favoriteGenres` included). `recommend()`
and `buildCurrentState()` both call it; `buildCurrentState` hydrates the user's
`MovieLensUserFeatures` through the existing `hydrateQuery` path and supplies the
same `popular` fallback, so s′ is derived identically to serve-time state.

### B3 — lost-update race on the Q-value (HIGH)

`currentQ` is read (`HGET`) in `buildTabularRlUpdate` (line ~834) **before** the
`executePipelined` block, and the new value is blind-`HSET` inside it (line ~356).
`executePipelined` is not a transaction, so two concurrent feedback events for the
same (state, action) both read the same `currentQ` and the second write discards
the first update.

**Fix:** perform the read-compute-write atomically with a **Lua script**
(`EVAL` via `DefaultRedisScript`): `q = HGET(key, action); updated =
q + α·(reward + γ·nextValue − q); HSET(key, action, updated); return {updated,
tdError}`. `nextValue` (the s′ bootstrap) is read in Java and passed as an arg.
`buildTabularRlUpdate` returns the script inputs; `recordFeedback` runs the script
first, then pipelines the `q_updates`/`q_td_error_total` metric increments using the
returned `tdError`. The Q `HSET` moves out of the batch (one extra round-trip, only
for `q-learning`/`sarsa`). WATCH/MULTI+retry was considered and rejected as more
code and a poorer fit with the existing pipeline.

### B4 — ONNX batch output length not validated (MEDIUM)

`DeepLearningPredictionService.predictBatch` writes `scores[i]` for `i < n`
(line ~130) using `readBatchScores` output without checking its length equals `n`.
A model that emits a different-length output throws `ArrayIndexOutOfBounds` →
`IllegalStateException`, and because `predictBatch` is called unguarded (line ~215)
it crashes the whole `recommend` request with a 500.

**Fix:** make `readBatchScores` always return a length-`n` array — fill missing
indices with `0.0`, ignore extras. A shape drift then degrades gracefully (the DL
term contributes 0) instead of crashing.

## Work items & acceptance

- **B1.** *Accept:* serve-time pending `set` uses the configured TTL; a second
  feedback for the same (user, item) applies **no** Q-update (pending key gone);
  `pending-ttl` is configurable via `RECSYS_REPLAY_PENDING_TTL`.
- **B2.** *Accept:* a pure derivation helper returns identical genre/tag signatures
  for the same inputs; `buildCurrentState` and `recommend` both use it; for a user
  with non-empty history the feedback-time s′ signature equals the serve-time state
  signature.
- **B3.** *Accept:* the Q-update goes through the atomic Lua script; a unit test of
  the TD arithmetic matches `q + α·(r + γ·nextValue − q)`; the `q_updates` and
  `q_td_error_total` metrics still update. (Concurrency itself is not unit-tested;
  atomicity is structural.)
- **B4.** *Accept:* `readBatchScores` returns exactly `n` entries for shorter and
  longer raw outputs, defaulting missing to `0.0`, with no exception.

## Testing strategy

- **B1:** `MovieLensServingSideEffectsTest` asserts the pending `set` carries a TTL;
  a feedback-path test asserts the pending key is deleted and a replayed feedback
  produces no second Q write.
- **B2:** unit test the derivation helper (same inputs → same signature; includes
  `favoriteGenres`).
- **B3:** unit test the TD-update arithmetic and assert the atomic script path is
  invoked (mock `redis.execute(script, …)`).
- **B4:** unit test `readBatchScores` length normalization (short, exact, long).
- Full module `mvn test` stays green (48 → ~53).

## Non-goals / risks

- Duplicate-feedback counter idempotency (clicks/reward/online-stats) remains a known
  gap; fixing it needs a per-event dedup key and is deferred.
- B3 keeps `nextValue` read non-atomically with the update (a bootstrap estimate);
  full transactional read of s′ is unnecessary for TD(0) and out of scope.
- Hydrating features at feedback time (B2) adds Redis reads to the feedback path;
  acceptable given feedback volume is far below serve volume.
