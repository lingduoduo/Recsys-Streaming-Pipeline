# Spec: Consolidate the RL Replay-Buffer Event Schema

> The RL replay event (`user`, state/context, candidate snapshot, action, reward,
> clicked, timestamp, nextState, …) is built as an untyped `Map<String,Object>` in
> two places with string-literal field names: `MovieLensServingSideEffects.serializeReplayContext`
> writes the serve-time context to `replay:pending:*`, and `HybridRecommendationService.buildReplayPayload`
> reads it back at `/feedback`, completes it, and pushes to `replay:recommendations`.
> The two build the same event with scattered literals, and `state`/`context` are
> exact duplicates. This spec defines the schema once and centralizes the feedback
> completion. **The stored JSON is unchanged except the removal of the duplicate
> `context` field** (which no consumer reads).

## Objective

One source of truth for the replay-event field names + type/version, and one place
for the feedback-completion logic — replacing the scattered string literals across two
classes.

## Scope

- **In:** a new `ReplayEvent` schema class; `MovieLensServingSideEffects.serializeReplayContext`
  (use the constants, drop `context`); `HybridRecommendationService.buildReplayPayload`
  (delegate completion); a new `ReplayEventTest`.
- **Out:** the cross-language `user`/`action` ↔ Python `userId`/`itemId` mismatch (a
  contract fix, deferred/documented); the `replay:pending`/`replay:recommendations` keys,
  TTL, and trim; the serve-time field *set* apart from removing the `context` duplicate.

## Context correction

`serializeReplayContext` already writes `timestamp` (serve-time) and the full field set;
`buildReplayPayload` adds `clicked`/`reward`/`feedbackTimestamp`/`nextState` and
re-asserts `type`/`schemaVersion`/`user`/`action` via `putIfAbsent` (needed only when the
pending context is missing/expired). Both are preserved.

## Changes

### R1 — `ReplayEvent` schema class

New `com.demo.retrieval.service.replay.ReplayEvent` with public constants for every
field name used in the event (`TYPE`, `SCHEMA_VERSION`, `USER`, `REQUEST_ID`, `STATE`,
`CANDIDATES`, `ACTION_SPACE`, `ACTION`, `ACTION_POSITION`, `SLATE_SIZE`, `POLICY`,
`MODEL_PREDICTIONS`, `ESTIMATED_REWARD`, `ONLINE_SCORE`, `BANDIT_SCORE`, `COLD_START`,
`TIMESTAMP`, `CLICKED`, `REWARD`, `FEEDBACK_TIMESTAMP`, `NEXT_STATE`) plus
`EVENT_TYPE = "rl_experience"` and `SCHEMA_VERSION_VALUE = 1`.

### R2 — centralize feedback completion

`ReplayEvent.applyFeedback(Map<String,Object> event, String user, String item,
boolean clicked, double reward, long feedbackTimestamp, Map<String,Object> nextState)`
holds the current `buildReplayPayload` body: `putIfAbsent(TYPE, EVENT_TYPE)`,
`putIfAbsent(SCHEMA_VERSION, SCHEMA_VERSION_VALUE)`, `putIfAbsent(USER, user)`,
`putIfAbsent(ACTION, item)`, then `put(CLICKED, clicked)`, `put(REWARD, reward)`,
`put(FEEDBACK_TIMESTAMP, feedbackTimestamp)`, `put(NEXT_STATE, nextState)`.
`buildReplayPayload` calls it (passing `System.currentTimeMillis()` and
`buildCurrentState(user)`) then serializes — identical output.

### R3 — serve-time constants + drop `context`

`serializeReplayContext` builds its map using the `ReplayEvent.*` constants instead of
literals, and omits `event.put("context", request.state())` (a duplicate of `state`).
Every other serve-time field is unchanged.

## Work items & acceptance

- **R1.** *Accept:* `ReplayEvent` defines every replay field name + type/version once; no
  string literal for these fields remains in the two classes.
- **R2.** *Accept:* `buildReplayPayload` delegates to `ReplayEvent.applyFeedback`; the
  pushed event JSON is identical (same fields/values) for a populated pending context.
- **R3.** *Accept:* the serve-time pending JSON no longer contains `context`; all other
  fields unchanged.

## Testing strategy

- **New `ReplayEventTest`:** `applyFeedback` on an empty map sets type/schemaVersion/user/
  action/clicked/reward/feedbackTimestamp/nextState; on a map that already has
  type/user/action, those are preserved and the feedback fields are added.
- **`MovieLensServingSideEffectsTest`:** keep the serve-time pending-payload assertion;
  add that the payload contains `"state"` but not `"context"`.
- **Oracle (unchanged):** `RecommendationControllerTest` + the Docker-gated
  `HybridFeedbackRedisTest` exercise the feedback/replay path.
- Full module `mvn test` stays green.

## Non-goals / risks

- Java-side schema consolidation only; the sole stored-format change is removing the
  duplicate `context` field (unread by any consumer).
- The Java↔Python field-name mismatch (`user`/`action` vs `userId`/`itemId`) — which
  breaks `replay_export.py` against real events — is **not** fixed here; it is a separate,
  wire-changing contract fix, documented for follow-up.
