# Spec: Recurring Q-Learning State Keys

> Second follow-up to the scoring-formula audit (see
> [scoring-cutoff-centralization.md](2026-07-01-scoring-cutoff-centralization.md)).
> The audit's **finding 5**: the tabular Q-learning/SARSA state key is a SHA-256 hash
> of `{recent watched item-IDs, genres, tags}`. Because `recent` is a list of raw
> item-IDs, the state space is combinatorially huge and sparse — next-states in the
> Bellman update are almost never revisited, so the bootstrap term is ~0 and the
> update degenerates to `Q += α(reward − Q)`, a running average of immediate reward
> with no temporal credit. This spec makes states **recur** by keying the Q-table on
> the coarse genre/tag taste profile only. It also bundles **finding 6**, a
> comment-only clarification of Thompson's ranking-score semantics.

## Objective

Restore meaningful temporal bootstrapping to the tabular RL path by hashing the
Q-table on a **recurring signature** (sorted genre set + sorted tag set) instead of
the raw recent item-IDs — while keeping `recent` in the state payload so action
filtering is unaffected. No dataset is available to validate learning quality
(only 264-byte sample data), so success is defined **structurally**: equivalent
taste profiles must map to the same key, and distinct profiles to distinct keys.

## Scope

- **In:** the state-key derivation in `HybridRecommendationService` (`stateKey`,
  `buildState`); a new pure `TabularStateKey` helper; a one-line clarifying comment
  for finding 6 at `computeBanditArmScore`.
- **Out:** changing the Bellman/TD math, the reward signal, the action space
  definition, `application.yml`, or the two-tower/ONNX paths. No Redis migration.

## Design

**Key vs. payload separation.** `recent` has two roles: (1) part of the Q-key
[the bug], and (2) the exclusion set in `nextActionSpace` [must keep]. The fix
keeps `buildState` producing `{recent, genres, tags}` unchanged, but derives the
**key** from the genre/tag signature only.

**Single funnel.** Serve-time (`stateKey` at ~L196 → `batchFetchQValues`),
update-time (`qKeyForStateMap` at ~L844), and next-state (~L877) all route through
`stateKey(Object)`. Changing that one method keeps every site consistent.

**`TabularStateKey` helper (approach A — extracted, pure, unit-testable).**

```java
final class TabularStateKey {
    static String hash(Object genres, Object tags);   // base64url(SHA-256(canonical))
}
```

- Canonical form is order-independent and de-duplicated:
  `"g:" + sortedJoin(genres) + "|t:" + sortedJoin(tags)`, where a null/non-collection
  argument normalizes to empty. Self-contained: its own `MessageDigest`, no
  `ObjectMapper`.
- `HybridRecommendationService.stateKey(Object state)` becomes a thin delegate:
  for a `Map`, return `TabularStateKey.hash(map.get("genres"), map.get("tags"))`.

**Orphaned Q-values (accepted).** Existing `q-learning:q:*` / `sarsa:q:*` Redis
hashes were keyed by the old full-state hash and become unreferenced after this
change. This is harmless — there is no real learned Q-data in this demo — so there
is **no migration**; stale keys simply stop being read.

**Cleanup.** After the delegate change, the service's `SHA256_DIGEST` `ThreadLocal`
and any now-unused imports (`MessageDigest`, `NoSuchAlgorithmException`, `Base64`,
`StandardCharsets`) are removed if and only if nothing else references them.

## Finding 6 — Thompson ranking semantics (comment only)

The audit noted the formula's `BetaPosteriorMean + explorationBonus` framing is
literal only for **UCB**. For **Thompson**, `computeBanditArmScore` returns the
posterior **sample** as the ranking score; the reported `|sample − mean|` is a
diagnostic, not an addend. This is correct Thompson sampling. Add a short comment at
the Thompson branch making that explicit. No behavior change.

## Work items & acceptance

**F5.1 — `TabularStateKey` helper.**
- *Accept:* `TabularStateKey.hash(genres, tags)` returns equal keys for the same
  genre/tag sets regardless of element order or duplicates, and distinct keys for
  distinct sets; null/empty inputs are handled without throwing.

**F5.2 — Delegate `stateKey` to the signature.**
- *Accept:* two state maps with identical `genres`/`tags` but different `recent`
  lists produce the **same** Q-key; `buildState` still includes `recent`;
  `nextActionSpace` still filters on it. The full module test suite stays green.

**F5.3 — Cleanup.**
- *Accept:* no unused fields/imports remain from the old hashing path; the project
  compiles with no new warnings introduced by this change.

**F6.1 — Thompson clarifying comment.**
- *Accept:* a comment at the Thompson branch states the ranking score is the
  posterior sample; no code/behavior change.

## Testing strategy

- **Unit (pure):** new `TabularStateKeyTest` — order-independence, de-duplication,
  discrimination, null/empty handling.
- **Recurrence lock:** a test asserting two `{genres, tags}` payloads that differ
  only in `recent` hash to the same key (guards against re-adding item-IDs to the key).
- **Regression:** full `mvn test` remains green (currently 44 tests).

## Non-goals / risks

- Coarser state generalizes across users sharing a taste profile — intended, and
  beneficial for cold-start, but it means the RL no longer distinguishes users by
  exact history. Accepted per design.
- Without real data, we prove recurrence structurally, not that learned policies
  improve. That validation is deferred to whenever a real ratings stream exists.
