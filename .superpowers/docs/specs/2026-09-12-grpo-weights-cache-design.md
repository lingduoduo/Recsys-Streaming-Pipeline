# GRPO weights through the feature cache

**Date:** 2026-09-12
**Status:** Implemented and verified; PR pending

## Problem and scope

[The online GRPO design](2026-08-30-online-grpo-design.md) says the serving scorer "reads the
weight vector through `FeatureCache` on the same TTL pattern as `reward-model:*`". The shipped
`GrpoPolicyScorer.readWeights` instead issues one Redis hash read, parse, and validation per
shadow slate and per `on`-mode request. The weights change at most once per training micro-batch
(`TRIGGER_INTERVAL`, default ten seconds), so nearly every read returns the same nine doubles.

This change routes the read through `FeatureCache` with its own TTL. It is a behavior change:
after a training batch writes new weights, serving picks them up within one TTL rather than on
the next request. `off` mode still reads nothing. The read is one round trip per request, so the
saving is modest and only applies where GRPO runs in `shadow` or `on`; it was approved on that
understanding.

## Global constraints

- Java 17 with Maven; add no dependencies (Caffeine is already used).
- Keep `GrpoPolicyScorer`'s modes, `blendWeight`, `reRankOrder`, `recordShadowSlate`, the
  version, width, and finite guards, and the failure contract (any exception degrades to no
  re-rank and no log line) unchanged.
- Keep the existing `item_vectors` and `reward_stats` caches and their configuration unchanged.
- Keep `off` mode free of Redis reads.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Leave the per-request read | One round trip per request when GRPO is enabled; nothing when off. Deviates from the design without documenting why. |
| A third Caffeine cache in `FeatureCache`, TTL matching the job trigger | Selected: the pattern the reward-model stats already use, one config knob, additive stats key. |
| A private cache inside `GrpoPolicyScorer` | Hides the cache from `stats()` and the measurement layer, which is where operators look. |
| Invalidate on write | The writer is a Spark job in another process; there is no serving-side hook. TTL is the only lever. |

## Design

### FeatureCache

A third cache `grpoWeights: Cache<String, double[]>` with `maximumSize(1)` (the scorer reads one
fixed key), `expireAfterWrite(cfg.getGrpoWeightsTtlSeconds())`, and `recordStats()`. Accessors
`getGrpoWeights(key)` (null when not cached) and `putGrpoWeights(key, weights)`. A rejected or
absent vector is stored as an empty array so it is not re-read until the TTL expires. `stats()`
gains the entry `grpo_weights`.

### Configuration

`RecommendationProperties.Cache` gains `grpoWeightsTtlSeconds`, default 10. `application.yml`
gains `recsys.cache.grpo-weights-ttl-seconds: ${RECSYS_GRPO_WEIGHTS_TTL:10}`.

### Scorer

`GrpoPolicyScorer(StringRedisTemplate, RecommendationProperties, FeatureCache)`. `readWeights`
returns the cached array when present; otherwise it reads Redis, runs the existing parse and
guards (moved into a private `parseWeights(Map)`), stores the result or an empty array, and
returns it. An empty array maps to `Optional.empty()`. Exceptions from Redis propagate as before
so the callers' existing catch blocks keep the failure contract.

`HybridRecommendationService` passes its `featureCache` when constructing the scorer.

### Tests

- `GrpoPolicyScorerTest.scorer(...)` builds a fresh `FeatureCache` per scorer so every existing
  test keeps its isolation. Two new tests: a second slate within the TTL causes no second Redis
  read; an absent vector is likewise not re-read.
- `FeatureCacheStatsTest` expects three stats entries and gains a test that GRPO weight lookups
  count separately.
- `MovieLensServingSideEffectsTest.offScorer()` passes a cache.

### Documentation

The README cache table gains the new variable. The README and pipeline-doc in-memory rows list
GRPO policy weights alongside item vectors and reward stats. The pipeline doc's GRPO section
states the TTL and the staleness bound.

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/model/FeatureCache.java` | Third cache, accessors, stats entry. |
| `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/config/RecommendationProperties.java` | `grpoWeightsTtlSeconds`. |
| `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml` | The property and its environment variable. |
| `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/grpo/GrpoPolicyScorer.java` | Cache-first read; `parseWeights`. |
| `recsys-pipeline/services/java-retrieval-service/src/main/java/com/demo/retrieval/service/HybridRecommendationService.java` | Constructor wiring. |
| `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/grpo/GrpoPolicyScorerTest.java` | Harness update; two cache tests. |
| `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/model/FeatureCacheStatsTest.java` | Three entries; GRPO lookup test. |
| `recsys-pipeline/services/java-retrieval-service/src/test/java/com/demo/retrieval/service/side_effects/MovieLensServingSideEffectsTest.java` | Harness update. |
| `recsys-pipeline/README.md`, `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md` | Variable, cache rows, staleness note. |
| `.superpowers/docs/plans/2026-09-12-grpo-weights-cache.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. Two shadow slates on one scorer cause exactly one `opsForHash()` call; the same holds when Redis
   holds no vector.
2. `FeatureCache.stats()` has three entries, and GRPO weight lookups count under `grpo_weights`
   without touching the other two.
3. Every pre-existing `GrpoPolicyScorerTest`, `FeatureCacheStatsTest`,
   `MovieLensServingSideEffectsTest`, and `RecommendationMeasurementServiceTest` case passes.
4. `off` mode still never calls `opsForHash()`.
5. The full retrieval Maven suite passes, with unrelated failures reported against a baseline.

## Risks and limits

Serving may apply weights up to one TTL old; at the default the bound equals the training
trigger, so a batch's weights reach serving no later than the next batch would have. Lowering the
TTL toward zero restores today's behavior. A rejected vector is also cached, so an operator who
fixes a bad key sees the fix within one TTL, not instantly. Rollback is a revert; the Redis
layout is unchanged.

## Verification record

- Baseline on the unchanged code, four affected classes: `GrpoPolicyScorerTest` 29,
  `FeatureCacheStatsTest` 3, `MovieLensServingSideEffectsTest` 3,
  `RecommendationMeasurementServiceTest` 11 (46 total, 0 failures).
- Task 1: the new stats test failed to compile on `getGrpoWeights` first; after the cache landed,
  `FeatureCacheStatsTest` 4 passed.
- Task 2: the harness change failed to compile on the three-argument constructor first; after the
  cache-first read and wiring, the four classes reported 31 / 4 / 3 / 11 (49 total, 0 failures).
  `offModeReadsNothingFromRedis` still passes, so `off` mode reads nothing.
- Full retrieval Maven suite on 2026-09-12: 332 run, 0 failures, 0 errors, 1 skipped
  (`UserProfileIntegrationTest`, Testcontainers blocked on Docker 29).
