# Spec: Simplify the Online Reward Model (OnlineLearningService)

> `OnlineLearningService` maintains per-global/item/genre/tag reward statistics in
> Redis (updated from the `/feedback` stream) and blends them into `onlineScore`,
> which feeds `learnedPrior`. Its `score()` repeats the same confidence-weighted
> accumulation block four times, and the `count`/`reward_total` decode is duplicated
> across `batchWarmRewardStats` and `readRewardEstimate`. This spec removes that
> repetition and adds the missing unit test. **Behavior-preserving**; the
> `reward-model:*` Redis keys are untouched.

## Objective

One data-driven blend loop instead of four copy-pasted blocks, one stats-parse
helper instead of two, and a focused test that pins the confidence-weighted blend —
with `onlineScore` output byte-identical.

## Scope

- **In:** `OnlineLearningService` (`score`, `batchWarmRewardStats`, `readRewardEstimate`,
  local `clamp`); a new `OnlineLearningServiceTest`.
- **Out:** the `reward-model:*` key format and the local `normalize` (a cross-language
  contract — the Python `movielens_pipeline.py` reads these keys); the 4-level blend
  model and its weights; `pipelineUpdate`; config.

## Changes

### S1 — collapse `score()`'s four blend blocks

Today `score()` has four near-identical blocks (global, item, genre, tag), each:
`if (est.count() > 0) { weight = configWeight · confidence(est.count()); weightedReward
+= est.mean()·weight; totalWeight += weight; }`. Replace with a list of
`(configWeight, estimate)` contributions and one fold:

```java
    public double score(String itemId, MovieProfile profile, double fallback) {
        RecommendationProperties.RewardModel cfg = properties.getRewardModel();
        List<WeightedEstimate> contributions = new ArrayList<>();
        contributions.add(new WeightedEstimate(cfg.getGlobalWeight(), readRewardEstimate(GLOBAL_KEY)));
        contributions.add(new WeightedEstimate(cfg.getItemWeight(), readRewardEstimate(ITEM_PREFIX + itemId)));
        if (profile != null) {
            contributions.add(new WeightedEstimate(cfg.getGenreWeight(),
                aggregateFeatureEstimates(GENRE_PREFIX, normalize(profile.getGenres()))));
            contributions.add(new WeightedEstimate(cfg.getTagWeight(),
                aggregateFeatureEstimates(TAG_PREFIX, normalize(profile.getTags()))));
        }
        double weightedReward = 0.0;
        double totalWeight = 0.0;
        for (WeightedEstimate c : contributions) {
            if (c.estimate().count() > 0) {
                double weight = c.configWeight() * confidence(c.estimate().count());
                weightedReward += c.estimate().mean() * weight;
                totalWeight += weight;
            }
        }
        return totalWeight == 0.0 ? clamp(fallback) : clamp(weightedReward / totalWeight);
    }

    private record WeightedEstimate(double configWeight, RewardEstimate estimate) {
    }
```

Same accumulation order and conditions → identical output.

### S2 — extract stats parsing

The `count = readLong(raw.get("count")); rewardTotal = readDouble(raw.get("reward_total"))`
decode appears in both `batchWarmRewardStats` and `readRewardEstimate`. Extract one
helper and use it in both:

```java
    private FeatureCache.RewardModelStats parseStats(Map<Object, Object> raw) {
        return new FeatureCache.RewardModelStats(
            raw == null ? 0L : readLong(raw.get("count")),
            raw == null ? 0.0 : readDouble(raw.get("reward_total")));
    }
```

### S3 — reuse `RecommendationConstants.clamp`

Replace the private `clamp` with `RecommendationConstants.clamp` (equivalent for the
finite reward means used here) and delete the local method.

## Work items & acceptance

- **S1.** *Accept:* `score()` has one blend loop; output identical for the same stats;
  the `recommend()` oracle stays green.
- **S2.** *Accept:* one `parseStats` helper feeds both `batchWarmRewardStats` and
  `readRewardEstimate`; no duplicated decode remains.
- **S3.** *Accept:* no local `clamp`; `RecommendationConstants.clamp` is used.
- **S4 (test).** *Accept:* `OnlineLearningServiceTest` asserts the confidence-weighted
  blend for seeded stats and the empty→`clamp(fallback)` path.

## Testing strategy

- **New `OnlineLearningServiceTest`** (no dedicated test exists today): construct a real
  `FeatureCache` + mock `StringRedisTemplate`; seed reward stats via `putRewardStats`;
  assert e.g. global mean 0.8 @ 0.15 + item mean 0.5 @ 0.45 → `0.575`, and empty stats →
  `clamp(fallback)`. Direct, Redis-free.
- **Oracle (unchanged):** `RecommendationControllerTest` + `HybridRecommendationServiceTest`
  exercise `score()` through `learnedPrior` end-to-end.
- Full module `mvn test` stays green.

## Non-goals / risks

- Purely a DRY tidy + test; no scoring change, no key-format change.
- The 4-level blend and its weights are unchanged; reworking the model is deferred,
  behavior-changing work.
