# Spec: Consolidate Ranking Score Signals

> A signal-map investigation of the ranking path found the final rank key is
> `predictionScore × diversityMultiplier`, and that `MovieLensOutcomeProbabilities.overall()`
> (surfaced as `outcomeProbability`) is a **second, redundant blend** of the same six
> outcome probabilities that `weightedOutcome` already ranks with — it is computed and
> echoed in the response but never affects ranking. This spec removes that dead blend so
> there is one outcome blend, and names the `1.0` diversity default. **Ranking output is
> unchanged.**

## Objective

One outcome blend (`weightedOutcome`) instead of two, and no hardcoded magic `1.0` in
the scorer — without changing what gets ranked.

## Scope

- **In:** `MovieLensOutcomeScorer` (`overall()`, the `ScoringResult.outcomeProbability`
  field, the `1.0` diversity default); `HybridRecommendationService` (the `ScoredCandidate.outcomeProbability`
  field + its two response surfacings); `MovieLensOutcomeScorerTest`.
- **Out:** the ranking formula (`predictionScore`, `weightedOutcome`, `estimatedReward`,
  exploitation weights) — unchanged; `estimatedReward` stays (it feeds pseudoRegret +
  `avgEstimatedReward` telemetry); the two-tower head collapse and the Thompson
  exploration-bonus term (both behavior-changing — explicitly deferred).

## Investigation correction

The investigation flagged `ScoringResult.diversityScore = 1.0` as a dead placeholder.
It is **not** dead: `applyDiversity` overwrites it only when there are ≥2 candidates, so
for a single-result case the `1.0` is the legitimate identity multiplier. This spec
**names** it rather than removing it.

## Changes

### C1 — remove the redundant `outcomeProbability` blend

`overall()` = `clamp((positiveRating + preference + click + watch + novelDiscovery)/5 −
0.25·negativeFeedback)`; `weightedOutcome` = a different weighted blend of the same six
probabilities. Only `weightedOutcome` enters `predictionScore`. Remove the dead sibling:

- delete `MovieLensOutcomeProbabilities.overall()`;
- remove the `outcomeProbability` field from `MovieLensOutcomeScorer.ScoringResult`
  (and stop passing `probabilities.overall()` into it);
- remove the `outcomeProbability` field from `HybridRecommendationService.ScoredCandidate`
  and the value threaded from `ScoringResult`;
- remove the two response surfacings: `row.put("outcomeProbability", …)` (diagnostics)
  and `predictions.put("outcomeProbability", …)` (modelPredictions).

Ranking is unaffected. The response loses the `outcomeProbability` diagnostic field —
observable but ranking-irrelevant (same class of change as #116's `deepLearningScore`).

### C2 — name the diversity identity default

In `MovieLensOutcomeScorer`, add `private static final double NO_DIVERSITY_MULTIPLIER = 1.0;`
and use it where `score()` currently returns the hardcoded `1.0` diversity slot. No
behavior change.

## Work items & acceptance

- **C1.** *Accept:* `overall()` and every `outcomeProbability` field/surfacing are gone;
  `weightedOutcome` is the only outcome blend; `predictionScore`/`finalScore` values are
  identical to before for the same inputs.
- **C2.** *Accept:* no bare `1.0` diversity literal in `score()`; the named constant is
  used; single-candidate diversity behavior is unchanged.

## Testing strategy

- **Ranking oracle (must stay green, unchanged):** `RecommendationControllerTest` and
  `HybridRecommendationServiceTest` — proves ranking is unchanged. If a controller test
  asserts the `outcomeProbability` response field, drop that one assertion (a removed
  diagnostic, not a ranking value).
- **`MovieLensOutcomeScorerTest`:** remove the two assertions on `outcomeProbability()` /
  `overall()` (they test the deleted blend); keep the `weightedOutcomeScore` /
  `finalScore` / probability-name assertions.
- Full module `mvn test` stays green.

## Non-goals / risks

- Ranking-preserving only; no scoring-formula change.
- Observable response change: the `outcomeProbability` field is removed.
- Deferred (behavior-changing, unvalidatable without data): wiring the two-tower's five
  per-task heads into the outcome model instead of `max`-collapsing to one `dlScore`;
  removing Thompson's diagnostic-only `|sample−mean|` term from `predictionScore`.
