# Spec: Simplify the Offline ONNX MLP Score Path

> The deep-learning MLP score (`DeepLearningPredictionService`, blended into
> `offlineScore` via `deep-learning-weight`) carries two avoidable costs: the single
> and batch ONNX decoders duplicate the same 4-shape tensor handling, and the ONNX
> model runs on **every** recommend request even though `deep-learning-weight`
> defaults to `0.0` (the term is multiplied by 0). Nothing in the service is dead —
> both `predict` overloads, `metadata`, `reload`, and `predictBatch` have callers.
> This spec unifies the decoder and skips the inference when DL is disabled.
> **Ranking behavior is unchanged.**

## Objective

One ONNX output decoder shared by the single and batch paths, and no wasted ONNX
inference when `deep-learning-weight` is `0.0`.

## Scope

- **In:** `DeepLearningPredictionService.readScore`; the DL/two-tower blend block in
  `HybridRecommendationService`; one added unit assertion.
- **Out:** the ONNX loading/lookup/reload logic, the two-tower service, the
  `blendOfflineScore` math, config values, and the controller endpoints (all `predict`
  overloads + `metadata` + `reload` stay — they are used by `RecommendationController`
  and `ModelReloadController`).

## Changes

### D1 — unify the ONNX score decoder

`readScore(OnnxValue)` currently reimplements the same `float[][]/float[]/double[][]/
double[]` shape ladder as `normalizeBatchScores`. Replace its body with a delegate:

```java
private double readScore(OnnxValue value) throws OrtException {
    return normalizeBatchScores(value.getValue(), 1)[0];
}
```

`normalizeBatchScores` becomes the single ONNX decoder. Edge alignment: an *empty*
model output now yields `0.0` for single-predict (matching the batch path) instead of
throwing; real models never emit empty output, so this is harmless.

### D2 — skip DL inference when the weight is 0

`HybridRecommendationService` runs `predictionService.predictBatch(...)` unconditionally
before blending it with `deep-learning-weight` (default `0.0`). Guard both the batch
call and the two-tower fusion on a positive weight:

```java
Map<String, Double> dlScoresRaw = properties.getBandit().getDeepLearningWeight() > 0.0
    ? predictionService.predictBatch(user, eligibleList)
    : Map.of();
if (properties.getBandit().getDeepLearningWeight() > 0.0 && twoTowerPredictionService.isEnabled()) {
    // existing clamp-both-then-max fusion, unchanged
}
```

When the weight is `0.0`, `dlScores` is empty and every `dlScore` defaults to `0.0` —
identical to the prior `× 0` contribution. The only observable change: the response's
diagnostic `deepLearningScore`/`dlScore` fields read `0.0` when DL is disabled, rather
than the raw (unused) model value.

## Work items & acceptance

- **D1.** *Accept:* `readScore` delegates to `normalizeBatchScores`; the duplicated shape
  ladder is gone; `DeepLearningPredictionServiceTest` (classpath-model `predict` +
  `normalizeBatchScores`) passes unchanged.
- **D2.** *Accept:* at the default weight, `predictBatch` is not invoked during
  `recommend`; ranking output is unchanged; with a positive weight the DL blend runs as
  before.

## Testing strategy

- **Oracle (unchanged):** `RecommendationControllerTest` + `HybridRecommendationServiceTest`
  prove ranking is unchanged end-to-end.
- **D1:** existing `DeepLearningPredictionServiceTest` exercises both decoder entry points.
- **D2:** add a `HybridRecommendationServiceTest` assertion that
  `predictionService.predictBatch(...)` is never called at the default (`0.0`) weight
  (`verify(..., never())`).
- Full module `mvn test` stays green.

## Non-goals / risks

- Purely a tidy + skip-when-disabled; no ranking/quality change.
- The disabled-path diagnostic `deepLearningScore` becomes `0.0` — an intentional,
  documented change to informational output only.
- If an operator sets `deep-learning-weight > 0`, behavior is byte-identical to today.
- **Deferred (separate, behavior-changing work — a signal-map investigation flagged these):**
  (a) `outcomeProbability` (`probabilities.overall()`) and `estimatedReward` are computed and
  surfaced in the response but never used to rank — `outcomeProbability` is a redundant blend of
  the same outcome probabilities that `weightedOutcome` already ranks with; (b) the two-tower
  model's 5 task heads are collapsed to one Double inside `TwoTowerPredictionService`, then
  `Math.max`-fused with the MLP score — per-task signal is lost before ranking; (c) a hardcoded
  `ScoringResult.diversityScore = 1.0` placeholder and Thompson's diagnostic-only exploration
  bonus being added into `predictionScore`. None are addressed here.
