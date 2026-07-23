# Spec: Tidy UCB/Thompson Bandit Exploration

> `HybridRecommendationService.computeBanditArmScore` builds a Beta posterior from
> `learnedPrior` + clicks/failures, then adds a UCB confidence bonus or returns a
> Thompson sample. The method inlines the UCB and Thompson branches, computes
> `clamp(baseScore)` twice, and — for cold-start/low-exposure items — has a confusing
> double application of `coldStartBoost` that nearly cancels. It also has no unit test.
> This spec makes the method readable and adds a pin test. **Behavior is unchanged**;
> the cold-start exploration math is left as-is (documented).

## Objective

A short, legible `computeBanditArmScore` (named UCB/Thompson helpers, one `clamp`), a
comment that explains the cold-start boost interplay, and a unit test that pins the
current UCB and Thompson behavior.

## Scope

- **In:** `computeBanditArmScore` + a couple of extracted private helpers; a code
  comment on the cold-start boost; visibility tweaks for testing; a new
  `BanditExplorationTest`.
- **Out:** the cold-start exploration formula (the double `coldStartBoost` — behavior-
  changing, deferred), the `sampleBeta`/`sampleGamma` sampler, config, and the
  Thompson/UCB semantics.

## Changes

### B1 — de-duplicate + extract (behavior-preserving)

- Compute `clamp(baseScore)` once into a local `base`, use it for `priorAlpha`/`priorBeta`.
- Extract the UCB confidence bonus into
  `double ucbExplorationBonus(long itemImpressions, long totalImpressions, double priorStrength, boolean coldStart)` —
  the existing `effectivePulls`/`confidence`/`explorationAlpha`/`coldStartBoost`/cap logic verbatim.
- Extract the Thompson branch into
  `BanditArmScore thompsonArmScore(double posteriorAlpha, double posteriorBeta, double posteriorMean)` —
  the existing `sampleBeta` + magnitude-cap logic verbatim.

`computeBanditArmScore` reduces to: build the Beta posterior → if `thompson`,
`return thompsonArmScore(...)` → else `return new BanditArmScore(mean, bonus, mean + bonus)`
with `bonus = ucbExplorationBonus(...)`. Same formulas → identical output.

### B2 — document the cold-start boost interplay

Add a short comment where `priorStrength` is boosted for cold-start, noting that the
boosted prior raises `effectivePulls` (shrinking the UCB confidence bonus) while
`coldStartBoost` also multiplies the bonus, so the two nearly offset — i.e. cold-start's
net UCB boost is small. Documentation only.

### B3 — pin test

Make `computeBanditArmScore` and the `BanditArmScore` record package-private so a
same-package test can call them. Add `BanditExplorationTest` (reusing the minimal-service
mock scaffolding from `HybridRecommendationServiceTest`):
- **UCB (deterministic):** an uncapped case asserting `posteriorMean`, the confidence
  bonus `explorationAlpha · √(ln(N+2)/(2·(effectivePulls+1)))`, and `rankingScore =
  mean + bonus` (tolerance `1e-6`); plus a capped case asserting `bonus =
  maxExplorationBonus` exactly.
- **Thompson (random):** assert `posteriorMean` exactly, `rankingScore ∈ [0,1]`, and
  `explorationBonus ≤ maxExplorationBonus`.

## Work items & acceptance

- **B1.** *Accept:* `computeBanditArmScore` delegates to `ucbExplorationBonus` /
  `thompsonArmScore`; `clamp(baseScore)` computed once; UCB/Thompson output identical
  for the same inputs.
- **B2.** *Accept:* a comment explains the cold-start double-boost near-cancellation.
- **B3.** *Accept:* `BanditExplorationTest` passes for the UCB (uncapped + capped) and
  Thompson cases.

## Testing strategy

- **New `BanditExplorationTest`** pins UCB (deterministic) + Thompson (bounds).
- **Oracle (unchanged):** `RecommendationControllerTest` + `HybridRecommendationServiceTest`
  (`recommendsFresh` ranks a cold-start item) prove ranking is unchanged.
- Full module `mvn test` stays green.

## Non-goals / risks

- Purely a readability tidy + test; no exploration-behavior change.
- The cold-start double-`coldStartBoost` (near self-cancelling) is intentionally left in
  place and only documented — fixing it is behavior-changing and unvalidatable without data.
