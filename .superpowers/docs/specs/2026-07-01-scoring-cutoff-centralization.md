# Spec: Centralize In-Code Scoring Cutoffs

> Follow-up to an audit of the `java-retrieval-service` scoring path against its
> intended formula (`offlineScore → learnedPrior → banditScore`). The audit found
> the algebra correct but several **cutoff/scale bugs** where the `[0, 1]` invariant
> the pipeline assumes was silently violated by magic-number weights scattered
> across the ranking code. Phase A (already shipped) fixed the correctness bugs and
> introduced `RecommendationConstants`. This spec covers **Phase B**: giving the
> remaining tunable ranking weights documented names, with a drift-detecting test —
> a pure refactor with **no behavior change**.

## Objective

Make every tunable ranking weight in the scoring path greppable, self-documenting,
and guarded against silent drift, by naming the inline magic numbers in
`MovieLensOutcomeScorer` (following its existing `DIVERSITY_DECAY`/`DIVERSITY_FLOOR`
pattern). Preserve exact numerical behavior; add an invariant test that the
convex blends sum to `1.0` so a future edit cannot reintroduce the original
"weights sum to 1.15" class of bug.

## Scope

- **In:** `MovieLensOutcomeScorer.java` (the inline blend weights in `score()` and
  `weightedOutcome()`) and its test.
- **Out:** the `movieLensOutcomeProbabilities` logit coefficients (`-1.15, 1.20,
  0.85, …` and per-head sigmoid offsets) — a fitted linear model, not cutoffs; the
  novelty formula `1/(impr+1)`; UCB confidence internals; anything in
  `application.yml` (operator-tunable surface stays there); the Python modeling repo.

## Starting state

Phase A (shipped) established the pattern and fixed the correctness bugs:

| Item | Status |
|------|--------|
| `RecommendationConstants` (clamp bounds, `WARM_PRIOR_STRENGTH`, cold-start multiplier, content genre/tag weights) | Done |
| Finding 1 — offlineScore weights summed to 1.15 → `blendOfflineScore` normalizes to `[0,1]` | Done |
| Finding 2 — dlScore entered raw → clamped in the blend | Done |
| Finding 3 — DL-weight doc drift (`0.0` vs `0.15`) | Done |
| Finding 4 — unbounded Q-value vs `[0,1]` explore path → `clamp()` both | Done |
| Finding 7 — `WARM_PRIOR_STRENGTH` magic number → named constant | Done |

Remaining inline magic numbers in `MovieLensOutcomeScorer` (Phase B target):

| Group | Location | Current inline values |
|-------|----------|-----------------------|
| Exploitation blend | `score()` | `0.55 / 0.25 / 0.15 / 0.05` |
| Estimated-reward blend | `score()` | `0.65 / 0.35` |
| Weighted-outcome blend | `weightedOutcome()` | `0.30 / 0.22 / 0.18 / 0.22 / 0.08` + `0.35` penalty |
| Diversity curve | fields | `0.72 / 0.55` — **already named**, no change |

## Work items & acceptance

**B1 — Name the exploitation blend.**
Introduce `EXPLOITATION_BANDIT_WEIGHT = 0.55`, `EXPLOITATION_OUTCOME_WEIGHT = 0.25`,
`EXPLOITATION_DL_WEIGHT = 0.15`, `EXPLOITATION_Q_WEIGHT = 0.05` as `private static
final` fields; use them in `score()`.
- *Accept:* `score()` contains no numeric literals for this blend; output byte-identical
  to before for any input (existing `MovieLensOutcomeScorerTest` passes unchanged).

**B2 — Name the estimated-reward blend.**
Introduce `ESTIMATED_REWARD_POSTERIOR_WEIGHT = 0.65`,
`ESTIMATED_REWARD_OUTCOME_WEIGHT = 0.35`; use them in `score()`.
- *Accept:* no inline literals for this blend; behavior unchanged.

**B3 — Name the weighted-outcome blend.**
Introduce `OUTCOME_POSITIVE_RATING_WEIGHT = 0.30`, `OUTCOME_PREFERENCE_WEIGHT = 0.22`,
`OUTCOME_CLICK_WEIGHT = 0.18`, `OUTCOME_WATCH_WEIGHT = 0.22`,
`OUTCOME_NOVEL_DISCOVERY_WEIGHT = 0.08`, `OUTCOME_NEGATIVE_FEEDBACK_PENALTY = 0.35`;
use them in `weightedOutcome()`.
- *Accept:* no inline literals for this blend; behavior unchanged.

**B4 — Add the sum-to-1 invariant test.**
New test asserting the three convex blends each sum to `1.0`:
exploitation (`0.55+0.25+0.15+0.05`), estimated-reward (`0.65+0.35`), and the
positive-outcome weights (`0.30+0.22+0.18+0.22+0.08`). The negative-feedback penalty
is a subtractive term and is excluded from the sum.
- *Accept:* the test fails if any of these constants is later changed such that its
  group no longer sums to `1.0` (the drift detector that would have caught finding 1).

## Testing strategy

- **Behavior-preservation:** the existing `MovieLensOutcomeScorerTest` is the oracle —
  it must pass without modification, proving B1–B3 are a pure rename.
- **Invariant:** B4 adds the convex-sum guard.
- Full module `mvn test` stays green (43 → 44 tests).

## Non-goals / risks

- Not making these weights `application.yml`-configurable — they are in-code cutoffs by
  decision; promoting them to config is a separate change if ever wanted.
- The logit coefficients remain inline on purpose; naming them would imply they are
  independently tunable knobs, which they are not.
