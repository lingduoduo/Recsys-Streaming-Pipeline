# Spec: Extract Content Retriever + Shared Scoring Core

> Completes the extraction deferred in
> [simplify-content-retrieval.md](2026-07-01-simplify-content-retrieval.md) / PR #114.
> #114 collapsed the 9-stage pipeline into an inline `generateCandidates` but left
> retrieval inside the 1455-line `HybridRecommendationService`, because retrieval and
> scoring share catalog + content-scoring helpers. This spec pulls that shared core
> into its own component and moves the retrieval flow into a dedicated retriever.
> **Behavior-preserving** — but, unlike #114, it **touches the scoring path**.

## Objective

Give retrieval and the shared catalog/content core clear homes: a `CatalogContentScoring`
component used by both the retriever and the scorer, and a `ContentCandidateRetriever`
that owns the retrieval flow. `HybridRecommendationService` keeps ranking orchestration
and delegates catalog/content + retrieval out.

## Scope

- **In:** `HybridRecommendationService` (extract the shared helpers + retrieval methods;
  rewire `scoreCandidate` and `generateCandidates`); new `CatalogContentScoring`,
  `ContentCandidateRetriever`, `TextNormalization`; move `NormalizedProfile` to a top-level type.
- **Out:** ranking math / bandit / RL; embeddings; retrieval behavior; config values.
- **Base:** `master` (PR #114 merged); PR targets `master`.

## Helper map (from exploration)

- **Shared (retrieval + scoring):** `getNormalizedCatalog` (+`CatalogCache`, `NormalizedProfile`),
  `contentScore(NormalizedProfile,…)` (+`overlapRatio`). `scoreCandidate` uses both.
- **Retrieval-only:** `generateCandidates`, `fetchPopularCandidates`, `fetchColdStartCandidates`,
  `filterEligibleCandidates`, `preRankCandidates`, `selectDistinctCandidates`, `mergeCandidate`,
  `contentScoreForItem`, `isNewRelease`, `isEligibleCandidate`, `RetrievalOutcome`.
- **Broad text helpers:** `normalize(List)`, `normalizeValue(String)` — used by catalog-building,
  `deriveTasteProfile` (scoring-side), and eligibility.

## Target design

**`TextNormalization`** (new, `com.demo.retrieval.service.text`): two pure static methods
`normalize(List<String>) → Set<String>` and `normalizeValue(String) → String` (lower-case/trim,
blank-filtering). Callable from anywhere; no state.

**`CatalogContentScoring`** (new, `com.demo.retrieval.service.content`): the shared catalog +
content core. Constructor takes `RecommendationProperties`. API:
- `Map<String, NormalizedProfile> normalizedCatalog()` — the cached normalized catalog (owns
  `CatalogCache`, built via `TextNormalization`).
- `NormalizedProfile profileFor(String itemId)` — convenience `normalizedCatalog().get(itemId)`.
- `double contentScore(NormalizedProfile, Set<String> genres, Set<String> tags)` (+ `overlapRatio`).
- `boolean isNewRelease(String itemId)`.
- `NormalizedProfile` becomes a top-level record here (was nested in the service).

**`ContentCandidateRetriever`** (new, `com.demo.retrieval.service.retrieval`): the retrieval flow.
Constructor: `StringRedisTemplate`, `RecommendationProperties`, `CatalogContentScoring`. Public
`RetrievalOutcome retrieve(ScoredMoviesQuery query, Map<String,Double> popularityMap,
Set<String> excludedItems, Set<String> userGenres, Set<String> userTags, FilterContext filterCtx,
int limit)`. Moves `fetch*`/`filter*`/`preRank`/`selectDistinct`/`mergeCandidate`/`contentScoreForItem`/
`isNewRelease`/`isEligibleCandidate` verbatim, calling `CatalogContentScoring` for the normalized
catalog + content score. `RetrievalOutcome` and `MovieCandidate` live in this package.

**`HybridRecommendationService`:** constructs `CatalogContentScoring` + `ContentCandidateRetriever`
(sharing the one `CatalogContentScoring`). Deletes all the moved helpers. `generateCandidates`
becomes a one-line delegate to `retriever.retrieve(...)`. `scoreCandidate` rewires: the two shared
calls become `catalogContentScoring.profileFor(id)` and `catalogContentScoring.contentScore(...)`;
`deriveTasteProfile`/eligibility text calls become `TextNormalization.normalize(...)`.

## Work items & acceptance

- **X1 — `TextNormalization`.** *Accept:* pure statics; every prior `normalize`/`normalizeValue`
  call routes through it; behavior identical.
- **X2 — `CatalogContentScoring`.** *Accept:* both retriever and `scoreCandidate` obtain the
  normalized catalog + content score from it; the cache still keys on catalog identity;
  `NormalizedProfile` is top-level.
- **X3 — `ContentCandidateRetriever`.** *Accept:* `generateCandidates` delegates to `retrieve`;
  retrieval output + the retrieved/filtered/scored/selected metrics are identical.
- **X4 — service rewired + shrunk.** *Accept:* the moved helpers/methods are gone from the
  service; it compiles; `scoreCandidate` uses the shared component.
- **X5 — behavior preserved.** *Accept:* `RecommendationControllerTest` (13) and
  `HybridRecommendationServiceTest` pass unchanged.

## Testing strategy

- **Oracle (unchanged):** `RecommendationControllerTest` + `recommend()`/`deriveTasteProfile`
  tests prove retrieval **and** scoring outputs are identical end-to-end — the guardrail for the
  scoring-path touch.
- **New `CatalogContentScoringTest`** (real coverage gain, now that it's isolated): normalized-catalog
  build (genres/tags/keywords/newRelease), `contentScore` Jaccard blend, `isNewRelease`, cache reuse.
- **New `TextNormalizationTest`:** lower-case/trim, blank-filtering, null handling.
- Full `mvn test` stays green; net test count rises (47 + the two new unit classes).

## Non-goals / risks

- **Scoring-path blast radius:** `scoreCandidate` is rewired to the shared component. Behavior is
  identical, but this is the first change to touch ranking; the end-to-end oracle is the guard.
- Package/type moves (`NormalizedProfile`, `MovieCandidate`, `FilterContext`, `RetrievalOutcome`)
  cause mechanical import churn.
- Larger diff than #114; still purely structural — no ranking/quality change.
