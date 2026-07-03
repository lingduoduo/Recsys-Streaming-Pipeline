# Spec: Simplify Content-Based Retrieval

> Collapse the over-engineered candidate-generation pipeline in
> `java-retrieval-service` into a direct linear flow. A map of the current path
> found a generic 9-stage `HunkkerCandidatePipeline` driven by **7 single-method
> interfaces with zero concrete implementers** (every stage is a lambda in the
> 1455-line `HybridRecommendationService`), plus a dead `Builder`, dead
> introspection, a dead hydrator stage, dead config, and a duplicated content
> score. The actual work is: **fetch popular + cold-start → filter → weight-sort/
> trim → dedup.** This spec makes the code match that reality. **No behavior change.**
>
> _Design note: an earlier draft proposed extracting a `ContentCandidateRetriever`
> class. Exploration showed retrieval shares `getNormalizedCatalog`/`contentScore`/
> `normalize`/`NormalizedProfile` with the scoring path, so a clean extraction would
> force a shared-helper refactor touching scoring too. We chose the lower-risk
> **inline collapse**: delete the abstraction, keep the retrieval methods in the
> service, wire a direct `generateCandidates`._

## Objective

Replace the generic pipeline machinery with a straightforward `generateCandidates`
that performs content-based retrieval from **popularity (seed) + genre/tag overlap** —
the signals that actually drive retrieval — producing an identical candidate pool and
identical serving metrics. (Embeddings are not part of retrieval; they enter only in
scoring and are untouched.)

## Scope

- **In:** the `candidate_pipeline/` package; `candidate_hydrators/CandidateHydrator`;
  `HybridRecommendationService` retrieval wiring/methods;
  `RecommendationProperties.CandidateGeneration` (dead config); the duplicate
  `contentScore` overload; the pipeline's test.
- **Out:** retrieval *behavior* (sources, filters, sort, dedup, and the
  retrieved/filtered/scored metric semantics all preserved verbatim); the scoring path;
  embeddings; `application.yml` values that are read.

## Current shape (to collapse)

`buildCandidatePipeline` wires a `HunkkerCandidatePipeline` whose live stages are
only: sources `[fetchPopularCandidates, fetchColdStartCandidates]`, filters
`[3 history filters + filterEligibleCandidates]`, one scorer `preRankCandidates`,
one selector `selectDistinctCandidates`. `hydrators`, `sideEffects`,
`queryHydrators`, `finalizer`, `truncateToResultSize` are empty/unset.

Metric semantics to preserve (from `HunkkerCandidatePipeline.execute`):
`retrievedCandidates` = all from sources; `filteredCandidates` = the set **removed**
across all filters; `scoredCandidates` = the pre-rank output; `selectedCandidates` =
the dedup output.

## Target design (inline)

`generateCandidates` becomes a direct linear method in `HybridRecommendationService`:

```java
private RetrievalOutcome generateCandidates(ScoredMoviesQuery query,
        Map<String,Double> popularityMap, Set<String> excludedItems,
        Set<String> userGenres, Set<String> userTags, FilterContext filterCtx, int limit) {
    // sources
    List<MovieCandidate> retrieved = new ArrayList<>();
    retrieved.addAll(fetchPopularCandidates(popularityMap, userGenres, userTags));
    retrieved.addAll(fetchColdStartCandidates(excludedItems, userGenres, userTags, filterCtx, limit));
    // filters (history + eligibility), accumulating the removed set
    List<MovieCandidate> kept = List.copyOf(retrieved);
    List<MovieCandidate> removed = new ArrayList<>();
    for (CandidateFilter f : List.of(new PreviouslySeenMoviesFilter(),
            new PreviouslySeenMoviesBackupFilter(), new PreviouslyServedMoviesFilter())) {
        if (f.enable(query)) {
            CandidateFilterResult r = f.filter(query, kept);
            kept = r.kept();
            removed.addAll(r.removed());
        }
    }
    CandidateFilterResult eligible = filterEligibleCandidates(kept, excludedItems, filterCtx);
    kept = eligible.kept();
    removed.addAll(eligible.removed());
    // pre-rank + dedup
    List<MovieCandidate> scored = preRankCandidates(kept, limit);
    List<MovieCandidate> selected = selectDistinctCandidates(scored);
    return new RetrievalOutcome(List.copyOf(retrieved), List.copyOf(removed), scored, selected);
}

private record RetrievalOutcome(List<MovieCandidate> retrievedCandidates,
    List<MovieCandidate> filteredCandidates, List<MovieCandidate> scoredCandidates,
    List<MovieCandidate> selectedCandidates) {}
```

The existing retrieval methods stay in the service but drop the
`CandidatePipelineContext` parameter, taking the values they used directly
(`fetchPopularCandidates`, `fetchColdStartCandidates`, `filterEligibleCandidates`
→ returns `filters.CandidateFilterResult`, `preRankCandidates`,
`selectDistinctCandidates` → returns `List<MovieCandidate>`). `mergeCandidate`,
`contentScoreForItem`, `isEligibleCandidate`, `isNewRelease`, `getNormalizedCatalog`
are unchanged. `RetrievalOutcome`'s accessor names match the old
`CandidatePipelineResult`, so the caller (`recommend`) is untouched.

## Deletions & moves

- **Delete** the `candidate_pipeline/` machinery: `HunkkerCandidatePipeline` (+`Builder`),
  `CandidateSource`, `CandidateScorer`, `CandidateSelector`, `CandidateFinalizer`,
  `CandidateSideEffect`, `PipelineQueryHydrator`, `PipelineCandidateFilter`,
  `PipelineStage`, `PipelineComponents`, `CandidateSideEffectInput`,
  `CandidatePipelineContext`, `CandidatePipelineResult`, `CandidateSelection`.
- **Delete** `candidate_hydrators/CandidateHydrator` (keep `MovieCandidate`).
- **Delete** the service's pipeline plumbing: `candidatePipeline`/`candidatePipelineCatalog`
  fields, `currentCandidatePipeline()`, `buildCandidatePipeline()`, `historyFilter(...)`.
- **Move** `FilterContext` out of `candidate_pipeline` into
  `com.demo.retrieval.service.filters` (it's filter config; used by eligibility + scoring).
- **Delete** dead config `CandidateGeneration.topNRandomizationPool`.
- **Delete** the duplicate `contentScore(MovieProfile, …)` overload; simplify the
  cold-start fallback to the `NormalizedProfile` overload (normalized catalog covers
  every catalog item).

## Work items & acceptance

- **R1 — inline `generateCandidates`.** *Accept:* it produces the same selected set and
  the same retrieved/filtered/scored counts as the old pipeline; retrieval methods lose
  the context param; the `recommend` caller is unchanged.
- **R2 — machinery deleted.** *Accept:* `candidate_pipeline/` is gone (except `FilterContext`
  relocated) and `CandidateHydrator` is gone; project compiles; no reference remains.
- **R3 — dead cleanups.** *Accept:* `topNRandomizationPool` and the duplicate `contentScore`
  overload are removed; nothing references them.
- **R4 — behavior preserved.** *Accept:* `RecommendationControllerTest` (13) and
  `HybridRecommendationServiceTest.recommendsFreshMoviesFromMovieLensBehaviorSignals`
  pass unchanged.

## Testing strategy

- **Oracle (unchanged):** `RecommendationControllerTest` + the `recommend()` behavior test
  prove retrieval output and metrics are identical end-to-end. These already exercise the
  full retrieve path.
- **Delete `CandidatePipelineTest`** — it tests only the deleted abstraction (builder, stage
  order, hydrator-count invariant, `components()`), never retrieval behavior, so removing it
  is not a retrieval-coverage loss.
- No new unit test: the inline flow is private and fully covered by the end-to-end oracle;
  adding a reflection/whitebox test would duplicate that coverage.
- Full module `mvn test` stays green (drops the 3 `CandidatePipelineTest` cases; net 47).

## Non-goals / risks

- Purely structural; no ranking/quality change.
- `FilterContext` moves packages → mechanical import churn.
- The god-class shrink is modest (the retrieval methods stay in the service); the win is
  removing ~14 dead/abstraction types and the indirection, not relocation.
