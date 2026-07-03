# Simplify Content-Based Retrieval Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the generic 9-stage candidate pipeline with a direct linear `generateCandidates`, deleting the dead abstraction, with retrieval behavior and metrics byte-identical.

**Architecture:** Inline collapse — keep the retrieval methods in `HybridRecommendationService`, drop their `CandidatePipelineContext` parameter, and wire `generateCandidates` as `fetch → filter → pre-rank → dedup`. Move `FilterContext` to the `filters` package, then delete the whole `candidate_pipeline/` machinery + `CandidateHydrator`. Remove dead config and a duplicate `contentScore`.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-01-simplify-content-retrieval.md](../specs/2026-07-01-simplify-content-retrieval.md).
- No retrieval behavior/metric change; the end-to-end tests are the oracle.
- Metric semantics preserved: `retrieved`=sources, `filtered`=removed-across-filters, `scored`=pre-rank output, `selected`=dedup output.
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Base a new branch on `master`; PR targets `master`.

---

## File Structure

- Move: `service/candidate_pipeline/FilterContext.java` → `service/filters/FilterContext.java`.
- Modify: `service/HybridRecommendationService.java` — inline `generateCandidates`, adapt retrieval methods, add `RetrievalOutcome`, remove pipeline plumbing + imports.
- Delete: everything else under `service/candidate_pipeline/`; `service/candidate_hydrators/CandidateHydrator.java`; `test/.../CandidatePipelineTest.java`.
- Modify: `config/RecommendationProperties.java` — drop `CandidateGeneration.topNRandomizationPool`.

---

### Task 1: Relocate `FilterContext` to the `filters` package

Must happen before deleting `candidate_pipeline/`, since `FilterContext` is used by eligibility + scoring.

**Files:**
- Move: `service/candidate_pipeline/FilterContext.java` → `service/filters/FilterContext.java`
- Modify: `service/HybridRecommendationService.java` (import)

- [ ] **Step 1: git-move the file and fix its package.**

```bash
cd recsys-pipeline/services/java-retrieval-service
git mv src/main/java/com/demo/retrieval/service/candidate_pipeline/FilterContext.java \
       src/main/java/com/demo/retrieval/service/filters/FilterContext.java
```
Then change its first line from `package com.demo.retrieval.service.candidate_pipeline;` to `package com.demo.retrieval.service.filters;`.

- [ ] **Step 2: Update the import in `HybridRecommendationService.java`.**

Replace `import com.demo.retrieval.service.candidate_pipeline.FilterContext;` with `import com.demo.retrieval.service.filters.FilterContext;`.

- [ ] **Step 3: Find any other references.**

Run: `rg -n "candidate_pipeline.FilterContext" src`
Expected: no output (only `HybridRecommendationService` imported it; if others appear, update them the same way).

---

### Task 2: Inline `generateCandidates` and adapt the retrieval methods

**Files:**
- Modify: `service/HybridRecommendationService.java`

**Interfaces:**
- Produces: `private record RetrievalOutcome(List<MovieCandidate> retrievedCandidates, List<MovieCandidate> filteredCandidates, List<MovieCandidate> scoredCandidates, List<MovieCandidate> selectedCandidates)` — accessor names match the old `CandidatePipelineResult` so `recommend` is untouched.

- [ ] **Step 1: Replace `generateCandidates`, `currentCandidatePipeline`, `buildCandidatePipeline`, `historyFilter` with the inline flow + record.** Delete those four methods and the `candidatePipeline`/`candidatePipelineCatalog` fields; add:

```java
    private RetrievalOutcome generateCandidates(
        ScoredMoviesQuery query,
        Map<String, Double> popularityMap,
        Set<String> excludedItems,
        Set<String> userGenres,
        Set<String> userTags,
        FilterContext filterCtx,
        int limit
    ) {
        List<MovieCandidate> retrieved = new ArrayList<>();
        retrieved.addAll(fetchPopularCandidates(popularityMap, userGenres, userTags));
        retrieved.addAll(fetchColdStartCandidates(excludedItems, userGenres, userTags, filterCtx, limit));

        List<MovieCandidate> kept = List.copyOf(retrieved);
        List<MovieCandidate> removed = new ArrayList<>();
        for (com.demo.retrieval.service.filters.CandidateFilter filter : List.of(
                new PreviouslySeenMoviesFilter(),
                new PreviouslySeenMoviesBackupFilter(),
                new PreviouslyServedMoviesFilter())) {
            if (filter.enable(query)) {
                CandidateFilterResult result = filter.filter(query, kept);
                kept = result.kept();
                removed.addAll(result.removed());
            }
        }
        CandidateFilterResult eligible = filterEligibleCandidates(kept, excludedItems, filterCtx);
        kept = eligible.kept();
        removed.addAll(eligible.removed());

        List<MovieCandidate> scored = preRankCandidates(kept, limit);
        List<MovieCandidate> selected = selectDistinctCandidates(scored);
        return new RetrievalOutcome(List.copyOf(retrieved), List.copyOf(removed), scored, selected);
    }

    private record RetrievalOutcome(
        List<MovieCandidate> retrievedCandidates,
        List<MovieCandidate> filteredCandidates,
        List<MovieCandidate> scoredCandidates,
        List<MovieCandidate> selectedCandidates
    ) {
    }
```

- [ ] **Step 2: Adapt `fetchPopularCandidates`** — drop the context param:

```java
    private List<MovieCandidate> fetchPopularCandidates(
        Map<String, Double> popularityMap, Set<String> userGenres, Set<String> userTags) {
        return popularityMap.entrySet().stream()
            .map(entry -> new MovieCandidate(
                entry.getKey(),
                entry.getValue(),
                contentScoreForItem(entry.getKey(), userGenres, userTags),
                false
            ))
            .toList();
    }
```

- [ ] **Step 3: Adapt `filterEligibleCandidates`** — direct params, return `filters.CandidateFilterResult`:

```java
    private CandidateFilterResult filterEligibleCandidates(
        List<MovieCandidate> candidates, Set<String> excludedItems, FilterContext filterCtx) {
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate ->
                isEligibleCandidate(candidate, excludedItems, filterCtx)));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }
```

(Ensure the import is `com.demo.retrieval.service.filters.CandidateFilterResult`.)

- [ ] **Step 4: Adapt `selectDistinctCandidates`** — return `List<MovieCandidate>` (dedup; nonSelected was unused):

```java
    private List<MovieCandidate> selectDistinctCandidates(List<MovieCandidate> candidates) {
        LinkedHashMap<String, MovieCandidate> selected = new LinkedHashMap<>();
        for (MovieCandidate candidate : candidates) {
            selected.merge(candidate.movieId(), candidate, this::mergeCandidate);
        }
        return List.copyOf(selected.values());
    }
```

- [ ] **Step 5: Adapt `fetchColdStartCandidates`** — drop context, take direct params; simplify the content-score to the normalized overload:

```java
    private List<MovieCandidate> fetchColdStartCandidates(
        Set<String> excludedItems, Set<String> userGenres, Set<String> userTags,
        FilterContext filterCtx, int resultSize) {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        if (catalog.isEmpty()) {
            return List.of();
        }

        Map<String, NormalizedProfile> normalizedCatalog = getNormalizedCatalog();
        int poolSize = Math.max(1, properties.getCandidateGeneration().getColdStartPoolSize());
        int probeSize = Math.max(poolSize, Math.max(
            resultSize,
            poolSize * properties.getCandidateGeneration().getPopularityFetchMultiplier()
        ));
        List<MovieCandidate> probeCandidates = catalog.entrySet().stream()
            .filter(entry -> isEligibleCandidate(entry.getKey(), excludedItems, filterCtx))
            .map(entry -> {
                NormalizedProfile np = normalizedCatalog.get(entry.getKey());
                double cs = np == null ? 0.0 : contentScore(np, userGenres, userTags);
                return new MovieCandidate(entry.getKey(), 0.0, cs, true);
            })
            .sorted(
                Comparator.comparing((MovieCandidate candidate) -> isNewRelease(candidate.movieId())).reversed()
                    .thenComparing(Comparator.comparingDouble(MovieCandidate::contentScore).reversed())
            )
            .limit(probeSize)
            .toList();

        List<String> impressionKeys = probeCandidates.stream()
            .map(candidate -> "bandit:item:" + candidate.movieId() + ":impressions")
            .toList();
        List<String> impressionValues = Optional.ofNullable(redis.opsForValue().multiGet(impressionKeys))
            .orElseGet(() -> Collections.nCopies(probeCandidates.size(), null));

        Map<String, Long> impressionMap = new LinkedHashMap<>();
        for (int i = 0; i < probeCandidates.size(); i++) {
            impressionMap.put(probeCandidates.get(i).movieId(), readLong(impressionValues.get(i)));
        }

        int threshold = properties.getBandit().getColdStartExposureThreshold();
        return probeCandidates.stream()
            .filter(candidate -> isNewRelease(candidate.movieId()) || impressionMap.getOrDefault(candidate.movieId(), 0L) < threshold)
            .limit(poolSize)
            .toList();
    }
```

- [ ] **Step 6: Adapt `preRankCandidates`** — drop context, take `resultSize`:

```java
    private List<MovieCandidate> preRankCandidates(List<MovieCandidate> candidates, int resultSize) {
        int configured = properties.getCandidateGeneration().getCandidatePoolSize();
        int poolSize = configured > 0 ? configured
            : resultSize * properties.getCandidateGeneration().getPopularityFetchMultiplier();
        double popWeight = properties.getBandit().getPopularityWeight();
        double contentWeight = properties.getBandit().getContentWeight();
        double totalWeight = popWeight + contentWeight;
        double normPop = totalWeight == 0.0 ? 0.5 : popWeight / totalWeight;
        double normContent = totalWeight == 0.0 ? 0.5 : contentWeight / totalWeight;
        return candidates.stream()
            .sorted(Comparator.comparingDouble(
                (MovieCandidate c) -> normPop * c.popularityScore()
                    + normContent * c.contentScore()
            ).reversed())
            .limit(poolSize)
            .toList();
    }
```

- [ ] **Step 7: Remove now-unused imports** (the deleted pipeline types). After the edits, run:

Run: `rg -n "candidate_pipeline|candidate_hydrators.CandidateHydrator|HunkkerCandidatePipeline|CandidatePipeline|CandidateSelection|PipelineCandidateFilter" src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
Expected: only the `import ...candidate_hydrators.MovieCandidate;` line remains. Delete every other `candidate_pipeline.*` import.

- [ ] **Step 8: Compile.**

Run: `mvn -o test-compile`
Expected: `BUILD SUCCESS` (main sources compile; the pipeline package is now unreferenced by main).

---

### Task 3: Delete the pipeline machinery, `CandidateHydrator`, and its test

**Files:** deletions only.

- [ ] **Step 1: Delete the files.**

```bash
cd recsys-pipeline/services/java-retrieval-service
git rm src/main/java/com/demo/retrieval/service/candidate_pipeline/HunkkerCandidatePipeline.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateSource.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateScorer.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateSelector.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateFinalizer.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateSideEffect.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateSideEffectInput.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/PipelineQueryHydrator.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/PipelineCandidateFilter.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/PipelineStage.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/PipelineComponents.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidatePipelineContext.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidatePipelineResult.java \
       src/main/java/com/demo/retrieval/service/candidate_pipeline/CandidateSelection.java \
       src/main/java/com/demo/retrieval/service/candidate_hydrators/CandidateHydrator.java \
       src/test/java/com/demo/retrieval/service/CandidatePipelineTest.java
```

- [ ] **Step 2: Confirm the package is empty and nothing references the deleted types.**

Run: `ls src/main/java/com/demo/retrieval/service/candidate_pipeline/ 2>/dev/null; rg -rn "candidate_pipeline" src || echo "clean"`
Expected: the directory is empty/gone and `clean` (no references).

- [ ] **Step 3: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 47` (50 − 3 deleted `CandidatePipelineTest` cases), 0 failures. `RecommendationControllerTest` and the `recommend()` behavior test pass unchanged.

- [ ] **Step 4: Commit R1+R2 together (the inline rewrite is meaningless without the deletions).**

```bash
git add -A
git commit -m "refactor(retrieval): collapse 9-stage candidate pipeline into direct generateCandidates"
```

---

### Task 4: Remove dead config + duplicate `contentScore`

**Files:**
- Modify: `config/RecommendationProperties.java`
- Modify: `service/HybridRecommendationService.java`

- [ ] **Step 1: Delete `topNRandomizationPool`.** In `RecommendationProperties.CandidateGeneration`, remove the field and its getter/setter:

```java
        private int topNRandomizationPool = 5;
        public int getTopNRandomizationPool() { return topNRandomizationPool; }
        public void setTopNRandomizationPool(int topNRandomizationPool) { this.topNRandomizationPool = topNRandomizationPool; }
```

Run: `rg -n "topNRandomizationPool|top-n-randomization" src` → expected: no output after removal.

- [ ] **Step 2: Delete the duplicate `contentScore(MovieProfile, …)` overload.** Remove:

```java
    private double contentScore(MovieProfile profile, Set<String> userGenres, Set<String> userTags) {
        Set<String> genres = normalize(profile.getGenres());
        Set<String> tags = normalize(profile.getTags());
        double genreOverlap = overlapRatio(userGenres, genres);
        double tagOverlap = overlapRatio(userTags, tags);
        return clamp((genreOverlap * RecommendationConstants.CONTENT_GENRE_WEIGHT)
            + (tagOverlap * RecommendationConstants.CONTENT_TAG_WEIGHT));
    }
```

Run: `rg -n "contentScore\(" src/main/java/com/demo/retrieval/service/HybridRecommendationService.java` → expected: only the `NormalizedProfile` overload definition and its call sites remain (the `MovieProfile` call in cold-start was removed in Task 2 Step 5).

- [ ] **Step 3: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 47`, 0 failures.

- [ ] **Step 4: Commit.**

```bash
git add -A
git commit -m "chore(retrieval): drop dead topNRandomizationPool config and duplicate contentScore"
```

---

### Task 5: Branch, docs, push, PR

- [ ] **Step 1: Branch off master carrying the uncommitted spec/plan.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/simplify-content-retrieval
```

Do Tasks 1–4 on this branch, then commit the docs:

```bash
git add docs/specs/2026-07-01-simplify-content-retrieval.md docs/plans/2026-07-01-simplify-content-retrieval.md
git commit -m "docs(retrieval): spec + plan for content-retrieval simplification"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, `Tests run: 47`, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/simplify-content-retrieval
gh pr create --base master --title "Simplify content-based retrieval: collapse the candidate pipeline" --body "See docs/specs/2026-07-01-simplify-content-retrieval.md. Replaces the generic 9-stage HunkkerCandidatePipeline (7 zero-implementer interfaces, dead Builder/introspection/hydrator) with a direct linear generateCandidates. Deletes ~15 dead types, dead config, and a duplicate contentScore; moves FilterContext into filters. Behavior + metrics identical (RecommendationControllerTest + recommend() oracle green). mvn test: 47 passing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

- **Spec coverage:** R1→Task 2; R2→Tasks 1+3; R3→Task 4; R4→Task 3 Step 3; artifacts+PR→Task 5. All items covered.
- **Placeholders:** none — full adapted method bodies and exact delete lists provided.
- **Type consistency:** `RetrievalOutcome` accessors (`retrievedCandidates/filteredCandidates/scoredCandidates/selectedCandidates`) match the caller's existing calls; `filterEligibleCandidates`/`selectDistinctCandidates` new return types (`filters.CandidateFilterResult`, `List<MovieCandidate>`) are consumed correctly by `generateCandidates`; `CandidateFilter`/`CandidateFilterResult` are the single `filters` types.
- **Ordering:** `FilterContext` relocated (Task 1) before the package delete (Task 3); inline rewrite (Task 2) removes references before deletion so each `mvn` gate is green.
