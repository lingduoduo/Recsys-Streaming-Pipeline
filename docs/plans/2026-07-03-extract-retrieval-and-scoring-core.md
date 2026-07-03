# Extract Retriever + Shared Scoring Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract retrieval and the shared catalog/content core out of the 1455-line `HybridRecommendationService` into `ContentCandidateRetriever` + `CatalogContentScoring` + `TextNormalization`, behavior-preserving.

**Architecture:** `TextNormalization` (pure string statics) ← `CatalogContentScoring` (normalized catalog + content score, used by BOTH retriever and scorer) ← `ContentCandidateRetriever` (retrieval flow). The service constructs the two components (sharing one `CatalogContentScoring`), delegates `generateCandidates`, and rewires `scoreCandidate` to the shared component.

**Tech Stack:** Java 17, Spring Boot 3.3.5, JUnit 5, Mockito, Maven.

## Global Constraints

- Spec: [docs/specs/2026-07-03-extract-retrieval-and-scoring-core.md](../specs/2026-07-03-extract-retrieval-and-scoring-core.md).
- Behavior-preserving, incl. the scoring path; end-to-end tests are the oracle.
- Base a new branch on `master` (PR #114 merged); PR targets `master`.
- Module dir for `mvn`: `recsys-pipeline/services/java-retrieval-service`. Path prefix: `src/{main,test}/java/com/demo/retrieval`.
- Moved method bodies transfer **verbatim** unless a step shows an edit; only their home changes.

---

## File Structure

- Create: `service/text/TextNormalization.java` — pure `normalize`/`normalizeValue`.
- Create: `service/content/CatalogContentScoring.java` + `service/content/NormalizedProfile.java` — shared normalized catalog + content score.
- Create: `service/retrieval/ContentCandidateRetriever.java` + `service/retrieval/RetrievalOutcome.java` — retrieval flow.
- Move: `candidate_hydrators/MovieCandidate.java` → `retrieval/MovieCandidate.java`.
- Modify: `service/HybridRecommendationService.java` — construct components, delegate, rewire, delete moved code.
- Test: `service/text/TextNormalizationTest.java`, `service/content/CatalogContentScoringTest.java`.

---

### Task 1: `TextNormalization`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/text/TextNormalization.java`
- Test: `src/test/java/com/demo/retrieval/service/text/TextNormalizationTest.java`
- Modify: `service/HybridRecommendationService.java`

**Interfaces:**
- Produces: `static Set<String> TextNormalization.normalize(List<String>)`, `static String TextNormalization.normalizeValue(String)`.

- [ ] **Step 1: Failing test** — `TextNormalizationTest.java`:

```java
package com.demo.retrieval.service.text;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Set;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TextNormalizationTest {
    @Test
    void normalizeLowercasesTrimsAndDropsBlanks() {
        assertEquals(Set.of("drama", "sci-fi"),
            TextNormalization.normalize(List.of("  Drama ", "SCI-FI", "   ")));
        assertEquals(Set.of(), TextNormalization.normalize(null));
    }

    @Test
    void normalizeValueLowercasesTrimsAndNullSafe() {
        assertEquals("drama", TextNormalization.normalizeValue("  Drama "));
        assertEquals("", TextNormalization.normalizeValue(null));
    }
}
```

- [ ] **Step 2: Run — expect FAIL** (class missing).

Run: `mvn -o test -Dtest=TextNormalizationTest`
Expected: FAIL (compile).

- [ ] **Step 3: Create `TextNormalization.java`** (bodies moved verbatim from `HybridRecommendationService.normalize`/`normalizeValue`, made static):

```java
package com.demo.retrieval.service.text;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public final class TextNormalization {

    private TextNormalization() {
    }

    public static Set<String> normalize(List<String> values) {
        return values == null ? Set.of() : values.stream()
            .map(TextNormalization::normalizeValue)
            .filter(value -> !value.isBlank())
            .collect(Collectors.toSet());
    }

    public static String normalizeValue(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT).trim();
    }
}
```

- [ ] **Step 4: Run — expect PASS.**

Run: `mvn -o test -Dtest=TextNormalizationTest`
Expected: PASS (2).

- [ ] **Step 5: Route all callers in `HybridRecommendationService`.** Delete the private `normalize`/`normalizeValue` methods; add `import com.demo.retrieval.service.text.TextNormalization;`; replace each internal call `normalize(` → `TextNormalization.normalize(`, `normalizeValue(` → `TextNormalization.normalizeValue(`. (Callers include `deriveTasteProfile` and any `this::normalizeValue` method refs → `TextNormalization::normalizeValue`.)

Run: `rg -n "\bnormalize\(|normalizeValue\(|this::normalizeValue" src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
Expected: after fixes, no bare `normalize(`/`normalizeValue(`; only `TextNormalization.` forms (some remaining calls also move out in Tasks 2–3).

- [ ] **Step 6: Compile.**

Run: `mvn -o test-compile`
Expected: `BUILD SUCCESS` (note: `getNormalizedCatalog`/`contentScore` still in the service call `TextNormalization.*` fine).

- [ ] **Step 7: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/text/ src/test/java/com/demo/retrieval/service/text/ src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "refactor(text): extract TextNormalization static helpers"
```

---

### Task 2: `CatalogContentScoring` + top-level `NormalizedProfile`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/content/NormalizedProfile.java`
- Create: `src/main/java/com/demo/retrieval/service/content/CatalogContentScoring.java`
- Test: `src/test/java/com/demo/retrieval/service/content/CatalogContentScoringTest.java`
- Modify: `service/HybridRecommendationService.java`

**Interfaces:**
- Produces: `CatalogContentScoring(RecommendationProperties)`; `Map<String,NormalizedProfile> normalizedCatalog()`; `NormalizedProfile profileFor(String)`; `double contentScore(NormalizedProfile, Set<String>, Set<String>)`; `boolean isNewRelease(String)`; top-level `record NormalizedProfile(String productType, Set<String> genres, Set<String> tags, Set<String> allKeywords, String title, boolean newRelease, long expiresAtEpochMillis)`.

- [ ] **Step 1: Create `NormalizedProfile.java`** (promoted from the service's nested record):

```java
package com.demo.retrieval.service.content;

import java.util.Set;

public record NormalizedProfile(
    String productType,
    Set<String> genres,
    Set<String> tags,
    Set<String> allKeywords,
    String title,
    boolean newRelease,
    long expiresAtEpochMillis) {
}
```

- [ ] **Step 2: Failing test** — `CatalogContentScoringTest.java`:

```java
package com.demo.retrieval.service.content;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogContentScoringTest {

    private CatalogContentScoring scoringFor(MovieProfile profile) {
        RecommendationProperties properties = new RecommendationProperties();
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("m1", profile);
        properties.setCatalog(catalog);
        return new CatalogContentScoring(properties);
    }

    private static MovieProfile movie(List<String> genres, List<String> tags, boolean newRelease) {
        MovieProfile p = new MovieProfile();
        p.setGenres(genres);
        p.setTags(tags);
        p.setNewRelease(newRelease);
        return p;
    }

    @Test
    void buildsNormalizedProfileAndCachesByCatalogIdentity() {
        CatalogContentScoring scoring = scoringFor(movie(List.of("Drama"), List.of("Dark"), true));
        NormalizedProfile np = scoring.profileFor("m1");
        assertTrue(np.genres().contains("drama"));
        assertTrue(np.tags().contains("dark"));
        assertTrue(scoring.isNewRelease("m1"));
        assertSame(scoring.normalizedCatalog(), scoring.normalizedCatalog()); // cache reuse
    }

    @Test
    void contentScoreIsGenreTagJaccardBlend() {
        CatalogContentScoring scoring = scoringFor(movie(List.of("drama"), List.of("dark"), false));
        NormalizedProfile np = scoring.profileFor("m1");
        double full = scoring.contentScore(np, Set.of("drama"), Set.of("dark"));
        double none = scoring.contentScore(np, Set.of("comedy"), Set.of("light"));
        assertTrue(full > none);
        assertEquals(0.0, none, 1e-9);
    }
}
```

- [ ] **Step 3: Run — expect FAIL** (class missing).

Run: `mvn -o test -Dtest=CatalogContentScoringTest`
Expected: FAIL (compile).

- [ ] **Step 4: Create `CatalogContentScoring.java`** — move `getNormalizedCatalog`, `CatalogCache`, `contentScore(NormalizedProfile,…)`, `overlapRatio`, `isNewRelease` verbatim; use `TextNormalization`, `RecommendationConstants`:

```java
package com.demo.retrieval.service.content;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.RecommendationConstants;
import com.demo.retrieval.service.text.TextNormalization;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CatalogContentScoring {

    private final RecommendationProperties properties;
    private volatile CatalogCache catalogCache;

    public CatalogContentScoring(RecommendationProperties properties) {
        this.properties = properties;
    }

    public Map<String, NormalizedProfile> normalizedCatalog() {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        CatalogCache cache = catalogCache;
        if (cache != null && cache.source() == catalog) {
            return cache.normalized();
        }
        Map<String, NormalizedProfile> built = new HashMap<>(catalog.size() * 4 / 3 + 1);
        catalog.forEach((id, p) -> {
            Set<String> normalizedTags = TextNormalization.normalize(p.getTags());
            Set<String> allKeywords = new HashSet<>(normalizedTags);
            allKeywords.addAll(TextNormalization.normalize(p.getKeywords()));
            built.put(id, new NormalizedProfile(
                TextNormalization.normalizeValue(p.getProductType()),
                TextNormalization.normalize(p.getGenres()),
                Collections.unmodifiableSet(normalizedTags),
                Collections.unmodifiableSet(allKeywords),
                TextNormalization.normalizeValue(p.getTitle()),
                p.isNewRelease(),
                p.getExpiresAtEpochMillis()
            ));
        });
        synchronized (this) {
            CatalogCache c2 = catalogCache;
            if (c2 != null && c2.source() == catalog) {
                return c2.normalized();
            }
            CatalogCache newCache = new CatalogCache(catalog, Collections.unmodifiableMap(built));
            catalogCache = newCache;
            return newCache.normalized();
        }
    }

    public NormalizedProfile profileFor(String itemId) {
        return normalizedCatalog().get(itemId);
    }

    public boolean isNewRelease(String itemId) {
        NormalizedProfile profile = profileFor(itemId);
        return profile != null && profile.newRelease();
    }

    public double contentScore(NormalizedProfile profile, Set<String> userGenres, Set<String> userTags) {
        return RecommendationConstants.clamp(
            (overlapRatio(userGenres, profile.genres()) * RecommendationConstants.CONTENT_GENRE_WEIGHT)
            + (overlapRatio(userTags, profile.tags()) * RecommendationConstants.CONTENT_TAG_WEIGHT));
    }

    private double overlapRatio(Set<String> left, Set<String> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return 0.0;
        }
        long intersection = left.stream().filter(right::contains).count();
        long union = left.size() + right.size() - intersection;
        return union == 0 ? 0.0 : (double) intersection / union;
    }

    private record CatalogCache(
        Map<String, MovieProfile> source,
        Map<String, NormalizedProfile> normalized) {
    }
}
```

**Note:** copy `overlapRatio` verbatim from the service if its current body differs from the equivalent above; the behavior (Jaccard) must match exactly.

- [ ] **Step 5: Run — expect PASS.**

Run: `mvn -o test -Dtest=CatalogContentScoringTest`
Expected: PASS (2).

- [ ] **Step 6: Wire the service to `CatalogContentScoring` and rewire `scoreCandidate`.** In `HybridRecommendationService`: add `import com.demo.retrieval.service.content.CatalogContentScoring;` + `import com.demo.retrieval.service.content.NormalizedProfile;`; add a field `private final CatalogContentScoring catalogContentScoring;` initialized in the constructor as `this.catalogContentScoring = new CatalogContentScoring(properties);`. Replace, in `scoreCandidate` and everywhere else the service still calls them:
  - `getNormalizedCatalog().get(id)` → `catalogContentScoring.profileFor(id)`
  - `getNormalizedCatalog()` → `catalogContentScoring.normalizedCatalog()`
  - `contentScore(np, g, t)` → `catalogContentScoring.contentScore(np, g, t)`
  - `isNewRelease(id)` → `catalogContentScoring.isNewRelease(id)`
Delete the now-moved service members: `NormalizedProfile` record, `CatalogCache` record, `catalogCache` field, `getNormalizedCatalog`, `contentScore(NormalizedProfile,…)`, `overlapRatio`, `isNewRelease`.

Run: `rg -n "getNormalizedCatalog|record NormalizedProfile|record CatalogCache|private double overlapRatio|private boolean isNewRelease|private double contentScore\(NormalizedProfile" src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
Expected: no output.

- [ ] **Step 7: Full suite** (scoring path exercised by the oracle).

Run: `mvn -o test`
Expected: `BUILD SUCCESS`; `RecommendationControllerTest` + `HybridRecommendationServiceTest` green. (Retrieval methods `fetch*`/`isEligibleCandidate`/`contentScoreForItem` still in the service, now calling `catalogContentScoring.*` — that's fine; they move in Task 3.)

- [ ] **Step 8: Commit.**

```bash
git add src/main/java/com/demo/retrieval/service/content/ src/test/java/com/demo/retrieval/service/content/ src/main/java/com/demo/retrieval/service/HybridRecommendationService.java
git commit -m "refactor(content): extract CatalogContentScoring shared core; rewire scoreCandidate"
```

---

### Task 3: `ContentCandidateRetriever` + move `MovieCandidate` + delegate

**Files:**
- Move: `candidate_hydrators/MovieCandidate.java` → `retrieval/MovieCandidate.java`
- Create: `service/retrieval/RetrievalOutcome.java`
- Create: `service/retrieval/ContentCandidateRetriever.java`
- Modify: `service/HybridRecommendationService.java`

**Interfaces:**
- Consumes: `CatalogContentScoring` (Task 2), `FilterContext`, `MovieCandidate`, the `filters.*` classes.
- Produces: `ContentCandidateRetriever(StringRedisTemplate, RecommendationProperties, CatalogContentScoring)`; `RetrievalOutcome retrieve(ScoredMoviesQuery, Map<String,Double>, Set<String>, Set<String>, Set<String>, FilterContext, int)`; `record RetrievalOutcome(List<MovieCandidate> retrievedCandidates, List<MovieCandidate> filteredCandidates, List<MovieCandidate> scoredCandidates, List<MovieCandidate> selectedCandidates)`.

- [ ] **Step 1: Move `MovieCandidate`.**

```bash
cd recsys-pipeline/services/java-retrieval-service
git mv src/main/java/com/demo/retrieval/service/candidate_hydrators/MovieCandidate.java \
       src/main/java/com/demo/retrieval/service/retrieval/MovieCandidate.java
```
Change its package line to `package com.demo.retrieval.service.retrieval;`. Update the import in `HybridRecommendationService` (`candidate_hydrators.MovieCandidate` → `retrieval.MovieCandidate`). Run `rg -n "candidate_hydrators" src` → fix any remaining (the `candidate_hydrators` dir is now empty).

- [ ] **Step 2: Create `RetrievalOutcome.java`:**

```java
package com.demo.retrieval.service.retrieval;

import java.util.List;

public record RetrievalOutcome(
    List<MovieCandidate> retrievedCandidates,
    List<MovieCandidate> filteredCandidates,
    List<MovieCandidate> scoredCandidates,
    List<MovieCandidate> selectedCandidates) {
}
```

- [ ] **Step 3: Create `ContentCandidateRetriever.java`** — move `generateCandidates` body + `fetchPopularCandidates`, `fetchColdStartCandidates`, `filterEligibleCandidates`, `preRankCandidates`, `selectDistinctCandidates`, `mergeCandidate`, `contentScoreForItem`, `isEligibleCandidate` (both overloads) verbatim, with these substitutions: the public entry is `retrieve(...)` (the old `generateCandidates` body); `getNormalizedCatalog()` → `catalogContentScoring.normalizedCatalog()`; `contentScore(np, …)` → `catalogContentScoring.contentScore(np, …)`; `isNewRelease(id)` → `catalogContentScoring.isNewRelease(id)`; `readLong(...)` → an inlined private `readLong` (copy from the service); `properties`/`redis` are fields.

```java
package com.demo.retrieval.service.retrieval;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.content.CatalogContentScoring;
import com.demo.retrieval.service.content.NormalizedProfile;
import com.demo.retrieval.service.filters.CandidateFilter;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import com.demo.retrieval.service.filters.FilterContext;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesBackupFilter;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesFilter;
import com.demo.retrieval.service.filters.PreviouslyServedMoviesFilter;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ContentCandidateRetriever {

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final CatalogContentScoring catalogContentScoring;

    public ContentCandidateRetriever(StringRedisTemplate redis, RecommendationProperties properties,
                                     CatalogContentScoring catalogContentScoring) {
        this.redis = redis;
        this.properties = properties;
        this.catalogContentScoring = catalogContentScoring;
    }

    public RetrievalOutcome retrieve(ScoredMoviesQuery query, Map<String, Double> popularityMap,
            Set<String> excludedItems, Set<String> userGenres, Set<String> userTags,
            FilterContext filterCtx, int limit) {
        List<MovieCandidate> retrieved = new ArrayList<>();
        retrieved.addAll(fetchPopularCandidates(popularityMap, userGenres, userTags));
        retrieved.addAll(fetchColdStartCandidates(excludedItems, userGenres, userTags, filterCtx, limit));

        List<MovieCandidate> kept = List.copyOf(retrieved);
        List<MovieCandidate> removed = new ArrayList<>();
        for (CandidateFilter filter : List.of(
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

    // --- moved verbatim from HybridRecommendationService (getNormalizedCatalog/contentScore/isNewRelease
    //     calls swapped to catalogContentScoring.*): fetchPopularCandidates, fetchColdStartCandidates,
    //     filterEligibleCandidates, preRankCandidates, selectDistinctCandidates, mergeCandidate,
    //     contentScoreForItem, isEligibleCandidate(String) + isEligibleCandidate(MovieCandidate), readLong ---
}
```

Paste the moved method bodies (from the current service) into the marked block, applying the `catalogContentScoring.*` substitutions and using the local `readLong`. Copy `readLong` from the service:

```java
    private long readLong(Object raw) {
        if (raw == null) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
```

- [ ] **Step 4: Wire + delegate in the service.** Add `import`s for `ContentCandidateRetriever`, `RetrievalOutcome`; add field `private final ContentCandidateRetriever contentCandidateRetriever;`; in the constructor after `catalogContentScoring`, `this.contentCandidateRetriever = new ContentCandidateRetriever(redis, properties, catalogContentScoring);`. Replace the whole inline `generateCandidates` method with:

```java
    private RetrievalOutcome generateCandidates(
        ScoredMoviesQuery query, Map<String, Double> popularityMap, Set<String> excludedItems,
        Set<String> userGenres, Set<String> userTags, FilterContext filterCtx, int limit) {
        return contentCandidateRetriever.retrieve(
            query, popularityMap, excludedItems, userGenres, userTags, filterCtx, limit);
    }
```

Delete from the service the now-moved methods: `fetchPopularCandidates`, `fetchColdStartCandidates`, `filterEligibleCandidates`, `preRankCandidates`, `selectDistinctCandidates`, `mergeCandidate`, `contentScoreForItem`, both `isEligibleCandidate` overloads, and the service's own `RetrievalOutcome` record (now imported from `retrieval`). Keep `mergeCandidate` usage at the `recommend` call site — **note:** `recommend` uses `this::mergeCandidate` (line ~183); repoint it to a local kept copy or `MovieCandidate`-merge. **Check:** if `recommend` still references `mergeCandidate`, keep a private `mergeCandidate` in the service OR inline the merge; confirm via `rg -n "mergeCandidate" src/main/java/.../HybridRecommendationService.java` and resolve so it compiles.

- [ ] **Step 5: Remove dangling imports/refs.**

Run: `rg -n "candidate_hydrators|contentScoreForItem|isEligibleCandidate|fetchPopularCandidates|fetchColdStartCandidates|preRankCandidates|selectDistinctCandidates" src/main/java/com/demo/retrieval/service/HybridRecommendationService.java`
Expected: no output (all moved). Remove any now-unused imports flagged by the compiler.

- [ ] **Step 6: Full suite.**

Run: `mvn -o test`
Expected: `BUILD SUCCESS`, `RecommendationControllerTest` + `HybridRecommendationServiceTest` green; `Tests run` = 47 + 4 new (Task 1: 2, Task 2: 2).

- [ ] **Step 7: Commit.**

```bash
git add -A
git commit -m "refactor(retrieval): extract ContentCandidateRetriever; service delegates generateCandidates"
```

---

### Task 4: Branch, docs, push, PR

- [ ] **Step 1: Branch off master (do Tasks 1–3 on it).**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git checkout -b refactor/extract-retrieval-scoring-core
```

Then commit the docs:

```bash
git add docs/specs/2026-07-03-extract-retrieval-and-scoring-core.md docs/plans/2026-07-03-extract-retrieval-and-scoring-core.md
git commit -m "docs(retrieval): spec + plan for retriever + shared scoring-core extraction"
```

- [ ] **Step 2: Full suite green.**

Run: `cd recsys-pipeline/services/java-retrieval-service && mvn -o test`
Expected: `BUILD SUCCESS`, 51 tests, 0 failures.

- [ ] **Step 3: Push + PR.**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git push -u origin refactor/extract-retrieval-scoring-core
gh pr create --base master --title "Extract ContentCandidateRetriever + shared CatalogContentScoring" --body "See docs/specs/2026-07-03-extract-retrieval-and-scoring-core.md. Completes the #114 deferral: extracts retrieval into ContentCandidateRetriever and the shared normalized-catalog + content-score core into CatalogContentScoring (used by both retriever and scorer), plus TextNormalization. Behavior-preserving incl. the scoring path (rewired scoreCandidate); end-to-end oracle green. Adds CatalogContentScoringTest + TextNormalizationTest. mvn test: 51 passing.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review

- **Spec coverage:** X1→Task 1; X2→Task 2; X3→Task 3; X4→Tasks 2–3 (rewire+delete); X5→Task 2/3 full-suite gates; artifacts+PR→Task 4. Covered.
- **Placeholders:** new classes shown in full; moved bodies marked verbatim with exact substitutions (no `TODO`).
- **Type consistency:** `CatalogContentScoring` API (`normalizedCatalog`/`profileFor`/`contentScore`/`isNewRelease`) used identically in the service rewire and the retriever; `RetrievalOutcome` accessors match the `recommend` caller; `NormalizedProfile` single top-level type in `content`.
- **Risk gate:** the scoring-path rewire (Task 2 Step 6) is validated by the Task 2 full-suite run before retrieval is even moved, isolating the risky change.
- **Watch item:** `recommend`'s `this::mergeCandidate` usage (Task 3 Step 4) — resolve explicitly so the service compiles after `mergeCandidate` moves.
