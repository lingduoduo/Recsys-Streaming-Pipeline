# Profile Audit Walker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-account lookup (`GET /actuator/profile-audit/{user}`) on a shared bounded-neighbors walker, make the Profile → Content edge a count-plus-capped-sample probe, and rewire the bulk audit to stream chunks so at most `parallelism` chunks are resident.

**Architecture:** One new package-private `ProfileAuditWalker` owns both edges: Account → Profile is a single pipelined `store.readProfiles` for a batch (≤1 neighbor per account), Preference → Content is `CatalogPreferenceIndex.probe(kind, value, limit)` returning `(total, sample)`. The per-account endpoint calls the walker with a one-element list; the bulk audit feeds it chunks through a bounded in-flight deque, joining the oldest future first so rows stay in scan order and failure semantics are unchanged.

**Tech Stack:** Java 17, Spring Boot 3.3.5, Spring Data Redis 3.3.5 (Lettuce), Jackson, JUnit 5, Mockito, `@WebMvcTest`. Build/test with Maven from `recsys-pipeline/services/java-retrieval-service`.

**Spec:** `.superpowers/docs/specs/2026-09-07-profile-audit-walker-design.md`

## Global Constraints

- All commands run from `recsys-pipeline/services/java-retrieval-service` unless stated. JDK 17 is on PATH. A single test class takes ~20 s (`mvn -q test -Dtest=<ClassName>`); the full suite takes a few minutes and currently stands at 308 run / 0 failures / 1 skipped (`UserProfileIntegrationTest`, Testcontainers on Docker 29 — the skip is expected and accepted).
- Work on branch `feat/profile-audit-walker` (already created from master). **Never commit to master; the user merges the PR.** The checkout carries unrelated uncommitted files (`.planning/.active_plan`, `recsys-pipeline/frontend/data/dashboard.json`, untracked `.planning/2026-08-25-*/`) that must never be staged. `git add <explicit paths>` only.
- Commit messages end with the trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- **Every pre-existing test must keep passing unchanged.** No existing test file is edited except to ADD tests. If an existing test fails, fix the production code, never the test.
- Reason strings and finding types are fixed and shared with serving: `missing_active_run`, `missing_profile`, `invalid_json`, `unsupported_version`, `user_mismatch`, `run_mismatch`; `no_profile`, `new_or_unknown`, `empty_preferences`, `preference_without_content`. Preference kinds: `genre`, `tag`.
- Per-account path variable pattern: `[a-zA-Z0-9_:-]{1,64}` (identical to `RecommendationController`'s profile route). Bad id → 400 `{"error":"Invalid input: id must be 1-64 alphanumeric characters"}`.
- No new configuration keys. `parallelism` (default 4) additionally bounds in-flight chunks; `chunk-size` (500) bounds per-future residency; `sample-items` (5) bounds the content probe.
- Bulk failure semantics are unchanged: on any store failure, let in-flight futures settle, then throw `ProfileAuditFailedException` carrying the failure hit first in chunk order. No partial report.
- Bulk row order is unchanged: scan order.
- The endpoint records no serving measurement; `RecommendationMeasurementService` is not touched.
- Report JSON stays snake_case with nulls omitted (`@JsonProperty`, `@JsonInclude(NON_NULL)`), and `@JsonPropertyOrder` is preserved where present.

---

## File structure

| File | Responsibility |
|---|---|
| `.../service/audit/CatalogPreferenceIndex.java` (modify) | add `ContentNeighbors` record + `probe(kind, value, limit)`; `lookup` unchanged |
| `.../service/audit/UserAuditClassifier.java` (modify) | use `probe` in `resolve`; accept a null `activeRun` and emit the `missing_active_run` row |
| `.../service/audit/ProfileAuditWalker.java` (new) | the bounded-neighbors operation: one round trip per batch, rows in input order |
| `.../service/audit/AccountAuditReport.java` (new) | per-account response record |
| `.../service/audit/ProfileAuditService.java` (modify) | add `auditAccount`; replace `readAllProfiles` + classify loop with the bounded streaming pipeline |
| `.../controller/ProfileAuditController.java` (modify) | add the per-account route, `@Validated`, and the 400 handler |
| `docs/recommendation_architecture/API.md`, `README.md` (modify) | per-account section; streaming sentence; README pointer |

Tests: `CatalogPreferenceIndexTest` (+3), `UserAuditClassifierTest` (+1), `ProfileAuditWalkerTest` (new), `ProfileAuditServiceTest` (+6), `ProfileAuditControllerTest` (+3) — all additive.

---

### Task 1: Bounded content probe on `CatalogPreferenceIndex`

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/audit/CatalogPreferenceIndex.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/CatalogPreferenceIndexTest.java` (add tests only)

**Interfaces:**
- Consumes: nothing new.
- Produces: `CatalogPreferenceIndex.ContentNeighbors(int total, List<String> sample)` and `ContentNeighbors probe(String kind, String value, int limit)`. Used by Tasks 2 and 3.

- [ ] **Step 1: Write the failing tests**

Append these three tests inside `CatalogPreferenceIndexTest` (before the closing brace). The existing `index()` helper builds a 3-item catalog where `sci-fi` → `[item1, item9]`, `drama` → `[item5]`, `space` → `[item1, item9]`, `classic` → `[item1]`.

```java
    @Test
    void probeReturnsTotalAndACappedSample() {
        CatalogPreferenceIndex.ContentNeighbors neighbors =
            index().probe(CatalogPreferenceIndex.KIND_GENRE, "sci-fi", 1);

        assertEquals(2, neighbors.total());
        assertEquals(List.of("item1"), neighbors.sample());
    }

    @Test
    void probeSampleIsTheWholePostingListWhenLimitExceedsIt() {
        CatalogPreferenceIndex.ContentNeighbors neighbors =
            index().probe(CatalogPreferenceIndex.KIND_TAG, "space", 10);

        assertEquals(2, neighbors.total());
        assertEquals(List.of("item1", "item9"), neighbors.sample());
    }

    @Test
    void probeIsEmptyForMissUnknownKindNullValueOrNonPositiveLimit() {
        CatalogPreferenceIndex index = index();

        assertEquals(0, index.probe(CatalogPreferenceIndex.KIND_GENRE, "film-noir", 5).total());
        assertTrue(index.probe(CatalogPreferenceIndex.KIND_GENRE, "film-noir", 5).sample().isEmpty());
        assertTrue(index.probe("keyword", "space", 5).sample().isEmpty());
        assertTrue(index.probe(CatalogPreferenceIndex.KIND_GENRE, null, 5).sample().isEmpty());

        CatalogPreferenceIndex.ContentNeighbors zeroLimit =
            index.probe(CatalogPreferenceIndex.KIND_GENRE, "sci-fi", 0);
        assertEquals(2, zeroLimit.total());
        assertTrue(zeroLimit.sample().isEmpty());

        CatalogPreferenceIndex.ContentNeighbors negativeLimit =
            index.probe(CatalogPreferenceIndex.KIND_GENRE, "sci-fi", -3);
        assertEquals(2, negativeLimit.total());
        assertTrue(negativeLimit.sample().isEmpty());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -q test -Dtest=CatalogPreferenceIndexTest`
Expected: compilation FAILURE, `cannot find symbol: method probe(...)` / `class ContentNeighbors`.

- [ ] **Step 3: Add the record and the method**

In `CatalogPreferenceIndex`, add the record just below the two `KIND_*` constants:

```java
    /** Neighbor count plus a capped sample — the audit reads only what it reports. */
    public record ContentNeighbors(int total, List<String> sample) {
    }
```

and add this method directly after `lookup`:

```java
    /**
     * The bounded Preference → Content edge: how many catalog items carry {@code value} as a
     * {@code kind}, plus at most {@code limit} of them. The posting list itself is never copied,
     * so a preference matching thousands of items costs the same as one matching three.
     */
    public ContentNeighbors probe(String kind, String value, int limit) {
        List<String> postings = lookup(kind, value);
        int sampleSize = Math.min(Math.max(limit, 0), postings.size());
        return new ContentNeighbors(postings.size(), postings.subList(0, sampleSize));
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -q test -Dtest=CatalogPreferenceIndexTest`
Expected: PASS (8 tests: 5 existing + 3 new).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/CatalogPreferenceIndex.java \
        src/test/java/com/demo/retrieval/service/audit/CatalogPreferenceIndexTest.java
git commit -m "feat(audit): bounded content probe returning a count and a capped sample

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Classifier uses the probe and handles a null active run

**Files:**
- Modify: `src/main/java/com/demo/retrieval/service/audit/UserAuditClassifier.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/UserAuditClassifierTest.java` (add one test only)

**Interfaces:**
- Consumes: `CatalogPreferenceIndex.probe` / `ContentNeighbors` (Task 1).
- Produces: `UserAuditClassifier.classify(RawProfile, String activeRun /* nullable */, CatalogPreferenceIndex, ObjectMapper, int sampleItems)` — same signature, now null-tolerant on `activeRun`. Used by Task 3.

- [ ] **Step 1: Write the failing test**

Append inside `UserAuditClassifierTest` (before the closing brace). Note it calls `classify` directly rather than the file's private `classify(RawProfile)` helper, because it needs a null run:

```java
    @Test
    void nullActiveRunIsReportedAsMissingActiveRun() {
        UserRow row = UserAuditClassifier.classify(
            new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of(), false), 100L),
            null, INDEX, MAPPER, 1);

        assertFalse(row.hasProfile());
        assertNull(row.ttlSeconds());
        assertNull(row.preferences());
        assertEquals(List.of(new Finding("no_profile", "missing_active_run", null)), row.findings());
    }
```

If `assertFalse` is not already statically imported in this file, add `import static org.junit.jupiter.api.Assertions.assertFalse;`.

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q test -Dtest=UserAuditClassifierTest`
Expected: FAIL — a `NullPointerException` from `UserProfileValidation.validate` calling `activeRun.equals(...)`, because nothing guards a null run yet.

- [ ] **Step 3: Add the guard and switch `resolve` to the probe**

In `UserAuditClassifier.classify`, insert this as the first statement of the method body, before the `validate` call:

```java
        if (activeRun == null) {
            return new UserRow(raw.userId(), false, null,
                List.of(new Finding(ProfileAuditReport.FINDING_NO_PROFILE, ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN, null)), null);
        }
```

Then replace the body of the `for` loop in `resolve` (the three statements from `List<String> items = ...` through the `if (items.isEmpty())` block) with:

```java
            CatalogPreferenceIndex.ContentNeighbors neighbors = index.probe(kind, preference.value(), sampleItems);
            matches.add(new PreferenceMatch(kind, preference.value(), preference.score(), preference.evidenceCount(),
                neighbors.total(), neighbors.sample()));
            if (neighbors.total() == 0) {
                unmatched.add(new PreferenceRef(kind, preference.value()));
            }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q test -Dtest='UserAuditClassifierTest,CatalogPreferenceIndexTest'`
Expected: PASS (8 classifier tests: 7 existing unchanged + 1 new; 8 index tests). The seven existing classifier tests passing unchanged is what proves the probe swap is behavior-preserving.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/UserAuditClassifier.java \
        src/test/java/com/demo/retrieval/service/audit/UserAuditClassifierTest.java
git commit -m "feat(audit): classify via the bounded probe and report a null active run as a row

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `ProfileAuditWalker`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/audit/ProfileAuditWalker.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/ProfileAuditWalkerTest.java`

**Interfaces:**
- Consumes: `ProfileAuditStore` (+ nested `RawProfile`, `ScanResult`), `CatalogPreferenceIndex` (Task 1), `UserAuditClassifier.classify` (Task 2), Jackson `ObjectMapper`.
- Produces: package-private `ProfileAuditWalker(ProfileAuditStore store, ObjectMapper mapper, CatalogPreferenceIndex index, int sampleItems)` and `List<ProfileAuditReport.UserRow> walk(String activeRun, List<String> accounts)`. Used by Task 4.

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.content.NormalizedProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.demo.retrieval.service.audit.UserAuditClassifierTest.profileJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileAuditWalkerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final CatalogPreferenceIndex INDEX = CatalogPreferenceIndex.build(Map.of(
        "item1", new NormalizedProfile("", Set.of("sci-fi"), Set.of("space"), Set.of("space"), "", false, 0L)));

    /** Records every readProfiles batch so the test can assert one round trip per walk. */
    private static final class RecordingStore implements ProfileAuditStore {
        final List<List<String>> batches = new ArrayList<>();
        final Map<String, RawProfile> profiles;

        RecordingStore(Map<String, RawProfile> profiles) {
            this.profiles = profiles;
        }

        @Override
        public ScanResult scanUserIds(String pattern, int limit) {
            throw new UnsupportedOperationException("the walker never scans");
        }

        @Override
        public Optional<String> activeRun() {
            throw new UnsupportedOperationException("the walker never reads the pointer");
        }

        @Override
        public List<RawProfile> readProfiles(String run, List<String> userIds) {
            batches.add(List.copyOf(userIds));
            return userIds.stream().map(profiles::get).toList();
        }
    }

    private static RawProfile healthy(String user) {
        return new RawProfile(user, profileJson(user, "run-7", List.of("sci-fi"), List.of("space"), false), 3400L);
    }

    @Test
    void readsTheWholeBatchInOneRoundTripAndKeepsInputOrder() {
        RecordingStore store = new RecordingStore(Map.of(
            "u1", healthy("u1"), "u2", new RawProfile("u2", null, -2L), "u3", healthy("u3")));
        ProfileAuditWalker walker = new ProfileAuditWalker(store, MAPPER, INDEX, 5);

        List<UserRow> rows = walker.walk("run-7", List.of("u1", "u2", "u3"));

        assertEquals(1, store.batches.size());
        assertEquals(List.of("u1", "u2", "u3"), store.batches.get(0));
        assertEquals(List.of("u1", "u2", "u3"), rows.stream().map(UserRow::userId).toList());
        assertTrue(rows.get(0).findings().isEmpty());
        assertEquals("missing_profile", rows.get(1).findings().get(0).reason());
    }

    @Test
    void aNullActiveRunProducesMissingActiveRunRowsWithoutReadingTheStore() {
        RecordingStore store = new RecordingStore(Map.of());
        ProfileAuditWalker walker = new ProfileAuditWalker(store, MAPPER, INDEX, 5);

        List<UserRow> rows = walker.walk(null, List.of("u1", "u2"));

        assertTrue(store.batches.isEmpty());
        assertEquals(List.of("u1", "u2"), rows.stream().map(UserRow::userId).toList());
        rows.forEach(row -> {
            assertFalse(row.hasProfile());
            assertEquals("missing_active_run", row.findings().get(0).reason());
        });
    }

    @Test
    void anEmptyBatchTouchesTheStoreNotAtAll() {
        RecordingStore store = new RecordingStore(Map.of());
        ProfileAuditWalker walker = new ProfileAuditWalker(store, MAPPER, INDEX, 5);

        assertTrue(walker.walk("run-7", List.of()).isEmpty());
        assertTrue(store.batches.isEmpty());
    }

    @Test
    void aStoreFailurePropagatesUnwrapped() {
        ProfileAuditStore failing = new RecordingStore(Map.of()) {
            @Override
            public List<RawProfile> readProfiles(String run, List<String> userIds) {
                throw new IllegalStateException("redis unavailable");
            }
        };
        ProfileAuditWalker walker = new ProfileAuditWalker(failing, MAPPER, INDEX, 5);

        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalStateException.class, () -> walker.walk("run-7", List.of("u1")));
        assertEquals("redis unavailable", e.getMessage());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `mvn -q test -Dtest=ProfileAuditWalkerTest`
Expected: compilation FAILURE, `cannot find symbol: class ProfileAuditWalker`.

- [ ] **Step 3: Write the walker**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * "Neighbors of these accounts", as one bounded operation.
 *
 * <p>Account → Profile is a small fan-out: at most one profile per account, fetched for the whole
 * batch in a single pipelined round trip. Preference → Content is a large fan-out kept cheap by
 * {@link CatalogPreferenceIndex#probe}, which reports a count and a capped sample instead of
 * materializing a posting list. Nothing here scans, aggregates, or holds state across calls, so
 * both entry points — one account or one chunk of a bulk audit — traverse identically.
 */
final class ProfileAuditWalker {

    private final ProfileAuditStore store;
    private final ObjectMapper mapper;
    private final CatalogPreferenceIndex index;
    private final int sampleItems;

    ProfileAuditWalker(ProfileAuditStore store, ObjectMapper mapper, CatalogPreferenceIndex index, int sampleItems) {
        this.store = store;
        this.mapper = mapper;
        this.index = index;
        this.sampleItems = sampleItems;
    }

    /**
     * Classified rows for {@code accounts}, in input order. A null {@code activeRun} yields a
     * {@code missing_active_run} row per account and reads nothing.
     */
    List<UserRow> walk(String activeRun, List<String> accounts) {
        if (accounts.isEmpty()) {
            return List.of();
        }
        if (activeRun == null) {
            List<UserRow> rows = new ArrayList<>(accounts.size());
            for (String account : accounts) {
                rows.add(UserAuditClassifier.classify(new RawProfile(account, null, -2L), null, index, mapper, sampleItems));
            }
            return rows;
        }
        List<RawProfile> raws = store.readProfiles(activeRun, accounts);
        List<UserRow> rows = new ArrayList<>(raws.size());
        for (RawProfile raw : raws) {
            rows.add(UserAuditClassifier.classify(raw, activeRun, index, mapper, sampleItems));
        }
        return rows;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `mvn -q test -Dtest=ProfileAuditWalkerTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/ProfileAuditWalker.java \
        src/test/java/com/demo/retrieval/service/audit/ProfileAuditWalkerTest.java
git commit -m "feat(audit): ProfileAuditWalker as the bounded neighbors operation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Per-account audit and streaming bulk audit

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/audit/AccountAuditReport.java`
- Modify: `src/main/java/com/demo/retrieval/service/audit/ProfileAuditService.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/ProfileAuditServiceTest.java` (add tests and two `FakeStore` fields only)

**Interfaces:**
- Consumes: `ProfileAuditWalker` (Task 3), `CatalogPreferenceIndex` (Task 1).
- Produces: `AccountAuditReport(String status, String activeRun, String generatedAt, long elapsedMs, int catalogSize, ProfileAuditReport.UserRow user)` and `ProfileAuditService.auditAccount(String userId)`. Used by Task 5.

- [ ] **Step 1: Write the per-account response record**

```java
package com.demo.retrieval.service.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** Response of GET /actuator/profile-audit/{user}: one account's row, always present. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"status", "active_run", "generated_at", "elapsed_ms", "catalog_size", "user"})
public record AccountAuditReport(
    String status,
    @JsonProperty("active_run") String activeRun,
    @JsonProperty("generated_at") String generatedAt,
    @JsonProperty("elapsed_ms") long elapsedMs,
    @JsonProperty("catalog_size") int catalogSize,
    ProfileAuditReport.UserRow user
) {
}
```

- [ ] **Step 2: Write the failing tests**

First add two fields and an overlap hook to the existing `FakeStore` in `ProfileAuditServiceTest` — add these fields beside the existing ones:

```java
        final java.util.concurrent.atomic.AtomicInteger inFlight = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxInFlight = new java.util.concurrent.atomic.AtomicInteger();
        long readDelayMillis = 0L;
```

and wrap the body of `readProfiles` so the counters bracket the whole read (keep every existing statement, including `chunksSeen.add(userIds)` and the `blockReads`/`failOnUser` handling, inside the `try`):

```java
        @Override
        public List<RawProfile> readProfiles(String run, List<String> userIds) {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
                chunksSeen.add(userIds);
                if (blockReads != null) {
                    try { blockReads.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                if (readDelayMillis > 0L) {
                    try { Thread.sleep(readDelayMillis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                if (failOnUser != null && userIds.contains(failOnUser)) {
                    throw new IllegalStateException("redis unavailable");
                }
                return userIds.stream().map(profiles::get).toList();
            } finally {
                inFlight.decrementAndGet();
            }
        }
```

Then append these six tests (before the class's closing brace):

```java
    @Test
    void neverKeepsMoreThanParallelismChunksInFlight() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 18; i++) {
            store.user("u" + i, null, -2L);
        }
        store.readDelayMillis = 20L;

        ProfileAuditReport report = service(store, 2, 2).audit(null);

        assertEquals(9, store.chunksSeen.size());
        assertEquals(18, report.summary().usersScanned());
        assertTrue(store.maxInFlight.get() <= 2, "max in flight was " + store.maxInFlight.get());
    }

    @Test
    void streamingKeepsRowsInScanOrderAcrossManySmallChunks() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 7; i++) {
            store.user("u" + i, null, -2L);
        }

        ProfileAuditReport report = service(store, 1, 3).audit(null);

        assertEquals(List.of("u0", "u1", "u2", "u3", "u4", "u5", "u6"),
            report.users().stream().map(UserRow::userId).toList());
    }

    @Test
    void aFailureInTheLastChunkStillFailsTheWholeAudit() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 6; i++) {
            store.user("u" + i, null, -2L);
        }
        store.failOnUser = "u5";

        ProfileAuditService.ProfileAuditFailedException e = assertThrows(
            ProfileAuditService.ProfileAuditFailedException.class, () -> service(store, 2, 2).audit(null));

        assertEquals("redis unavailable", e.getMessage());
        assertEquals(3, store.chunksSeen.size());
    }

    @Test
    void auditAccountReturnsTheRowForOneHealthyAccount() {
        FakeStore store = new FakeStore()
            .user("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of("space"), false), 3400L)
            .user("u2", null, -2L);

        AccountAuditReport report = service(store, 500, 4).auditAccount("u1");

        assertEquals("ok", report.status());
        assertEquals("run-7", report.activeRun());
        assertEquals(2, report.catalogSize());
        assertEquals("u1", report.user().userId());
        assertTrue(report.user().hasProfile());
        assertTrue(report.user().findings().isEmpty());
        assertEquals(2, report.user().preferences().size());
        assertEquals(List.of(List.of("u1")), store.chunksSeen);
    }

    @Test
    void auditAccountReportsMissingActiveRun() {
        FakeStore store = new FakeStore().user("u1", null, -2L);
        store.activeRun = Optional.empty();

        AccountAuditReport report = service(store, 500, 4).auditAccount("u1");

        assertEquals("missing_active_run", report.status());
        assertNull(report.activeRun());
        assertEquals("missing_active_run", report.user().findings().get(0).reason());
        assertTrue(store.chunksSeen.isEmpty());
    }

    @Test
    void auditAccountWrapsAStoreFailure() {
        FakeStore store = new FakeStore().user("u1", null, -2L);
        store.failOnUser = "u1";

        ProfileAuditService.ProfileAuditFailedException e = assertThrows(
            ProfileAuditService.ProfileAuditFailedException.class, () -> service(store, 500, 4).auditAccount("u1"));

        assertEquals("redis unavailable", e.getMessage());
    }
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `mvn -q test -Dtest=ProfileAuditServiceTest`
Expected: compilation FAILURE, `cannot find symbol: method auditAccount(String)` / `class AccountAuditReport`. (`neverKeepsMoreThanParallelismChunksInFlight` would also fail on the current code, which submits all 9 chunks up front.)

- [ ] **Step 4: Add `auditAccount` and make the bulk audit stream**

In `ProfileAuditService`, add `auditAccount` directly above `audit`:

```java
    /**
     * One account, one round trip. Deliberately unguarded and executor-free: this path is a
     * bounded neighbor lookup, so it must not queue behind a full audit.
     */
    public AccountAuditReport auditAccount(String userId) {
        long started = System.nanoTime();
        try {
            CatalogPreferenceIndex index = CatalogPreferenceIndex.build(catalogScoring.normalizedCatalog());
            String activeRun = store.activeRun().orElse(null);
            UserRow row = new ProfileAuditWalker(store, objectMapper, index, config.getSampleItems())
                .walk(activeRun, List.of(userId)).get(0);
            return new AccountAuditReport(
                activeRun == null ? ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN : ProfileAuditReport.STATUS_OK,
                activeRun,
                Instant.now().toString(),
                (System.nanoTime() - started) / 1_000_000L,
                index.catalogSize(),
                row);
        } catch (RuntimeException e) {
            if (e instanceof ProfileAuditFailedException) {
                throw e;
            }
            throw new ProfileAuditFailedException(e);
        }
    }
```

In `audit`, replace the `else` branch's loop

```java
                for (RawProfile raw : readAllProfiles(activeRun, scan.userIds())) {
                    aggregation.add(UserAuditClassifier.classify(raw, activeRun, index, objectMapper, config.getSampleItems()));
                }
```

with

```java
                streamRows(activeRun, scan.userIds(), index, aggregation);
```

and replace the whole `readAllProfiles` method with:

```java
    /**
     * Streams chunks through the walker with at most {@code parallelism} futures in flight,
     * joining the oldest first so rows stay in scan order and only a bounded number of chunks is
     * ever resident. Failure semantics are unchanged: every in-flight read settles, then the
     * failure hit first in chunk order is rethrown and the whole report is discarded.
     */
    private void streamRows(String activeRun, List<String> userIds, CatalogPreferenceIndex index, Aggregation aggregation) {
        ProfileAuditWalker walker = new ProfileAuditWalker(store, objectMapper, index, config.getSampleItems());
        Deque<CompletableFuture<List<UserRow>>> inFlight = new ArrayDeque<>();
        try {
            for (int from = 0; from < userIds.size(); from += config.getChunkSize()) {
                if (inFlight.size() >= config.getParallelism()) {
                    drainOldest(inFlight, aggregation);
                }
                List<String> chunk = userIds.subList(from, Math.min(from + config.getChunkSize(), userIds.size()));
                inFlight.add(CompletableFuture.supplyAsync(() -> walker.walk(activeRun, chunk), executor));
            }
            while (!inFlight.isEmpty()) {
                drainOldest(inFlight, aggregation);
            }
        } catch (ProfileAuditFailedException e) {
            CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new)).exceptionally(t -> null).join();
            throw e;
        }
    }

    private static void drainOldest(Deque<CompletableFuture<List<UserRow>>> inFlight, Aggregation aggregation) {
        CompletableFuture<List<UserRow>> oldest = inFlight.poll();
        try {
            oldest.join().forEach(aggregation::add);
        } catch (CompletionException e) {
            throw new ProfileAuditFailedException(e.getCause() == null ? e : e.getCause());
        }
    }
```

Adjust the imports in `ProfileAuditService`: add `java.util.ArrayDeque` and `java.util.Deque`; remove `com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile` if it is now unused (the compiler's unused-import warning will not flag it, so check by hand — `RawProfile` no longer appears in the file).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `mvn -q test -Dtest=ProfileAuditServiceTest`
Expected: PASS (13 tests: 7 existing unchanged + 6 new). The existing `oneFailingChunkFailsTheWholeAudit` still expecting `chunksSeen.size() == 2` must pass: with 4 users, chunk size 2 and parallelism 2, both chunks are submitted before the first join.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/AccountAuditReport.java \
        src/main/java/com/demo/retrieval/service/audit/ProfileAuditService.java \
        src/test/java/com/demo/retrieval/service/audit/ProfileAuditServiceTest.java
git commit -m "feat(audit): per-account lookup and bounded streaming fan-out

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: Per-account route and documentation

**Files:**
- Modify: `src/main/java/com/demo/retrieval/controller/ProfileAuditController.java`
- Test: `src/test/java/com/demo/retrieval/controller/ProfileAuditControllerTest.java` (add tests only)
- Modify: `../../docs/recommendation_architecture/API.md`
- Modify: `../../README.md`

**Interfaces:**
- Consumes: `ProfileAuditService.auditAccount(String)` and `AccountAuditReport` (Task 4).
- Produces: `GET /actuator/profile-audit/{user}` → 200 `AccountAuditReport` | 400 invalid id | 503 store failure.

- [ ] **Step 1: Write the failing tests**

Append inside `ProfileAuditControllerTest` (before the closing brace), and add the imports it needs: `com.demo.retrieval.service.audit.AccountAuditReport` and `static org.mockito.ArgumentMatchers.anyString`.

```java
    private static AccountAuditReport accountReport() {
        return new AccountAuditReport("ok", "run-7", "2026-09-07T10:00:00Z", 2L, 12,
            new UserRow("u2", true, 3400L, List.of(), List.of()));
    }

    @Test
    void returnsOneAccountRow() throws Exception {
        when(auditService.auditAccount("u2")).thenReturn(accountReport());

        mvc.perform(get("/actuator/profile-audit/u2"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"))
            .andExpect(jsonPath("$.active_run").value("run-7"))
            .andExpect(jsonPath("$.catalog_size").value(12))
            .andExpect(jsonPath("$.user.user_id").value("u2"))
            .andExpect(jsonPath("$.user.ttl_seconds").value(3400))
            .andExpect(jsonPath("$.user.findings").isEmpty());
    }

    @Test
    void anInvalidAccountIdIs400() throws Exception {
        mvc.perform(get("/actuator/profile-audit/bad id!"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value("Invalid input: id must be 1-64 alphanumeric characters"));
    }

    @Test
    void accountStoreFailureIs503() throws Exception {
        when(auditService.auditAccount(anyString())).thenThrow(
            new ProfileAuditService.ProfileAuditFailedException(new IllegalStateException("redis unavailable")));

        mvc.perform(get("/actuator/profile-audit/u2"))
            .andExpect(status().isServiceUnavailable())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.message").value("redis unavailable"));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `mvn -q test -Dtest=ProfileAuditControllerTest`
Expected: compilation FAILURE, `cannot find symbol: class AccountAuditReport` / `method auditAccount`.

- [ ] **Step 3: Add the route**

In `ProfileAuditController`, add `@Validated` to the class (below `@RestController`), and add the route plus the validation handler after the existing `audit` method:

```java
    @GetMapping("/actuator/profile-audit/{user}")
    public ResponseEntity<?> auditAccount(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user
    ) {
        try {
            return ResponseEntity.ok(auditService.auditAccount(user));
        } catch (ProfileAuditFailedException e) {
            log.error("Profile audit failed for account {}", user, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
        }
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleValidation(ConstraintViolationException e) {
        return Map.of("error", "Invalid input: id must be 1-64 alphanumeric characters");
    }
```

New imports: `jakarta.validation.ConstraintViolationException`, `jakarta.validation.constraints.Pattern`, `org.springframework.validation.annotation.Validated`, `org.springframework.web.bind.annotation.ExceptionHandler`, `org.springframework.web.bind.annotation.PathVariable`, `org.springframework.web.bind.annotation.ResponseStatus`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `mvn -q test -Dtest=ProfileAuditControllerTest`
Expected: PASS (8 tests: 5 existing unchanged + 3 new). The existing `passesLimitThrough` test passing is what proves the two routes stay disjoint.

- [ ] **Step 5: Document both changes**

In `../../docs/recommendation_architecture/API.md`, append this new section immediately after the bulk profile-audit section — that is, directly before the line `## \`GET /predict/{user}/{item}\``:

````markdown
## `GET /actuator/profile-audit/{user}`

The same walk for a single account: one pipelined round trip for its profile, then one bounded
probe per preference. Unguarded and executor-free, so it never queues behind a bulk audit.

```bash
curl -s localhost:8080/actuator/profile-audit/user_1 | jq .
```

```json
{
  "status": "ok",
  "active_run": "run-7",
  "generated_at": "2026-09-07T10:00:00Z",
  "elapsed_ms": 2,
  "catalog_size": 12,
  "user": {"user_id": "user_1", "has_profile": true, "ttl_seconds": 3400, "findings": [],
           "preferences": [{"kind": "genre", "value": "sci-fi", "score": 0.72, "evidence_count": 8,
                            "matched_items": 8, "sample_items": ["item1", "item4"]}]}
}
```

The `user` object is the same row the bulk report lists, and it is always returned — a healthy
account has an empty `findings` array. `matched_items` is the full count of catalog items carrying
that preference while `sample_items` holds at most `RECSYS_PROFILE_AUDIT_SAMPLE_ITEMS` of them, so
a preference matching thousands of items costs no more to report than one matching three.

Status codes: 200; 400 if the id is outside `[a-zA-Z0-9_:-]{1,64}`; 503
`{"status":"error","message":...}` if Redis fails. There is no 409 — only the bulk route is
single-flight.
````

In the same file, in the bulk section's `Status codes:` paragraph, append one sentence at the end (after the `RECSYS_PROFILE_AUDIT_CHUNK_SIZE` sentence):

```markdown
Chunks are classified as they complete, with at most `RECSYS_PROFILE_AUDIT_PARALLELISM` chunks
resident at a time, so `max-users` bounds how much work one call does rather than how much memory
it holds.
```

In `../../README.md`, replace the existing three-line pointer

```markdown
To check every user at once — who lacks a valid profile, and which preferences match no catalog
item — call `GET /actuator/profile-audit`; see
[API.md](docs/recommendation_architecture/API.md#get-actuatorprofile-audit).
```

with

```markdown
To check every user at once — who lacks a valid profile, and which preferences match no catalog
item — call `GET /actuator/profile-audit`; for a single account, `GET
/actuator/profile-audit/{user}`. See
[API.md](docs/recommendation_architecture/API.md#get-actuatorprofile-audit).
```

- [ ] **Step 6: Run the full suite**

Run: `mvn -q test`
Expected: BUILD SUCCESS. Surefire totals should read 322 run / 0 failures / 0 errors / 1 skipped (308 before, plus 3 index + 1 classifier + 4 walker + 6 service + 3 controller = 325 … the precise total is whatever the run reports; what matters is **0 failures, 0 errors, and exactly 1 skip** (`UserProfileIntegrationTest`). Confirm by summing the reports:

```bash
cd target/surefire-reports && grep -h '^Tests run:' *.txt \
  | awk -F'[:,]' '{r+=$2; f+=$4; e+=$6; s+=$8} END {print "run="r" failures="f" errors="e" skipped="s}'
```

Note the ERROR lines that Spring and Testcontainers print during the negative-path and Docker-less tests are expected log output, not failures — trust the surefire totals.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/demo/retrieval/controller/ProfileAuditController.java \
        src/test/java/com/demo/retrieval/controller/ProfileAuditControllerTest.java \
        ../../docs/recommendation_architecture/API.md ../../README.md
git commit -m "feat(audit): per-account profile-audit route and docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: Live smoke

**Files:** none. This verifies both routes against a real Redis.

- [ ] **Step 1: Start Redis and the service**

The Docker daemon may be down; a throwaway local Redis is equivalent for this check. From the module directory:

```bash
redis-server --port 6379 --save "" --appendonly no --daemonize yes
redis-cli ping   # PONG
mvn -q spring-boot:run   # background; wait for curl -s localhost:8080/predict/metadata to return JSON
```

- [ ] **Step 2: Seed and exercise both routes**

```bash
redis-cli SET user-profile:v1:active-run run-7
redis-cli HSET user:u1:features age 30
redis-cli HSET user:u2:features age 40
redis-cli SET user-profile:v1:run-7:u1 "$(sed 's/fixture-user/u1/; s/fixture-run/run-7/' ../../integration-tests/fixtures/user_profile_v1.json)" EX 3600
curl -s localhost:8080/actuator/profile-audit/u1 | jq .
curl -s localhost:8080/actuator/profile-audit/u2 | jq .
curl -s -o /dev/null -w '%{http_code}\n' 'localhost:8080/actuator/profile-audit/bad%20id!'
curl -s 'localhost:8080/actuator/profile-audit' | jq .summary
```

Expected:
- `u1`: `status` `ok`, `user.has_profile` true, a `new_or_unknown` finding (the fixture's persona), `matched_items` ≥ 1 for `sci-fi` and `space`, `elapsed_ms` in the low single digits.
- `u2`: `status` `ok`, `user.has_profile` false, `findings[0].reason` `missing_profile`.
- The invalid id: `400`.
- The bulk route: `users_scanned` 2, `users_with_profile` 1 — proving the two routes coexist.

- [ ] **Step 3: Clean up**

```bash
kill $(lsof -nP -iTCP:8080 -sTCP:LISTEN -t)
redis-cli DEL user:u1:features user:u2:features user-profile:v1:run-7:u1 user-profile:v1:active-run
redis-cli shutdown nosave
```

Verify nothing listens on 6379 or 8080.

- [ ] **Step 4: Report**

No commit. Report observed vs expected for every bullet above; any mismatch is a finding, not something to rationalize.

---

## Self-review notes

- **Spec coverage:** Component 1 (probe) → Task 1; Component 2 (classifier) → Task 2; Component 3 (walker) → Task 3; Components 4 and 5 (per-account entry point, streaming) → Task 4; controller + docs → Task 5; the spec's testing table is distributed across Tasks 1-5 one row per task; the risk section's "route pattern" check is Task 5 Step 4.
- **Deliberate deviation:** the spec lists the per-account service method and the streaming rewrite as separate components, but they land in one task because both are edits to `ProfileAuditService` and share the walker construction — splitting them would leave the file half-migrated between two reviews.
- **Existing-test coupling noted:** `oneFailingChunkFailsTheWholeAudit` asserts `chunksSeen.size() == 2`, which under streaming holds only because parallelism 2 submits both chunks before the first join. Task 4 keeps it passing unchanged and adds `aFailureInTheLastChunkStillFailsTheWholeAudit` to pin the semantics independently of submission timing.
- **Type consistency:** `ContentNeighbors(int total, List<String> sample)` is produced in Task 1 and consumed in Task 2 only. `ProfileAuditWalker(store, mapper, index, sampleItems)` / `walk(activeRun, accounts)` is produced in Task 3 and consumed in Task 4 (both `auditAccount` and `streamRows`). `AccountAuditReport(status, activeRun, generatedAt, elapsedMs, catalogSize, user)` is produced in Task 4 and consumed in Task 5's test in that exact order. `UserAuditClassifier.classify`'s signature is unchanged throughout.
