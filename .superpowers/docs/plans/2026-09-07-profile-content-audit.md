# Profile → Content Audit Endpoint Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `GET /actuator/profile-audit` to the Java retrieval service: it walks every user → has_profile → profile preferences → catalog content and returns a findings-only JSON report.

**Architecture:** A narrow `ProfileAuditStore` interface is the only thing that touches Redis (SCAN for users, one pipelined GET+TTL round trip per chunk). `ProfileAuditService` fans chunks over a small executor, classifies each user with the *same* validation serving uses (extracted from `RedisUserProfileClient` into a pure `UserProfileValidation`), resolves preferences against a per-call inverted index over the in-memory catalog, and aggregates a summary. A thin controller maps busy/failed/bad-limit to 409/503/400.

**Tech Stack:** Java 17, Spring Boot 3.3.5, Spring Data Redis 3.3.5 (Lettuce, pooled), Jackson, JUnit 5, Mockito, `@WebMvcTest`. Build/test with Maven from `recsys-pipeline/services/java-retrieval-service`.

**Spec:** `.superpowers/docs/specs/2026-09-07-profile-content-audit-design.md`

## Global Constraints

- All commands below run from `recsys-pipeline/services/java-retrieval-service` unless stated. JDK 17 is on PATH (`java -version` → 17.0.12). A single test class takes ~20 s: `mvn -q test -Dtest=<ClassName>`.
- Work on branch `feat/profile-content-audit` (already created). **Never commit to master; the user merges the PR.**
- Validation reason strings are fixed and must match serving exactly: `missing_active_run`, `missing_profile`, `invalid_json`, `unsupported_version`, `user_mismatch`, `run_mismatch`.
- Finding type strings: `no_profile`, `new_or_unknown`, `empty_preferences`, `preference_without_content`. Preference kinds: `genre`, `tag`.
- Profile key prefix comes only from the existing property `recsys.user-profile.key-prefix` (default `user-profile:v1`). Do not add a second copy.
- Config block `recsys.profile-audit` with defaults: `user-key-pattern=user:*:features`, `max-users=10000`, `chunk-size=500`, `parallelism=4`, `sample-items=5`. Env overrides `RECSYS_PROFILE_AUDIT_USER_KEY_PATTERN`, `RECSYS_PROFILE_AUDIT_MAX_USERS`, `RECSYS_PROFILE_AUDIT_CHUNK_SIZE`, `RECSYS_PROFILE_AUDIT_PARALLELISM`, `RECSYS_PROFILE_AUDIT_SAMPLE_ITEMS`.
- User discovery uses SCAN (cursor), never KEYS. A Redis failure fails the whole call; no partial reports.
- The existing `RedisUserProfileClientTest` must pass unchanged after Task 1.
- The endpoint records no measurement (same as `/actuator/model-reload`). Do not touch `RecommendationMeasurementService`.
- Report JSON uses snake_case field names via `@JsonProperty`; null fields are omitted (`@JsonInclude(NON_NULL)`).
- The profile fixture used by tests is `../../integration-tests/fixtures/user_profile_v1.json` (relative to the module dir, which is the test working directory). Its user is `fixture-user`, run `fixture-run`, one genre `sci-fi`, one tag `space`, persona `new_or_unknown`.

---

## File structure

| File | Responsibility |
|---|---|
| `src/main/java/com/demo/retrieval/service/clients/UserProfileValidation.java` (new) | Pure: raw JSON + userId + activeRun → `Valid(profile)` or `Invalid(reason)`; owns preference-name normalization |
| `src/main/java/com/demo/retrieval/service/clients/RedisUserProfileClient.java` (modify) | Reads Redis, delegates validation to `UserProfileValidation`; metrics/fallbacks unchanged |
| `src/main/java/com/demo/retrieval/service/audit/CatalogPreferenceIndex.java` (new) | Pure: normalized catalog → `genre→[ids]`, `tag→[ids]` (sorted); `lookup(kind, value)` |
| `src/main/java/com/demo/retrieval/service/audit/ProfileAuditStore.java` (new) | Interface: `scanUserIds`, `activeRun`, `readProfiles`; records `RawProfile`, `ScanResult` |
| `src/main/java/com/demo/retrieval/service/audit/RedisProfileAuditStore.java` (new) | Redis implementation (SCAN + pipelined GET/TTL) |
| `src/main/java/com/demo/retrieval/service/audit/ProfileAuditReport.java` (new) | Report records (JSON shape) |
| `src/main/java/com/demo/retrieval/service/audit/UserAuditClassifier.java` (new) | Pure: one `RawProfile` → one `UserRow` with findings and preference matches |
| `src/main/java/com/demo/retrieval/service/audit/ProfileAuditService.java` (new) | Orchestration: single-flight guard, executor, chunk fan-out, aggregation, exceptions |
| `src/main/java/com/demo/retrieval/config/RecommendationProperties.java` (modify) | Add nested `ProfileAudit` config class |
| `src/main/resources/application.yml` (modify) | Add `recsys.profile-audit` block |
| `src/main/java/com/demo/retrieval/controller/ProfileAuditController.java` (new) | `GET /actuator/profile-audit`, status mapping |
| `docs/recommendation_architecture/API.md`, `README.md` (modify) | Endpoint docs |

---

### Task 1: Extract `UserProfileValidation` from `RedisUserProfileClient`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/clients/UserProfileValidation.java`
- Modify: `src/main/java/com/demo/retrieval/service/clients/RedisUserProfileClient.java`
- Test: `src/test/java/com/demo/retrieval/service/clients/UserProfileValidationTest.java`

**Interfaces:**
- Consumes: `com.demo.retrieval.model.UserBehaviorProfile` (existing record), Jackson `ObjectMapper`.
- Produces: `UserProfileValidation.validate(String rawProfile, String userId, String activeRun, ObjectMapper mapper) -> UserProfileValidation.Result`, where `Result` is sealed: `Valid(UserBehaviorProfile profile)` | `Invalid(String reason)`. Constant `UserProfileValidation.PROFILE_VERSION = 1`. Used by Task 5.

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.clients;

import com.demo.retrieval.service.clients.UserProfileValidation.Invalid;
import com.demo.retrieval.service.clients.UserProfileValidation.Result;
import com.demo.retrieval.service.clients.UserProfileValidation.Valid;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class UserProfileValidationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE_JSON = fixture()
        .replace("fixture-user", "u1")
        .replace("fixture-run", "run-7");

    @Test
    void nullRawProfileIsMissingProfile() {
        assertReason("missing_profile", UserProfileValidation.validate(null, "u1", "run-7", MAPPER));
    }

    @Test
    void unparseableJsonIsInvalidJson() {
        assertReason("invalid_json", UserProfileValidation.validate("not-json", "u1", "run-7", MAPPER));
    }

    @Test
    void wrongVersionIsUnsupportedVersion() {
        String json = PROFILE_JSON.replace("\"profile_version\":1", "\"profile_version\":2");
        assertReason("unsupported_version", UserProfileValidation.validate(json, "u1", "run-7", MAPPER));
    }

    @Test
    void differentUserIdIsUserMismatch() {
        String json = PROFILE_JSON.replace("\"u1\"", "\"u2\"");
        assertReason("user_mismatch", UserProfileValidation.validate(json, "u1", "run-7", MAPPER));
    }

    @Test
    void differentRunIsRunMismatch() {
        String json = PROFILE_JSON.replace("\"run-7\"", "\"run-8\"");
        assertReason("run_mismatch", UserProfileValidation.validate(json, "u1", "run-7", MAPPER));
    }

    @Test
    void validProfileIsReturnedWithNormalizedPreferenceNames() {
        String json = PROFILE_JSON.replace("\"sci-fi\"", "\"  SCI-FI  \"").replace("\"space\"", "\"Outer   Space\"");
        Result result = UserProfileValidation.validate(json, "u1", "run-7", MAPPER);
        Valid valid = assertInstanceOf(Valid.class, result);
        assertEquals("u1", valid.profile().userId());
        assertEquals("sci-fi", valid.profile().preferences().genres().get(0).value());
        assertEquals("outer space", valid.profile().preferences().tags().get(0).value());
        assertEquals(null, valid.profile().behavioralFeatures().averageRating());
    }

    private static void assertReason(String expected, Result result) {
        Invalid invalid = assertInstanceOf(Invalid.class, result);
        assertEquals(expected, invalid.reason());
    }

    private static String fixture() {
        try {
            return Files.readString(Path.of("../../integration-tests/fixtures/user_profile_v1.json"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=UserProfileValidationTest`
Expected: compilation FAILURE, `cannot find symbol: class UserProfileValidation`.

- [ ] **Step 3: Write the validation class**

```java
package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Locale;

/**
 * The single definition of "this user has a usable profile". Serving ({@link RedisUserProfileClient})
 * and the profile audit both call this, so they cannot disagree about what counts as a profile.
 * Pure: no Redis, no metrics.
 */
public final class UserProfileValidation {
    public static final int PROFILE_VERSION = 1;

    private UserProfileValidation() {
    }

    public sealed interface Result permits Valid, Invalid {
    }

    /** A profile that passed every check, with preference names normalized for serving. */
    public record Valid(UserBehaviorProfile profile) implements Result {
    }

    /** One of: missing_profile, invalid_json, unsupported_version, user_mismatch, run_mismatch. */
    public record Invalid(String reason) implements Result {
    }

    public static Result validate(String rawProfile, String userId, String activeRun, ObjectMapper mapper) {
        if (rawProfile == null) {
            return new Invalid("missing_profile");
        }
        UserBehaviorProfile profile;
        try {
            profile = mapper.readValue(rawProfile, UserBehaviorProfile.class);
        } catch (JsonProcessingException e) {
            return new Invalid("invalid_json");
        }
        if (profile.profileVersion() != PROFILE_VERSION) {
            return new Invalid("unsupported_version");
        }
        if (!userId.equals(profile.userId())) {
            return new Invalid("user_mismatch");
        }
        if (profile.runId() == null || profile.runId().isBlank() || !activeRun.equals(profile.runId())) {
            return new Invalid("run_mismatch");
        }
        return new Valid(normalizePreferenceNames(profile));
    }

    static UserBehaviorProfile normalizePreferenceNames(UserBehaviorProfile profile) {
        UserBehaviorProfile.Preferences preferences = profile.preferences();
        if (preferences == null) {
            return profile;
        }
        return new UserBehaviorProfile(
            profile.userId(),
            profile.profileVersion(),
            profile.runId(),
            profile.generatedAt(),
            profile.sourceWindow(),
            profile.evidenceCount(),
            new UserBehaviorProfile.Preferences(normalize(preferences.genres()), normalize(preferences.tags())),
            profile.behavioralFeatures(),
            profile.personas()
        );
    }

    private static List<UserBehaviorProfile.Preference> normalize(List<UserBehaviorProfile.Preference> preferences) {
        return preferences.stream().map(preference -> new UserBehaviorProfile.Preference(
            preference.value() == null ? null : preference.value().trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT),
            preference.score(),
            preference.evidenceCount()
        )).toList();
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=UserProfileValidationTest`
Expected: PASS (6 tests).

- [ ] **Step 5: Make `RedisUserProfileClient` delegate to it**

Replace the body of `getProfile` and delete the private `normalizePreferenceNames`/`normalize` methods and the `PROFILE_VERSION` constant from `RedisUserProfileClient`. The class becomes:

```java
package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.clients.UserProfileValidation.Invalid;
import com.demo.retrieval.service.clients.UserProfileValidation.Result;
import com.demo.retrieval.service.clients.UserProfileValidation.Valid;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/** Reads the profile belonging to the run selected by the Redis active-run pointer. */
@Component
public class RedisUserProfileClient implements UserProfileClient {
    private static final Logger log = LoggerFactory.getLogger(RedisUserProfileClient.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final String keyPrefix;

    public RedisUserProfileClient(
        StringRedisTemplate redis,
        ObjectMapper objectMapper,
        MeterRegistry meterRegistry,
        @Value("${recsys.user-profile.key-prefix:user-profile:v1}") String keyPrefix
    ) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public Optional<UserBehaviorProfile> getProfile(String userId) {
        Timer.Sample timer = Timer.start(meterRegistry);
        try {
            String runId = redis.opsForValue().get(keyPrefix + ":active-run");
            if (runId == null || runId.isBlank()) {
                return fallback("missing_active_run");
            }
            String rawProfile = redis.opsForValue().get(keyPrefix + ":" + runId + ":" + userId);
            Result result = UserProfileValidation.validate(rawProfile, userId, runId, objectMapper);
            if (result instanceof Valid valid) {
                return Optional.of(valid.profile());
            }
            String reason = ((Invalid) result).reason();
            if ("invalid_json".equals(reason)) {
                log.warn("Unable to parse user profile for user {}", userId);
            }
            return fallback(reason);
        } catch (RuntimeException e) {
            log.warn("User profile lookup failed for user {}", userId);
            return fallback("redis_error");
        } finally {
            timer.stop(Timer.builder("profile.lookup").register(meterRegistry));
        }
    }

    private Optional<UserBehaviorProfile> fallback(String reason) {
        Counter.builder("profile.lookup.fallback").tag("reason", reason).register(meterRegistry).increment();
        return Optional.empty();
    }
}
```

- [ ] **Step 6: Run the existing client test unchanged, plus the new one**

Run: `mvn -q test -Dtest='RedisUserProfileClientTest,UserProfileValidationTest,UserBehaviorProfileQueryHydratorTest'`
Expected: PASS. If `RedisUserProfileClientTest` fails, the extraction changed behavior; fix the client, not the test.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/clients/UserProfileValidation.java \
        src/main/java/com/demo/retrieval/service/clients/RedisUserProfileClient.java \
        src/test/java/com/demo/retrieval/service/clients/UserProfileValidationTest.java
git commit -m "refactor: extract profile validity checks into UserProfileValidation

Serving and the upcoming profile audit must agree on what a usable profile is.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: `CatalogPreferenceIndex`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/audit/CatalogPreferenceIndex.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/CatalogPreferenceIndexTest.java`

**Interfaces:**
- Consumes: `com.demo.retrieval.service.content.NormalizedProfile` (existing record: `genres()`, `tags()` are already-normalized `Set<String>`).
- Produces: `CatalogPreferenceIndex.build(Map<String, NormalizedProfile>) -> CatalogPreferenceIndex`; `List<String> lookup(String kind, String value)` (sorted ids, empty on miss); `int catalogSize()`; constants `KIND_GENRE = "genre"`, `KIND_TAG = "tag"`. Used by Tasks 5 and 6.

- [ ] **Step 1: Write the failing test**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.content.NormalizedProfile;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogPreferenceIndexTest {

    private static NormalizedProfile item(Set<String> genres, Set<String> tags) {
        return new NormalizedProfile("", genres, tags, tags, "", false, 0L);
    }

    private static CatalogPreferenceIndex index() {
        Map<String, NormalizedProfile> catalog = new LinkedHashMap<>();
        catalog.put("item9", item(Set.of("sci-fi", "adventure"), Set.of("space")));
        catalog.put("item1", item(Set.of("sci-fi"), Set.of("space", "classic")));
        catalog.put("item5", item(Set.of("drama"), Set.of()));
        return CatalogPreferenceIndex.build(catalog);
    }

    @Test
    void genreLookupReturnsMatchingItemsSortedById() {
        assertEquals(List.of("item1", "item9"), index().lookup(CatalogPreferenceIndex.KIND_GENRE, "sci-fi"));
        assertEquals(List.of("item5"), index().lookup(CatalogPreferenceIndex.KIND_GENRE, "drama"));
    }

    @Test
    void tagLookupIsSeparateFromGenres() {
        assertEquals(List.of("item1", "item9"), index().lookup(CatalogPreferenceIndex.KIND_TAG, "space"));
        assertEquals(List.of("item1"), index().lookup(CatalogPreferenceIndex.KIND_TAG, "classic"));
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_TAG, "sci-fi").isEmpty());
    }

    @Test
    void missingValueOrUnknownKindReturnsEmpty() {
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_GENRE, "film-noir").isEmpty());
        assertTrue(index().lookup("keyword", "space").isEmpty());
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_GENRE, null).isEmpty());
    }

    @Test
    void catalogSizeCountsItemsNotPostings() {
        assertEquals(3, index().catalogSize());
        assertEquals(0, CatalogPreferenceIndex.build(Map.of()).catalogSize());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=CatalogPreferenceIndexTest`
Expected: compilation FAILURE, `cannot find symbol: class CatalogPreferenceIndex`.

- [ ] **Step 3: Write the index**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.content.NormalizedProfile;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Inverted index over the normalized catalog: genre → item ids, tag → item ids. Built once per
 * audit call and shared across every user, so resolving a preference is one hash lookup instead
 * of a catalog scan per user per preference. Posting lists are sorted so output is deterministic.
 */
public final class CatalogPreferenceIndex {
    public static final String KIND_GENRE = "genre";
    public static final String KIND_TAG = "tag";

    private final Map<String, List<String>> byGenre;
    private final Map<String, List<String>> byTag;
    private final int catalogSize;

    private CatalogPreferenceIndex(Map<String, List<String>> byGenre, Map<String, List<String>> byTag, int catalogSize) {
        this.byGenre = byGenre;
        this.byTag = byTag;
        this.catalogSize = catalogSize;
    }

    public static CatalogPreferenceIndex build(Map<String, NormalizedProfile> normalizedCatalog) {
        Map<String, List<String>> byGenre = new HashMap<>();
        Map<String, List<String>> byTag = new HashMap<>();
        normalizedCatalog.forEach((itemId, profile) -> {
            profile.genres().forEach(genre -> byGenre.computeIfAbsent(genre, g -> new ArrayList<>()).add(itemId));
            profile.tags().forEach(tag -> byTag.computeIfAbsent(tag, t -> new ArrayList<>()).add(itemId));
        });
        byGenre.values().forEach(Collections::sort);
        byTag.values().forEach(Collections::sort);
        return new CatalogPreferenceIndex(byGenre, byTag, normalizedCatalog.size());
    }

    /** Sorted item ids carrying {@code value} as a {@code kind}; empty for a miss or an unknown kind. */
    public List<String> lookup(String kind, String value) {
        if (value == null) {
            return List.of();
        }
        Map<String, List<String>> postings = switch (kind == null ? "" : kind) {
            case KIND_GENRE -> byGenre;
            case KIND_TAG -> byTag;
            default -> Map.of();
        };
        return postings.getOrDefault(value, List.of());
    }

    public int catalogSize() {
        return catalogSize;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=CatalogPreferenceIndexTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/CatalogPreferenceIndex.java \
        src/test/java/com/demo/retrieval/service/audit/CatalogPreferenceIndexTest.java
git commit -m "feat(audit): inverted genre/tag index over the normalized catalog

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: `ProfileAuditStore` interface and `RedisProfileAuditStore`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/audit/ProfileAuditStore.java`
- Create: `src/main/java/com/demo/retrieval/service/audit/RedisProfileAuditStore.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/RedisProfileAuditStoreTest.java`

**Interfaces:**
- Consumes: `StringRedisTemplate` (`scan(ScanOptions)`, `opsForValue().get`, `executePipelined(SessionCallback)`, `getExpire`), property `recsys.user-profile.key-prefix`.
- Produces:
  ```java
  public interface ProfileAuditStore {
      record ScanResult(List<String> userIds, boolean truncated) {}
      record RawProfile(String userId, String json, long ttlSeconds) {}   // json null = key absent; ttl -2 absent, -1 no expiry
      ScanResult scanUserIds(String pattern, int limit);
      Optional<String> activeRun();
      List<RawProfile> readProfiles(String run, List<String> userIds);   // same order as userIds
  }
  ```
  Used by Task 5.

- [ ] **Step 1: Write the interface**

```java
package com.demo.retrieval.service.audit;

import java.util.List;
import java.util.Optional;

/**
 * The only seam between the profile audit and Redis. Mirrors the Spark side's RedisProfileStore:
 * everything above this interface is pure and tested with an in-memory fake.
 */
public interface ProfileAuditStore {

    /** {@code truncated} is true when {@code limit} stopped the scan before the cursor was exhausted. */
    record ScanResult(List<String> userIds, boolean truncated) {
    }

    /**
     * One user's profile blob as stored. {@code json} is null when the key is absent.
     * {@code ttlSeconds} follows Redis TTL semantics: -2 absent, -1 no expiry, else seconds left.
     */
    record RawProfile(String userId, String json, long ttlSeconds) {
    }

    /** Level 0: distinct user ids whose key matches {@code pattern}, at most {@code limit}. */
    ScanResult scanUserIds(String pattern, int limit);

    /** Level 1: the active-run pointer, empty when unset or blank. */
    Optional<String> activeRun();

    /** Level 1: profile blob + TTL for every user in one round trip, in the same order as {@code userIds}. */
    List<RawProfile> readProfiles(String run, List<String> userIds);
}
```

- [ ] **Step 2: Write the failing test**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.audit.ProfileAuditStore.ScanResult;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class RedisProfileAuditStoreTest {

    private static Cursor<String> cursorOf(String... keys) {
        Cursor<String> cursor = mock(Cursor.class);
        Iterator<String> it = List.of(keys).iterator();
        when(cursor.hasNext()).thenAnswer(inv -> it.hasNext());
        when(cursor.next()).thenAnswer(inv -> it.next());
        return cursor;
    }

    @Test
    void scanExtractsDistinctUserIdsAndStopsAtLimit() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.scan(any(ScanOptions.class))).thenReturn(
            cursorOf("user:u1:features", "user:u2:features", "user:u1:features", "user:u3:features"));
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        ScanResult result = store.scanUserIds("user:*:features", 2);

        assertEquals(List.of("u1", "u2"), result.userIds());
        assertTrue(result.truncated());
    }

    @Test
    void scanReportsNotTruncatedWhenCursorIsExhausted() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.scan(any(ScanOptions.class))).thenReturn(cursorOf("user:u1:features", "malformed"));
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        ScanResult result = store.scanUserIds("user:*:features", 10);

        assertEquals(List.of("u1"), result.userIds());
        assertFalse(result.truncated());
    }

    @Test
    void activeRunIsEmptyWhenUnsetOrBlank() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        when(values.get("user-profile:v1:active-run")).thenReturn(null);
        assertEquals(Optional.empty(), store.activeRun());
        when(values.get("user-profile:v1:active-run")).thenReturn("  ");
        assertEquals(Optional.empty(), store.activeRun());
        when(values.get("user-profile:v1:active-run")).thenReturn("run-7");
        assertEquals(Optional.of("run-7"), store.activeRun());
    }

    @Test
    void readProfilesZipsPipelinedGetAndTtlByPosition() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        List<Object> pipelineResults = new ArrayList<>();
        when(values.get("user-profile:v1:run-7:u1")).thenAnswer(inv -> { pipelineResults.add("{\"a\":1}"); return null; });
        when(redis.getExpire("user-profile:v1:run-7:u1")).thenAnswer(inv -> { pipelineResults.add(3400L); return null; });
        when(values.get("user-profile:v1:run-7:u2")).thenAnswer(inv -> { pipelineResults.add(null); return null; });
        when(redis.getExpire("user-profile:v1:run-7:u2")).thenAnswer(inv -> { pipelineResults.add(-2L); return null; });
        when(redis.executePipelined(any(SessionCallback.class))).thenAnswer(inv -> {
            SessionCallback callback = inv.getArgument(0);
            pipelineResults.clear();
            callback.execute(redis);
            return new ArrayList<>(pipelineResults);
        });
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        List<RawProfile> profiles = store.readProfiles("run-7", List.of("u1", "u2"));

        assertEquals(2, profiles.size());
        assertEquals(new RawProfile("u1", "{\"a\":1}", 3400L), profiles.get(0));
        assertEquals("u2", profiles.get(1).userId());
        assertNull(profiles.get(1).json());
        assertEquals(-2L, profiles.get(1).ttlSeconds());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q test -Dtest=RedisProfileAuditStoreTest`
Expected: compilation FAILURE, `cannot find symbol: class RedisProfileAuditStore`.

- [ ] **Step 4: Write the Redis store**

```java
package com.demo.retrieval.service.audit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Redis-backed store. SCAN is cursor-based so it never blocks Redis the way KEYS does; it may
 * return a key twice, hence the LinkedHashSet. Each readProfiles call is one pipelined round trip
 * (GET + TTL per user) on a connection dedicated to that call, which is what makes concurrent
 * chunks safe: no pipeline is ever shared between threads.
 */
@Component
public class RedisProfileAuditStore implements ProfileAuditStore {
    private static final long SCAN_COUNT_HINT = 1000L;

    private final StringRedisTemplate redis;
    private final String keyPrefix;

    public RedisProfileAuditStore(
        StringRedisTemplate redis,
        @Value("${recsys.user-profile.key-prefix:user-profile:v1}") String keyPrefix
    ) {
        this.redis = redis;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public ScanResult scanUserIds(String pattern, int limit) {
        Set<String> userIds = new LinkedHashSet<>();
        ScanOptions options = ScanOptions.scanOptions().match(pattern).count(SCAN_COUNT_HINT).build();
        try (Cursor<String> cursor = redis.scan(options)) {
            while (cursor.hasNext() && userIds.size() < limit) {
                String[] segments = cursor.next().split(":");
                if (segments.length >= 2 && !segments[1].isEmpty()) {
                    userIds.add(segments[1]);
                }
            }
            return new ScanResult(new ArrayList<>(userIds), cursor.hasNext());
        }
    }

    @Override
    public Optional<String> activeRun() {
        return Optional.ofNullable(redis.opsForValue().get(keyPrefix + ":active-run"))
            .filter(run -> !run.isBlank());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RawProfile> readProfiles(String run, List<String> userIds) {
        List<Object> results = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> ops) {
                RedisOperations<String, String> s = (RedisOperations<String, String>) ops;
                for (String userId : userIds) {
                    String key = keyPrefix + ":" + run + ":" + userId;
                    s.opsForValue().get(key);
                    s.getExpire(key);
                }
                return null;
            }
        });
        List<RawProfile> profiles = new ArrayList<>(userIds.size());
        for (int i = 0; i < userIds.size(); i++) {
            String json = (String) results.get(2 * i);
            Long ttl = (Long) results.get(2 * i + 1);
            profiles.add(new RawProfile(userIds.get(i), json, ttl == null ? -2L : ttl));
        }
        return profiles;
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=RedisProfileAuditStoreTest`
Expected: PASS (4 tests). If `redis.scan` cannot be stubbed because Mockito reports it as final, `StringRedisTemplate` is not final and `RedisTemplate.scan` is a public instance method, so re-check the import (`org.springframework.data.redis.core.ScanOptions`).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/ProfileAuditStore.java \
        src/main/java/com/demo/retrieval/service/audit/RedisProfileAuditStore.java \
        src/test/java/com/demo/retrieval/service/audit/RedisProfileAuditStoreTest.java
git commit -m "feat(audit): Redis store with SCAN user discovery and pipelined profile reads

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Report records and `UserAuditClassifier`

**Files:**
- Create: `src/main/java/com/demo/retrieval/service/audit/ProfileAuditReport.java`
- Create: `src/main/java/com/demo/retrieval/service/audit/UserAuditClassifier.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/UserAuditClassifierTest.java`

**Interfaces:**
- Consumes: `UserProfileValidation.validate` (Task 1), `CatalogPreferenceIndex` (Task 2), `ProfileAuditStore.RawProfile` (Task 3).
- Produces:
  - `ProfileAuditReport(String status, String activeRun, String generatedAt, long elapsedMs, boolean truncated, Summary summary, List<UserRow> users)` and nested `Summary`, `UserRow`, `Finding`, `PreferenceRef`, `PreferenceMatch` (fields listed in code).
  - `UserAuditClassifier.classify(RawProfile raw, String activeRun, CatalogPreferenceIndex index, ObjectMapper mapper, int sampleItems) -> ProfileAuditReport.UserRow`. A row with an empty `findings()` list is a healthy user. Used by Task 5.

- [ ] **Step 1: Write the report records**

```java
package com.demo.retrieval.service.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Response of GET /actuator/profile-audit. Null fields are omitted from JSON. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ProfileAuditReport(
    String status,
    @JsonProperty("active_run") String activeRun,
    @JsonProperty("generated_at") String generatedAt,
    @JsonProperty("elapsed_ms") long elapsedMs,
    boolean truncated,
    Summary summary,
    List<UserRow> users
) {
    public static final String STATUS_OK = "ok";
    public static final String STATUS_MISSING_ACTIVE_RUN = "missing_active_run";

    public static final String FINDING_NO_PROFILE = "no_profile";
    public static final String FINDING_NEW_OR_UNKNOWN = "new_or_unknown";
    public static final String FINDING_EMPTY_PREFERENCES = "empty_preferences";
    public static final String FINDING_PREFERENCE_WITHOUT_CONTENT = "preference_without_content";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Summary(
        @JsonProperty("users_scanned") int usersScanned,
        @JsonProperty("users_with_profile") int usersWithProfile,
        @JsonProperty("users_healthy") int usersHealthy,
        @JsonProperty("no_profile_by_reason") Map<String, Integer> noProfileByReason,
        @JsonProperty("findings_by_type") Map<String, Integer> findingsByType,
        @JsonProperty("unmatched_preferences") Map<String, Map<String, Integer>> unmatchedPreferences,
        @JsonProperty("min_profile_ttl_seconds") Long minProfileTtlSeconds,
        @JsonProperty("catalog_size") int catalogSize
    ) {
    }

    /** One user with at least one finding. {@code ttlSeconds} and {@code preferences} are present only for valid profiles. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record UserRow(
        @JsonProperty("user_id") String userId,
        @JsonProperty("has_profile") boolean hasProfile,
        @JsonProperty("ttl_seconds") Long ttlSeconds,
        List<Finding> findings,
        List<PreferenceMatch> preferences
    ) {
    }

    /** {@code reason} only for no_profile; {@code preferences} only for preference_without_content. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Finding(String type, String reason, List<PreferenceRef> preferences) {
    }

    public record PreferenceRef(String kind, String value) {
    }

    public record PreferenceMatch(
        String kind,
        String value,
        double score,
        @JsonProperty("evidence_count") long evidenceCount,
        @JsonProperty("matched_items") int matchedItems,
        @JsonProperty("sample_items") List<String> sampleItems
    ) {
    }
}
```

- [ ] **Step 2: Write the failing classifier test**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.content.NormalizedProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserAuditClassifierTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final CatalogPreferenceIndex INDEX = CatalogPreferenceIndex.build(Map.of(
        "item1", new NormalizedProfile("", Set.of("sci-fi"), Set.of("space"), Set.of("space"), "", false, 0L),
        "item2", new NormalizedProfile("", Set.of("sci-fi"), Set.of(), Set.of(), "", false, 0L),
        "item3", new NormalizedProfile("", Set.of("drama"), Set.of(), Set.of(), "", false, 0L)));

    /** Builds a profile JSON for {@code user}/{@code run} using the real record so the shape cannot drift. */
    static String profileJson(String user, String run, List<String> genres, List<String> tags, boolean newOrUnknown) {
        List<UserBehaviorProfile.Preference> g = genres.stream()
            .map(v -> new UserBehaviorProfile.Preference(v, 0.7, 8L)).toList();
        List<UserBehaviorProfile.Preference> t = tags.stream()
            .map(v -> new UserBehaviorProfile.Preference(v, 0.5, 3L)).toList();
        List<UserBehaviorProfile.Persona> personas = newOrUnknown
            ? List.of(new UserBehaviorProfile.Persona("new_or_unknown", "New or unknown", 1.0, Map.of()))
            : List.of();
        UserBehaviorProfile profile = new UserBehaviorProfile(
            user, 1, run, "2026-09-07T00:00:00Z",
            new UserBehaviorProfile.SourceWindow("2026-08-07T00:00:00Z", "2026-09-07T00:00:00Z"),
            12L, new UserBehaviorProfile.Preferences(g, t),
            new UserBehaviorProfile.BehavioralFeatures(0.5, 0.1, 0.4, 0.7, 0.6, null, "medium"),
            personas);
        try {
            return MAPPER.writeValueAsString(profile);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static UserRow classify(RawProfile raw) {
        return UserAuditClassifier.classify(raw, "run-7", INDEX, MAPPER, 1);
    }

    @Test
    void missingBlobIsNoProfileWithReason() {
        UserRow row = classify(new RawProfile("u1", null, -2L));

        assertFalse(row.hasProfile());
        assertNull(row.ttlSeconds());
        assertNull(row.preferences());
        assertEquals(List.of(new Finding("no_profile", "missing_profile", null)), row.findings());
    }

    @Test
    void wrongRunIsNoProfileWithRunMismatch() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-6", List.of("sci-fi"), List.of(), false), 100L));

        assertEquals("run_mismatch", row.findings().get(0).reason());
    }

    @Test
    void healthyUserHasNoFindingsButKeepsMatchesAndTtl() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("Sci-Fi"), List.of("space"), false), 3400L));

        assertTrue(row.hasProfile());
        assertEquals(3400L, row.ttlSeconds());
        assertTrue(row.findings().isEmpty());
        assertEquals(2, row.preferences().size());
        assertEquals("genre", row.preferences().get(0).kind());
        assertEquals("sci-fi", row.preferences().get(0).value());
        assertEquals(2, row.preferences().get(0).matchedItems());
        assertEquals(List.of("item1"), row.preferences().get(0).sampleItems());   // sampleItems = 1
        assertEquals("tag", row.preferences().get(1).kind());
        assertEquals(1, row.preferences().get(1).matchedItems());
    }

    @Test
    void lowEvidencePersonaIsAFinding() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of(), true), 100L));

        assertEquals(List.of(new Finding("new_or_unknown", null, null)), row.findings());
    }

    @Test
    void noPositivePreferenceIsEmptyPreferences() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of(), List.of(), false), 100L));

        assertEquals(List.of(new Finding("empty_preferences", null, null)), row.findings());
        assertTrue(row.preferences().isEmpty());
    }

    @Test
    void unmatchedPreferencesAreListedInOneFinding() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi", "film-noir"), List.of("robots"), false), 100L));

        assertEquals(1, row.findings().size());
        Finding finding = row.findings().get(0);
        assertEquals("preference_without_content", finding.type());
        assertEquals(List.of(new PreferenceRef("genre", "film-noir"), new PreferenceRef("tag", "robots")), finding.preferences());
        assertEquals(0, row.preferences().get(1).matchedItems());
    }

    @Test
    void noExpiryTtlIsReportedAsNull() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of(), false), -1L));

        assertNull(row.ttlSeconds());
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q test -Dtest=UserAuditClassifierTest`
Expected: compilation FAILURE, `cannot find symbol: class UserAuditClassifier`.

- [ ] **Step 4: Write the classifier**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceMatch;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.clients.UserProfileValidation;
import com.demo.retrieval.service.clients.UserProfileValidation.Invalid;
import com.demo.retrieval.service.clients.UserProfileValidation.Result;
import com.demo.retrieval.service.clients.UserProfileValidation.Valid;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Levels 1–3 for one user: validate the blob exactly as serving does, take the positive genre/tag
 * preferences, resolve each against the catalog index. Pure; a row with no findings is healthy.
 */
final class UserAuditClassifier {
    private static final String PERSONA_NEW_OR_UNKNOWN = "new_or_unknown";

    private UserAuditClassifier() {
    }

    static UserRow classify(RawProfile raw, String activeRun, CatalogPreferenceIndex index, ObjectMapper mapper, int sampleItems) {
        Result result = UserProfileValidation.validate(raw.json(), raw.userId(), activeRun, mapper);
        if (result instanceof Invalid invalid) {
            return new UserRow(raw.userId(), false, null,
                List.of(new Finding(ProfileAuditReport.FINDING_NO_PROFILE, invalid.reason(), null)), null);
        }
        UserBehaviorProfile profile = ((Valid) result).profile();
        List<Finding> findings = new ArrayList<>();

        if (profile.personas().stream().anyMatch(p -> PERSONA_NEW_OR_UNKNOWN.equals(p.type()))) {
            findings.add(new Finding(ProfileAuditReport.FINDING_NEW_OR_UNKNOWN, null, null));
        }

        List<PreferenceMatch> matches = new ArrayList<>();
        List<PreferenceRef> unmatched = new ArrayList<>();
        UserBehaviorProfile.Preferences preferences = profile.preferences();
        if (preferences != null) {
            resolve(CatalogPreferenceIndex.KIND_GENRE, preferences.genres(), index, sampleItems, matches, unmatched);
            resolve(CatalogPreferenceIndex.KIND_TAG, preferences.tags(), index, sampleItems, matches, unmatched);
        }
        if (matches.isEmpty()) {
            findings.add(new Finding(ProfileAuditReport.FINDING_EMPTY_PREFERENCES, null, null));
        } else if (!unmatched.isEmpty()) {
            findings.add(new Finding(ProfileAuditReport.FINDING_PREFERENCE_WITHOUT_CONTENT, null, List.copyOf(unmatched)));
        }

        Long ttl = raw.ttlSeconds() >= 0 ? raw.ttlSeconds() : null;
        return new UserRow(raw.userId(), true, ttl, List.copyOf(findings), List.copyOf(matches));
    }

    private static void resolve(
        String kind,
        List<UserBehaviorProfile.Preference> preferences,
        CatalogPreferenceIndex index,
        int sampleItems,
        List<PreferenceMatch> matches,
        List<PreferenceRef> unmatched
    ) {
        for (UserBehaviorProfile.Preference preference : preferences) {
            if (preference.value() == null || preference.score() <= 0.0) {
                continue;
            }
            List<String> items = index.lookup(kind, preference.value());
            matches.add(new PreferenceMatch(kind, preference.value(), preference.score(), preference.evidenceCount(),
                items.size(), items.subList(0, Math.min(sampleItems, items.size()))));
            if (items.isEmpty()) {
                unmatched.add(new PreferenceRef(kind, preference.value()));
            }
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=UserAuditClassifierTest`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/demo/retrieval/service/audit/ProfileAuditReport.java \
        src/main/java/com/demo/retrieval/service/audit/UserAuditClassifier.java \
        src/test/java/com/demo/retrieval/service/audit/UserAuditClassifierTest.java
git commit -m "feat(audit): report records and per-user classification

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: `ProfileAuditService` with config, executor, single-flight guard, aggregation

**Files:**
- Modify: `src/main/java/com/demo/retrieval/config/RecommendationProperties.java` (add field after `grpo`, getter/setter, nested class after `Sequence`)
- Modify: `src/main/resources/application.yml` (add block after the `grpo:` block)
- Create: `src/main/java/com/demo/retrieval/service/audit/ProfileAuditService.java`
- Test: `src/test/java/com/demo/retrieval/service/audit/ProfileAuditServiceTest.java`

**Interfaces:**
- Consumes: `ProfileAuditStore` (Task 3), `UserAuditClassifier` (Task 4), `CatalogPreferenceIndex` (Task 2), `CatalogContentScoring` (existing; construct with `new CatalogContentScoring(properties)`, its `normalizedCatalog()` is cached on catalog identity).
- Produces:
  - `RecommendationProperties.getProfileAudit()` → `ProfileAudit { getUserKeyPattern(), getMaxUsers(), getChunkSize(), getParallelism(), getSampleItems() }`.
  - `ProfileAuditService.audit(Integer limit) -> ProfileAuditReport` (null limit = `max-users`); throws `IllegalArgumentException` for a limit outside `1..max-users`, `ProfileAuditService.AuditBusyException` when another audit is running, `ProfileAuditService.ProfileAuditFailedException` (message = cause message) when the store fails. Used by Task 6.

- [ ] **Step 1: Add the config class and YAML block**

In `RecommendationProperties`, add the field next to the others:

```java
    private ProfileAudit profileAudit = new ProfileAudit();
```

the accessor pair next to `getGrpo`/`setGrpo`:

```java
    public ProfileAudit getProfileAudit() {
        return profileAudit;
    }

    public void setProfileAudit(ProfileAudit profileAudit) {
        this.profileAudit = profileAudit;
    }
```

and the nested class right after `public static class Sequence { ... }`:

```java
    /** GET /actuator/profile-audit: bounds on how much work one call may do. */
    public static class ProfileAudit {
        /** SCAN pattern for level 0; the user id is the second ':'-separated segment. */
        private String userKeyPattern = "user:*:features";
        private int maxUsers = 10000;
        private int chunkSize = 500;
        /** Must stay well below spring.data.redis.lettuce.pool.max-active (32) so an audit cannot starve serving. */
        private int parallelism = 4;
        private int sampleItems = 5;

        public String getUserKeyPattern() {
            return userKeyPattern;
        }

        public void setUserKeyPattern(String userKeyPattern) {
            this.userKeyPattern = userKeyPattern;
        }

        public int getMaxUsers() {
            return maxUsers;
        }

        public void setMaxUsers(int maxUsers) {
            this.maxUsers = maxUsers;
        }

        public int getChunkSize() {
            return chunkSize;
        }

        public void setChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
        }

        public int getParallelism() {
            return parallelism;
        }

        public void setParallelism(int parallelism) {
            this.parallelism = parallelism;
        }

        public int getSampleItems() {
            return sampleItems;
        }

        public void setSampleItems(int sampleItems) {
            this.sampleItems = sampleItems;
        }
    }
```

In `application.yml`, directly after the `grpo:` block (before `embeddings:`), matching the file's two-space indentation under `recsys:`:

```yaml
  # GET /actuator/profile-audit — operator tool; one audit at a time, bounded by max-users.
  profile-audit:
    user-key-pattern: ${RECSYS_PROFILE_AUDIT_USER_KEY_PATTERN:user:*:features}
    max-users: ${RECSYS_PROFILE_AUDIT_MAX_USERS:10000}
    chunk-size: ${RECSYS_PROFILE_AUDIT_CHUNK_SIZE:500}
    # keep well below spring.data.redis.lettuce.pool.max-active (32)
    parallelism: ${RECSYS_PROFILE_AUDIT_PARALLELISM:4}
    sample-items: ${RECSYS_PROFILE_AUDIT_SAMPLE_ITEMS:5}
```

Run: `mvn -q compile` — Expected: BUILD SUCCESS (no output with `-q`).

- [ ] **Step 2: Write the failing service test**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.demo.retrieval.service.audit.UserAuditClassifierTest.profileJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileAuditServiceTest {

    /** In-memory store: users in insertion order, per-user (json, ttl), optional failure and blocking. */
    static final class FakeStore implements ProfileAuditStore {
        final List<String> users = new ArrayList<>();
        final Map<String, RawProfile> profiles = new LinkedHashMap<>();
        Optional<String> activeRun = Optional.of("run-7");
        boolean truncated = false;
        String failOnUser = null;
        CountDownLatch blockReads = null;
        final List<List<String>> chunksSeen = new CopyOnWriteArrayList<>();

        FakeStore user(String id, String json, long ttl) {
            users.add(id);
            profiles.put(id, new RawProfile(id, json, ttl));
            return this;
        }

        @Override
        public ScanResult scanUserIds(String pattern, int limit) {
            return new ScanResult(users.subList(0, Math.min(limit, users.size())), truncated);
        }

        @Override
        public Optional<String> activeRun() {
            return activeRun;
        }

        @Override
        public List<RawProfile> readProfiles(String run, List<String> userIds) {
            chunksSeen.add(userIds);
            if (blockReads != null) {
                try { blockReads.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if (failOnUser != null && userIds.contains(failOnUser)) {
                throw new IllegalStateException("redis unavailable");
            }
            return userIds.stream().map(profiles::get).toList();
        }
    }

    private static RecommendationProperties properties(int chunkSize, int parallelism) {
        RecommendationProperties properties = new RecommendationProperties();
        MovieProfile item1 = new MovieProfile();
        item1.setGenres(List.of("Sci-Fi"));
        item1.setTags(List.of("space"));
        MovieProfile item2 = new MovieProfile();
        item2.setGenres(List.of("drama"));
        properties.getCatalog().put("item1", item1);
        properties.getCatalog().put("item2", item2);
        properties.getProfileAudit().setChunkSize(chunkSize);
        properties.getProfileAudit().setParallelism(parallelism);
        properties.getProfileAudit().setMaxUsers(100);
        return properties;
    }

    private ProfileAuditService service;

    private ProfileAuditService service(FakeStore store, int chunkSize, int parallelism) {
        service = new ProfileAuditService(store, properties(chunkSize, parallelism), new ObjectMapper());
        return service;
    }

    @AfterEach
    void shutdown() {
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    void listsOnlyUsersWithFindingsAndCountsTheRest() {
        FakeStore store = new FakeStore()
            .user("healthy", profileJson("healthy", "run-7", List.of("sci-fi"), List.of("space"), false), 3400L)
            .user("missing", null, -2L)
            .user("stale", profileJson("stale", "run-6", List.of("sci-fi"), List.of(), false), 100L)
            .user("newbie", profileJson("newbie", "run-7", List.of("sci-fi"), List.of(), true), 900L)
            .user("empty", profileJson("empty", "run-7", List.of(), List.of(), false), 2000L)
            .user("gap", profileJson("gap", "run-7", List.of("film-noir"), List.of("space"), false), 1200L);

        ProfileAuditReport report = service(store, 500, 1).audit(null);

        assertEquals("ok", report.status());
        assertEquals("run-7", report.activeRun());
        assertFalse(report.truncated());
        assertEquals(6, report.summary().usersScanned());
        assertEquals(4, report.summary().usersWithProfile());
        assertEquals(1, report.summary().usersHealthy());
        assertEquals(Map.of("missing_profile", 1, "run_mismatch", 1), report.summary().noProfileByReason());
        assertEquals(Map.of("new_or_unknown", 1, "empty_preferences", 1, "preference_without_content", 1),
            report.summary().findingsByType());
        assertEquals(Map.of("genre", Map.of("film-noir", 1)), report.summary().unmatchedPreferences());
        assertEquals(900L, report.summary().minProfileTtlSeconds());
        assertEquals(2, report.summary().catalogSize());
        assertEquals(List.of("missing", "stale", "newbie", "empty", "gap"),
            report.users().stream().map(UserRow::userId).toList());
    }

    @Test
    void missingActiveRunShortCircuitsWithoutPerUserRows() {
        FakeStore store = new FakeStore().user("u1", null, -2L).user("u2", null, -2L);
        store.activeRun = Optional.empty();

        ProfileAuditReport report = service(store, 500, 1).audit(null);

        assertEquals("missing_active_run", report.status());
        assertNull(report.activeRun());
        assertEquals(2, report.summary().usersScanned());
        assertEquals(0, report.summary().usersWithProfile());
        assertEquals(Map.of("missing_active_run", 2), report.summary().noProfileByReason());
        assertTrue(report.users().isEmpty());
        assertTrue(store.chunksSeen.isEmpty());
    }

    @Test
    void chunksFanOutInParallelAndRowsKeepScanOrder() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 7; i++) {
            store.user("u" + i, null, -2L);
        }

        ProfileAuditReport report = service(store, 2, 3).audit(null);

        assertEquals(4, store.chunksSeen.size());
        assertEquals(List.of("u0", "u1", "u2", "u3", "u4", "u5", "u6"),
            report.users().stream().map(UserRow::userId).toList());
    }

    @Test
    void truncationAndLimitArePassedThrough() {
        FakeStore store = new FakeStore().user("u1", null, -2L).user("u2", null, -2L).user("u3", null, -2L);
        store.truncated = true;

        ProfileAuditReport report = service(store, 500, 1).audit(2);

        assertTrue(report.truncated());
        assertEquals(2, report.summary().usersScanned());
    }

    @Test
    void limitOutsideBoundsIsRejected() {
        ProfileAuditService s = service(new FakeStore(), 500, 1);

        assertThrows(IllegalArgumentException.class, () -> s.audit(0));
        assertThrows(IllegalArgumentException.class, () -> s.audit(101));
    }

    @Test
    void oneFailingChunkFailsTheWholeAudit() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 4; i++) {
            store.user("u" + i, null, -2L);
        }
        store.failOnUser = "u2";

        ProfileAuditService.ProfileAuditFailedException e = assertThrows(
            ProfileAuditService.ProfileAuditFailedException.class, () -> service(store, 2, 2).audit(null));

        assertEquals("redis unavailable", e.getMessage());
        assertEquals(2, store.chunksSeen.size());
    }

    @Test
    void secondConcurrentAuditIsRejectedAsBusy() throws Exception {
        FakeStore store = new FakeStore().user("u1", null, -2L);
        store.blockReads = new CountDownLatch(1);
        ProfileAuditService s = service(store, 500, 1);
        ExecutorService runner = Executors.newSingleThreadExecutor();
        try {
            Future<ProfileAuditReport> first = runner.submit(() -> s.audit(null));
            while (store.chunksSeen.isEmpty()) {
                Thread.sleep(5);
            }

            assertThrows(ProfileAuditService.AuditBusyException.class, () -> s.audit(null));

            store.blockReads.countDown();
            assertEquals(1, first.get().summary().usersScanned());
            assertEquals(1, s.audit(null).summary().usersScanned());   // guard released
        } finally {
            runner.shutdownNow();
        }
    }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `mvn -q test -Dtest=ProfileAuditServiceTest`
Expected: compilation FAILURE, `cannot find symbol: class ProfileAuditService`.

- [ ] **Step 4: Write the service**

```java
package com.demo.retrieval.service.audit;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.demo.retrieval.service.audit.ProfileAuditReport.Summary;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.audit.ProfileAuditStore.ScanResult;
import com.demo.retrieval.service.content.CatalogContentScoring;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Walks user → has_profile → preferences → catalog content, breadth-first by level, and returns
 * a findings-only report. One audit at a time; level-1 chunks fan out over a small dedicated
 * executor where each task owns exactly one pipelined round trip.
 */
@Component
public class ProfileAuditService {

    public static final class AuditBusyException extends RuntimeException {
        public AuditBusyException() {
            super("a profile audit is already running");
        }
    }

    public static final class ProfileAuditFailedException extends RuntimeException {
        public ProfileAuditFailedException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    private final ProfileAuditStore store;
    private final RecommendationProperties.ProfileAudit config;
    private final CatalogContentScoring catalogScoring;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ProfileAuditService(ProfileAuditStore store, RecommendationProperties properties, ObjectMapper objectMapper) {
        this.store = store;
        this.config = properties.getProfileAudit();
        this.catalogScoring = new CatalogContentScoring(properties);
        this.objectMapper = objectMapper;
        this.executor = Executors.newFixedThreadPool(config.getParallelism());
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }

    /** {@code limit} null means {@code max-users}; otherwise it must lie in 1..max-users. */
    public ProfileAuditReport audit(Integer limit) {
        int maxUsers = config.getMaxUsers();
        int effectiveLimit = limit == null ? maxUsers : limit;
        if (effectiveLimit < 1 || effectiveLimit > maxUsers) {
            throw new IllegalArgumentException("limit must be between 1 and " + maxUsers);
        }
        if (!running.compareAndSet(false, true)) {
            throw new AuditBusyException();
        }
        long started = System.nanoTime();
        try {
            CatalogPreferenceIndex index = CatalogPreferenceIndex.build(catalogScoring.normalizedCatalog());
            ScanResult scan = store.scanUserIds(config.getUserKeyPattern(), effectiveLimit);
            String activeRun = store.activeRun().orElse(null);
            Aggregation aggregation = new Aggregation();

            if (activeRun == null) {
                aggregation.noProfileByReason.put(ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN, scan.userIds().size());
            } else {
                for (RawProfile raw : readAllProfiles(activeRun, scan.userIds())) {
                    aggregation.add(UserAuditClassifier.classify(raw, activeRun, index, objectMapper, config.getSampleItems()));
                }
            }

            return new ProfileAuditReport(
                activeRun == null ? ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN : ProfileAuditReport.STATUS_OK,
                activeRun,
                Instant.now().toString(),
                (System.nanoTime() - started) / 1_000_000L,
                scan.truncated(),
                aggregation.summary(scan.userIds().size(), index.catalogSize()),
                aggregation.rows
            );
        } catch (RuntimeException e) {
            if (e instanceof AuditBusyException || e instanceof ProfileAuditFailedException) {
                throw e;
            }
            throw new ProfileAuditFailedException(e);
        } finally {
            running.set(false);
        }
    }

    /** Fans chunks over the executor, waits for all of them, then rethrows the first failure in chunk order. */
    private List<RawProfile> readAllProfiles(String activeRun, List<String> userIds) {
        List<CompletableFuture<List<RawProfile>>> futures = new ArrayList<>();
        for (int from = 0; from < userIds.size(); from += config.getChunkSize()) {
            List<String> chunk = userIds.subList(from, Math.min(from + config.getChunkSize(), userIds.size()));
            futures.add(CompletableFuture.supplyAsync(() -> store.readProfiles(activeRun, chunk), executor));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).exceptionally(t -> null).join();
        List<RawProfile> all = new ArrayList<>(userIds.size());
        for (CompletableFuture<List<RawProfile>> future : futures) {
            try {
                all.addAll(future.join());
            } catch (CompletionException e) {
                throw new ProfileAuditFailedException(e.getCause() == null ? e : e.getCause());
            }
        }
        return all;
    }

    private static final class Aggregation {
        final List<UserRow> rows = new ArrayList<>();
        final Map<String, Integer> noProfileByReason = new TreeMap<>();
        final Map<String, Integer> findingsByType = new TreeMap<>();
        final Map<String, Map<String, Integer>> unmatchedPreferences = new TreeMap<>();
        int usersWithProfile;
        int usersHealthy;
        Long minTtl;

        void add(UserRow row) {
            if (row.hasProfile()) {
                usersWithProfile++;
                if (row.ttlSeconds() != null && (minTtl == null || row.ttlSeconds() < minTtl)) {
                    minTtl = row.ttlSeconds();
                }
            }
            if (row.findings().isEmpty()) {
                usersHealthy++;
                return;
            }
            rows.add(row);
            for (Finding finding : row.findings()) {
                if (ProfileAuditReport.FINDING_NO_PROFILE.equals(finding.type())) {
                    noProfileByReason.merge(finding.reason(), 1, Integer::sum);
                    continue;
                }
                findingsByType.merge(finding.type(), 1, Integer::sum);
                if (finding.preferences() != null) {
                    for (PreferenceRef ref : finding.preferences()) {
                        unmatchedPreferences.computeIfAbsent(ref.kind(), k -> new TreeMap<>()).merge(ref.value(), 1, Integer::sum);
                    }
                }
            }
        }

        Summary summary(int usersScanned, int catalogSize) {
            return new Summary(usersScanned, usersWithProfile, usersHealthy,
                noProfileByReason, findingsByType, unmatchedPreferences, minTtl, catalogSize);
        }
    }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=ProfileAuditServiceTest`
Expected: PASS (7 tests). If `secondConcurrentAuditIsRejectedAsBusy` hangs, the `blockReads` latch was not counted down on the assertion path; check the `finally`.

- [ ] **Step 6: Verify the application context still starts with the new bean**

Run: `mvn -q test -Dtest='HybridRecommendationServiceTest,RecommendationControllerTest'`
Expected: PASS. (`ProfileAuditService` and `RedisProfileAuditStore` are plain `@Component`s; nothing else injects them yet.)

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/demo/retrieval/config/RecommendationProperties.java \
        src/main/resources/application.yml \
        src/main/java/com/demo/retrieval/service/audit/ProfileAuditService.java \
        src/test/java/com/demo/retrieval/service/audit/ProfileAuditServiceTest.java
git commit -m "feat(audit): ProfileAuditService with bounded fan-out, single-flight guard, and summary

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: `ProfileAuditController` and documentation

**Files:**
- Create: `src/main/java/com/demo/retrieval/controller/ProfileAuditController.java`
- Test: `src/test/java/com/demo/retrieval/controller/ProfileAuditControllerTest.java`
- Modify: `../../docs/recommendation_architecture/API.md` (insert a new section before the line `## \`GET /predict/{user}/{item}\``, currently line 122)
- Modify: `../../README.md` (append one sentence to the paragraph ending at line 228, "…for the response and 404 contract.")

**Interfaces:**
- Consumes: `ProfileAuditService.audit(Integer)` and its two exceptions (Task 5), `ProfileAuditReport` (Task 4).
- Produces: `GET /actuator/profile-audit?limit=N` → 200 report | 400 `{"error": ...}` | 409 `{"status":"busy"}` | 503 `{"status":"error","message": ...}`.

- [ ] **Step 1: Write the failing controller test**

```java
package com.demo.retrieval.controller;

import com.demo.retrieval.service.audit.ProfileAuditReport;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.Summary;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ProfileAuditController.class)
class ProfileAuditControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    ProfileAuditService auditService;

    private static ProfileAuditReport report() {
        return new ProfileAuditReport("ok", "run-7", "2026-09-07T10:00:00Z", 812L, false,
            new Summary(3, 2, 1, Map.of("missing_profile", 1), Map.of("new_or_unknown", 1), Map.of(), 3400L, 12),
            List.of(
                new UserRow("u1", false, null, List.of(new Finding("no_profile", "missing_profile", null)), null),
                new UserRow("u2", true, 3400L, List.of(new Finding("new_or_unknown", null, null)), List.of())));
    }

    @Test
    void returnsTheReportWithSnakeCaseFieldsAndNoNulls() throws Exception {
        when(auditService.audit(isNull())).thenReturn(report());

        mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"))
            .andExpect(jsonPath("$.active_run").value("run-7"))
            .andExpect(jsonPath("$.summary.users_scanned").value(3))
            .andExpect(jsonPath("$.summary.no_profile_by_reason.missing_profile").value(1))
            .andExpect(jsonPath("$.summary.min_profile_ttl_seconds").value(3400))
            .andExpect(jsonPath("$.users[0].user_id").value("u1"))
            .andExpect(jsonPath("$.users[0].has_profile").value(false))
            .andExpect(jsonPath("$.users[0].ttl_seconds").doesNotExist())
            .andExpect(jsonPath("$.users[0].findings[0].reason").value("missing_profile"))
            .andExpect(jsonPath("$.users[1].findings[0].reason").doesNotExist())
            .andExpect(jsonPath("$.users[1].ttl_seconds").value(3400));
    }

    @Test
    void passesLimitThrough() throws Exception {
        when(auditService.audit(25)).thenReturn(report());

        mvc.perform(get("/actuator/profile-audit").param("limit", "25"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));
    }

    @Test
    void badLimitIs400() throws Exception {
        when(auditService.audit(any())).thenThrow(new IllegalArgumentException("limit must be between 1 and 10000"));

        mvc.perform(get("/actuator/profile-audit").param("limit", "0"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value("limit must be between 1 and 10000"));
    }

    @Test
    void busyIs409() throws Exception {
        when(auditService.audit(any())).thenThrow(new ProfileAuditService.AuditBusyException());

        mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.status").value("busy"));
    }

    @Test
    void storeFailureIs503() throws Exception {
        when(auditService.audit(any())).thenThrow(
            new ProfileAuditService.ProfileAuditFailedException(new IllegalStateException("redis unavailable")));

        mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isServiceUnavailable())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.message").value("redis unavailable"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=ProfileAuditControllerTest`
Expected: compilation FAILURE, `cannot find symbol: class ProfileAuditController`.

- [ ] **Step 3: Write the controller**

```java
package com.demo.retrieval.controller;

import com.demo.retrieval.service.audit.ProfileAuditService;
import com.demo.retrieval.service.audit.ProfileAuditService.AuditBusyException;
import com.demo.retrieval.service.audit.ProfileAuditService.ProfileAuditFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Operator tool, like /actuator/model-reload: walks user → has_profile → preferences → catalog
 * content and returns a findings-only report. Records no serving measurement.
 */
@RestController
public class ProfileAuditController {
    private static final Logger log = LoggerFactory.getLogger(ProfileAuditController.class);

    private final ProfileAuditService auditService;

    public ProfileAuditController(ProfileAuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping("/actuator/profile-audit")
    public ResponseEntity<?> audit(@RequestParam(required = false) Integer limit) {
        try {
            return ResponseEntity.ok(auditService.audit(limit));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (AuditBusyException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("status", "busy"));
        } catch (ProfileAuditFailedException e) {
            log.error("Profile audit failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=ProfileAuditControllerTest`
Expected: PASS (5 tests).

- [ ] **Step 5: Document the endpoint**

In `../../docs/recommendation_architecture/API.md`, insert this section immediately before `## \`GET /predict/{user}/{item}\``:

````markdown
## `GET /actuator/profile-audit`

Operator tool. Walks every user → has_profile → profile preferences → catalog content and returns
a findings-only report: users with no usable profile, profiles with no usable preference, and
preferences that match no catalog item. Healthy users are counted, not listed. One audit runs at a
time; the call is synchronous and bounded by `limit` (default and maximum
`RECSYS_PROFILE_AUDIT_MAX_USERS`, 10000).

```bash
curl -s 'http://localhost:8080/actuator/profile-audit?limit=1000' | jq .summary
```

Users are discovered with a cursor SCAN over `RECSYS_PROFILE_AUDIT_USER_KEY_PATTERN` (default
`user:*:features`). A profile counts as present only if it passes the same checks
`/users/{user}/profile` applies; the reason otherwise is one of `missing_profile`,
`invalid_json`, `unsupported_version`, `user_mismatch`, `run_mismatch`. If the active-run pointer
is unset, `status` is `missing_active_run`, every user is counted under that reason, and no
per-user rows are returned. Content is the service catalog (`RECSYS_CATALOG_PATH` merged over the
inline catalog); `summary.catalog_size` says how much content the audit could match against.

```json
{
  "status": "ok",
  "active_run": "run-7",
  "generated_at": "2026-09-07T10:00:00Z",
  "elapsed_ms": 812,
  "truncated": false,
  "summary": {
    "users_scanned": 6040,
    "users_with_profile": 5900,
    "users_healthy": 5547,
    "no_profile_by_reason": {"missing_profile": 130, "invalid_json": 10},
    "findings_by_type": {"new_or_unknown": 300, "empty_preferences": 12, "preference_without_content": 41},
    "unmatched_preferences": {"genre": {"film-noir": 12}, "tag": {"space": 3}},
    "min_profile_ttl_seconds": 3400,
    "catalog_size": 12
  },
  "users": [
    {"user_id": "u1", "has_profile": false, "findings": [{"type": "no_profile", "reason": "missing_profile"}]},
    {"user_id": "u2", "has_profile": true, "ttl_seconds": 3400,
     "findings": [{"type": "preference_without_content", "preferences": [{"kind": "genre", "value": "film-noir"}]}],
     "preferences": [{"kind": "genre", "value": "sci-fi", "score": 0.72, "evidence_count": 8,
                      "matched_items": 8, "sample_items": ["item1", "item4"]}]}
  ]
}
```

Finding types: `no_profile` (with `reason`), `new_or_unknown` (the profile job's low-evidence
persona), `empty_preferences` (no genre or tag with a positive score), `preference_without_content`
(lists each unmatched `{kind, value}`). `unmatched_preferences` counts users per unmatched value so
a catalog gap appears once with its blast radius. `min_profile_ttl_seconds` is the smallest TTL
among valid profiles; profiles expire after one day while the active-run pointer does not, so a
small value warns that the pointer is about to outlive its blobs. `truncated` is true when `limit`
stopped the scan early.

Status codes: 200 report; 400 `limit` outside `1..max-users`; 409 `{"status":"busy"}` while another
audit runs; 503 `{"status":"error","message":...}` if Redis fails mid-walk (no partial report is
returned). Tunables: `RECSYS_PROFILE_AUDIT_CHUNK_SIZE` (500 users per pipelined round trip),
`RECSYS_PROFILE_AUDIT_PARALLELISM` (4; keep well below the Lettuce pool's `max-active` of 32),
`RECSYS_PROFILE_AUDIT_SAMPLE_ITEMS` (5 ids per matched preference).

````

In `../../README.md`, after the sentence ending "for the response and 404 contract." (line 228), add:

```markdown
To check every user at once — who lacks a valid profile, and which preferences match no catalog
item — call `GET /actuator/profile-audit`; see
[API.md](docs/recommendation_architecture/API.md#get-actuatorprofile-audit).
```

- [ ] **Step 6: Run the full Java suite**

Run: `mvn -q test`
Expected: BUILD SUCCESS. `UserProfileIntegrationTest` is skipped (Docker 29 / Testcontainers, accepted). Every other test passes, including the five new test classes and the unchanged `RedisUserProfileClientTest`.

- [ ] **Step 7: Commit**

```bash
git add src/main/java/com/demo/retrieval/controller/ProfileAuditController.java \
        src/test/java/com/demo/retrieval/controller/ProfileAuditControllerTest.java \
        ../../docs/recommendation_architecture/API.md ../../README.md
git commit -m "feat(audit): GET /actuator/profile-audit endpoint and docs

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: Smoke against a live service and open the PR

**Files:** none created. This task verifies the endpoint end to end and hands off.

- [ ] **Step 1: Start Redis and the service**

From `recsys-pipeline/` (two levels up):

```bash
docker compose up -d redis
cd services/java-retrieval-service && mvn -q spring-boot:run &
```

Wait until `curl -s localhost:8080/predict/metadata` returns JSON.

- [ ] **Step 2: Seed a tiny fixture and call the endpoint**

```bash
redis-cli SET user-profile:v1:active-run run-7
redis-cli HSET user:u1:features age 30
redis-cli HSET user:u2:features age 40
redis-cli HSET user:u3:features age 50
redis-cli SET user-profile:v1:run-7:u1 "$(sed 's/fixture-user/u1/; s/fixture-run/run-7/' ../../integration-tests/fixtures/user_profile_v1.json)" EX 3600
redis-cli SET user-profile:v1:run-7:u2 'not-json' EX 3600
curl -s 'localhost:8080/actuator/profile-audit' | jq .
```

Expected, with the inline demo catalog (`item1` has genre `sci-fi` and tag `space`):
- `status` = `ok`, `summary.users_scanned` = 3, `users_with_profile` = 1
- `no_profile_by_reason` = `{"invalid_json": 1, "missing_profile": 1}`
- `u1` listed with a `new_or_unknown` finding (the fixture's persona), `matched_items` ≥ 1 for both `sci-fi` and `space`
- `min_profile_ttl_seconds` ≤ 3600

Then: `curl -s 'localhost:8080/actuator/profile-audit?limit=0'` → 400.
Then: `redis-cli DEL user-profile:v1:active-run; curl -s localhost:8080/actuator/profile-audit | jq .status` → `"missing_active_run"`.

- [ ] **Step 3: Stop the service, clean up the seed keys**

```bash
kill %1
redis-cli DEL user:u1:features user:u2:features user:u3:features user-profile:v1:run-7:u1 user-profile:v1:run-7:u2
```

- [ ] **Step 4: Push and open the PR (do not merge)**

```bash
git push -u origin feat/profile-content-audit
gh pr create --title "feat: profile → content audit endpoint" --body "$(cat <<'EOF'
## Summary
- `GET /actuator/profile-audit` walks user → has_profile → profile preferences → catalog content and returns a findings-only report (spec: `.superpowers/docs/specs/2026-09-07-profile-content-audit-design.md`).
- Profile validity checks extracted from `RedisUserProfileClient` into pure `UserProfileValidation` so serving and the audit cannot disagree; the client's tests are unchanged.
- Users via cursor SCAN, one pipelined GET+TTL round trip per 500-user chunk fanned over 4 threads, one audit at a time, whole-call failure on any Redis error.

## Test plan
- [ ] `mvn -q test` green (UserProfileIntegrationTest skips as usual)
- [ ] Smoke per plan Task 7 against a local Redis: ok / missing_active_run / 400 paths

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

The user merges the PR.

---

## Self-review notes

- **Spec coverage:** Component 1 → Task 1; Component 2 → Task 3; Component 3 → Task 2; Component 4 (guard, executor, classification, short-circuit, aggregation) → Tasks 4–5; Component 5 + docs → Task 6; report shape → Task 4 records + Task 6 test; configuration → Task 5 Step 1; testing table → one test class per row; smoke → Task 7.
- **Deviation from spec, deliberate:** none. Measurement recording was removed from the spec before this plan was written.
- **Type consistency:** `ProfileAuditStore.RawProfile(String userId, String json, long ttlSeconds)` and `ScanResult(List<String>, boolean)` are used identically in Tasks 3, 4, 5. `UserAuditClassifier.classify(RawProfile, String, CatalogPreferenceIndex, ObjectMapper, int)` matches between Tasks 4 and 5. `ProfileAuditService.audit(Integer)` matches between Tasks 5 and 6. `UserAuditClassifierTest.profileJson` is package-visible static and reused by `ProfileAuditServiceTest` in the same package.
