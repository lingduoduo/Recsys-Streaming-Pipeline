# Profile → content audit endpoint — design

Add an admin endpoint to the Java retrieval service that walks every user through three levels —
user → has_profile → Profiles → Content — and reports which users have no usable profile, which
profiles carry no usable preference, and which preferences point at no catalog content.

## Why this document exists

The question asked was: *investigate each user/account, check `has_profile` → Profiles → Content*,
reading storage efficiently, traversing breadth-first by level, running operations in parallel but
safely, filtering to what matters, and caching repeat queries.

Nothing in the repository links users to profiles to content today. The pieces exist, but each is
read by a different component and none of them is ever read together:

| Level | Where it lives | Who reads it today |
|---|---|---|
| Users | Redis `user:{id}:features` hashes (MovieLens demographics + rating context) | `UserDemographicsQueryHydrator`, `SegmentReportJob` |
| has_profile | Redis `user-profile:v1:active-run` pointer, then `user-profile:v1:{run}:{user}` JSON blob | `RedisUserProfileClient` (serving), which applies five validity checks |
| Profiles | The blob: genre and tag preferences with score and evidence count, behavioral features, personas | `UserBehaviorProfileQueryHydrator` |
| Content | The in-memory catalog (`recsys.catalog`, optionally merged from `RECSYS_CATALOG_PATH`): title, genres, tags, keywords | `CatalogContentScoring`, `ContentCandidateRetriever` |

Two facts found during investigation shape the design:

- **The Redis movie hashes are not the content store for this walk.** `movie:{id}:features` holds
  title, genres, and release year only. Profiles also carry *tag* preferences, and only the
  service's catalog carries tags. Walking against the catalog makes every preference matchable and
  keeps the audit inside the process that already owns profile validation.
- **`has_profile` is not "the key exists".** Serving treats a profile as absent for any of five
  reasons: `missing_profile`, `invalid_json`, `unsupported_version`, `user_mismatch`,
  `run_mismatch` (and globally `missing_active_run`). Profile blobs expire after one day while the
  active-run pointer never expires, so "pointer set, every blob gone" is a real state. The audit
  must use serving's definition, not its own.

## Decisions taken

| Decision | Alternative rejected | Reason |
|---|---|---|
| Java admin endpoint in the retrieval service | Standalone Python report script; Scala report job | Chosen by the user. The service already owns profile validation and the catalog; both other options would re-implement the validity rules and could drift from serving |
| Content edge is profile preferences → catalog items sharing that genre or tag | User rating history → movies | The profile is what serving acts on; the audit should test what the profile asserts. History bypasses the profile entirely |
| Report lists only users with a finding; healthy users are counted | Every user; a `--all` flag | Keeps the response bounded at scale. Nothing asked for the full list |
| Cache is a per-call inverted index over the catalog | On-disk cache across calls | The catalog is in memory and can be swapped at startup; a cross-call cache would need invalidation for no measurable gain |
| Synchronous HTTP call with a `limit` on users scanned and a single-flight guard | Background job with polling | Bounded work per call and one audit at a time are enough to keep the serving process safe; a job registry is speculative |
| Redis failure fails the whole call (503) | Return partial counts | A partial count reads as a real count. The filtering/projection audit already recorded this lesson |

## Architecture

```
GET /actuator/profile-audit?limit=N
  |
  v
ProfileAuditController                   validates limit, maps 409 / 503
  |
  v
ProfileAuditService                      single-flight guard, bounded executor, assembles report
  |
  |  level 0 ─ store.scanUserIds(pattern, limit)        one SCAN cursor, never KEYS
  |  level 1 ─ store.activeRun()                        one GET
  |            store.readProfiles(chunk)  × chunks       one pipelined round trip per chunk (GET + TTL per user),
  |                                                      chunks fanned out over the executor
  |            UserProfileValidation.validate(...)       the same five checks serving applies
  |  level 2 ─ positive genre/tag preferences, normalized as serving normalizes them
  |  level 3 ─ CatalogPreferenceIndex.lookup(kind, value) hash lookup into a per-call inverted index
  |
  v
ProfileAuditReport                       summary block + findings-only user rows
```

The store boundary (`ProfileAuditStore`) is the only thing that touches Redis, mirroring the
`RedisProfileStore` seam on the Spark side. Everything above it is pure and unit-tested without
Redis.

## Component 1 — `UserProfileValidation` (extracted, not new)

`RedisUserProfileClient.getProfile` currently reads Redis and validates in one method. The five
checks move to a pure static function:

```java
static Validation validate(String rawProfile, String userId, String activeRun, ObjectMapper mapper)
// Validation = Valid(UserBehaviorProfile normalized) | Invalid(reason)
```

Reason strings are unchanged: `missing_profile`, `invalid_json`, `unsupported_version`,
`user_mismatch`, `run_mismatch`. Preference-name normalization (trim, collapse whitespace,
lowercase) moves with it, because a valid profile is a normalized profile everywhere.

`RedisUserProfileClient` keeps its public behavior, metrics, and fallback counters; it calls the
extracted function. Its existing tests must pass unchanged. This is the one edit to existing
code, and it is what stops the audit from drifting from serving.

## Component 2 — `ProfileAuditStore` and `RedisProfileAuditStore`

```java
interface ProfileAuditStore {
    List<String> scanUserIds(String pattern, int limit);         // level 0
    Optional<String> activeRun();                                // level 1
    List<RawProfile> readProfiles(String run, List<String> users); // level 1, one pipeline
}
record RawProfile(String userId, String json /* nullable */, long ttlSeconds /* -2 missing, -1 no expiry */) {}
```

- `scanUserIds` uses `RedisConnection.scan` with `MATCH pattern COUNT 1000` and stops at `limit`.
  The user id is the second `:`-separated segment of each key. SCAN is cursor-based and does not
  block Redis the way `KEYS` does; it may return a key twice, so ids are collected into a
  `LinkedHashSet`.
- `readProfiles` issues one `executePipelined` with `GET` and `TTL` for every user in the chunk and
  zips the results by position. This is the same pattern `RedisUserMovieHistoryClient` and
  `RedisSequenceClient` use.
- Any `RuntimeException` from Redis propagates; the service turns it into a 503.

## Component 3 — `CatalogPreferenceIndex`

Built once per audit call from `CatalogContentScoring.normalizedCatalog()`, which is already cached
on catalog-map identity and already normalizes genres and tags with `TextNormalization`:

```
genre -> [itemId...]      tag -> [itemId...]
```

`lookup(kind, value)` returns the item list or empty. Item ids are kept in catalog iteration order
so `sample_items` is deterministic. Building the index is O(catalog × attributes) once; level 3 is
then O(1) per preference for every user. This is the "repeat queries faster" piece: without it,
each user would rescan the catalog per preference.

## Component 4 — `ProfileAuditService`

**Single flight.** An `AtomicBoolean running`; a call that finds it set returns
`AuditBusyException` → 409. The flag is cleared in `finally`.

**Executor.** A fixed pool of `parallelism` threads (default 4) created in the constructor and
shut down in `@PreDestroy`. Level-1 chunks are submitted as `CompletableFuture`s; each task calls
`store.readProfiles` once, so every pipeline lives on exactly one thread. Lettuce gives each
pipelined call its own dedicated connection drawn from the configured pool (`max-active: 32`),
which is what makes the fan-out safe; nothing in the audit shares a pipeline or interleaves reads
on one. `parallelism` must stay well below the pool size so an audit cannot starve serving of
connections; the default of 4 leaves 28. Results are joined in chunk order so the
report is deterministic. If any chunk fails, the first failure is rethrown after the others settle.

Against a local Redis, 10,000 users is 20 round trips and completes well under a second
sequentially; the parallelism pays off only for a remote Redis. It is kept because it was asked for
and costs little.

**Per-user classification** (pure; input is a `RawProfile`, the active run, the index):

| Finding | When |
|---|---|
| `no_profile` + `reason` | `UserProfileValidation` returns Invalid |
| `new_or_unknown` | Valid profile whose personas contain type `new_or_unknown` (the profile job's own low-evidence marker) |
| `empty_preferences` | Valid profile with no genre or tag preference whose score > 0 |
| `preference_without_content` | At least one positive preference whose index lookup is empty; the finding lists each `{kind, value}` |

A user with no finding is healthy and appears only in the counts.

**Global short-circuit.** If `activeRun()` is empty, `status` is `missing_active_run`, every
scanned user is counted under `no_profile_by_reason.missing_active_run`, no per-user rows are
emitted (they would all say the same thing), and levels 2–3 are skipped.

## Component 5 — `ProfileAuditController`

```
GET /actuator/profile-audit?limit=10000
```

- `limit`: optional, 1..`max-users` (default and max 10,000); out of range → 400.
- 200 with the report; 409 `{"status":"busy"}`; 503 `{"status":"error","message":...}`.
- Records the request through `RecommendationMeasurementService.recordRequest("profile-audit", ...)`
  the way `/users/{user}/profile` does, so it shows up in `/metrics` like every other endpoint.

## Report shape

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

`truncated` is true when `limit` stopped the scan before the cursor was exhausted.
`unmatched_preferences` counts users per unmatched value, so a catalog gap shows up once with its
blast radius instead of once per user. `min_profile_ttl_seconds` is the smallest TTL seen among
valid profiles; it is the early warning for the pointer-outlives-blobs state.

## Configuration

Under `recsys.profile-audit` in `application.yml`, each with an env override:

| Key | Default | Env |
|---|---|---|
| `user-key-pattern` | `user:*:features` | `RECSYS_PROFILE_AUDIT_USER_KEY_PATTERN` |
| `max-users` | `10000` | `RECSYS_PROFILE_AUDIT_MAX_USERS` |
| `chunk-size` | `500` | `RECSYS_PROFILE_AUDIT_CHUNK_SIZE` |
| `parallelism` | `4` | `RECSYS_PROFILE_AUDIT_PARALLELISM` |
| `sample-items` | `5` | `RECSYS_PROFILE_AUDIT_SAMPLE_ITEMS` |

The profile key prefix is the existing `recsys.user-profile.key-prefix`; the audit must not
introduce a second copy of it.

## Testing

| Test | Covers | Needs Redis |
|---|---|---|
| `UserProfileValidationTest` | each of the five reasons, normalization, a valid profile | no |
| `RedisUserProfileClientTest` (existing, unchanged) | the client still reads, validates, and counts fallbacks the same way | no (mocked template) |
| `CatalogPreferenceIndexTest` | index built from a small catalog; lookup hit, miss, deterministic order | no |
| `ProfileAuditServiceTest` | in-memory `ProfileAuditStore` fake: all four findings, healthy users counted not listed, missing-active-run short-circuit, truncation, chunk-order determinism, failure in one chunk fails the call, busy guard | no |
| `RedisProfileAuditStoreTest` | SCAN duplicate handling and limit; pipelined GET+TTL zipped by position (mocked template, as `RedisUserProfileClientTest` does) | no |
| `ProfileAuditControllerTest` (`@WebMvcTest`) | 200 body shape, 400 on bad limit, 409 busy, 503 on store failure | no |

Nothing here needs Testcontainers, which is blocked on Docker 29 in this repo.

## Documentation

- `docs/recommendation_architecture/API.md`: a `GET /actuator/profile-audit` section with the
  report shape, the finding types, and the status codes.
- `README.md`: one line under the existing "Build behavioral user profiles" step pointing at it.

## Scope boundaries

- No text/summary response format; the `summary` block is the terminal summary (`curl | jq .summary`).
- No per-user `?user=` variant; `/users/{user}/profile` already exists.
- No walk from user history (`recentlyRatedMovieIds`, `seq:*`) to content.
- No cross-call cache, no background job, no persistence of reports.
- No change to what the profile job writes or to how serving reads profiles.

## Risks

- **Catalog coverage, not profile quality, may dominate the findings.** The built-in demo catalog
  holds `item1..itemN`; simulations produce `movie_*` ids and MovieLens genres. A service started
  without `RECSYS_CATALOG_PATH` will report most genres as `preference_without_content`. That is a
  true statement about that service instance, and `catalog_size` in the summary makes it obvious.
- **SCAN over a large keyspace.** `MATCH` filters after the cursor step, so the scan visits every
  key in the database once. With `COUNT 1000` this is bounded, incremental work, but on a very
  large shared Redis it is still a full pass. `limit` bounds the number of *users* collected, not
  the keys visited.
- **The executor shares the process with serving.** Four threads doing pipelined reads is small,
  and the single-flight guard prevents pile-up, but the endpoint is an operator tool and should be
  called as one.
