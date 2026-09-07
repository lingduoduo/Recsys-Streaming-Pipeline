# Profile audit walker — per-account lookup, bounded neighbors, streaming fan-out — design

Extend the merged `GET /actuator/profile-audit` (PR #217) so that one account can be looked up in
a single low-millisecond round trip, the large Profile → Content fan-out is read as a count plus a
capped sample, and the bulk audit streams chunks through the same traversal instead of holding
every profile blob in memory.

## Why this document exists

The merged audit answers "which accounts have a problem" but not "what does *this* account link
to". Its traversal is three inline levels in `ProfileAuditService.audit`, and it materializes every
`RawProfile` (all JSON blobs) before classifying any — parked in the final review of PR #217 as
"acceptable at max-users=10000, revisit before raising it".

The request: Account → Profiles → Content, where

- small fan-outs (Account → Profile) are predictable, low-millisecond lookups,
- large fan-outs (Profile → Content) stay efficient by reading only what is needed,
- traversal logic treats "neighbors of this node" as a cheap, bounded operation.

Scope decisions taken with the user: all three items are in scope; an account's "Profiles"
neighbor is the versioned behavioral profile under the active run only (not `user:{id}:features`,
not the sequence store).

## Decisions taken

| Decision | Alternative rejected | Reason |
|---|---|---|
| One `ProfileAuditWalker` owning both edges, shared by the per-account endpoint and the bulk audit | Generic Node/Edge/BFS framework | Two edges do not justify a framework; a single named class makes "neighbors are bounded" a real boundary without speculative abstraction |
| Per-account endpoint is unguarded and executor-free | Route it through the single-flight guard | It is exactly one pipelined round trip (GET + TTL) and one index probe; guarding it would make the cheap path wait on the expensive one |
| Bounded content probe returns `(total, sample)` | Keep returning the full posting list and slicing at the call site | The probe is the explicit "read only what's needed" operation; the posting list may be thousands of ids |
| Stream chunks with at most `parallelism` in flight, classify inside the worker | Keep materialize-then-classify | Peak residency becomes `parallelism` chunks of classified rows; raw JSON never leaves the worker thread |
| Determinism and failure semantics unchanged | Complete-in-any-order aggregation | Joining in submission order keeps rows in scan order and keeps "first failure in chunk order, no partial report" |
| Missing active run becomes a classifier-level row (`no_profile` / `missing_active_run`) | Separate error path in the per-account endpoint | One code path for both entry points; the bulk short-circuit (no reads at all) is kept as an optimisation |

## Architecture

```
GET /actuator/profile-audit/{user}            GET /actuator/profile-audit?limit=N
        |                                              |
        v                                              v
ProfileAuditService.auditAccount(user)        ProfileAuditService.audit(limit)
        |                                              |  scan users; bounded in-flight chunk queue
        |                                              |  (≤ parallelism futures), join oldest first
        v                                              v
        ProfileAuditWalker.walk(activeRun, accounts) → List<UserRow>, input order
              |  edge 1: Account → Profile      store.readProfiles(run, accounts)   one round trip, ≤1 neighbor each
              |  edge 2: Preference → Content   index.probe(kind, value, sampleItems) count + capped sample
              v
        UserAuditClassifier.classify(raw, activeRun, index, mapper, sampleItems)  (unchanged contract)
```

`ProfileAuditStore` is unchanged. `CatalogPreferenceIndex` gains one method. `UserAuditClassifier`
changes in two places: it calls `probe` instead of `lookup` + `subList`, and it accepts a null
`activeRun`.

## Component 1 — `CatalogPreferenceIndex.probe`

```java
public record ContentNeighbors(int total, List<String> sample) {}
public ContentNeighbors probe(String kind, String value, int limit)
```

`total` is the posting list size; `sample` is the first `min(limit, total)` ids of the sorted,
unmodifiable posting list (a `subList` view is fine — the list is immutable). A miss, unknown kind,
or null value gives `(0, [])`. `limit < 0` is treated as `0`. `lookup` remains for existing tests.

## Component 2 — `UserAuditClassifier` changes

- `resolve(...)` uses `index.probe(kind, value, sampleItems)`: `matched_items = total`,
  `sample_items = sample`. Output for every existing test is byte-identical.
- If `activeRun == null`, return `new UserRow(userId, false, null,
  [Finding(no_profile, "missing_active_run", null)], null)` before calling
  `UserProfileValidation.validate` (which requires a non-null run). The reason string is the same
  one serving emits for this state.

## Component 3 — `ProfileAuditWalker`

```java
final class ProfileAuditWalker {                       // package-private, audit package
    ProfileAuditWalker(ProfileAuditStore store, ObjectMapper mapper, CatalogPreferenceIndex index, int sampleItems)
    /** Edge 1 for the whole batch in one round trip, then edges 2 per row. Rows come back in input order. */
    List<UserRow> walk(String activeRun /* nullable */, List<String> accounts)
}
```

- `activeRun == null`: no store call; every account gets the `missing_active_run` row.
- Otherwise exactly one `store.readProfiles(activeRun, accounts)` call, then `classify` per
  `RawProfile` in order.
- Pure apart from the store call; no executor, no guard, no aggregation. The walker is the
  "neighbors of these nodes" operation: bounded to one round trip and `accounts.size()` rows.

The index is per-call state (built from the current catalog at the start of each audit), so the
walker is constructed per call, not injected as a bean.

## Component 4 — Per-account entry point

`ProfileAuditService.auditAccount(String userId)`:

```java
public AccountAuditReport auditAccount(String userId)
// AccountAuditReport(status, active_run, generated_at, elapsed_ms, catalog_size, user: UserRow)
```

1. Build the index (cached normalized catalog → cheap).
2. `activeRun = store.activeRun().orElse(null)`.
3. `row = new ProfileAuditWalker(...).walk(activeRun, List.of(userId)).get(0)`.
4. `status` is `missing_active_run` when `activeRun == null`, else `ok`. The row is always present,
   including for a healthy account (empty `findings`).

No single-flight guard, no executor. Store failures are wrapped in `ProfileAuditFailedException`
exactly as `audit` does.

`ProfileAuditController` adds:

```java
@GetMapping("/actuator/profile-audit/{user}")
public ResponseEntity<?> auditAccount(@PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user)
```

with `@Validated` on the controller and a `ConstraintViolationException` handler returning 400
`{"error":"Invalid input: id must be 1-64 alphanumeric characters"}` — the same pattern and message
`RecommendationController` uses. 503 mapping as for the bulk route. Because the id pattern excludes
`?`, `/actuator/profile-audit?limit=5` and `/actuator/profile-audit/u1` cannot collide.

Response example:

```json
{
  "status": "ok",
  "active_run": "run-7",
  "generated_at": "2026-09-07T10:00:00Z",
  "elapsed_ms": 2,
  "catalog_size": 12,
  "user": {"user_id": "u2", "has_profile": true, "ttl_seconds": 3400, "findings": [],
           "preferences": [{"kind": "genre", "value": "sci-fi", "score": 0.72, "evidence_count": 8,
                            "matched_items": 8, "sample_items": ["item1", "item4"]}]}
}
```

## Component 5 — Streaming bulk audit

`ProfileAuditService.audit` replaces `readAllProfiles` + the classify loop with a bounded pipeline:

```
walker = new ProfileAuditWalker(store, mapper, index, sampleItems)
inFlight: ArrayDeque<CompletableFuture<List<UserRow>>>  (capacity = parallelism)
for each chunk of userIds (size chunkSize, in scan order):
    if inFlight.size() == parallelism: drain one   // join head, aggregate its rows
    inFlight.add(supplyAsync(() -> walker.walk(activeRun, chunk), executor))
while inFlight not empty: drain one
```

`drain one` = `head = inFlight.poll(); rows = head.join(); rows.forEach(aggregation::add)`.

**Failure semantics (unchanged from PR #217):** when a `join` throws, stop submitting, let every
remaining in-flight future settle (`allOf(...).exceptionally(t -> null).join()`), and rethrow the
failure that was hit first in chunk order as `ProfileAuditFailedException(cause)`. Chunks that
were already aggregated are discarded with the rest — no partial report.

**Residency:** at most `parallelism` futures exist, each holding one chunk's classified rows (raw
JSON is dropped inside the worker after `classify`). The bulk `Aggregation` still keeps only rows
with findings.

**Determinism:** unchanged — the head of the deque is always the oldest submitted chunk.

The `missing_active_run` short-circuit (count every scanned user, no reads) is kept as is.

## Configuration

No new keys. `parallelism` now also bounds in-flight chunks; `chunk-size` bounds residency per
future; `sample-items` bounds the content probe. The API.md tunables sentence is updated to say so.

## Testing

| Test | Covers | Needs Redis |
|---|---|---|
| `CatalogPreferenceIndexTest` (+3) | probe hit (total > limit), probe with limit ≥ total, probe miss / unknown kind / negative limit | no |
| `UserAuditClassifierTest` (+1, existing unchanged) | null activeRun → `no_profile`/`missing_active_run` row | no |
| `ProfileAuditWalkerTest` (new) | one `readProfiles` call per walk, rows in input order, null run → no store call and every row `missing_active_run`, empty accounts → empty list without a store call | no |
| `ProfileAuditServiceTest` (+3, existing unchanged) | streaming: fake store records max concurrent `readProfiles` ≤ parallelism with 9 chunks and parallelism 2; rows stay in scan order with chunkSize 1 and parallelism 3; a failure in the last chunk after earlier chunks were aggregated still throws `ProfileAuditFailedException` and every chunk was attempted | no |
| `ProfileAuditServiceTest` (+3) | `auditAccount`: healthy row with empty findings; `missing_active_run` status and row; store failure → `ProfileAuditFailedException` | no |
| `ProfileAuditControllerTest` (+3) | per-account 200 shape (`user.user_id`, `catalog_size`), 400 on `bad id!`, 503 on failure | no |

The existing `FakeStore` in `ProfileAuditServiceTest` gains an `AtomicInteger` pair
(`inFlight`, `maxInFlight`) incremented/decremented around the read (plus an optional per-call
sleep so overlap is observable).

## Documentation

- API.md: new `## \`GET /actuator/profile-audit/{user}\`` section directly after the bulk
  section; one sentence in the bulk section: chunks are classified as they complete with at most
  `parallelism` chunks resident, so `max-users` bounds work, not memory.
- README: extend the existing pointer sentence to mention the per-account variant.
- Memory note `project_profile_content_audit.md`: close the "all blobs materialized" parked item.

## Scope boundaries

- No history-based content edge (`recentlyRatedMovieIds`, `seq:*`) — user chose profile-only.
- No generic graph/BFS framework, no new config keys, no change to `ProfileAuditStore`.
- No per-account bulk-style summary; the per-account response is one row.
- No change to serving, `RedisUserProfileClient`, or `UserProfileValidation`.

## Risks

- **Ordering vs. throughput.** Draining the oldest future first means one slow chunk delays
  aggregation of faster later ones, but never blocks their reads (they run in the pool). This is
  the cost of determinism and is the same shape as before, only with bounded residency.
- **Per-account path and the catalog.** Building the index per call is O(catalog) even for one
  account. The normalized catalog is cached on identity so this is a hash-map pass over the
  catalog, microseconds for the demo catalog and low milliseconds for a 10k-item one. If it ever
  matters, cache the index on the same identity — deliberately not done now.
- **Route pattern.** `@Pattern` on the path variable keeps the two routes disjoint; a test asserts
  the bulk route still answers with `limit`.
