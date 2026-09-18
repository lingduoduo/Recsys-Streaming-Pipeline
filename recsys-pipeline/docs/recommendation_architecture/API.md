# API

REST endpoints served by the retrieval service, which lives in a separate repository — see the
[repository boundary](../../../README.md#repository-boundary).

Startup requires Java 17 and Redis reachable at the configured host and port. Wait for
`Started RetrievalServiceApplication` before sending requests. Every endpoint below sits under the
service's versioned prefix `/api/v1/retrieval`, so the local base URL is
`http://localhost:8080/api/v1/retrieval`. See the canonical
[retrieval-service workflow](../../README.md#optional-reference-experiment-pipeline--retrieval-service-8080)
for the surrounding local run sequence.

## `GET /api/v1/retrieval/recommend/{user}?limit=6`

Returns recent interactions, selected recommendations, per-item diagnostics, and request-level metrics.

```bash
curl 'http://localhost:8080/api/v1/retrieval/recommend/user_1?limit=6'
```

```json
{
  "user": "user_1",
  "recent": ["item_7", "item_2"],
  "recommendations": ["item_5", "item_4", "item_1"],
  "diagnostics": [
    {
      "item": "item_5",
      "estimatedReward": 0.71,
      "relevanceScore": 0.62,
      "contentScore": 0.67,
      "rewardModelScore": 0.58,
      "explorationBonus": 0.19,
      "banditScore": 0.78,
      "coldStart": true,
      "impressions": 2,
      "clicks": 1
    }
  ],
  "metrics": {
    "algorithm": "ucb",
    "eligibleCandidateCount": 8,
    "randomizationPool": 5,
    "pseudoRegret": 0.04,
    "avgEstimatedReward": 0.68,
    "avgExplorationBonus": 0.12,
    "coldStartShare": 0.5,
    "catalogCoverage": 0.57
  }
}
```

- `limit` defaults to `6`, clamped to `1..50`.
- User and item IDs must match `[a-zA-Z0-9_:-]{1,64}`.

Behavioral preferences from the active version-one profile are added during query hydration and
can break otherwise tied popularity/content candidates. If the active pointer or requested profile
is absent—or the value has invalid JSON, an unsupported version, a mismatched user, or Redis is
unavailable—the recommender fails closed to its established non-profile signals. `/recommend`
continues returning a normal response; a profile failure never makes recommendation serving depend
on a partial snapshot.

## `GET /api/v1/retrieval/users/{user}/profile`

Returns the profile selected by `RECSYS_USER_PROFILE_KEY_PREFIX:active-run` (default prefix
`user-profile:v1`). Preference names are normalized for serving, while list order, explicit JSON
nulls, run metadata, persona confidence, and evidence are preserved.

```bash
curl http://localhost:8080/api/v1/retrieval/users/user_1/profile
```

```json
{
  "user_id": "user_1",
  "profile_version": 1,
  "run_id": "2026-08-06-run",
  "generated_at": "2026-08-06T15:00:00Z",
  "source_window": {
    "start": "2026-07-07T15:00:00Z",
    "end": "2026-08-06T15:00:01Z"
  },
  "evidence_count": 12,
  "preferences": {
    "genres": [{"value": "sci-fi", "score": 0.72, "evidence_count": 8}],
    "tags": [{"value": "space", "score": 0.61, "evidence_count": 5}]
  },
  "behavioral_features": {
    "engagement_rate": 0.5,
    "conversion_rate": 0.08,
    "genre_diversity": 0.42,
    "preference_concentration": 0.71,
    "recent_release_affinity": 0.64,
    "average_rating": null,
    "activity_level": "medium"
  },
  "personas": [{
    "type": "genre_enthusiast",
    "label": "Sci Fi enthusiast",
    "confidence": 0.3,
    "evidence": {"preference_score": 0.72, "evidence_count": 8.0}
  }]
}
```

When no valid profile is available, this endpoint returns HTTP 404:

```json
{"error":"profile_not_found","user_id":"user_1"}
```

User IDs outside `[a-zA-Z0-9_:-]{1,64}` return HTTP 400. Profile lookups record the Micrometer timer
`profile.lookup`; safe fallbacks increment `profile.lookup.fallback` with reason
`missing_active_run`, `missing_profile`, `unsupported_version`, `user_mismatch`, `invalid_json`, or
`redis_error`. Configure a non-default namespace with `RECSYS_USER_PROFILE_KEY_PREFIX`; it must
match `USER_PROFILE_REDIS_KEY_PREFIX` used by the Spark publisher.

## `GET /api/v1/retrieval/profile-audit`

Operator tool. Walks every user → has_profile → profile preferences → catalog content and returns
a findings-only report: users with no usable profile, profiles with no usable preference, and
preferences that match no catalog item. Healthy users are counted, not listed. One audit runs at a
time; the call is synchronous and bounded by `limit` (default and maximum
`RECSYS_PROFILE_AUDIT_MAX_USERS`, 10000).

```bash
curl -s 'http://localhost:8080/api/v1/retrieval/profile-audit?limit=1000' | jq .summary
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
`RECSYS_PROFILE_AUDIT_SAMPLE_ITEMS` (5 ids per matched preference). Each chunk is one pipelined
round trip that must complete within the Redis command timeout (`spring.data.redis.timeout`, 1 s);
if a remote Redis times out with a 503, lower `RECSYS_PROFILE_AUDIT_CHUNK_SIZE` before anything else.
Chunks are classified as they complete, with at most `RECSYS_PROFILE_AUDIT_PARALLELISM` chunks
resident at a time, so `max-users` bounds how much work one call does rather than how much memory
it holds.

## `GET /api/v1/retrieval/profile-audit/{user}`

The same walk for a single account: one read of the active-run pointer, one pipelined round trip
for its profile, then one bounded probe per preference. Unguarded and executor-free, so it never
queues behind a bulk audit.

```bash
curl -s localhost:8080/api/v1/retrieval/profile-audit/user_1 | jq .
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
a preference matching thousands of items costs no more to report than one matching three. Unlike
the bulk route, an unset active-run pointer still returns a row here: the status is
`missing_active_run` and the row carries a `no_profile` finding with that same reason.

Status codes: 200; 400 if the id is outside `[a-zA-Z0-9_:-]{1,64}`; 503
`{"status":"error","message":...}` if Redis fails. There is no 409 — only the bulk route is
single-flight.

## `GET /api/v1/retrieval/predict/{user}/{item}`

Scores a single (user, item) pair using the offline ONNX model. These are string IDs: the service
resolves both values through the model's user and item lookup tables before invoking ONNX. If
either value is absent, the response contains `unknown_user_or_item`.

```bash
curl http://localhost:8080/api/v1/retrieval/predict/user_employee_01/action_benefits
```

```json
{"model":"mlp_embedding","user":"user_employee_01","item":"action_benefits","userId":0,"itemId":0,"score":0.448}
```

The default classpath model (`mlp_embedding`) is an internal employee/action dataset. Its user
lookup contains `user_employee_01..08`, `user_manager_01..08`, `user_new_hire_01..08`, and
`user_payroll_admin_01..08`. Its item lookup contains twelve `action_*` IDs, including
`action_benefits`, `action_learning`, `action_onboarding`, and `action_payroll`. Unknown IDs return
`{"error":"unknown_user_or_item", ...}` with the model's lookup sizes.

## `GET /api/v1/retrieval/predict/id?userId=0&itemId=4`

Same as above but accepts raw, zero-based internal lookup indices directly. These values are not
external movie IDs. Inspect the loaded model's lookup sizes before choosing indices:

```bash
curl -s http://localhost:8080/api/v1/retrieval/predict/metadata
```

`userId` must be in `0..users-1`, and `itemId` must be in `0..items-1`, where `users` and `items`
come from the metadata response. An out-of-range index returns HTTP 400.

```bash
curl 'http://localhost:8080/api/v1/retrieval/predict/id?userId=0&itemId=4'
```

## `GET /api/v1/retrieval/predict/metadata`

Returns model name, lookup table sizes, and ONNX input/output names for the loaded offline model.

```bash
curl http://localhost:8080/api/v1/retrieval/predict/metadata
```

## `POST /api/v1/retrieval/feedback`

Records user feedback for an exposed item. All Redis writes are batched in a single `executePipelined` call (one round-trip instead of ~22). The three phases on each call:

1. **Read** — fetch the pending replay context written at serve time (`GET replay:pending:{user}:{item}`). Must happen before the pipeline because reads cannot be issued inside a write pipeline.
2. **Write (pipelined)** — batch all writes in one flush:
   - Increment bandit click counter and per-algorithm metrics hashes.
   - Update online reward stats for the item, its genres, its tags, and the global prior (`HINCRBY` on `reward-model:*` hashes).
   - Push the rewarded event to the replay buffer (`RPUSH` + `LTRIM`).
3. **Invalidate** — purge affected `reward-model:*` keys from the Caffeine in-memory cache so the next `/recommend` request reads fresh stats.

```bash
curl -X POST http://localhost:8080/api/v1/retrieval/feedback \
  -H 'Content-Type: application/json' \
  -d '{"user":"user_1","item":"item_5","clicked":true,"reward":1.0}'
```

## `GET /api/v1/retrieval/metrics`

Returns aggregate online metrics for the active algorithm and a per-algorithm comparison view — see
[Track Metrics](../recommendation_flows/9_Track_Metrics.md) for the full field and Redis-key tables.

```bash
curl http://localhost:8080/api/v1/retrieval/metrics
```

## `GET /api/v1/retrieval/embedding/{item}`

Returns an item embedding from Redis using key `i2vEmb:{item}`.

```bash
curl http://localhost:8080/api/v1/retrieval/embedding/item_5
```
