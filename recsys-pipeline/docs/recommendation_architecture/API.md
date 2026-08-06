# API

REST endpoints served by the `java-retrieval-service`. From the repository root, use this working
directory and start the service:

```bash
cd recsys-pipeline/services/java-retrieval-service
mvn spring-boot:run
```

Startup requires Java 17 and Redis reachable at the configured host and port. Wait for
`Started RetrievalServiceApplication` before sending requests. The base URL for the local service
is `http://localhost:8080`. See the canonical
[retrieval-service workflow](../../../README.md#3-experiment-pipeline--retrieval-service-8080)
for the surrounding local run sequence.

## `GET /recommend/{user}?limit=6`

Returns recent interactions, selected recommendations, per-item diagnostics, and request-level metrics.

```bash
curl 'http://localhost:8080/recommend/user_1?limit=6'
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
      "dlScore": 0.74,
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

## `GET /users/{user}/profile`

Returns the profile selected by `RECSYS_USER_PROFILE_KEY_PREFIX:active-run` (default prefix
`user-profile:v1`). Preference names are normalized for serving, while list order, explicit JSON
nulls, run metadata, persona confidence, and evidence are preserved.

```bash
curl http://localhost:8080/users/user_1/profile
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

## `GET /predict/{user}/{item}`

Scores a single (user, item) pair using the offline ONNX model. These are string IDs: the service
resolves both values through the model's user and item lookup tables before invoking ONNX. If
either value is absent, the response contains `unknown_user_or_item`.

```bash
curl http://localhost:8080/predict/user_employee_01/action_benefits
```

```json
{"model":"mlp_embedding","user":"user_employee_01","item":"action_benefits","userId":0,"itemId":0,"score":0.448}
```

The default classpath model (`mlp_embedding`) is an internal employee/action dataset. Its user
lookup contains `user_employee_01..08`, `user_manager_01..08`, `user_new_hire_01..08`, and
`user_payroll_admin_01..08`. Its item lookup contains twelve `action_*` IDs, including
`action_benefits`, `action_learning`, `action_onboarding`, and `action_payroll`. Unknown IDs return
`{"error":"unknown_user_or_item", ...}` with the model's lookup sizes.

## `GET /predict/id?userId=0&itemId=4`

Same as above but accepts raw, zero-based internal lookup indices directly. These values are not
external movie IDs. Inspect the loaded model's lookup sizes before choosing indices:

```bash
curl -s http://localhost:8080/predict/metadata
```

`userId` must be in `0..users-1`, and `itemId` must be in `0..items-1`, where `users` and `items`
come from the metadata response. An out-of-range index returns HTTP 400.

```bash
curl 'http://localhost:8080/predict/id?userId=0&itemId=4'
```

## `GET /predict/metadata`

Returns model name, lookup table sizes, and ONNX input/output names for the loaded offline model.

```bash
curl http://localhost:8080/predict/metadata
```

## `POST /feedback`

Records user feedback for an exposed item. All Redis writes are batched in a single `executePipelined` call (one round-trip instead of ~22). The three phases on each call:

1. **Read** — fetch the pending replay context written at serve time (`GET replay:pending:{user}:{item}`). Must happen before the pipeline because reads cannot be issued inside a write pipeline.
2. **Write (pipelined)** — batch all writes in one flush:
   - Increment bandit click counter and per-algorithm metrics hashes.
   - Update online reward stats for the item, its genres, its tags, and the global prior (`HINCRBY` on `reward-model:*` hashes).
   - Push the rewarded event to the replay buffer (`RPUSH` + `LTRIM`).
3. **Invalidate** — purge affected `reward-model:*` keys from the Caffeine in-memory cache so the next `/recommend` request reads fresh stats.

```bash
curl -X POST http://localhost:8080/feedback \
  -H 'Content-Type: application/json' \
  -d '{"user":"user_1","item":"item_5","clicked":true,"reward":1.0}'
```

## `GET /metrics`

Returns aggregate online metrics for the active algorithm and a per-algorithm comparison view — see
[Track Metrics](../recommendation_flows/9_Track_Metrics.md) for the full field and Redis-key tables.

```bash
curl http://localhost:8080/metrics
```

## `GET /embedding/{item}`

Returns an item embedding from Redis using key `i2vEmb:{item}`.

```bash
curl http://localhost:8080/embedding/item_5
```
