# User Behavioral Profiling Design

## Goal

Build periodically refreshed user profiles from behavioral events. Each profile must serve two consumers:

1. recommendation serving, through normalized positive preferences; and
2. people inspecting a user, through deterministic persona labels and supporting evidence.

The system must remain explainable, avoid an LLM dependency, and preserve existing recommendation behavior when a usable profile is unavailable.

## Scope

This change adds a Spark batch profiler, durable Parquet profile snapshots, Redis serving profiles, a Java retrieval-service client and query hydrator, and a read-only profile API. It uses the behavioral samples and catalog attributes already produced by the pipeline.

The first version does not add free-form persona generation, real-time profile updates, negative-preference filtering, or a profile-editing UI.

## Source Data

The profiler reads the enriched `training_samples` Parquet data produced by `OnlineJoinerStreamingJob`. Input records are expected to provide:

- user and event/request identity;
- event timestamp;
- impression, click, order, and rating outcomes when present;
- item genres and tags; and
- release year or equivalent freshness metadata when present.

The job accepts a configurable lookback window. It rejects records with a missing user, invalid timestamp, or invalid behavioral values into measured counters. Records are deduplicated using the strongest available event identity. When no explicit event identity exists, the job uses a documented composite identity derived from user, request, item, timestamp, and outcome fields.

## Architecture

`UserBehaviorProfileBatchJob` reads the source window and transforms each valid event into weighted preference evidence. Pure transformations aggregate evidence and derive behavioral features. A deterministic persona classifier applies fixed rules to those features. The job writes a complete Parquet snapshot, then publishes serving profiles to Redis.

Redis profiles use run-scoped versioned keys plus a single active-run pointer:

```text
user-profile:v1:{runId}:{userId}
user-profile:v1:active-run
```

The Java retrieval service adds a `UserProfileClient` and query hydrator. The hydrator maps positive genre and tag preferences into content-retrieval inputs. A read-only endpoint returns the complete explainable profile. Missing, expired, unsupported, or malformed profiles produce an empty result and leave the current recommendation path unchanged.

## Behavioral Evidence

Default base weights are configurable and begin with:

| Signal | Weight |
| --- | ---: |
| Impression without engagement | -0.1 |
| Click | 1.0 |
| Order | 3.0 |
| Rating | centered around the rating scale's neutral midpoint |

An event contributes its strongest applicable outcome rather than adding mutually dependent outcomes from the same interaction. Rating evidence is mapped to `[-1, 1]`, with the configured midpoint equal to zero.

Evidence receives exponential recency decay:

```text
decayed_weight = base_weight * 0.5 ^ (age / half_life)
```

The job aggregates decayed evidence independently by normalized genre and tag. It applies configurable shrinkage toward zero so sparse activity does not produce an overconfident profile, then bounds preference scores to `[-1, 1]`. Stable secondary ordering by normalized feature name makes tied outputs deterministic.

Serving consumes only preferences with positive scores. Non-positive preferences stay in the stored profile as explanatory evidence and do not act as exclusion rules in version 1.

## Behavioral Features

Each profile includes:

- evidence and activity counts;
- click-through rate;
- conversion rate;
- average rating when ratings exist;
- genre diversity;
- preference concentration;
- recent-release affinity; and
- an activity-level bucket.

Rates use impressions as their denominator and are absent rather than fabricated when their denominator is zero. Diversity and concentration use positively engaged items, not raw impressions, and remain absent when there is insufficient evidence. Recent-release affinity is the decayed share of positive evidence attached to items within the configured recent-release age.

Every derived value records enough supporting evidence for an inspector to understand why it exists.

## Persona Taxonomy

Persona assignment is deterministic and multi-label. Every rule has configurable thresholds and a minimum evidence requirement.

| Persona type | Meaning | Primary evidence |
| --- | --- | --- |
| `genre_enthusiast` | One genre has a dominant positive preference | top genre score and evidence count |
| `genre_explorer` | Engagement spans many genres without one dominant preference | genre diversity and concentration |
| `focused_viewer` | Positive preferences are strongly concentrated | preference concentration |
| `recent_release_seeker` | Positive activity favors recent releases | recent-release affinity |
| `high_intent_engager` | Click and order behavior is consistently strong | CTR, conversion rate, and counts |
| `casual_browser` | The user has enough impressions but low engagement | impressions, CTR, and conversion rate |
| `new_or_unknown` | Evidence is below the minimum for substantive inference | total usable evidence |

`new_or_unknown` is exclusive. Other personas may coexist. Each assigned persona includes a type, human-readable label, confidence in `[0, 1]`, and named evidence values. Confidence measures distance beyond the configured threshold and is capped; it is not presented as a probabilistic prediction.

## Profile Contract

The version 1 JSON representation is:

```json
{
  "user_id": "42",
  "profile_version": 1,
  "run_id": "20260806T120000Z",
  "generated_at": "2026-08-06T12:00:00Z",
  "source_window": {
    "start": "2026-07-07T00:00:00Z",
    "end": "2026-08-06T00:00:00Z"
  },
  "evidence_count": 120,
  "preferences": {
    "genres": [
      {"value": "sci-fi", "score": 0.82, "evidence_count": 37}
    ],
    "tags": [
      {"value": "space", "score": 0.71, "evidence_count": 18}
    ]
  },
  "behavioral_features": {
    "engagement_rate": 0.31,
    "conversion_rate": 0.08,
    "genre_diversity": 0.47,
    "preference_concentration": 0.64,
    "recent_release_affinity": 0.68,
    "average_rating": 4.2,
    "activity_level": "high"
  },
  "personas": [
    {
      "type": "genre_enthusiast",
      "label": "Sci-Fi enthusiast",
      "confidence": 0.82,
      "evidence": {"genre": "sci-fi", "preference_score": 0.82}
    }
  ]
}
```

Preference lists are ordered by descending score and then normalized value. Persona lists use a fixed taxonomy order. Optional metrics are nullable Parquet fields and explicit JSON `null` values in Redis and API responses. Contract tests enforce this representation.

## Batch Publication and Failure Handling

Every execution receives a unique run ID and performs these stages:

1. read, validate, deduplicate, and aggregate the configured source window;
2. write a run-scoped Parquet snapshot;
3. validate profile serialization and aggregate job invariants;
4. publish run-scoped versioned Redis profiles with a configurable TTL; and
5. atomically update `user-profile:v1:active-run` only after all profile writes succeed.

The job never deletes the preceding Parquet snapshot. A Redis publication failure fails the run and leaves the active-run pointer unchanged. Individual keys for the failed run may exist, but consumers cannot discover them through the active pointer and continue using the preceding completed run. This prevents a partial publication from becoming the active profile set.

Malformed Redis data, unsupported schema versions, incomplete runs, and expired profiles are recorded and treated as missing. These cases never fail a recommendation request.

## Serving Integration

The retrieval service introduces:

- a profile model matching the versioned JSON contract;
- a Redis client that resolves the active run and parses a user's run-scoped profile;
- a query hydrator that supplies positive genre/tag preferences to candidate retrieval; and
- `GET /users/{userId}/profile`, returning the current profile or HTTP 404 when none is usable.

The content scorer preserves preference strength rather than collapsing the profile to an unweighted set. Existing explicit request preferences, if present, take precedence; inferred profile preferences supplement them without overriding exclusions. Profile lookup latency and fallback counts are measured.

## Configuration

Configuration covers:

- input and output paths;
- source lookback window;
- recency half-life;
- signal weights and rating midpoint;
- shrinkage and minimum evidence;
- maximum stored genres and tags;
- persona thresholds;
- recent-release age;
- Redis key prefix and TTL; and
- profile schema version.

Defaults are checked into the repository and jobs log the effective configuration without secrets.

## Observability

The batch job reports input, valid, rejected, and deduplicated event counts; generated profile count; insufficient-evidence rate; persona distribution; preference coverage; source lag; Redis write failures; and run duration.

The serving service reports profile hits, misses, malformed profiles, unsupported versions, incomplete-run fallbacks, profile age, and lookup latency.

## Testing

Development follows test-first increments.

Unit tests cover signal precedence, rating mapping, recency decay, sparse-evidence shrinkage, score bounds, deterministic ordering, behavioral metrics, every persona boundary, insufficient evidence, and JSON round trips.

Spark tests cover validation, rejection counters, fallback deduplication identity, per-user aggregation, null metadata, Parquet output, and deterministic reruns.

Java tests cover Redis parsing, schema rejection, completion-marker behavior, query hydration, weighted preference use in content scoring, explicit-preference precedence, the profile endpoint, and safe fallback.

An integration fixture runs behavioral samples through the batch job, Redis publication, profile API, and recommendation request. It proves that an engaged genre receives a higher content contribution while a missing profile preserves baseline behavior.

## Acceptance Criteria

- Running the batch job on valid behavioral samples produces deterministic version 1 profiles in Parquet and Redis.
- Recent positive engagement increases the corresponding preference more than older engagement; unengaged impressions provide weak negative evidence.
- Sparse users are labeled `new_or_unknown` and receive no invented preferences.
- Every persona other than `new_or_unknown` is supported by named evidence and meets its configured minimum evidence.
- The retrieval path uses positive weighted preferences from a completed profile run.
- Missing, malformed, expired, incomplete-run, and unsupported profiles do not fail recommendation requests or change baseline behavior.
- `GET /users/{userId}/profile` returns the explainable contract for a usable profile and 404 otherwise.
- Unit, Spark, Java, and end-to-end integration tests pass.
