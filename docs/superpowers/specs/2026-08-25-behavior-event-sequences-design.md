# Behavioral Event Sequences Design

## Objective

Create a backward-compatible foundation that represents search, result views, detail views, and clicks as ordered user behavior. Persist this behavior through the existing sequence infrastructure and hydrate it into recommendation serving. This PR does not unify agent replay formats or implement a closed-loop simulator.

## Current Problem

The canonical event contract exposes an unrestricted `event_type`, but has no typed search or view context. `UserEventStreamingJob` discards every action except `click`, writes click-only sequences, and serving reads rating sequences only. Consequently, search and view intent cannot contribute to online user state, while stored click sequences are not used by recommendation serving.

## Compatibility Strategy

Create `recsys-event-v3.avsc` as the canonical writer schema and retain v1 and v2 in decoder catalogs. V3 gains nullable fields with defaults, preserving old payload decoding and compatibility with tolerant consumers. `item_id` changes from required `string` to `["null", "string"]` with a null default because searches are user actions without an item. Existing item-dependent consumers already gate null item IDs; the generalized user-event consumer replaces its global item gate with action-specific validation. New writers validate required identity per action instead of globally requiring `item_id`.

- `query_id`: stable identity for a search or its result set.
- `query_text`: raw query text for the initial foundation; downstream privacy controls may normalize or redact it.
- `result_set_id`: identity shared by result views and clicks from one returned set.
- `referrer`: prior surface or navigation source.
- `view_kind`: `result` or `detail` for view actions.
- `view_duration_ms`: measured view duration when available.

Existing feedback actions remain valid. The behavioral subset is `search`, `result_view`, `detail_view`, and `click`. The change does not convert `event_type` into an Avro enum because that would break existing feedback events and independently deployed clients.

## Validation Contract

Behavioral validation runs after canonical parsing and identity gating:

| Action | Required fields | Item semantics |
|---|---|---|
| `search` | `user_id`, `timestamp_ms`, `query_id`, nonblank `query_text` | `item_id` is not required |
| `result_view` | `user_id`, `timestamp_ms`, `query_id`, `result_set_id`, `item_id` | viewed result item |
| `detail_view` | `user_id`, `timestamp_ms`, `item_id` | viewed detail item |
| `click` | `user_id`, `timestamp_ms`, `item_id` | clicked item; query fields optional |

Invalid behavioral events are excluded from business sinks and reported through the repository's existing drop-metrics convention. Other event types are ignored by this job rather than treated as invalid.

## Sequence Representation

`UserEventStreamingJob` will produce a unified sequence kind named `behavior` while continuing to write the legacy `click` sequence for click events during migration.

Each behavior row uses the existing ordered columnar representation:

- `item_id`: actual item ID for item-bearing actions; an empty sentinel only inside the sequence row for search actions because the current sequence schema requires aligned columns. The canonical search event itself keeps `item_id = null`.
- `ts`: canonical event timestamp in milliseconds.
- `action`: behavioral action name.
- Existing rating, genres, and release-year columns remain null.

The sequence writer maintains timestamp ordering and buckets using the existing encoder and sink implementation. No new Redis data structure is introduced.

## Serving Hydration

A `BehaviorSequencesQueryHydrator` reads the `behavior` sequence with action and item columns. It exposes recent item-bearing actions to the recommendation query while omitting search sentinel items. To minimize public-model churn, the first slice populates the existing watched/recent item view with stable de-duplication and a configurable mode:

- `off`: use legacy history only.
- `shadow`: read and compare behavior history without changing the query.
- `on`: prepend behavior-derived item history, then legacy history, de-duplicated and bounded.

The hydrator follows the configuration and observability pattern already used by `RatingSequencesQueryHydrator`.

## Data Flow

1. Producers serialize optional behavioral context through canonical Avro.
2. `UserEventStreamingJob` parses, watermarks, and deduplicates events by `event_id`.
3. Behavioral validation retains structurally usable search/view/click actions.
4. The job writes unified behavior sequences and legacy click sequences.
5. Serving reads behavior sequences and converts item-bearing actions into recent user history.
6. Existing retrieval, filtering, and scoring components consume the hydrated history without interface changes.

## Failure Handling

- Missing action-specific fields cause a drop-metrics rejection and no Redis mutation.
- Unknown actions are ignored by this job so recommendation feedback pipelines remain unaffected.
- Missing behavior sequence data falls back to existing history.
- Redis read failures preserve current query state according to the sequence client's existing failure behavior.
- Search query values are not logged by the hydrator.

## Testing

Implementation follows test-driven development:

- Avro contract tests prove old records still deserialize and new fields round-trip.
- Scala tests prove action-specific validation, ordering, search sentinel handling, and dual-write click compatibility.
- Sequence encoder/sink tests prove `behavior` bucket encoding remains column-aligned.
- Java tests prove `off`, `shadow`, and `on` behavior, stable de-duplication, omission of search sentinel items, and bounded history.
- Producer tests prove a deterministic search → result view → detail view → click trace carries shared identities.
- Existing Spark, Java, and Python suites guard compatibility.

## Rollout

1. Deploy the v3 schema and tolerant consumers while retaining v1/v2 decoder catalog entries.
2. Deploy the generalized streaming writer; continue legacy click writes.
3. Deploy the serving hydrator in `off` mode.
4. Enable `shadow`, compare behavior and legacy histories, then enable `on` after validation.
5. Remove legacy click dual writes only in a separate migration after all consumers have moved.

## Explicit Follow-ups

- A canonical trace lake joining observations, candidate sets, actions, propensities, outcomes, and next state.
- Statistically valid randomized logging propensities.
- A closed-loop behavior simulator with evolving latent state.
- Query normalization, privacy classification, retention, and redaction beyond the raw optional field introduced here.
- A dedicated temporal representation model rather than mapping behavior into legacy recent-item history.
