# Event Schema v2 — Context and Feedback Signals — Design

**Date:** 2026-08-19
**Status:** approved
**Depends on:** [2026-08-09-avro-kafka-ingestion-backfill-design.md](2026-08-09-avro-kafka-ingestion-backfill-design.md), [2026-08-11-late-feedback-join-design.md](2026-08-11-late-feedback-join-design.md)

## Problem

An audit of the data this pipeline generates, measured against the fields a
recommendation training record is normally expected to carry, found the event
contract thin in two places.

**Context is three keys wide, and empty where it matters most.**
`recsys-event-v1.avsc` declares `context_features` as an untyped
`map<string,string>`. Across all four producers only `device`, `country`, and
`platform` (a second name for `device`) are ever written into it — and
`movie_segment_producer.py`, the sim behind the 98,850-row dataset the committed
dashboard snapshot reports on, writes `context_features: {}` on every event. In
the dataset this project showcases, context is entirely absent. There is no
surface, no locale, and no timezone anywhere, so nothing downstream can
distinguish a home-feed impression from a search result, and no analysis can
segment by language or local time.

Because the map is untyped, the keys that do exist are invisible in the schema
and discoverable only by grepping for string literals.
`CtrRankingModelTrainingJob` reaches into it with `element_at(context_features,
"device")` and nothing would report a typo — and because the flagship sim writes
no context at all, that expression already resolves to its `"NA"` default for
every row of that dataset.

**The same dimension has two names and two homes.** The event calls it `device`;
`governance_measurements.DEFAULT_DIMENSIONS` calls it `platform`. The dashboard's
`_with_demographic_columns` hoists governance dimensions out of `user_features`
only, while the MovieLens sim writes `platform` into `context_features`, so the
dimension can never resolve. The committed snapshot records the consequence
directly: `"missing demographic dimension: platform"`, with fairness coverage at
`0.5714` — four of seven declared dimensions.

**Valence signals are missing.** The event vocabulary is `impression`, `click`,
`order` (with `purchase` as an alias), and `rating`. There is no thumb up or
down, and no abandon. The nearest proxies are `negative_feedback_reason`, whose
only produced value is `not_interested` when `completion_rate < 0.10`, and the
`favorite` / `rewatch` heads in `movielens_pipeline.py`, which are supervised
from three hardcoded user histories rather than from anything the stream emits.

A third defect surfaced while designing the fix. `OnlineJoinerStreamingJob`
attributes feedback with a single `max_by` over the latest feedback event and
takes that event's whole struct, so any later feedback event blanks the fields
the earlier one set. This is already live: `movie_segment_producer.py` copies
`completion_rate`, `dwell_millis`, and `negative_feedback_reason` forward onto
each `order` purely to stop the order from erasing its own click's engagement
signals. Adding thumb and abandon as later-arriving events would multiply that
bug rather than inherit it.

## Goal

The canonical event declares its **request context** as typed fields, carries
thumb and abandon as first-class user actions, and lands all of it in
`training_samples` — with v1 events still decoding after the bump.

This spec also settles the boundary that today is ad-hoc:

> **Request context** — what was true of the request — is a typed field on the
> event. **User attributes** — what is true of the person regardless of request —
> stay in `user_features` until sub-project B gives them their own typed
> contract. `context_features` is for ad-hoc experiment keys only.

Under that rule `device`, `surface`, `locale`, and `timezone` become typed
fields, and `country` stays in `user_features`, where `DEFAULT_DIMENSIONS` and
the dashboard's hoisting already expect it.

## Non-goals

Deliberately excluded, each deserving its own spec:

| Deferred | Why separate |
|---|---|
| CTR and reward-model *features* from the new fields | Changes what models learn and shifts every metric baseline; this spec only keeps the existing trainer working |
| Fairness or diversity broken down *by* surface or locale | Adding a published dimension changes what the dashboard asserts about groups; the `platform` → `device` rename in section 5 is in scope only because leaving it would break an existing dimension |
| Serving-side surface awareness | The serving path does not read context today; making it do so is a scoring change, not a data change |
| User tenure and first-class subscription plan | A different contract: `movielens_context` UserUpdated → `user:{id}:features`. This also carries the unresolved `tier` (`new`/`standard`/`vip`) versus `subscription` (`premium`/`basic`/`free`) split — two producers, two vocabularies, and only `subscription` declared as a dimension |
| Item synopsis and popularity trend | A different contract: MovieUpdated → `movie:{id}:features`, plus a new derived time series |
| Next-item prediction task | Consumes the existing sequence store; needs no schema change at all |

## Approach

Three alternatives were weighed for where the new context fields live.

**Typed top-level fields, with `device` promoted out of the map** (chosen). The
contract becomes self-describing and queryable without `element_at`, and it
matches how `dwell_millis`, `completion_rate`, and `published_at` are already
modeled. The cost is two lines in `CtrRankingModelTrainingJob` and the rename of
one governance dimension, both of which must be updated as part of this change
rather than after it.

`country` was originally slated for promotion too. It is excluded because it is a
user attribute, not request context: the flagship sim already writes it into
`user_features`, and `DEFAULT_DIMENSIONS` reads it from there. Promoting it would
break the fairness hoisting rather than being neutral.

**New keys inside the existing map** (rejected). Zero schema change for context
and nothing downstream breaks, but it reproduces exactly the defect this spec
exists to fix: stable dimensions staying invisible and unvalidated.

**Typed fields for the three new dimensions, map keeps the old two** (rejected).
Cheapest to build and the worst outcome — context becomes split-brain, with
`device` in a map and `surface` in a column, and no rule for which goes where.

Thumb and abandon are modeled as `event_type` values rather than as fields,
because a thumb is a distinct user action that arrives at its own time. Fields on
the click event would make a late thumb unrepresentable.

## 1. Schema v2

Add `schemas/recsys-event-v2.avsc`. `recsys-event-v1.avsc` stays on disk,
byte-for-byte unchanged, because it remains a valid writer schema for records
already in Kafka and in the archive.

Four new fields, each `["null", "string"]` with `default: null`, matching every
other optional field on the record:

| Field | Contents | Vocabulary |
|---|---|---|
| `surface` | Where the slate was rendered | `home_feed`, `search_results`, `detail_page`, `continue_watching` |
| `locale` | BCP-47 language tag | `en-US`, `en-CA`, `fr-CA`, `en-GB`, `de-DE` |
| `timezone` | IANA zone name | `America/New_York`, `America/Chicago`, `America/Denver`, `America/Los_Angeles`, `America/Toronto`, `Europe/London`, `Europe/Berlin` |
| `device` | Client platform, promoted out of `context_features` | `ios`, `android`, `web` |

`country` is deliberately not here; see the boundary rule under Goal.

The vocabularies are producer conventions, not Avro enums. Declaring them as
enums would make an unrecognized value a decode failure, which turns a
mislabeled surface into data loss; a nullable string lets an unknown value flow
through and be counted.

`context_features` survives, and its documented purpose narrows: ad-hoc
experiment keys only. Stable dimensions belong in typed fields from here on. The
schema comment and `Data_Pipeline.md` both say so.

Three new `event_type` values — `thumb_up`, `thumb_down`, `abandon` — add no
new fields to the event. A `thumb_down` carries the existing
`negative_feedback_reason`; an `abandon` carries the existing `completion_rate`
as its stopping point. They do produce two derived columns on `training_samples`,
described in section 3; the Avro record itself gains only the four context
fields above.

The schema file is duplicated: Python reads `recsys-pipeline/schemas/`, Scala
reads its own classpath resource under
`services/spark-streaming-job/src/main/resources/schemas/`. Both copies of v2
must be added, and the existing drift guard in `EventAvroCodecSpec` ("keep its
checked-in schema semantically identical to the canonical schema") extends to
cover v2.

## 2. Decoding both versions

`EventAvroCodec` today computes one `fingerprint` from one resource and returns
`DecodeFailure.UnknownFingerprint` for anything else, which `ExecutionEngine`
routes to the dead-letter archive. Bumping the schema without changing this
would dead-letter every v1 event still inside the topic's 24-hour retention the
moment a job restarts, and would leave archived v1 events permanently
unredrivable.

The codec gains a fingerprint → schema catalog built from both resources.
`decode` resolves the writer schema by the payload's fingerprint and reads it
through Avro's resolving decoder into the v2 reader schema, so a v1 record
arrives with `null` in the four new fields. Encoding always writes v2. A
fingerprint in neither entry still fails as `unknown_fingerprint`, unchanged.

Python already accepts a `catalog` argument in `decode_event`; only the default
catalog changes, from one schema to both. `encode_event` switches its default
writer schema to v2.

## 3. Joiner: per-signal attribution

Replace the single `max_by(feedback struct)` with per-field attribution: each of
`rating`, `negative_feedback_reason`, `dwell_millis`, and `completion_rate` gets
its own `max_by` over the events where *that field* is non-null, tie-broken on
`(timestamp, event_id)` exactly as today. A later event that sets only one field
then leaves the others standing.

`isFeedback` extends from `click`/`order`/`purchase` to include `thumb_up`,
`thumb_down`, and `abandon`. A narrower `isEngagementFeedback`
(`click`/`order`/`purchase` only) governs `last_feedback_ts` and
`last_feedback_ts_ms` — and therefore `feedback_delay_ms` — instead: widening
those timestamp aggregates to thumbs and abandons would have silently
redefined that existing metric, which `ExperienceCollectorStreamingJob` and
`RecommendationResponseStatsJob` both read as time-to-engagement, so a thumb
arriving long after a click would inflate it. Two output columns are added to
`training_samples`:

- `thumb` — `1` from the latest `thumb_up`, `-1` from the latest `thumb_down`,
  `null` when the user did neither. A user who thumbs up and later thumbs down
  ends at `-1`, because the latest expression wins.
- `abandoned` — `1` when an `abandon` event exists for the sample, else `0`.

The four context fields carry through from the impression, using the same
`first(when(isImpression, ...), ignoreNulls = true)` treatment as
`user_features`, and appear as four columns on `training_samples`.

Two fixed lists must learn the new names or late-arriving feedback silently
loses them: `OnlineJoinerStreamingJob.MeasurementFields` and
`LateFeedbackJoin.SnapshotColumns`.

`label` is unchanged — `2.0` ordered, `1.0` clicked, `0.0` otherwise. Thumbs do
not enter the label function, because what models learn is out of scope here.
The signals are recorded now so a later spec can use them deliberately.

With per-field attribution in place, the copy-forward workaround in
`movie_segment_producer.py` (`for field in ("completion_rate", "dwell_millis",
"negative_feedback_reason"): order[field] = click[field]`) is deleted, along
with the comment explaining it.

## 4. Producers

`locale` and `timezone` derive deterministically from each user's already-assigned
attributes and stay stable across every slate that user appears in. Values that
varied per event would be noise no analysis could find signal in; values tied to
the user are recoverable the same way the existing demographic effects are.

The movie-category sim derives both from the user's `country`: `us` →
`en-US` / `America/New_York`, `gb` → `en-GB` / `Europe/London`, `de` → `de-DE` /
`Europe/Berlin`, and `ca` → `America/Toronto` with a fixed one-in-four share of
Canadian users assigned `fr-CA` and the rest `en-CA`. The share is a documented
constant and the assignment is a function of the user index, not a per-slate
draw, so a given user's locale never changes.

The MovieLens sim has US ZIP codes only, so every user gets `en-US`, and the
timezone follows the region `derive_geo` already computes: Northeast,
Mid-Atlantic, and Southeast → `America/New_York`; Midwest and South-Central →
`America/Chicago`; Mountain → `America/Denver`; West → `America/Los_Angeles`;
`unknown` → `null`, which exercises the nullable path end to end.

`surface` is sampled per slate, with an additive ground-truth click effect table
alongside the existing `PLATFORM_EFF` and `SUBSCRIPTION_EFF`, so a report can
recover what was injected.

Thumb and abandon fire off clicks with documented probabilities tied to
`completion_rate`: high completion raises `thumb_up`, completion below the
existing `NEGATIVE_COMPLETION_CUTOFF` of 0.10 produces `abandon`, and
`thumb_down` accompanies the `not_interested` reason already emitted there.

`FEEDBACK_TYPES` in `feedback_schedule.py` must gain the three new types.
`split_slate` sends anything not in that set immediately, so without this a
thumb would be published at slate time regardless of the timestamp in its own
payload — defeating the reason for modeling it as a late event.

Producers stop writing `device` (and its alias `platform`) into
`context_features` and write the typed `device` field instead. `country` moves
the other way: `producer.py` and `backfill_producer.py` currently put it in
`context_features`, while `movie_segment_producer.py` already puts it in
`user_features`; both converge on `user_features`, so every sim presents it where
`DEFAULT_DIMENSIONS` reads it. After this change no producer writes
`context_features` at all, which is the intended end state — it holds experiment
keys only, and there are none.

## 5. Required by the change: trainer and governance

Two consumers read the moved keys by their old names and would degrade silently
rather than fail.

**`CtrRankingModelTrainingJob`** builds `cf_device` and `cf_country` from
`element_at(context_features, ...)`. Once producers stop populating those keys,
both yield the `"NA"` default for every new row — no error, just two features
quietly gone. For the flagship movie-category dataset this is already the state
today, since that sim never wrote context at all. `cf_device` therefore reads the
typed column and `cf_country` reads `element_at(user_features, "country")`, each
falling back to the legacy map key when the newer source is absent, so Parquet
written before this change stays trainable.

**`governance_measurements.DEFAULT_DIMENSIONS`** declares `platform`, which is
this event's `device` under a second name, and the dashboard hoists dimensions
out of `user_features` only — so `platform` resolves for no sim and the committed
snapshot warns `"missing demographic dimension: platform"`. The dimension is
renamed `platform` → `device`, and the hoisting reads the typed `device` column
when present, falling back to `user_features`. That closes the warning and raises
fairness coverage; `occupation` and `geo`, the other two missing dimensions, have
their own unrelated cause and stay out of scope.

Renaming a published dimension changes the dashboard snapshot, so the snapshot
fixture is regenerated as part of this work and the change is called out in the
commit.

## Error handling

| Condition | Behavior |
|---|---|
| v1 payload after the bump | Decodes through the catalog; four new fields are `null` |
| Fingerprint in neither schema | `unknown_fingerprint` dead letter, unchanged |
| Unrecognized `event_type` | Ignored by the joiner's conditional aggregation, as today — not a dead letter |
| Unrecognized `surface` / `locale` value | Flows through as data; no validation rejects it |
| `thumb_up` with no matching impression in the batch | Handled by the existing late-feedback path, like a late click |
| Old Parquet without the new columns | CTR trainer and dimension hoisting fall back to the legacy map key; other readers select by name and are unaffected |
| A sim that still writes `country` to `context_features` | Fallback path keeps it working; the dimension resolves either way |

## Testing

- **Python codec** — v2 round-trip; a v1 payload decodes through the default
  catalog with the new fields `null`; required-field validation unchanged.
- **Scala codec** — v1 bytes decode into the v2 reader shape; both checked-in
  copies stay semantically identical to their canonical counterparts; an
  unregistered fingerprint still returns `unknown_fingerprint`.
- **Joiner** — a `thumb_up` arriving after a click does **not** erase that
  click's `dwell_millis` and `completion_rate`; this test fails against today's
  code and is the regression proof for section 3. Plus: `abandon` sets
  `abandoned = 1`; a later `thumb_down` overrides an earlier `thumb_up`; the
  four context fields reach `training_samples`.
- **Late feedback** — a snapshot round-trip preserves the new columns.
- **Producers** — `locale` and `timezone` are stable per user across slates and
  consistent with that user's country; `surface` stays inside its vocabulary;
  the new event types are deferred by `split_slate` rather than sent immediately.
- **CTR trainer** — trains on a DataFrame with typed columns and on one with
  only the legacy map, producing the same features from each.
- **Governance** — `device` resolves as a dimension from both the typed column
  and a legacy `user_features` map; the fairness section no longer warns about
  it, and coverage rises accordingly.
- **Archive and replay** — a manifest spanning both versions lists both
  fingerprints.
