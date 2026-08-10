# Dead-Letter Re-Drive Design

## Purpose

Give operators a bounded, auditable way to recover events that were dead-lettered by a *recoverable*
decode failure, by republishing their original Kafka bytes to `recsys_events.backfill`.

Today the dead-letter archive is write-only. `RawArchiveSink` writes it with the same commit
protocol as the valid archive, but `archive_replay` reads only manifests whose `kind` is `valid`, and
nothing else in the repository reads the dead-letter path at all.

## Motivation

The Spark decoder classifies failures into four codes: `invalid_marker`, `unknown_fingerprint`,
`corrupt_payload`, and `required_field`. Three describe genuinely bad data. One does not.

`unknown_fingerprint` fires when a producer ships a new schema before the consuming pipeline's
catalog knows it. That is a deploy-ordering mistake, not corruption. The events are perfectly valid;
the reader simply could not resolve their writer schema yet. Once the catalog is updated, the same
bytes decode cleanly — but there is no supported path to get them back into the pipeline.

The Avro ingestion design anticipated recovery from *offset loss* ("restore the missing range from
the archive through the replay path"). It did not cover *decode failure*. This closes that gap.

## Eligibility: The Decodability Gate

Each selected dead-letter row is re-decoded against the **current** local schema catalog with
`event_avro.decode_event(raw_value)`. Rows that decode cleanly are published. Rows that do not are
skipped and counted.

`error_code` is recorded for observability but never used as a filter. The gate subsumes it:

- `invalid_marker` and `corrupt_payload` can never pass — the bytes are unreadable by construction.
- `unknown_fingerprint` passes once, and only once, the catalog genuinely knows the schema.
- `required_field` passes only if the contract legitimately changed to accept the record.

This makes the command self-correcting. It cannot republish anything that would immediately
re-dead-letter, so a mistaken or incomplete catalog fix produces a report of zero published rows
rather than a loop of records cycling through the dead-letter archive.

The gate also supplies data the command cannot otherwise obtain: dead-letter rows have no `event_id`
column — only `raw_value` and Kafka lineage — and `event_id` is required for the key contract. The
decoded record is where it comes from.

## Payload: Raw Bytes, Verbatim

Published values are the archived `raw_value` bytes unchanged. They are not re-encoded from the
decoded record.

Re-encoding would stamp the *local* schema fingerprint onto a record written by a different writer.
Republishing verbatim preserves the original writer fingerprint and byte-exact content, so the Spark
decoder treats a re-driven record exactly as it treated the live one — the same code path, the same
fingerprint resolution, no second encoding to diverge.

**Residual risk, stated plainly:** the gate proves the record decodes under the *Python* catalog.
The pipeline decodes under the Scala catalog. Both resolve the same checked-in `.avsc` by
fingerprint and cross-language compatibility is covered by the existing fixture test, but they are
two implementations. The operational rule is therefore: deploy the catalog fix to the producer **and**
the Spark job before re-driving. This is documented in the runbook, not enforced in code.

## Bounds Select Ingestion Time, Not Event Time

A deliberate and load-bearing difference from replay.

`RawArchiveSink.writeDeadLetters` derives the `date` partition from `kafka_timestamp` — when the
broker received the record. `writeValid` derives it from `timestamp_ms` — when the event happened. A
dead-letter row often has no usable event time at all, since it may never have decoded.

Re-drive bounds therefore select by **ingestion date**, which is the correct axis for incident
recovery: an operator knows when the bad deploy was live, not what event times it happened to
contain.

Because sharing a flag name across two different meanings is a trap, the re-drive command names its
bounds `--start-ingest-date` and `--end-ingest-date`, and the manifest records them as
`start_ingest_date` / `end_ingest_date`. The range stays half-open UTC, `start <= date < end`.

## Safety Contract

The re-drive inherits every safety property of replay:

- **Fixed target.** Publishes only to `recsys_events.backfill`. No configurable topic.
- **Explicit bounds.** Both ingestion dates required; no defaults, no open ends.
- **Row cap before Kafka.** `--max-rows` is checked against the selected row count, read from
  Parquet metadata, before a producer is constructed. `--override-limit` is the only bypass.
- **Rate limited.** `--records-per-second` throttles sends.
- **Explicit query namespace.** Reads one archive query namespace, supplied by the operator, and
  only its validated committed numeric batch directories.
- **Durable operation identity.** A stable `--operation-id` names a resumable manifest. A completed
  operation rerun is a no-op.
- **At-least-once.** A broker acknowledgement can precede cursor persistence, so a crash in that
  window may repeat one record. Downstream `event_id` deduplication remains required.

## Manifest and Resumption

The re-drive writes its own manifest. Because `archive_path` points at the dead-letter root, the
default location `<dead-letter-root>/_replay_manifests/<operation-id>.json` is naturally separate
from replay manifests.

The cursor is `acknowledged_position` — the physical `(file, row_group, row)` triple — exactly as in
replay. Resumption must be independent of gate outcomes, so position, not published count, is the
resume key. A crash immediately after a *skipped* row resumes correctly.

Counts are reported separately because they genuinely differ:

- `selected_rows` — rows in range, known before publishing.
- `examined_rows` — rows the gate has evaluated.
- `published_rows` — rows that passed and were acknowledged.
- `skipped_rows` and `skipped_by_error_code` — a per-code breakdown of what did not pass.

The immutable source contract is retained: on resume, `selected_rows`, `source_paths`,
`source_signature`, and schema fingerprints are re-derived and must match the manifest, or the
operation refuses to continue.

Skipped rows are never silently dropped. A run that publishes nothing reports why, by code.

## Key and Header Contract

Key is `{operation_id}:{event_id}`, matching replay, so downstream deduplication needs no change.
Headers are `replay_operation_id` and `replay_event_id`, again matching replay, plus
`redrive_error_code` carrying the original dead-letter classification for observability.

Reusing the `replay_*` names is intentional: consumers of the backfill topic should not need to
learn a second contract to accept recovered records.

## Reuse and Boundaries

The command consumes the `kind`-parameterized, date-pruned batch reader introduced by
`2026-08-10-replay-verification-scoping-design.md`, called with `kind="dead-letter"`. That reader
carries the entire commit-protocol check, so the two flows cannot drift on the property that matters
most.

It writes its own manifest and publish loop rather than generalizing replay's. The record shapes,
the gate, and the count semantics differ enough that a shared abstraction today would be built for
two callers with one guess about the third. This duplication is accepted and noted; if a third
consumer appears, unify then.

## Scope

Includes:

- `services/python-modeling/dead_letter_redrive.py` — config, validation, gate, publish loop,
  manifest.
- `scripts/run-dead-letter-redrive.sh` — operator wrapper following the replay wrapper's pattern.
- Tests for the gate, byte fidelity, bounds, counts, resumption, and caps.
- Runbook documentation in `docs/recommendation_architecture/Data_Pipeline.md`.

Excludes:

- Automatic or scheduled re-drive. Operator-initiated only.
- Deleting, compacting, or marking dead-letter data after a successful re-drive. The archive stays
  append-only; the manifest is the record of what was recovered.
- Publishing to the live topic under any flag.
- Any change to `RawArchiveSink`, the dead-letter schema, or the Scala decoder.

## Testing

- **Gate:** an `unknown_fingerprint` row that now decodes is published; `corrupt_payload` and
  `invalid_marker` rows are skipped; a `required_field` row that still fails validation is skipped;
  a `required_field` row that now passes is published.
- **Byte fidelity:** the published value is byte-identical to the archived `raw_value`.
- **Key contract:** `event_id` is taken from the decoded record and forms the key; original
  `error_code` rides in the `redrive_error_code` header.
- **Counts:** `selected_rows`, `examined_rows`, `published_rows`, `skipped_rows`, and
  `skipped_by_error_code` are all correct for a mixed batch.
- **Resumption:** a crash mid-operation resumes without republishing acknowledged rows, including
  when the crash immediately follows a skipped row.
- **Caps:** `max_rows` raises before any producer is constructed; `--override-limit` bypasses it.
- **Bounds:** selection follows `kafka_timestamp` partitions; a row outside the ingestion range is
  not selected even when its decoded event time is inside it.
- **Idempotence:** rerunning a completed operation publishes nothing and returns the existing
  manifest.
- **Target:** the topic is fixed; no argument can redirect it.

## Success Criteria

- An operator can recover a window of `unknown_fingerprint` dead letters after a catalog fix, with
  explicit ingestion-date bounds, a row cap, a rate limit, and a durable manifest.
- No record is published that would not decode under the current catalog.
- Published bytes are byte-identical to what the producer originally sent.
- Every skipped row is counted and attributed to its original error code.
- An interrupted operation resumes; a completed one is a no-op.
- The command reads the dead-letter archive through the same validated commit-protocol reader as
  replay.

## Sequencing

Lands **after** `2026-08-10-replay-verification-scoping-design.md`, which provides the
`kind`-parameterized reader this command depends on.
