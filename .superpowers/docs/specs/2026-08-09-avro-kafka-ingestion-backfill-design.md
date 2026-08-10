# Avro Kafka Ingestion and Backfill Design

## Purpose

Implement one production-oriented vertical slice for `recsys_events` that replaces JSON Kafka values with vendor-neutral Apache Avro, applies explicit topic retention policies, archives source records with Kafka lineage to date-partitioned Parquet, and supports controlled replay from that archive.

This slice establishes reusable boundaries for later migration of derived topics without migrating them now. It does not add a Flink job or integrate a specific centralized schema-registry vendor.

## Scope

The slice includes:

- A checked-in Avro schema for the existing unified `recsys_events` contract.
- Avro single-object encoding for self-identifying Kafka values.
- Python producer serialization and validation.
- Spark decoding, validation, dead-letter handling, and existing event transformations.
- Explicit provisioning of `recsys_events` and `recsys_events.backfill`.
- Per-topic retention calculation and storage-budget validation.
- A raw, date-partitioned Parquet archive with Kafka lineage.
- A bounded, rate-limited archive-to-Kafka replay command.
- Unit, compatibility, and locally runnable integration tests.

The slice excludes:

- Migration of `training_samples`, `training_experiences`, or other derived topics.
- A production warehouse connector. Parquet is the local warehouse-table analogue.
- Confluent, Apicurio, AWS Glue, or another vendor-specific registry protocol.
- A new Flink processing job.
- Capacity certification at one million messages per second. The topic calculator and configuration prepare for capacity planning, but production throughput requires representative load testing and infrastructure sizing.

## Wire Contract and Schema Evolution

Kafka values use Avro single-object encoding. Each value contains Avro's standard two-byte marker, the little-endian 64-bit parsing canonical form fingerprint, and the binary Avro record. This avoids a proprietary envelope and allows Spark and Flink consumers to identify the writer schema without a registry-vendor wire format.

The canonical schema is checked in as `recsys-event-v1.avsc`. It contains:

- Required identity and classification fields: `event_id`, `user_id`, `item_id`, and `event_type`.
- Optional correlation fields: `request_id` and `session_id`.
- Required millisecond event time: `timestamp_ms`.
- Optional position, feature maps, model and policy versions, rating and feedback measurements, publication metadata, and safety/filter fields already accepted by the current Spark pipeline.

The legacy seconds-based `timestamp` field is not written by the new producer. The Spark boundary normalizes decoded `timestamp_ms` to the existing internal seconds representation where existing transformations require it.

Schema evolution is additive:

- New fields must have defaults.
- Existing field names and semantic meanings remain stable.
- Existing field types may change only through a compatible Avro promotion.
- Removing required fields or changing record identity is rejected.

A schema compatibility test compares the current schema with the versioned baseline. A local schema-catalog interface resolves schemas by fingerprint. Its boundary is deliberately narrow so a future internal-registry adapter can replace local lookup without changing producer or pipeline logic.

## Architecture and Data Flow

```text
Python producer
  -> validate against recsys-event-v1.avsc
  -> Avro single-object bytes
  -> Kafka recsys_events
  -> Spark decode and fingerprint validation
       -> valid records: archive raw canonical event + Kafka lineage to Parquet
       -> invalid records: dead-letter Parquet with coordinates and error code
       -> existing deduplication, join, Redis, and derived-data paths

Parquet archive/date=YYYY-MM-DD
  -> bounded replay command
  -> preserve event_id and timestamp_ms
  -> Avro single-object bytes
  -> rate-limited Kafka recsys_events.backfill
```

The schema is the source of truth at the Kafka boundary. Spark obtains the SQL schema from Avro rather than maintaining an independent input `StructType`. Existing transformation-specific schemas may remain where they describe derived internal frames rather than the Kafka contract.

The primary and replay topics remain separate. Consumers must explicitly subscribe to the replay topic or combine it with the live topic. This prevents an accidental replay from entering every live consumer group.

## Topic Provisioning and Retention

Broker-side automatic topic creation is disabled in the local stack after a provisioning script creates the required topics.

The declarative topic catalog contains, per topic:

- Partition count.
- Replication factor.
- Expected peak messages per second.
- Expected average encoded record bytes.
- Retention duration.
- Retention byte ceiling.
- Cleanup policy.
- Schema subject and fingerprint.

`recsys_events` uses `cleanup.policy=delete` and independently configurable `retention.ms` and `retention.bytes`. `recsys_events.backfill` has a shorter retention window, its own partitions, and a lower expected publish rate.

The policy calculator uses:

```text
daily_bytes = messages_per_second * average_encoded_bytes * 86,400
required_storage = daily_bytes * retention_days * replication_factor * overhead_factor
```

It rejects invalid values and configurations whose required storage exceeds the declared budget. It emits the exact Kafka topic configuration arguments used by the provisioning script. The calculator does not claim achievable throughput; partition count and broker capacity must be confirmed with production-representative load tests.

## Archive Model

The raw archive stores the decoded canonical event together with:

- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_timestamp`
- `schema_fingerprint`
- `archived_at`
- `date`, derived from event time and used as the Parquet partition

Archival occurs before transformations that aggregate or discard source fields. Kafka coordinates provide lineage and permit precise investigation. `event_id` is the logical idempotency key for replay and downstream deduplication.

Parquet is the local warehouse-table analogue. A future warehouse sink must preserve the same columns and partition semantics so the replay reader can be adapted without changing its safety contract.

## Backfill and Replay Safety

The replay command:

- Requires the explicit archive query namespace and reads only its validated committed numeric
  batch directories; attempts, dedupe state, incomplete batches, and other query owners are not
  eligible.
- Requires a stable operator-supplied operation ID. Its deterministic manifest and acknowledged
  cursor allow an interrupted operation to resume and make a completed rerun a no-op.
- Requires an explicit inclusive start date and exclusive end date.
- Reads only matching archive partitions.
- Requires a maximum row count and fails when the selection exceeds it unless the operator explicitly overrides the guard.
- Publishes only to `recsys_events.backfill`; arbitrary target topics are not accepted.
- Preserves `event_id`, `timestamp_ms`, and the canonical record contents.
- Re-encodes records with their resolved writer schema.
- Applies a configurable records-per-second limit.
- Produces a manifest with operation ID, ordered source signature and date bounds, selected row
  count, acknowledged cursor, schema fingerprints, publish target, timestamps, and final status.
- Publishes a stable `(operation_id, event_id)` key/metadata contract while retaining `event_id` in
  the canonical value.

Stable event IDs let downstream consumers deduplicate records. The separate topic prevents replay
from surprising live-only consumer groups. Replay is at-least-once: a Kafka acknowledgement can
precede cursor persistence, so a crash in that interval can repeat one record and downstream
event-ID deduplication remains required.

## Error Handling and Availability

The Spark decoder distinguishes at least:

- Invalid single-object marker.
- Unknown schema fingerprint.
- Truncated or corrupt Avro payload.
- Decoded record failing required-field validation.

Invalid records are written to a dead-letter Parquet path with the original Kafka coordinates, raw bytes, error code, error detail, and detection timestamp. Valid records in the same micro-batch continue through the pipeline.

The primary Kafka source fails on aged-out or missing offsets instead of silently setting `failOnDataLoss=false`. Operational recovery is then an explicit choice: restore the missing range from the archive through the replay path, or deliberately reset the checkpoint/offset with an audit trail.

Sink failures continue to fail the micro-batch after configured retry attempts so Spark does not commit source progress for an incomplete batch. Archive and transformed sinks must be written using deterministic batch identity or idempotent output conventions to tolerate Spark retry.

## Testing

The implementation follows test-driven development and includes:

- Python tests for valid serialization, required-field rejection, defaults, deterministic schema fingerprinting, and malformed input.
- Scala tests that decode bytes produced by the Python implementation, proving cross-language compatibility.
- Scala tests for invalid markers, unknown fingerprints, corrupt payloads, required-field validation, and dead-letter classification.
- Schema-evolution tests that permit additive defaulted fields and reject incompatible changes.
- Topic-policy tests for daily-byte calculations, storage-budget enforcement, invalid inputs, and exact provisioning arguments.
- Spark tests for raw archive columns, Kafka lineage preservation, event-date partition derivation, and separation from transformed output.
- Replay tests for mandatory bounds, maximum-row enforcement, fixed target topic, event preservation, rate-limit configuration, and manifest contents.
- An opt-in local integration test that provisions Kafka, produces Avro, consumes and archives it through Spark, and replays a bounded range to the backfill topic.

Flink interoperability is documented using the standard Avro single-object contract and shared `.avsc` file. It is not claimed as tested until a Flink compatibility fixture or job is added in a later slice.

## Delivery Sequence

Implementation will proceed as one vertical slice in these dependency-ordered stages:

1. Schema artifact, fingerprint/catalog boundary, and Python codec.
2. Spark codec and Python-to-Scala compatibility fixture.
3. Topic catalog, retention calculator, and provisioning.
4. Raw archive and dead-letter paths in the Spark ingestion boundary.
5. Bounded Parquet replay to `recsys_events.backfill`.
6. Local integration coverage and operational documentation.

Each stage remains independently testable. Derived-topic Avro migration is deferred until this slice is verified.

## Success Criteria

The slice is complete when:

- The Python producer emits Avro single-object records for `recsys_events`.
- Spark decodes the same records using the checked-in schema and rejects or dead-letters invalid values explicitly.
- Topic creation is declarative and applies distinct retention policies to live and replay topics.
- Every valid source record can be found in date-partitioned Parquet with its Kafka lineage and schema fingerprint.
- An operator can safely replay an explicitly bounded archive range only to `recsys_events.backfill`, with rate limiting and an audit manifest.
- Unit and compatibility tests pass, and the opt-in local integration path is documented.
