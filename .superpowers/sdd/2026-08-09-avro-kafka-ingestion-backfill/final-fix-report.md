# Avro Kafka Final Fix Report

## Result

All accepted final-review findings were resolved in implementation commit
`4a47f171861cf69cf788ba133c20eafee8919fcc`. The complete required Python,
shell, and Scala suites are green, and the static/scope/artifact checks are clean.

Verification was performed in the isolated worktree at
`/Users/linghuang/Git/Recsys-Streaming-Pipeline/.worktrees/avro-kafka-ingestion`
against the reviewed base/head `465aa2b..130f3a7`, followed by the consolidated
fix commit above.

## Finding-by-finding corrections

### 1. Replay selects only committed batches in an explicit query namespace

- `ReplayConfig`, the CLI, and `run-archive-replay.sh` now require an explicit,
  path-safe archive query namespace.
- Replay enumerates only
  `_queries/<selected-query>/_batches/<numeric-batch>` and requires both
  `_SUCCESS` and an exact `_COMMITTED` manifest containing version, query,
  `kind=valid`, and batch identity.
- It rejects incomplete, nonnumeric, marker-mismatched, and data-empty batch
  directories. Orphan `_attempts`, `_dedupe`, and other query namespaces are
  outside the selected traversal.
- Tests cover orphan attempts, dedupe state, incomplete batches, invalid commit
  identity, multiple query namespaces, and blank/ambiguous selection.

### 2. Replay has a durable operator identity and resumable progress

- The operator must supply a stable, path-safe operation ID. Its single durable
  JSON manifest records the immutable replay contract, exact content-hashed
  source selection, row count, schema fingerprints, status, and acknowledged
  cursor.
- A completed operation ID is a no-op. An interrupted operation resumes after
  its last durably persisted Kafka acknowledgement. Reusing an operation ID for
  a different contract or source selection is refused without rewriting the
  original operation manifest.
- Source identity includes ordered relative paths, sizes, and per-file SHA-256,
  so a same-path/same-size content change is detected.
- Every publication retains the canonical `event_id`, uses Kafka key
  `<operation_id>:<event_id>`, and adds `replay_operation_id` and
  `replay_event_id` headers.
- The manifest is atomically replaced and fsynced after every acknowledged
  record. Tests cover completed reruns, post-ack interruption/resume, changed
  source refusal, stable key/headers, and atomic failure progress.

This is deliberately documented as **at-least-once**, not exactly-once. A
process can fail after Kafka acknowledges a record and before its cursor update
is persisted. The cursor narrows that window; downstream event-ID or stable-key
deduplication remains required.

### 3. Migrated Avro business sinks have stable completion contracts

- The Avro engine accepts only `DurableSink`s with nonblank, unique stable sink
  identities. Plain sinks remain supported through the legacy non-Avro engine
  overload, while an unsafe sink on the migrated route fails configuration
  before decoding or side effects.
- The engine derives a stable `(query identity, sink identity, batchId)` context
  and atomically commits a validated per-sink completion marker beneath the raw
  archive query namespace. On retry, a completed earlier sink is skipped even
  if a later sink failed.
- Kafka output preserves the deterministic business key, emits stable
  query/sink/batch headers, and enables producer idempotence. Cross-producer
  retry remains at-least-once and requires downstream stable-key deduplication.
- Parquet output writes to a UUID attempt and atomically renames it into a
  deterministic query/sink/batch directory with `_SUCCESS` and an exact commit
  manifest. Existing destinations are validated and treated as complete.
- Redis popularity increments use a Lua-guarded per-item batch ledger in the
  same Redis command as `ZINCRBY`. Sequence Redis hash updates likewise use a
  per-sequence-key batch ledger in the same Lua command; the ledger has no TTL
  so it remains valid for the checkpoint lifetime.
- The composite sequence sink combines retry-safe Redis and deterministic
  Parquet writes, so a failure after either inner effect can safely retry.
- Tests cover unsupported and duplicate identities, first-sink-success / second-
  sink-failure retry, a partial effect failure inside one sink, deterministic
  Parquet repeats, and Redis/sequence ledger identity.

### 4. Watermark bounds stored prior state, not unique current rows

- `RawArchiveSink.deduplicateValid` now applies the watermark cutoff only to
  retained prior-batch dedupe state. It no longer filters unique rows in the
  current static micro-batch merely because another current row has a much newer
  timestamp.
- The regression sends two unique events one hour apart through a ten-minute
  watermark and verifies that both reach the business sink.

### Minor findings

- Python Avro decoding now rejects trailing bytes and has a counterpart test.
- Scala test resource streams are closed.
- Kafka module stubs use pytest monkeypatch restoration rather than leaking
  through global `sys.modules`.
- `serialize_event` is annotated `Mapping[str, object] -> bytes`.

## TDD evidence

| Area | RED evidence | GREEN evidence |
| --- | --- | --- |
| Committed replay namespace and durable operation | Initial focused run: **21 failed, 2 passed** because the required config/API and behavior did not exist. | Replay focused suite: **24 passed** after implementation and the final immutable-manifest audit regression. |
| Wrapper arguments | Both new wrapper tests failed before the query namespace and operation ID were forwarded. | **2 passed** after the wrapper became explicit and narrow. |
| Python minor fixes | Trailing-byte rejection and serializer-annotation tests both failed first. | Both passed after the codec/type fixes; included in the final Python suite. |
| Watermark regression | The older unique current-batch event was dropped. | Focused Scala regression passed with both unique events delivered. |
| Durable engine contract | New configuration/retry tests first failed to compile because durable sink/context APIs were absent. | Engine and RawArchive focused specs passed after stable completion handling. |
| Kafka/Parquet/Redis/sequence sinks | Concrete durable-sink tests initially failed on missing contracts; Redis/sequence wave produced **15 compile errors** before implementation. | All affected sink, sequence, and UserEvent specs passed together and in the full Scala suite. |
| Replay manifest immutability audit | A valid same-size source rewrite was not detected and the producer factory was reached. | Content SHA-256 detection now refuses the changed source and preserves the manifest; full replay suite is **24 passed**. |
| Full-suite isolation | First full Scala run had one test-only failure: a RawArchive ordering stub reused persistent `unused-valid`, so a completion marker from an earlier run correctly skipped its sink. | The stubs now use unique temporary roots; focused RawArchive rerun and fresh full Scala rerun passed. |

## Fresh verification results

| Command | Exit | Exact result |
| --- | ---: | --- |
| Required Python pytest list from Task 7 Step 5 | 0 | **98 passed, 1 skipped, 0 failed** in 1.84s. The skipped test is the opt-in Kafka integration because `RUN_KAFKA_INTEGRATION` was not `1`. |
| `bash integration-tests/test_run_streaming_job_topic_bootstrap.sh` | 0 | **PASS**. It was run with loopback permission because the sandbox cannot bind its temporary listener. |
| `sbt -error test` | 0 | **209 passed, 0 failed, 0 errors, 0 skipped**, summed from the fresh JUnit reports. |
| `git diff --check` | 0 | No output. |
| Task 7 forbidden-pattern `rg` | 1 | No matches, the expected no-match status. |
| `bash -n` for the replay wrapper and shell integration test | 0 | No output. |
| Tracked artifact scan | 1 | No `.ua`, `kafka.png`, environment, Parquet, replay-manifest, credential, build-output, or cache artifacts matched. |

## Scope confirmation

- Kafka provisioning remains exactly the two approved catalog topics,
  `recsys_events` and `recsys_events.backfill`.
- The replay wrapper remains narrow and requires explicit query/operation
  identities.
- No derived-topic consumer was migrated; legacy non-Avro entrypoints retain
  their compatibility path.

## Residual guarantees and limitations

- Replay and Kafka business output are at-least-once. Broker acknowledgement can
  precede filesystem completion/cursor persistence, so downstream deduplication
  by retained event identity or stable business key remains mandatory.
- Kafka producer idempotence suppresses producer-session retry duplicates; it
  does not make a new producer session plus the external completion marker a
  cross-system transaction.
- Raw archive, business completion, and Parquet commits require a Hadoop
  filesystem with atomic, non-overwriting directory rename semantics (such as
  local filesystem or HDFS). Unsupported object-store rename behavior is not a
  proven guarantee.
- Redis ledger scripts assume the repository's standalone Redis deployment.
  Redis Cluster would require deliberate hash-slot colocation or a different
  commit design. The non-expiring ledgers must be retained for at least the
  corresponding checkpoint lifetime.
- The real Kafka round-trip remains an explicit opt-in integration test and was
  not exercised in this local run.
