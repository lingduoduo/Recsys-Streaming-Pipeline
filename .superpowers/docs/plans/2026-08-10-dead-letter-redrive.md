# Dead-Letter Re-Drive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give operators a bounded, auditable command that recovers dead-lettered events which now decode — chiefly `unknown_fingerprint` records stranded by a deploy-ordering mistake — by republishing their original Kafka bytes to `recsys_events.backfill`.

**Architecture:** A new `dead_letter_redrive.py` reads the dead-letter archive through the `kind`-parameterized, date-pruned batch reader from the scoping work. Each selected row's `raw_value` is re-decoded against the current local catalog; rows that decode are published verbatim, rows that do not are counted and attributed to their original `error_code`. A durable operation manifest records the immutable source contract, a physical cursor for resumption, and separate examined/published/skipped counts.

**Tech Stack:** Python 3, `fastavro` via `event_avro`, Kafka Python client, pyarrow, pytest.

**Spec:** `.superpowers/docs/specs/2026-08-10-dead-letter-redrive-design.md`

**Depends on:** `2026-08-10-replay-verification-scoping.md` Task 2, which provides `_committed_parquet_files(config, kind=...)`. Do not start before it lands.

> **As built (PR #169) — shipped as planned.** Every task landed in the form written here:
> `dead_letter_redrive.py` with `validate_config`, `select_dead_letters`, `evaluate_row`,
> `RedriveManifest`, `run_redrive`, `parse_args` and `main`; `target_topic` a `ClassVar` pinned to
> `recsys_events.backfill`; the `--start-ingest-date` / `--end-ingest-date` / `--max-rows` CLI;
> `scripts/run-dead-letter-redrive.sh`; 31 tests; and the runbook section in `Data_Pipeline.md`.

## Global Constraints

- Publish only to `recsys_events.backfill`. The topic is a class variable, never an argument.
- Publish `raw_value` bytes verbatim. Never re-encode from the decoded record.
- Never publish a row that does not decode under the current local catalog.
- Never filter on `error_code`. It is recorded, never used as an eligibility test.
- Bounds select by **ingestion** date (`kafka_timestamp` partitions), not event time. Name the flags `--start-ingest-date` / `--end-ingest-date` so they cannot be confused with replay's event-time bounds.
- Enforce `--max-rows` against the selected row count before constructing a producer.
- Resumption keys on the physical `(file, row_group, row)` position, never on a published count.
- Report every skipped row; never drop one silently.
- Do not modify `RawArchiveSink.scala`, the dead-letter schema, the Scala decoder, or `run_replay`.
- Preserve existing untracked `.ua/` directories and `recsys-pipeline/kafka.png`; they are outside this implementation.

## File Structure

- `recsys-pipeline/services/python-modeling/dead_letter_redrive.py`: config, validation, selection, gate, publish loop, manifest.
- `recsys-pipeline/scripts/run-dead-letter-redrive.sh`: operator wrapper, following `run-archive-replay.sh`.
- `recsys-pipeline/integration-tests/python_modeling/test_dead_letter_redrive.py`: full coverage.
- `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`: runbook section.

---

### Task 1: Config, Validation, and Dead-Letter Selection

**Files:**
- Create: `recsys-pipeline/services/python-modeling/dead_letter_redrive.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_dead_letter_redrive.py`

**Interfaces:**
- Produces: `RedriveConfig` (frozen dataclass) with `archive_path`, `archive_query_namespace`, `operation_id`, `start_ingest_date`, `end_ingest_date`, `max_rows`, `override_limit`, `records_per_second`, `bootstrap_servers`, `manifest_dir`, and `target_topic: ClassVar[str] = "recsys_events.backfill"`.
- Produces: `RedriveConfigError`, `RedriveLimitError`.
- Produces: `validate_config(config) -> None`, `select_dead_letters(config) -> Iterable[dict]`.

**Dead-letter row columns:** `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, optional `kafka_headers`, `raw_value`, `schema_fingerprint`, `error_code`, `error_detail`, `archived_at`, `date`.

- [x] **Step 1: Write failing config and selection tests**

Build a dead-letter archive fixture with `kind="dead-letter"` and `date` partitions derived from `kafka_timestamp`.

```python
def test_bounds_are_required_and_half_open(tmp_path):
    with pytest.raises(RedriveConfigError, match="end_ingest_date"):
        validate_config(config(tmp_path, start=date(2026, 8, 2), end=date(2026, 8, 2)))

def test_selection_uses_ingestion_date_not_event_time(tmp_path):
    # archived under date=2026-08-02 by kafka_timestamp; its decoded event time is 2026-07-01
    root = write_dead_letter_archive(tmp_path, rows=[
        dead_letter(ingest_date=date(2026, 8, 2), event_timestamp_ms=1751328000000),
    ])
    assert len(list(select_dead_letters(config(root, start=date(2026, 8, 2), end=date(2026, 8, 3))))) == 1
    assert len(list(select_dead_letters(config(root, start=date(2026, 7, 1), end=date(2026, 7, 2))))) == 0

def test_target_topic_is_fixed_and_not_configurable():
    assert RedriveConfig.target_topic == "recsys_events.backfill"
    assert not hasattr(RedriveConfig, "__dataclass_fields__") or \
        "target_topic" not in RedriveConfig.__dataclass_fields__
    with pytest.raises(SystemExit):
        parse_args(BASE_ARGV + ["--target-topic", "recsys_events"])
```

`target_topic` must be a `ClassVar`, so it must not appear in `__dataclass_fields__`; an unrecognized
`--target-topic` flag must make argparse exit.

- [x] **Step 2: Run to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py`

Expected: FAIL during import — `dead_letter_redrive.py` does not exist.

- [x] **Step 3: Implement config, validation, and selection**

Mirror `archive_replay.validate_config`: path-safe identity checks for `archive_query_namespace` and `operation_id`, ISO dates with `end > start`, positive integer `max_rows`, finite positive `records_per_second`, non-blank `bootstrap_servers`.

Selection delegates to the shared reader:

```python
from archive_replay import _committed_parquet_files, _open_archive
```

Call it with `kind="dead-letter"` and the ingestion-date bounds. Read the dead-letter columns rather than `ARCHIVE_COLUMNS`.

If importing private helpers across modules reads badly under review, promote them to public names in `archive_replay` in a separate commit rather than copying the commit-protocol logic. Do not duplicate it.

- [x] **Step 4: Verify GREEN**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py`

- [x] **Step 5: Commit Task 1**

Commit message: `feat: select bounded dead-letter archive ranges`

---

### Task 2: The Decodability Gate

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/dead_letter_redrive.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dead_letter_redrive.py`

**Interfaces:**
- Produces: `evaluate_row(row: Mapping[str, object]) -> tuple[bool, str | None, dict | None]` returning eligibility, the original `error_code`, and the decoded record when it decodes.

- [x] **Step 1: Write failing gate tests**

```python
def test_unknown_fingerprint_that_now_decodes_is_eligible():
    payload = event_avro.encode_event(CANONICAL_EVENT)
    eligible, code, decoded = evaluate_row(
        {"raw_value": payload, "error_code": "unknown_fingerprint"})
    assert eligible and code == "unknown_fingerprint"
    assert decoded["event_id"] == CANONICAL_EVENT["event_id"]

def test_corrupt_payload_is_never_eligible():
    eligible, code, decoded = evaluate_row(
        {"raw_value": b"\xc3\x01" + (0).to_bytes(8, "little") + b"junk",
         "error_code": "corrupt_payload"})
    assert not eligible and decoded is None

def test_invalid_marker_is_never_eligible():
    eligible, _, _ = evaluate_row({"raw_value": b"not-avro", "error_code": "invalid_marker"})
    assert not eligible

def test_required_field_row_recovers_only_if_it_now_decodes():
    assert evaluate_row({"raw_value": event_avro.encode_event(CANONICAL_EVENT),
                         "error_code": "required_field"})[0] is True

def test_error_code_is_never_used_as_a_filter():
    # a row mislabelled corrupt_payload whose bytes are actually fine is still eligible
    assert evaluate_row({"raw_value": event_avro.encode_event(CANONICAL_EVENT),
                         "error_code": "corrupt_payload"})[0] is True
```

- [x] **Step 2: Run to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py -k "eligible or decodes or filter"`

- [x] **Step 3: Implement the gate**

```python
def evaluate_row(row):
    error_code = row.get("error_code")
    payload = row.get("raw_value")
    if not isinstance(payload, (bytes, bytearray)):
        return False, error_code, None
    try:
        return True, error_code, event_avro.decode_event(bytes(payload))
    except (event_avro.SchemaFingerprintError, event_avro.EventValidationError, ValueError):
        return False, error_code, None
```

Catch only decode-shaped failures. Do not catch `BaseException`; a bug in the codec must surface, not silently mark every row ineligible.

- [x] **Step 4: Verify GREEN**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py`

- [x] **Step 5: Commit Task 2**

Commit message: `feat: gate dead-letter re-drive on current decodability`

---

### Task 3: Publish Loop, Manifest, and Resumption

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/dead_letter_redrive.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dead_letter_redrive.py`

**Interfaces:**
- Produces: `RedriveManifest` with `selected_rows`, `examined_rows`, `published_rows`, `skipped_rows`, `skipped_by_error_code`, `acknowledged_position`, `source_paths`, `source_signature`, `schema_fingerprints`, `status`, `started_at`, `updated_at`, `completed_at`, `error`.
- Produces: `run_redrive(config, producer_factory=..., clock=None, sleeper=None) -> RedriveManifest`.

- [x] **Step 1: Write failing publish, count, and resume tests**

Reuse the `FakeProducer` pattern from `test_archive_replay.py`.

```python
def test_published_value_is_byte_identical_to_raw_value(tmp_path):
    payload = event_avro.encode_event(CANONICAL_EVENT)
    root = write_dead_letter_archive(tmp_path, rows=[dead_letter(raw_value=payload)])
    producer = FakeProducer()
    run_redrive(config(root), producer_factory=lambda _: producer)
    assert producer.sent[0]["value"] == payload

def test_key_uses_event_id_from_the_decoded_record(tmp_path):
    # the archived row carries no event_id column; it can only come from the gate's decode
    root = write_dead_letter_archive(tmp_path, rows=[
        dead_letter(raw_value=event_avro.encode_event(CANONICAL_EVENT),
                    error_code="unknown_fingerprint"),
    ])
    producer = FakeProducer()
    run_redrive(config(root, operation_id="op-1"), producer_factory=lambda _: producer)
    assert producer.sent[0]["key"] == b"op-1:e-1"
    assert ("redrive_error_code", b"unknown_fingerprint") in producer.sent[0]["headers"]

def test_counts_are_reported_for_a_mixed_batch(tmp_path):
    root = write_dead_letter_archive(tmp_path, rows=[
        dead_letter(raw_value=event_avro.encode_event(CANONICAL_EVENT),
                    error_code="unknown_fingerprint"),
        dead_letter(raw_value=b"not-avro", error_code="invalid_marker"),
        dead_letter(raw_value=b"not-avro", error_code="invalid_marker"),
    ])
    manifest = run_redrive(config(root), producer_factory=lambda _: FakeProducer())
    assert manifest.selected_rows == 3
    assert manifest.published_rows == 1
    assert manifest.skipped_rows == 2
    assert manifest.skipped_by_error_code == {"invalid_marker": 2}

def test_resume_after_crash_following_a_skipped_row(tmp_path):
    # rows: [publishable, skipped, publishable]; crash on the third send
    root = write_dead_letter_archive(tmp_path, rows=[
        dead_letter(raw_value=event_avro.encode_event(CANONICAL_EVENT | {"event_id": "e-1"})),
        dead_letter(raw_value=b"not-avro", error_code="invalid_marker"),
        dead_letter(raw_value=event_avro.encode_event(CANONICAL_EVENT | {"event_id": "e-3"})),
    ])
    crashing = FakeProducer(delivery_error=RuntimeError("broker down"), fail_after=1)
    with pytest.raises(RuntimeError):
        run_redrive(config(root), producer_factory=lambda _: crashing)

    resumed = FakeProducer()
    manifest = run_redrive(config(root), producer_factory=lambda _: resumed)
    assert [sent["key"] for sent in resumed.sent] == [b"op-1:e-3"]   # e-1 not republished
    assert manifest.published_rows == 2                              # across both runs
    assert manifest.skipped_by_error_code == {"invalid_marker": 1}   # skip counted once

def test_max_rows_raises_before_a_producer_exists(tmp_path):
    root = write_dead_letter_archive(tmp_path, rows=[dead_letter(), dead_letter()])

    def explode(_):
        raise AssertionError("producer must not be constructed")

    with pytest.raises(RedriveLimitError, match="exceeds max_rows"):
        run_redrive(config(root, max_rows=1), producer_factory=explode)

def test_completed_operation_rerun_is_a_no_op(tmp_path):
    root = write_dead_letter_archive(tmp_path, rows=[dead_letter()])
    first = run_redrive(config(root), producer_factory=lambda _: FakeProducer())
    assert first.status == "completed"

    def explode(_):
        raise AssertionError("a completed operation must not open a producer")

    second = run_redrive(config(root), producer_factory=explode)
    assert second.published_rows == first.published_rows
```

`FakeProducer` needs a `fail_after` argument so a delivery can succeed before the crash; extend the
copy in this module rather than editing `test_archive_replay.py`'s.

- [x] **Step 2: Run to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py`

- [x] **Step 3: Implement the run loop**

Follow `run_replay`'s shape: read an existing manifest, validate the immutable source contract on resume (`selected_rows`, `source_paths`, `source_signature`, fingerprints), return early when `status == "completed"`.

Order of operations, which the tests pin:

1. `validate_config`, then open the archive and count selected rows from Parquet metadata.
2. Enforce `max_rows` unless `override_limit`. **Before** any `producer_factory` call.
3. Construct the producer, then iterate positions in the same deterministic file/row-group/row order `_iter_archive_rows` uses.
4. Skip forward to `acknowledged_position` on resume.
5. For each row: run the gate. If ineligible, increment `examined_rows` and the `skipped_by_error_code` bucket, advance `acknowledged_position`, persist, and continue — **without** rate-limit sleeping, since nothing was sent.
6. If eligible: rate-limit, send `raw_value` with key `{operation_id}:{event_id}` and headers `replay_operation_id`, `replay_event_id`, `redrive_error_code`; block on the delivery future; increment `examined_rows` and `published_rows`; advance the cursor; persist the manifest.

Persist after every row, eligible or not, so a crash after a skip does not re-examine it.

Reuse `DELIVERY_TIMEOUT_SECONDS` and the producer settings from `archive_replay._make_kafka_producer`.

- [x] **Step 4: Verify GREEN**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py`

- [x] **Step 5: Commit Task 3**

Commit message: `feat: publish re-driven dead letters with a durable manifest`

---

### Task 4: Operator Wrapper and CLI

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/dead_letter_redrive.py`
- Create: `recsys-pipeline/scripts/run-dead-letter-redrive.sh`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_dead_letter_redrive.py`

- [x] **Step 1: Write failing CLI tests**

Assert `parse_args` requires `--archive-path`, `--archive-query-namespace`, `--operation-id`, `--start-ingest-date`, `--end-ingest-date`, `--max-rows`, `--records-per-second`, `--bootstrap-servers`; accepts optional `--override-limit` and `--manifest-dir`; and exposes no topic argument.

- [x] **Step 2: Run to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py -k "cli or parse_args"`

- [x] **Step 3: Implement `parse_args` and `main`**

Mirror `archive_replay`. `main` prints status, published/skipped counts, and the manifest path.

- [x] **Step 4: Add the wrapper script**

Follow `run-archive-replay.sh` exactly: `set -euo pipefail`, `require_env` for `REDRIVE_ARCHIVE_PATH`, `REDRIVE_ARCHIVE_QUERY_NAMESPACE`, `REDRIVE_OPERATION_ID`, `REDRIVE_START_INGEST_DATE`, `REDRIVE_END_INGEST_DATE`, `REDRIVE_MAX_ROWS`, `REDRIVE_RECORDS_PER_SECOND`; optional `REDRIVE_MANIFEST_DIR` and `REDRIVE_OVERRIDE_LIMIT=1`; `KAFKA_BOOTSTRAP_SERVERS` defaulting to `localhost:9092`. Mark it executable.

- [x] **Step 5: Verify GREEN**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py && bash -n scripts/run-dead-letter-redrive.sh`

- [x] **Step 6: Commit Task 4**

Commit message: `feat: add the dead-letter re-drive operator command`

---

### Task 5: Runbook and Final Verification

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`

- [x] **Step 1: Document the recovery procedure**

Extend the dead-letter paragraph with a re-drive runbook covering:

- The precondition: deploy the catalog fix to **both** the producer and the Spark job first. The gate only proves Python-side decodability.
- That bounds are **ingestion** dates from `kafka_timestamp`, unlike replay's event-time bounds.
- That eligibility is decodability, not `error_code`, and that `corrupt_payload` / `invalid_marker` rows can never be recovered.
- That the archive is append-only: a successful re-drive does not delete or mark the source rows; the manifest is the record.
- A worked example:

```bash
REDRIVE_ARCHIVE_PATH=/data/recsys-events-dead-letter \
REDRIVE_ARCHIVE_QUERY_NAMESPACE=<checkpoint-hash> \
REDRIVE_OPERATION_ID=redrive-2026-08-10 \
REDRIVE_START_INGEST_DATE=2026-08-09 REDRIVE_END_INGEST_DATE=2026-08-10 \
REDRIVE_MAX_ROWS=50000 REDRIVE_RECORDS_PER_SECOND=2000 \
  ./scripts/run-dead-letter-redrive.sh
```

- [x] **Step 2: Run the complete relevant suites**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_dead_letter_redrive.py \
          integration-tests/python_modeling/test_archive_replay.py \
          integration-tests/python_modeling/test_event_avro.py \
          integration-tests/test_avro_kafka_round_trip.py
```

Expected: all pass; the round-trip test skips without a broker.

- [x] **Step 3: Confirm scope**

Run: `git diff --stat master`

Expected: the new module, the new script, the new test file, and `Data_Pipeline.md`. `archive_replay.py` appears only if private helpers were promoted to public names in Task 1.

- [x] **Step 4: Request code review**

Use superpowers:requesting-code-review. Ask specifically whether any path can publish a record that would not decode in the pipeline, and whether resumption is correct when a crash follows a skipped row.

- [x] **Step 5: Finalize the branch**

Open a PR against `master`. Do not merge; wait for the user.
