# Replay Verification Scoping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make archive replay verify only the batches it reads, so replaying one day costs one day, and a damaged batch elsewhere in the archive no longer blocks recovery.

**Architecture:** `_committed_parquet_files` gains a cheap pruning pass — list each numeric batch directory's `date=*` children and skip batches that do not intersect the requested range — before running the existing, unchanged commit-protocol validation on the batches that remain. The verified per-file digests it already computes are threaded into `_source_signature` so no selected file is hashed twice. The reader is parameterized by manifest `kind` so the dead-letter re-drive can reuse it.

**Tech Stack:** Python 3, pyarrow (`dataset`, `parquet`), pytest.

**Spec:** `.superpowers/docs/specs/2026-08-10-replay-verification-scoping-design.md`

## Global Constraints

- Do not weaken validation for any batch a replay reads from. The full check — `_SUCCESS`, `_COMMITTED`, version 2, query/kind/batch identity, exact path+size+SHA-256 inventory, declared row count equals actual row count — stays intact and complete.
- Do not change the commit protocol, the manifest format, or anything under `RawArchiveSink.scala`.
- `source_signature`, `selected_rows`, and `source_paths` must be byte-identical to their pre-change values for the same archive and range. Operations started before this change must resume after it.
- Do not change `run_replay`, the cursor, the publish loop, or the CLI surface.
- `kind` must default to `"valid"`, so every existing caller is unaffected.
- Preserve existing untracked `.ua/` directories and `recsys-pipeline/kafka.png`; they are outside this implementation.

## File Structure

- `recsys-pipeline/services/python-modeling/archive_replay.py`: pruning pass, `kind` parameter, digest reuse.
- `recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py`: new pruning/signature tests; existing corruption tests retargeted in-range.
- `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`: document the scoped verification guarantee.

---

### Task 1: Prune Batches by Partition Date Before Validating

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/archive_replay.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py`

**Interfaces:**
- Produces: `_batch_partition_dates(batch_directory: Path) -> tuple[date, ...]`
- Modifies: `_committed_parquet_files(config: ReplayConfig, kind: str = "valid") -> tuple[Path, ...]`

- [ ] **Step 1: Write failing tests for pruning and retained in-range validation**

Use the existing `write_archive` / `refresh_archive_commit` helpers. Build an archive with several batches across several dates.

```python
def test_untouched_batches_are_never_hashed(tmp_path, monkeypatch):
    root = write_archive(tmp_path, batches=[
        (0, date(2026, 8, 1)), (1, date(2026, 8, 2)), (2, date(2026, 8, 3)),
    ])
    hashed = []
    original = archive_replay._file_sha256
    monkeypatch.setattr(archive_replay, "_file_sha256",
                        lambda path: hashed.append(Path(path)) or original(path))
    files = archive_replay._committed_parquet_files(
        config(root, start_date=date(2026, 8, 2), end_date=date(2026, 8, 3)))
    assert len(files) == 1
    assert all("date=2026-08-02" in p.as_posix() for p in hashed)

def test_corrupt_batch_outside_range_does_not_block_replay(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1)), (1, date(2026, 8, 2))])
    (root / "_queries" / NAMESPACE / "_batches" / "0" / "_COMMITTED").write_text("version=2\ngarbage=1\n")
    files = archive_replay._committed_parquet_files(
        config(root, start_date=date(2026, 8, 2), end_date=date(2026, 8, 3)))
    assert len(files) == 1

def test_corrupt_batch_inside_range_still_raises(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1))])
    (root / "_queries" / NAMESPACE / "_batches" / "0" / "_COMMITTED").write_text("version=2\ngarbage=1\n")
    with pytest.raises(ReplayConfigError, match="commit identity mismatch"):
        archive_replay._committed_parquet_files(
            config(root, start_date=date(2026, 8, 1), end_date=date(2026, 8, 2)))

def test_zero_row_batch_in_range_is_skipped(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1))], empty_batches=[1])
    files = archive_replay._committed_parquet_files(
        config(root, start_date=date(2026, 8, 1), end_date=date(2026, 8, 2)))
    assert len(files) == 1

def test_batch_straddling_the_boundary_is_fully_validated(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1)), (0, date(2026, 8, 2))])
    files = archive_replay._committed_parquet_files(
        config(root, start_date=date(2026, 8, 2), end_date=date(2026, 8, 3)))
    assert len(files) == 1 and "date=2026-08-02" in files[0].as_posix()
```

Extend `write_archive` to accept multiple `(batch_id, date)` pairs and an `empty_batches` argument that commits a zero-row batch with an empty inventory.

- [ ] **Step 2: Run the focused tests to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py -k "hashed or outside_range or straddling or zero_row"`

Expected: FAIL — every batch is currently hashed, and a corrupt batch outside the range currently raises.

- [ ] **Step 3: Add the pruning pass**

Add a helper that reads partition dates from directory names only — no file opens:

```python
def _batch_partition_dates(batch_directory: Path) -> tuple[date, ...]:
    dates = []
    for child in batch_directory.iterdir():
        if not child.is_dir() or not child.name.startswith("date="):
            continue
        try:
            dates.append(date.fromisoformat(child.name.removeprefix("date=")))
        except ValueError as exc:
            raise ReplayConfigError(
                f"archive batch has invalid date partition: {child}") from exc
    return tuple(sorted(dates))
```

In `_committed_parquet_files`, immediately after the numeric-name check and before touching `_SUCCESS`, `_COMMITTED`, or any Parquet file:

```python
partition_dates = _batch_partition_dates(batch_directory)
if not any(config.start_date <= value < config.end_date for value in partition_dates):
    continue
```

Everything downstream of that guard stays exactly as it is.

- [ ] **Step 4: Verify GREEN and confirm no regression**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py`

Expected: the new tests pass. Some pre-existing corruption tests may now fail because their damaged batch sits outside the range being replayed — that is the intended behaviour change, and Task 3 retargets them. Do not weaken the guard to make them pass.

- [ ] **Step 5: Commit Task 1**

Commit message: `perf: prune archive batches by partition date before verifying`

---

### Task 2: Parameterize the Reader by Manifest Kind

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/archive_replay.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py`

**Interfaces:**
- Produces: `_committed_parquet_files(config, kind="valid")` accepting `"dead-letter"`.
- Consumed by the dead-letter re-drive plan, `2026-08-10-dead-letter-redrive.md`.

- [ ] **Step 1: Write failing kind tests**

```python
def test_dead_letter_kind_reads_dead_letter_batches(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1))], kind="dead-letter")
    files = archive_replay._committed_parquet_files(
        config(root, start_date=date(2026, 8, 1), end_date=date(2026, 8, 2)),
        kind="dead-letter")
    assert len(files) == 1

def test_kind_mismatch_is_rejected(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1))], kind="dead-letter")
    with pytest.raises(ReplayConfigError, match="commit identity mismatch"):
        archive_replay._committed_parquet_files(
            config(root, start_date=date(2026, 8, 1), end_date=date(2026, 8, 2)))
```

Extend `write_archive` and `refresh_archive_commit` to take a `kind` argument that is written into the `_COMMITTED` manifest.

- [ ] **Step 2: Run to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py -k "kind"`

Expected: FAIL — `kind` is hardcoded to `"valid"` in `expected_identity`.

- [ ] **Step 3: Thread the parameter through**

Add `kind: str = "valid"` to `_committed_parquet_files` and use it in `expected_identity`. Add the same defaulted parameter to `_open_archive`. Leave every existing call site unchanged so replay keeps reading `valid`.

- [ ] **Step 4: Verify GREEN**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py -k "kind"`

- [ ] **Step 5: Commit Task 2**

Commit message: `refactor: parameterize the archive batch reader by manifest kind`

---

### Task 3: Reuse Verified Digests and Retarget Corruption Tests

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/archive_replay.py`
- Modify: `recsys-pipeline/integration-tests/python_modeling/test_archive_replay.py`

**Interfaces:**
- Modifies: `_open_archive` to carry verified `_ArchiveFileIdentity` values.
- Modifies: `_source_signature(config, archive)` to consume them without changing its output value.

- [ ] **Step 1: Write the signature-stability and single-hash tests**

The golden value protects in-flight operations. Capture it from the current implementation **before** editing `_source_signature`, and hardcode it.

```python
def test_source_signature_is_unchanged(tmp_path):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1))], seed=1)
    replay_config = config(root, start_date=date(2026, 8, 1), end_date=date(2026, 8, 2))
    archive = archive_replay._open_archive(replay_config)
    assert archive_replay._source_signature(replay_config, archive) == GOLDEN_SIGNATURE

def test_each_selected_file_is_hashed_once(tmp_path, monkeypatch):
    root = write_archive(tmp_path, batches=[(0, date(2026, 8, 1))])
    hashed = []
    original = archive_replay._file_sha256
    monkeypatch.setattr(archive_replay, "_file_sha256",
                        lambda path: hashed.append(Path(path)) or original(path))
    replay_config = config(root, start_date=date(2026, 8, 1), end_date=date(2026, 8, 2))
    archive = archive_replay._open_archive(replay_config)
    archive_replay._source_signature(replay_config, archive)
    assert len(hashed) == len(set(hashed))
```

- [ ] **Step 2: Run to verify RED**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py -k "signature or hashed_once"`

Expected: `test_each_selected_file_is_hashed_once` FAILS — each selected file is hashed once during validation and again in `_source_signature`. `test_source_signature_is_unchanged` passes now and must keep passing.

- [ ] **Step 3: Thread verified identities through**

Have `_committed_parquet_files` return, alongside its paths, a mapping from absolute path to the verified `_ArchiveFileIdentity`. Carry it on `_CommittedArchive` as a new field. In `_source_signature`, look up `size` and `sha256` from that mapping instead of calling `path.stat()` and `_file_sha256`.

The JSON document `_source_signature` hashes must be unchanged: same keys, same order, same relative-path form, same values. Only the source of the size and digest changes.

- [ ] **Step 4: Retarget the pre-existing corruption tests**

Every existing test that damages a batch and asserts a raise must place the damaged batch **inside** the replayed range. Adjust the range or the batch's partition date; do not change the assertion or the expected error. Each of these still proves the commit protocol is enforced for data that is read.

Pair each with its counterpart from Task 1 proving the same damage outside the range is ignored.

- [ ] **Step 5: Run the full replay suite**

Run: `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py`

Expected: all tests pass, including the 28 pre-existing ones.

- [ ] **Step 6: Commit Task 3**

Commit message: `perf: hash each selected archive file once per replay`

---

### Task 4: Document and Verify

**Files:**
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`

- [ ] **Step 1: Document the scoped guarantee**

In the "Archive and bounded replay operations" section, replace the claim that replay validates all committed batches. State that replay validates every batch it reads from, in full; that batches whose partitions fall outside the requested range are skipped without validation; and that a damaged batch outside the range therefore no longer blocks recovery. Note explicitly that replay is not a whole-archive integrity audit.

- [ ] **Step 2: Run the complete relevant suites**

Run:

```bash
cd recsys-pipeline
pytest -q integration-tests/python_modeling/test_archive_replay.py \
          integration-tests/python_modeling/test_event_avro.py \
          integration-tests/python_modeling/test_topic_policy.py \
          integration-tests/test_avro_kafka_round_trip.py
```

Expected: all pass; the round-trip test skips without a broker.

- [ ] **Step 3: Confirm scope**

Run: `git diff --stat master`

Expected: only `archive_replay.py`, `test_archive_replay.py`, and `Data_Pipeline.md`. No Scala, no schema, no commit-protocol change.

- [ ] **Step 4: Request code review**

Use superpowers:requesting-code-review. Ask specifically whether any batch that contributes a published row can now escape full validation — that is the one property this change must not break.

- [ ] **Step 5: Finalize the branch**

Open a PR against `master`. Do not merge; wait for the user.
