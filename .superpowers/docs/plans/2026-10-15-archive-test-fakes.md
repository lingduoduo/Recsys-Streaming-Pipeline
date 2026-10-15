# Archive/Redrive Test Fakes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the two private copies of the archive test doubles with one shared module and a `make_producer` factory fixture.

**Architecture:** A plain helper module `archive_fakes.py` (no `test_` prefix, so not collected) holds `CANONICAL_EVENT`, `Sent`, `FakeFuture`, `FakeProducer`, `write_commit_manifest`. `conftest.py` exposes `make_producer`, a function-scoped fixture returning the `FakeProducer` factory. Both test files import the helpers and request the fixture. Test-only; no service code changes.

**Tech Stack:** Python 3, pytest (default `prepend` import mode), pyarrow.

**Spec:** `.superpowers/docs/specs/2026-10-15-archive-test-fakes-design.md`

## Global Constraints

- All paths below are relative to `recsys-pipeline/integration-tests/python_modeling/`; run pytest from `recsys-pipeline/integration-tests/`.
- Baseline: the two target files pass **87** tests; `python_modeling/` passes **600**. Both counts must be unchanged after every task.
- No assertion text changes except `sent["x"]` → `sent.x` in `test_dead_letter_redrive.py`.
- No file outside `recsys-pipeline/integration-tests/python_modeling/` changes.
- Before every `git commit`, in the same shell: `test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }`.
- Code branch: `refactor/archive-test-fakes`, created from `origin/master` (not stacked on the spec branch).

## Review Focus

1. **Two producers in one test** (interrupted + resumed): `make_producer` must return a fresh object per call — it returns the class, so each call constructs anew. Covered by `test_interrupted_operation_resumes_after_durable_acknowledged_cursor` and `test_resume_after_crash_following_a_skipped_row`.
2. **`headers` passed as `None`**: redrive's old fake stored `headers or []`; the shared one stores as passed. Both services always pass a list (`archive_replay.py:787`, `dead_letter_redrive.py:459`), so redrive's `in sent.headers` assertions still see a list. Covered by `test_key_and_headers_carry_event_id_and_original_error_code`.
3. **Manifest byte format drift**: `write_commit_manifest` must emit exactly `version=2\nquery=…\nkind=…\nbatch_id=…\nrow_count=…\n` then `file=<rel>\t<size>\t<sha256>\n` per sorted parquet — both services verify it. Covered by every replay/redrive test that reads a committed archive (they fail on any mismatch).
4. **`_SUCCESS` ownership**: redrive's `commit_batch` must still touch `_SUCCESS`; replay's `write_archive` already does. Covered by `test_selection_rejects_an_incomplete_batch_directory` and every redrive selection test.
5. **Module shadowing**: `archive_fakes` must not collide with a service module name. `ls ../../services/python-modeling/archive_fakes.py` must fail (checked in Task 1 Step 1).

---

### Task 1: Shared module + fixture; migrate `test_archive_replay.py`

**Files:**
- Create: `archive_fakes.py`
- Modify: `conftest.py`
- Modify: `test_archive_replay.py` (lines 1–124 helpers; producer call sites in 13 tests)

**Interfaces:**
- Produces:
  - `archive_fakes.CANONICAL_EVENT: dict`
  - `archive_fakes.Sent(topic: str, value: bytes, key: bytes, headers: list | None)` — frozen dataclass
  - `archive_fakes.FakeProducer(*, fail_on_send=False, fail_on_delivery=False, fail_on_close=False, fail_on_send_number=None)` with `.sent: list[Sent]`, `.flushes: int`, `.closes: int`
  - `archive_fakes.write_commit_manifest(batch_root: Path, *, query_namespace: str, batch_id: int, kind: str) -> None`
  - fixture `make_producer` → `FakeProducer` (callable with the kwargs above)

- [x] **Step 1: Branch and confirm baseline / no name collision**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
git fetch -q origin && git checkout -b refactor/archive-test-fakes origin/master
ls recsys-pipeline/services/python-modeling/archive_fakes.py   # expect: No such file
cd recsys-pipeline/integration-tests
python -m pytest -q python_modeling/test_archive_replay.py python_modeling/test_dead_letter_redrive.py | tail -1   # 87 passed
```

Observed: no `archive_fakes.py` under `services/python-modeling/`; baseline 87 passed.

- [x] **Step 2: Create `archive_fakes.py`**

```python
"""Test doubles shared by test_archive_replay.py and test_dead_letter_redrive.py."""
import hashlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


CANONICAL_EVENT = {
    "event_id": "e-1",
    "request_id": "request-1",
    "session_id": "session-1",
    "user_id": "u-1",
    "item_id": "i-1",
    "event_type": "click",
    "timestamp_ms": 1718400000000,
    "position": 2,
    "user_features": {"tier": "vip"},
    "item_features": {"genre": "drama"},
    "context_features": {"device": "web"},
    "model_version": "model-1",
    "policy_version": "policy-1",
    "algorithm_version": "algorithm-1",
    "rating": 4.5,
    "negative_feedback_reason": None,
    "dwell_millis": 900,
    "completion_rate": 0.75,
    "published_at": 1718400000100,
    "new_release": False,
    "filter_reason": None,
    "unsafe_label": False,
}


@dataclass(frozen=True)
class Sent:
    topic: str
    value: bytes
    key: bytes
    headers: list | None


class FakeFuture:
    def __init__(self, delivery_error: Exception | None = None) -> None:
        self.delivery_error = delivery_error

    def get(self, timeout: float) -> None:
        if self.delivery_error:
            raise self.delivery_error


class FakeProducer:
    def __init__(
        self,
        *,
        fail_on_send: bool = False,
        fail_on_delivery: bool = False,
        fail_on_close: bool = False,
        fail_on_send_number: int | None = None,
    ) -> None:
        self.fail_on_send = fail_on_send
        self.fail_on_delivery = fail_on_delivery
        self.fail_on_close = fail_on_close
        self.fail_on_send_number = fail_on_send_number
        self.sent: list[Sent] = []
        self.flushes = 0
        self.closes = 0

    def send(self, topic: str, *, value: bytes, key: bytes, headers=None) -> FakeFuture:
        if self.fail_on_send or self.fail_on_send_number == len(self.sent) + 1:
            raise RuntimeError("broker unavailable")
        self.sent.append(Sent(topic, value, key, headers))
        return FakeFuture(RuntimeError("delivery timed out") if self.fail_on_delivery else None)

    def flush(self) -> None:
        self.flushes += 1

    def close(self) -> None:
        self.closes += 1
        if self.fail_on_close:
            raise RuntimeError("close failed")


def write_commit_manifest(
    batch_root: Path, *, query_namespace: str, batch_id: int, kind: str
) -> None:
    """Write the v2 _COMMITTED manifest both services verify: identity, row count, file inventory."""
    inventory = []
    row_count = 0
    for path in sorted(batch_root.rglob("*.parquet")):
        relative = path.relative_to(batch_root).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        inventory.append(f"{relative}\t{path.stat().st_size}\t{digest}")
        row_count += pq.ParquetFile(path).metadata.num_rows
    (batch_root / "_COMMITTED").write_text(
        f"version=2\nquery={query_namespace}\nkind={kind}\n"
        f"batch_id={batch_id}\nrow_count={row_count}\n"
        + "".join(f"file={entry}\n" for entry in inventory),
        encoding="utf-8",
    )
```

- [x] **Step 3: Add the fixture to `conftest.py`** (append; keep the existing sys.path block first so services import before `archive_fakes` is used)

```python
import pytest

from archive_fakes import FakeProducer


@pytest.fixture
def make_producer():
    """Factory for FakeProducer; call it with the failure knobs a test needs."""
    return FakeProducer
```

Put `import pytest` with the other imports at the top; the `from archive_fakes import FakeProducer` line goes after the sys.path loop.

Observed: superseded after the final review (commit `2b30e86`). A module-level `from archive_fakes import FakeProducer` loads pyarrow at collection, so with pyarrow absent every `python_modeling/` test failed to collect, defeating `test_post_training_dpo.py`'s `importorskip("pyarrow")`. The import now lives inside the fixture body.

- [x] **Step 4: Replace the helpers in `test_archive_replay.py`**

Delete `CANONICAL_EVENT` (lines 18–41), `FakeFuture`, `FakeProducer`, `_commit_inventory` (lines 47–103). Add after the `from archive_replay import ...` line:

```python
from archive_fakes import CANONICAL_EVENT, write_commit_manifest
```

Replace the body of `refresh_archive_commit` (signature unchanged):

```python
def refresh_archive_commit(
    root: Path,
    *,
    query_namespace: str = ARCHIVE_QUERY_NAMESPACE,
    batch_id: int = 7,
    kind: str = "valid",
) -> None:
    write_commit_manifest(
        root / "_queries" / query_namespace / "_batches" / str(batch_id),
        query_namespace=query_namespace,
        batch_id=batch_id,
        kind=kind,
    )
```

Keep `import hashlib` (still used elsewhere in the file).

- [x] **Step 5: Switch producer call sites to the fixture**

In each of these 13 tests, add `make_producer` as the last parameter, replace every `FakeProducer(` with `make_producer(`, and drop `-> FakeProducer` return annotations on inner `producer_factory` functions:

```
test_row_limit_blocks_publish_before_creating_producer
test_replay_preserves_identity_and_writes_manifest
test_completed_operation_id_is_a_noop_on_rerun
test_interrupted_operation_resumes_after_durable_acknowledged_cursor
test_resume_uses_physical_cursor_when_arrow_dataset_order_reverses
test_interrupted_operation_refuses_changed_source_without_rewriting_manifest
test_rate_limit_uses_monotonic_deadlines_without_real_sleep
test_publish_failure_records_failed_manifest_atomically
test_running_manifest_has_updated_at_but_no_completed_at
test_close_failure_still_records_a_failed_manifest
test_unknown_archive_fingerprint_blocks_publish_and_writes_failed_manifest
test_replay_skips_validated_zero_row_commit_and_continues
test_replay_counts_then_streams_arrow_batches_without_whole_table_materialization
```

Example (`test_row_limit_blocks_publish_before_creating_producer`):

```python
def test_row_limit_blocks_publish_before_creating_producer(tmp_path: Path, make_producer) -> None:
    write_archive(tmp_path, events=3)
    producer_created = False

    def producer_factory(_config: ReplayConfig):
        nonlocal producer_created
        producer_created = True
        return make_producer()
```

Attribute access on `sent` (`producer.sent[0].topic`, `sent.value`) is unchanged.

Observed: 15 constructions renamed. The scripted signature edit put `, make_producer)` on its own line for `test_resume_uses_physical_cursor_when_arrow_dataset_order_reverses` (no trailing comma); fixed by hand.

- [x] **Step 6: Run and verify**

```bash
cd recsys-pipeline/integration-tests
grep -c "FakeProducer" python_modeling/test_archive_replay.py   # expect 0
python -m pytest -q python_modeling/test_archive_replay.py python_modeling/test_dead_letter_redrive.py | tail -1   # 87 passed
python -m pytest -q python_modeling | tail -1   # 600 passed
```

Observed: 0 `FakeProducer` references; 87 passed; 600 passed.

- [x] **Step 7: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/{archive_fakes.py,conftest.py,test_archive_replay.py}
git commit -m "refactor(tests): archive replay fakes from a shared module via make_producer"
```

Observed: `adddcbe`.

### Task 2: Migrate `test_dead_letter_redrive.py`

**Files:**
- Modify: `test_dead_letter_redrive.py` (lines 1–135 helpers; 11 tests; `redrive` helper)

**Interfaces:**
- Consumes: `CANONICAL_EVENT`, `write_commit_manifest`, fixture `make_producer`, `Sent` attribute names from Task 1.

- [x] **Step 1: Replace the helpers**

Delete `CANONICAL_EVENT` (lines 24–47), `FakeFuture`, `FakeProducer` (lines 64–94) and `import hashlib`. Add after the `from dead_letter_redrive import (...)` block:

```python
from archive_fakes import CANONICAL_EVENT, write_commit_manifest
```

Replace `commit_batch` (signature unchanged):

```python
def commit_batch(
    root: Path, *, batch_id: int, query_namespace: str, kind: str = "dead-letter"
) -> None:
    batch_root = root / "_queries" / query_namespace / "_batches" / str(batch_id)
    (batch_root / "_SUCCESS").touch()
    write_commit_manifest(
        batch_root, query_namespace=query_namespace, batch_id=batch_id, kind=kind
    )
```

Change the `redrive` helper's signature to drop the annotation:

```python
def redrive(root: Path, producer, **overrides: object):
```

- [x] **Step 2: Switch producer call sites and `sent` access**

In each of these 11 tests, add `make_producer` as the last parameter and replace `FakeProducer(` with `make_producer(` (including the `lambda _config: FakeProducer()` passed to `monkeypatch.setattr` in `test_main_reports_counts_and_the_manifest_path`):

```
test_published_value_is_byte_identical_to_raw_value
test_publishes_only_to_the_backfill_topic
test_key_and_headers_carry_event_id_and_original_error_code
test_counts_are_reported_for_a_mixed_batch
test_rate_limit_applies_to_published_rows_not_skipped_ones
test_resume_after_crash_following_a_skipped_row
test_completed_operation_rerun_is_a_no_op
test_override_limit_allows_exceeding_max_rows
test_resume_rejects_a_changed_source_selection
test_manifest_is_written_under_the_dead_letter_root
test_main_reports_counts_and_the_manifest_path
```

Then convert dict access to attribute access:

```python
assert [sent.value for sent in producer.sent] == [payload]
assert {sent.topic for sent in producer.sent} == {"recsys_events.backfill"}
assert ("replay_operation_id", b"op-1") in sent.headers
assert ("replay_event_id", b"e-1") in sent.headers
assert ("redrive_error_code", b"unknown_fingerprint") in sent.headers
assert [sent.key for sent in resumed.sent] == [b"op-1:e-3"]
```

Check none remain: `grep -nE 'sent\["' python_modeling/test_dead_letter_redrive.py` → no output.

Observed: 13 constructions renamed; 7 `sent["x"]` reads converted (the list above omits `sent.key == b"op-1:e-1"` in the headers test); none remain.

- [x] **Step 3: Run and verify**

```bash
cd recsys-pipeline/integration-tests
grep -c "FakeProducer" python_modeling/test_dead_letter_redrive.py   # expect 0
python -m pytest -q python_modeling/test_archive_replay.py python_modeling/test_dead_letter_redrive.py | tail -1   # 87 passed
python -m pytest -q python_modeling | tail -1   # 600 passed
```

Observed: 0 `FakeProducer` references; 87 passed; 600 passed.

- [x] **Step 4: Commit**

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git add recsys-pipeline/integration-tests/python_modeling/test_dead_letter_redrive.py
git commit -m "refactor(tests): dead-letter redrive uses the shared fakes and make_producer"
```

Observed: `32d0d5b`.

### Task 3: Spec success-criteria check and PR

**Files:** none modified.

- [x] **Step 1: Uniqueness check (spec criterion 2)** — each line must print exactly one match, in `archive_fakes.py`:

```bash
cd recsys-pipeline/integration-tests/python_modeling
grep -rn "class FakeProducer" .
grep -rn "class FakeFuture" .
grep -rn "^CANONICAL_EVENT = {" .
grep -rnF 'sorted(batch_root.rglob("*.parquet"))' .
```

Observed: each pattern matched once, all in `archive_fakes.py` (lines 52, 43, 9, 90).

- [x] **Step 2: Scope check (spec criterion 4)**

```bash
git diff --stat origin/master...HEAD   # only python_modeling/{archive_fakes,conftest,test_archive_replay,test_dead_letter_redrive}.py
```

Observed: 4 files, +182/−223 — exactly the four named.

- [x] **Step 3: Push and open PR**

```bash
test "$(git branch --show-current)" != "master" || { echo "REFUSING: on master"; exit 1; }
git push -u origin refactor/archive-test-fakes
gh pr create --base master --title "refactor(tests): one set of archive/redrive fakes behind make_producer" --body-file <body>
```

PR body (write to a scratch file): a Summary listing the new `archive_fakes.py`, the `make_producer` fixture, and the two migrated files; the duplicate count removed (CANONICAL_EVENT, FakeFuture, FakeProducer, commit-manifest writer); the one assertion-shape change (`sent["x"]` → `sent.x`); Test plan with the 87/600 counts and the four uniqueness greps; link to the spec; the repo's attribution footer.

Report the PR URL and stop; do not merge.

Observed: PR #292.

## Delivery evidence

Baseline (master `1137806`): 87 passed in the two files, 600 in `python_modeling/`. After each task and after the fix: unchanged. Fresh whole-branch review (opus): no Critical/Important; confirmed `write_commit_manifest` is byte-identical to both old writers and both services always pass `headers` as a list. Its Minor finding — `conftest.py` eagerly importing pyarrow — was re-graded and fixed in `2b30e86`: with pyarrow blocked, `test_post_training_dpo.py` went from a collection error to 29 passed, 8 skipped; suite 600/600. Deferred minor: `Sent.headers` is typed `list | None` though no caller passes `None`. CI on #292: python and scala jobs passed. Delivery: spec/plan #291, code #292, both merged 2026-10-15.
