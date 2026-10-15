# Archive replay / dead-letter redrive tests: one set of fakes — Design

## The gap

`test_archive_replay.py` and `test_dead_letter_redrive.py` (both under
`recsys-pipeline/integration-tests/python_modeling/`) each carry a private copy of the same test
doubles:

| piece | replay | redrive | difference |
|---|---|---|---|
| `CANONICAL_EVENT` | lines 18–41 | lines 24–47 | none — byte-identical |
| `FakeFuture` | records `timeouts` | no `timeouts` | `timeouts` is never asserted anywhere |
| `FakeProducer` | `fail_on_send`, `fail_on_delivery`, `fail_on_close`, `fail_on_send_number` | `fail_on_send_number` only | replay is a strict superset |
| sent record | ad-hoc `type("Sent", ...)` object, attribute access | dict, key access, `headers or []` | shape only |
| v2 `_COMMITTED` writer | `_commit_inventory` + `refresh_archive_commit` | `commit_batch` | redrive also touches `_SUCCESS`; replay's `write_archive` touches it itself |

The `_COMMITTED` format is a contract both services parse (`version=2`, `query`, `kind`,
`batch_id`, `row_count`, one `file=<path>\t<size>\t<sha256>` per parquet). Today a change to it
must be made twice in tests, and nothing notices if only one copy is updated.

`FakeProducer` is constructed ~30 times across the two files, in five failure variants — the
"many variations of one scenario" case where a factory beats per-variant fixtures.

Baseline (2026-10-15, master `1137806`): the two files pass 87 tests; `python_modeling/` passes 600.

## Design

### `python_modeling/archive_fakes.py` (new, not collected — no `test_` prefix)

Holds the single copy of:

- `CANONICAL_EVENT` — unchanged content.
- `Sent` — `@dataclass(frozen=True)` with `topic: str`, `value: bytes`, `key: bytes`,
  `headers: list | None`. Attribute access everywhere.
- `FakeFuture(delivery_error=None)` — `get(timeout)` raises `delivery_error` if set. No
  `timeouts` list (unused).
- `FakeProducer(*, fail_on_send=False, fail_on_delivery=False, fail_on_close=False,
  fail_on_send_number=None)` — the replay version's behaviour; `sent: list[Sent]`, `flushes`,
  `closes`. `headers` stored as passed (both services always pass a list, so redrive's
  `headers or []` was a no-op).
- `write_commit_manifest(batch_root, *, query_namespace, batch_id, kind)` — computes the
  inventory and row count from `batch_root.rglob("*.parquet")` and writes `_COMMITTED`. Does
  **not** touch `_SUCCESS`; callers decide that, as they do today.

pytest's default `prepend` import mode puts `python_modeling/` on `sys.path` (the directory has
no `__init__.py`), so `from archive_fakes import ...` works from both test files without touching
`conftest.py`'s path setup.

### `python_modeling/conftest.py` — `make_producer` factory fixture

```python
@pytest.fixture
def make_producer():
    """Factory for FakeProducer; call with the failure knobs a test needs."""
    return FakeProducer
```

Function-scoped: every call returns a fresh producer, so the per-test independence the suite
relies on is kept (several tests need two producers — one interrupted, one resuming).

### Test files

- `test_archive_replay.py`: delete local `CANONICAL_EVENT`, `FakeFuture`, `FakeProducer`,
  `_commit_inventory`; import `CANONICAL_EVENT` and `write_commit_manifest`.
  `refresh_archive_commit` keeps its signature and defaults (it has many callers) and delegates
  to `write_commit_manifest`. Every `FakeProducer(...)` becomes `make_producer(...)`, with
  `make_producer` added to that test's parameters. Return annotations `-> FakeProducer` on
  inner `producer_factory` functions are dropped.
- `test_dead_letter_redrive.py`: same deletions/imports. `commit_batch` keeps its signature,
  touches `_SUCCESS`, then calls `write_commit_manifest`. `FakeProducer(...)` →
  `make_producer(...)`. `sent["topic"|"value"|"key"|"headers"]` → `sent.topic` etc. The
  `redrive(root, producer: FakeProducer, ...)` helper drops the annotation.

Out of scope: `write_archive`, `write_dead_letter_archive`, `config(...)`, and the two
`*_SCHEMA`/fingerprint constants — they differ per service and have one copy each.

## Success criteria

1. Both files pass 87 tests; `python_modeling/` passes 600 — same counts as baseline.
2. Under `python_modeling/`, each of `class FakeProducer`, `class FakeFuture`,
   `^CANONICAL_EVENT = {` and `sorted(batch_root.rglob("*.parquet"))` (the inventory loop)
   matches exactly once, in `archive_fakes.py`.
3. No test assertion text changes other than dict-key → attribute access on `sent`.
4. No file outside `recsys-pipeline/integration-tests/python_modeling/` changes in the code PR.

## Delivery

Two PRs, per the repo's spec-then-code convention: this spec plus its plan, then the code. Both
branch from `origin/master`.
