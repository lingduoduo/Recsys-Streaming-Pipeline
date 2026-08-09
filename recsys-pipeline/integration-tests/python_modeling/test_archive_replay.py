import json
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


PYTHON_MODELING = Path(__file__).resolve().parents[2] / "services/python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

import event_avro
import archive_replay
from archive_replay import ReplayConfig, ReplayConfigError, ReplayLimitError, run_replay, select_archive


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
ARCHIVE_QUERY_NAMESPACE = "query-1"
OTHER_ARCHIVE_QUERY_NAMESPACE = "query-2"
SCHEMA_FINGERPRINT = 0x225B275F487979AB


class FakeFuture:
    def __init__(self, delivery_error: Exception | None = None) -> None:
        self.delivery_error = delivery_error
        self.timeouts = []

    def get(self, timeout: float) -> None:
        self.timeouts.append(timeout)
        if self.delivery_error:
            raise self.delivery_error


class FakeProducer:
    def __init__(
        self,
        fail_on_send: bool = False,
        fail_on_delivery: bool = False,
        fail_on_close: bool = False,
        fail_on_send_number: int | None = None,
    ) -> None:
        self.fail_on_send = fail_on_send
        self.fail_on_delivery = fail_on_delivery
        self.fail_on_close = fail_on_close
        self.fail_on_send_number = fail_on_send_number
        self.sent = []
        self.flushes = 0
        self.closes = 0

    def send(self, topic: str, *, value: bytes, key: bytes, headers=None):
        if self.fail_on_send or self.fail_on_send_number == len(self.sent) + 1:
            raise RuntimeError("broker unavailable")
        self.sent.append(
            type(
                "Sent",
                (),
                {"topic": topic, "value": value, "key": key, "headers": headers},
            )()
        )
        return FakeFuture(RuntimeError("delivery timed out") if self.fail_on_delivery else None)

    def flush(self) -> None:
        self.flushes += 1

    def close(self) -> None:
        self.closes += 1
        if self.fail_on_close:
            raise RuntimeError("close failed")


def write_archive(
    root: Path,
    events: int = 1,
    partition_date: str = "2024-06-15",
    *,
    query_namespace: str = ARCHIVE_QUERY_NAMESPACE,
    batch_id: int = 7,
    event_prefix: str = "e",
    area: str = "_batches",
    committed: bool = True,
    manifest: str | None = None,
) -> None:
    rows = []
    for index in range(events):
        event = dict(CANONICAL_EVENT)
        event["event_id"] = f"{event_prefix}-{index + 1}"
        event["timestamp_ms"] += index
        event.update(
            {
                "schema_fingerprint": SCHEMA_FINGERPRINT,
                "kafka_topic": "recsys_events",
                "kafka_partition": 2,
                "kafka_offset": 42 + index,
                "kafka_timestamp": 1718400000200,
                "archive_only": "must-not-reach-avro",
            }
        )
        rows.append(event)
    batch_root = root / "_queries" / query_namespace / area / str(batch_id)
    destination = batch_root / f"date={partition_date}"
    destination.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), destination / "part-0.parquet")
    if area == "_batches" and committed:
        (batch_root / "_SUCCESS").touch()
        inventory = sorted(
            path.relative_to(batch_root).as_posix()
            for path in batch_root.rglob("*.parquet")
        )
        committed_rows = sum(
            pq.ParquetFile(batch_root / path).metadata.num_rows for path in inventory
        )
        (batch_root / "_COMMITTED").write_text(
            manifest
            or (
                "version=2\n"
                f"query={query_namespace}\n"
                "kind=valid\n"
                f"batch_id={batch_id}\n"
                f"row_count={committed_rows}\n"
                + "".join(f"file={path}\n" for path in inventory)
            ),
            encoding="utf-8",
        )


def config(root: Path, **overrides: object) -> ReplayConfig:
    values = {
        "archive_path": root,
        "archive_query_namespace": ARCHIVE_QUERY_NAMESPACE,
        "operation_id": "operation-2024-06-15",
        "start_date": date(2024, 6, 15),
        "end_date": date(2024, 6, 16),
        "max_rows": 10,
        "override_limit": False,
        "records_per_second": 2.0,
        "bootstrap_servers": "kafka:9092",
        "manifest_dir": root / "_replay_manifests",
    }
    values.update(overrides)
    return ReplayConfig(**values)


def test_target_topic_is_fixed() -> None:
    assert ReplayConfig.target_topic == "recsys_events.backfill"
    assert "target_topic" not in ReplayConfig.__init__.__annotations__


def test_row_limit_blocks_publish_before_creating_producer(tmp_path: Path) -> None:
    write_archive(tmp_path, events=3)
    producer_created = False

    def producer_factory(_config: ReplayConfig) -> FakeProducer:
        nonlocal producer_created
        producer_created = True
        return FakeProducer()

    with pytest.raises(ReplayLimitError, match="3 rows exceeds max_rows=2"):
        run_replay(config(tmp_path, max_rows=2), producer_factory, lambda: 0.0, lambda _: None)

    assert not producer_created
    manifest = json.loads(next((tmp_path / "_replay_manifests").glob("*.json")).read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["selected_rows"] == 3


def test_selection_has_inclusive_start_and_exclusive_end_dates(tmp_path: Path) -> None:
    write_archive(tmp_path, partition_date="2024-06-15")
    write_archive(tmp_path, partition_date="2024-06-16")

    rows = list(select_archive(config(tmp_path)))

    assert [row["event_id"] for row in rows] == ["e-1"]
    assert "kafka_offset" not in rows[0]


def test_selection_ignores_orphan_attempts(tmp_path: Path) -> None:
    write_archive(tmp_path, event_prefix="committed")
    write_archive(
        tmp_path,
        event_prefix="orphan",
        batch_id=8,
        area="_attempts",
        committed=False,
    )

    rows = list(select_archive(config(tmp_path)))

    assert [row["event_id"] for row in rows] == ["committed-1"]


def test_selection_ignores_dedupe_snapshots(tmp_path: Path) -> None:
    write_archive(tmp_path, event_prefix="committed")
    write_archive(
        tmp_path,
        event_prefix="dedupe",
        batch_id=8,
        area="_dedupe",
        committed=False,
    )

    rows = list(select_archive(config(tmp_path)))

    assert [row["event_id"] for row in rows] == ["committed-1"]


def test_selection_rejects_an_incomplete_batch_directory(tmp_path: Path) -> None:
    write_archive(tmp_path, committed=False)

    with pytest.raises(ReplayConfigError, match="uncommitted or incomplete archive batch"):
        list(select_archive(config(tmp_path)))


@pytest.mark.parametrize(
    "manifest",
    [
        "version=1\nquery=wrong\nkind=valid\nbatch_id=7\n",
        "version=1\nquery=query-1\nkind=dead-letter\nbatch_id=7\n",
        "version=1\nquery=query-1\nkind=valid\nbatch_id=8\n",
    ],
)
def test_selection_rejects_a_commit_manifest_with_wrong_identity(
    tmp_path: Path, manifest: str
) -> None:
    write_archive(tmp_path, manifest=manifest)

    with pytest.raises(ReplayConfigError, match="commit identity mismatch"):
        list(select_archive(config(tmp_path)))


def test_selection_uses_only_the_explicit_query_when_multiple_exist(tmp_path: Path) -> None:
    write_archive(tmp_path, event_prefix="selected")
    write_archive(
        tmp_path,
        query_namespace=OTHER_ARCHIVE_QUERY_NAMESPACE,
        event_prefix="other",
    )

    rows = list(select_archive(config(tmp_path)))

    assert [row["event_id"] for row in rows] == ["selected-1"]


def test_selection_rejects_an_ambiguous_blank_query_namespace(tmp_path: Path) -> None:
    write_archive(tmp_path)
    write_archive(tmp_path, query_namespace=OTHER_ARCHIVE_QUERY_NAMESPACE)

    with pytest.raises(ReplayConfigError, match="archive_query_namespace"):
        list(select_archive(config(tmp_path, archive_query_namespace="")))


def test_replay_preserves_identity_and_writes_manifest(tmp_path: Path) -> None:
    write_archive(tmp_path)
    producer = FakeProducer()

    result = run_replay(config(tmp_path), lambda _: producer, lambda: 0.0, lambda _: None)

    decoded = event_avro.decode_event(producer.sent[0].value)
    assert decoded["event_id"] == "e-1"
    assert decoded["timestamp_ms"] == 1718400000000
    assert decoded["request_id"] == "request-1"
    assert producer.sent[0].topic == "recsys_events.backfill"
    assert producer.sent[0].key == b"operation-2024-06-15:e-1"
    assert producer.sent[0].headers == [
        ("replay_operation_id", b"operation-2024-06-15"),
        ("replay_event_id", b"e-1"),
    ]
    assert result.status == "completed"
    assert result.target_topic == "recsys_events.backfill"
    manifest = json.loads(result.path.read_text(encoding="utf-8"))
    assert manifest["status"] == "completed"
    assert manifest["selected_rows"] == 1
    assert manifest["schema_fingerprints"] == [SCHEMA_FINGERPRINT]
    assert manifest["operation_id"] == "operation-2024-06-15"
    assert manifest["archive_query_namespace"] == ARCHIVE_QUERY_NAMESPACE
    assert manifest["acknowledged_cursor"] == 1
    assert manifest["start_date"] == "2024-06-15"
    assert manifest["end_date"] == "2024-06-16"


def test_completed_operation_id_is_a_noop_on_rerun(tmp_path: Path) -> None:
    write_archive(tmp_path)
    first_producer = FakeProducer()
    first = run_replay(
        config(tmp_path), lambda _: first_producer, lambda: 0.0, lambda _: None
    )

    second = run_replay(
        config(tmp_path),
        lambda _: pytest.fail("completed operation must not create a producer"),
        lambda: 0.0,
        lambda _: None,
    )

    assert len(first_producer.sent) == 1
    assert second.status == "completed"
    assert second.path == first.path
    assert len(list((tmp_path / "_replay_manifests").glob("*.json"))) == 1


def test_interrupted_operation_resumes_after_durable_acknowledged_cursor(
    tmp_path: Path,
) -> None:
    write_archive(tmp_path, events=3)
    interrupted = FakeProducer(fail_on_send_number=2)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        run_replay(
            config(tmp_path), lambda _: interrupted, lambda: 0.0, lambda _: None
        )

    progress = json.loads(
        next((tmp_path / "_replay_manifests").glob("*.json")).read_text(
            encoding="utf-8"
        )
    )
    assert progress["status"] == "failed"
    assert progress["acknowledged_cursor"] == 1

    resumed = FakeProducer()
    result = run_replay(
        config(tmp_path), lambda _: resumed, lambda: 0.0, lambda _: None
    )

    assert [
        event_avro.decode_event(sent.value)["event_id"] for sent in resumed.sent
    ] == ["e-2", "e-3"]
    assert result.status == "completed"
    assert json.loads(result.path.read_text(encoding="utf-8"))[
        "acknowledged_cursor"
    ] == 3


def test_resume_uses_physical_cursor_when_arrow_dataset_order_reverses(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    write_archive(tmp_path, event_prefix="first", batch_id=7)
    write_archive(tmp_path, event_prefix="second", batch_id=8)

    with pytest.raises(RuntimeError, match="broker unavailable"):
        run_replay(
            config(tmp_path),
            lambda _: FakeProducer(fail_on_send_number=2),
            lambda: 0.0,
            lambda _: None,
        )

    original_open = archive_replay._open_archive

    def reverse_scanner_order(replay_config: ReplayConfig):
        opened = original_open(replay_config)
        return archive_replay._CommittedArchive(
            datasets=tuple(reversed(opened.datasets)),
            source_paths=opened.source_paths,
        )

    monkeypatch.setattr(archive_replay, "_open_archive", reverse_scanner_order)
    resumed = FakeProducer()
    result = run_replay(
        config(tmp_path), lambda _: resumed, lambda: 0.0, lambda _: None
    )

    assert [
        event_avro.decode_event(sent.value)["event_id"] for sent in resumed.sent
    ] == ["second-1"]
    durable = json.loads(result.path.read_text(encoding="utf-8"))
    assert durable["acknowledged_position"] == {
        "path": durable["source_paths"][1],
        "row_group": 0,
        "row": 0,
    }


def test_interrupted_operation_refuses_changed_source_without_rewriting_manifest(
    tmp_path: Path,
) -> None:
    write_archive(tmp_path, events=2)
    with pytest.raises(RuntimeError, match="broker unavailable"):
        run_replay(
            config(tmp_path),
            lambda _: FakeProducer(fail_on_send_number=2),
            lambda: 0.0,
            lambda _: None,
        )

    manifest_path = next((tmp_path / "_replay_manifests").glob("*.json"))
    original_manifest = manifest_path.read_bytes()
    archive = next(tmp_path.rglob("*.parquet"))
    table = pq.read_table(archive).drop(["date"])
    event_ids = pa.array([f"z-{index}" for index in range(1, table.num_rows + 1)])
    pq.write_table(
        table.set_column(table.schema.get_field_index("event_id"), "event_id", event_ids),
        archive,
    )

    with pytest.raises(ReplayConfigError, match="source selection changed"):
        run_replay(
            config(tmp_path),
            lambda _: pytest.fail("changed source must not create a producer"),
            lambda: 0.0,
            lambda _: None,
        )

    assert manifest_path.read_bytes() == original_manifest


def test_rate_limit_uses_monotonic_deadlines_without_real_sleep(tmp_path: Path) -> None:
    write_archive(tmp_path, events=3)
    producer = FakeProducer()
    now = [10.0]
    sleeps = []

    def sleeper(seconds: float) -> None:
        sleeps.append(seconds)
        now[0] += seconds

    run_replay(config(tmp_path, records_per_second=2.0), lambda _: producer, lambda: now[0], sleeper)

    assert sleeps == [0.5, 0.5]


def test_publish_failure_records_failed_manifest_atomically(tmp_path: Path) -> None:
    write_archive(tmp_path)

    with pytest.raises(RuntimeError, match="delivery timed out"):
        run_replay(config(tmp_path), lambda _: FakeProducer(fail_on_delivery=True), lambda: 0.0, lambda _: None)

    manifests = list((tmp_path / "_replay_manifests").glob("*.json"))
    assert len(manifests) == 1
    assert not list((tmp_path / "_replay_manifests").glob("*.tmp"))
    manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["error"] == "delivery timed out"
    assert manifest["updated_at"]
    assert manifest["completed_at"] is None


def test_running_manifest_has_updated_at_but_no_completed_at(tmp_path: Path) -> None:
    write_archive(tmp_path)

    def producer_factory(_: ReplayConfig) -> FakeProducer:
        manifest = json.loads(
            next((tmp_path / "_replay_manifests").glob("*.json")).read_text(
                encoding="utf-8"
            )
        )
        assert manifest["status"] == "running"
        assert manifest["updated_at"]
        assert manifest["completed_at"] is None
        return FakeProducer()

    completed = run_replay(
        config(tmp_path), producer_factory, lambda: 0.0, lambda _: None
    )

    assert completed.completed_at is not None


def test_close_failure_still_records_a_failed_manifest(tmp_path: Path) -> None:
    write_archive(tmp_path)

    with pytest.raises(RuntimeError, match="close failed"):
        run_replay(config(tmp_path), lambda _: FakeProducer(fail_on_close=True), lambda: 0.0, lambda _: None)

    manifest = json.loads(next((tmp_path / "_replay_manifests").glob("*.json")).read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["error"] == "close failed"


def test_invalid_config_writes_a_failed_manifest_before_reading_or_publishing(tmp_path: Path) -> None:
    invalid = config(tmp_path, end_date=date(2024, 6, 15))

    with pytest.raises(ReplayConfigError, match="end_date"):
        run_replay(invalid, lambda _: pytest.fail("producer must not be created"), lambda: 0.0, lambda _: None)

    manifest = json.loads(next((tmp_path / "_replay_manifests").glob("*.json")).read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["selected_rows"] == 0
    assert "end_date" in manifest["error"]


def test_unknown_archive_fingerprint_blocks_publish_and_writes_failed_manifest(tmp_path: Path) -> None:
    write_archive(tmp_path)
    archive = next(tmp_path.rglob("*.parquet"))
    table = pq.read_table(archive).drop(["date"])
    pq.write_table(table.set_column(table.schema.get_field_index("schema_fingerprint"), "schema_fingerprint", pa.array([7])), archive)
    producer_created = False

    def producer_factory(_: ReplayConfig) -> FakeProducer:
        nonlocal producer_created
        producer_created = True
        return FakeProducer()

    with pytest.raises(ReplayConfigError, match="schema_fingerprint"):
        run_replay(config(tmp_path), producer_factory, lambda: 0.0, lambda _: None)

    assert not producer_created
    manifest = json.loads(next((tmp_path / "_replay_manifests").glob("*.json")).read_text(encoding="utf-8"))
    assert manifest["status"] == "failed"
    assert manifest["schema_fingerprints"] == [7]


def test_mixed_archive_fingerprints_block_publish(tmp_path: Path) -> None:
    write_archive(tmp_path, events=2)
    archive = next(tmp_path.rglob("*.parquet"))
    table = pq.read_table(archive).drop(["date"])
    fingerprints = pa.array([SCHEMA_FINGERPRINT, 7])
    pq.write_table(table.set_column(table.schema.get_field_index("schema_fingerprint"), "schema_fingerprint", fingerprints), archive)

    with pytest.raises(ReplayConfigError, match="schema_fingerprint"):
        run_replay(config(tmp_path), lambda _: pytest.fail("producer must not be created"), lambda: 0.0, lambda _: None)


def test_zero_selected_rows_skips_producer_and_completes_manifest(tmp_path: Path) -> None:
    write_archive(tmp_path, partition_date="2024-06-16")

    result = run_replay(config(tmp_path), lambda _: pytest.fail("producer must not be created"), lambda: 0.0, lambda _: None)

    assert result.status == "completed"
    assert result.selected_rows == 0
    assert json.loads(result.path.read_text(encoding="utf-8"))["status"] == "completed"


def test_replay_skips_validated_zero_row_commit_and_continues(tmp_path: Path) -> None:
    empty = tmp_path / "_queries" / ARCHIVE_QUERY_NAMESPACE / "_batches" / "7"
    empty.mkdir(parents=True)
    (empty / "_SUCCESS").touch()
    (empty / "_COMMITTED").write_text(
        "version=2\nquery=query-1\nkind=valid\nbatch_id=7\nrow_count=0\n",
        encoding="utf-8",
    )
    write_archive(tmp_path, batch_id=8, event_prefix="replayable")
    producer = FakeProducer()

    result = run_replay(
        config(tmp_path), lambda _: producer, lambda: 0.0, lambda _: None
    )

    assert result.selected_rows == 1
    assert [
        event_avro.decode_event(sent.value)["event_id"] for sent in producer.sent
    ] == ["replayable-1"]


@pytest.mark.parametrize(
    "manifest",
    [
        "version=2\nquery=query-1\nkind=valid\nbatch_id=7\n",
        "version=2\nquery=query-1\nkind=valid\nbatch_id=7\nrow_count=1\n",
        "version=2\nquery=query-1\nkind=valid\nbatch_id=7\nrow_count=0\nfile=date=2024-06-15/missing.parquet\n",
    ],
)
def test_replay_rejects_empty_commit_without_exact_zero_row_metadata(
    tmp_path: Path, manifest: str
) -> None:
    batch = tmp_path / "_queries" / ARCHIVE_QUERY_NAMESPACE / "_batches" / "7"
    batch.mkdir(parents=True)
    (batch / "_SUCCESS").touch()
    (batch / "_COMMITTED").write_text(manifest, encoding="utf-8")

    with pytest.raises(ReplayConfigError, match="commit|inventory|row_count"):
        list(select_archive(config(tmp_path)))


def test_replay_counts_then_streams_arrow_batches_without_whole_table_materialization(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    write_archive(tmp_path)
    row = {**CANONICAL_EVENT, "schema_fingerprint": event_avro.schema_fingerprint(event_avro.load_schema())}

    class FakeBatch:
        def __init__(self, rows):
            self.rows = rows

        def to_pylist(self):
            return self.rows

    class FakeScanner:
        def __init__(self, rows):
            self.rows = rows

        def to_batches(self):
            return [FakeBatch(self.rows)]

    class FakeDataset:
        def __init__(self):
            self.count_filters = []
            self.scan_columns = []

        def count_rows(self, *, filter):
            self.count_filters.append(filter)
            return 1

        def scanner(self, *, columns, filter):
            self.scan_columns.append(columns)
            if columns == ["schema_fingerprint"]:
                return FakeScanner([{"schema_fingerprint": row["schema_fingerprint"]}])
            return FakeScanner([row])

        def to_table(self, **_kwargs):
            raise AssertionError("whole-table materialization is forbidden")

    dataset = FakeDataset()
    monkeypatch.setattr(archive_replay.ds, "dataset", lambda *_args, **_kwargs: dataset)
    producer = FakeProducer()

    result = run_replay(config(tmp_path), lambda _: producer, lambda: 0.0, lambda _: None)

    assert result.selected_rows == 1
    assert len(dataset.count_filters) == 1
    assert dataset.scan_columns[0] == ["schema_fingerprint"]
    assert producer.sent[0].topic == "recsys_events.backfill"


def test_requirements_declare_pyarrow_dependency() -> None:
    requirements = (PYTHON_MODELING / "requirements.txt").read_text(encoding="utf-8").splitlines()

    assert any(requirement.startswith("pyarrow>=") for requirement in requirements)
