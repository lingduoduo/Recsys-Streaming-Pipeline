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
    ) -> None:
        self.fail_on_send = fail_on_send
        self.fail_on_delivery = fail_on_delivery
        self.fail_on_close = fail_on_close
        self.sent = []
        self.flushes = 0
        self.closes = 0

    def send(self, topic: str, *, value: bytes, key: bytes):
        if self.fail_on_send:
            raise RuntimeError("broker unavailable")
        self.sent.append(type("Sent", (), {"topic": topic, "value": value, "key": key})())
        return FakeFuture(RuntimeError("delivery timed out") if self.fail_on_delivery else None)

    def flush(self) -> None:
        self.flushes += 1

    def close(self) -> None:
        self.closes += 1
        if self.fail_on_close:
            raise RuntimeError("close failed")


def write_archive(root: Path, events: int = 1, partition_date: str = "2024-06-15") -> None:
    rows = []
    for index in range(events):
        event = dict(CANONICAL_EVENT)
        event["event_id"] = f"e-{index + 1}"
        event["timestamp_ms"] += index
        event.update(
            {
                "schema_fingerprint": 0x225B275F487979AB,
                "kafka_topic": "recsys_events",
                "kafka_partition": 2,
                "kafka_offset": 42 + index,
                "kafka_timestamp": 1718400000200,
                "archive_only": "must-not-reach-avro",
            }
        )
        rows.append(event)
    destination = root / "_queries" / "query-1" / "_batches" / "7" / f"date={partition_date}"
    destination.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), destination / "part-0.parquet")


def config(root: Path, **overrides: object) -> ReplayConfig:
    values = {
        "archive_path": root,
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


def test_replay_preserves_identity_and_writes_manifest(tmp_path: Path) -> None:
    write_archive(tmp_path)
    producer = FakeProducer()

    result = run_replay(config(tmp_path), lambda _: producer, lambda: 0.0, lambda _: None)

    decoded = event_avro.decode_event(producer.sent[0].value)
    assert decoded["event_id"] == "e-1"
    assert decoded["timestamp_ms"] == 1718400000000
    assert decoded["request_id"] == "request-1"
    assert producer.sent[0].topic == "recsys_events.backfill"
    assert producer.sent[0].key == b"request-1"
    assert result.status == "completed"
    assert result.target_topic == "recsys_events.backfill"
    manifest = json.loads(result.path.read_text(encoding="utf-8"))
    assert manifest["status"] == "completed"
    assert manifest["selected_rows"] == 1
    assert manifest["schema_fingerprints"] == [0x225B275F487979AB]
    assert manifest["start_date"] == "2024-06-15"
    assert manifest["end_date"] == "2024-06-16"


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
    fingerprints = pa.array([0x225B275F487979AB, 7])
    pq.write_table(table.set_column(table.schema.get_field_index("schema_fingerprint"), "schema_fingerprint", fingerprints), archive)

    with pytest.raises(ReplayConfigError, match="schema_fingerprint"):
        run_replay(config(tmp_path), lambda _: pytest.fail("producer must not be created"), lambda: 0.0, lambda _: None)


def test_zero_selected_rows_skips_producer_and_completes_manifest(tmp_path: Path) -> None:
    write_archive(tmp_path, partition_date="2024-06-16")

    result = run_replay(config(tmp_path), lambda _: pytest.fail("producer must not be created"), lambda: 0.0, lambda _: None)

    assert result.status == "completed"
    assert result.selected_rows == 0
    assert json.loads(result.path.read_text(encoding="utf-8"))["status"] == "completed"


def test_replay_counts_then_streams_arrow_batches_without_whole_table_materialization(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
