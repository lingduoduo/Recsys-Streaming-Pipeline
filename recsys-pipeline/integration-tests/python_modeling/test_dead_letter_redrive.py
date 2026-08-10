import hashlib
import sys
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


PYTHON_MODELING = Path(__file__).resolve().parents[2] / "services/python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

import event_avro
import dead_letter_redrive
from dead_letter_redrive import (
    RedriveConfig,
    RedriveConfigError,
    select_dead_letters,
)


ARCHIVE_QUERY_NAMESPACE = "query-1"
INGEST_DATE = "2024-06-15"

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

DEAD_LETTER_SCHEMA = pa.schema(
    [
        ("kafka_topic", pa.string()),
        ("kafka_partition", pa.int32()),
        ("kafka_offset", pa.int64()),
        ("kafka_timestamp", pa.int64()),
        ("raw_value", pa.binary()),
        ("schema_fingerprint", pa.int64()),
        ("error_code", pa.string()),
        ("error_detail", pa.string()),
        ("archived_at", pa.int64()),
    ]
)


class FakeFuture:
    def __init__(self, delivery_error: Exception | None = None) -> None:
        self.delivery_error = delivery_error

    def get(self, timeout: float) -> None:
        if self.delivery_error:
            raise self.delivery_error


class FakeProducer:
    def __init__(self, fail_on_send_number: int | None = None) -> None:
        self.fail_on_send_number = fail_on_send_number
        self.sent = []
        self.flushes = 0
        self.closes = 0

    def send(self, topic: str, *, value: bytes, key: bytes, headers=None):
        if self.fail_on_send_number == len(self.sent) + 1:
            raise RuntimeError("broker unavailable")
        self.sent.append(
            {"topic": topic, "value": value, "key": key, "headers": headers or []}
        )
        return FakeFuture()

    def flush(self) -> None:
        self.flushes += 1

    def close(self) -> None:
        self.closes += 1


def encoded(**overrides: object) -> bytes:
    """Avro single-object bytes that decode cleanly under the current catalog."""
    return event_avro.encode_event({**CANONICAL_EVENT, **overrides})


def dead_letter(
    *,
    raw_value: bytes | None = None,
    error_code: str = "unknown_fingerprint",
    error_detail: str = "no schema for fingerprint 7",
    schema_fingerprint: int | None = 7,
    kafka_offset: int = 42,
) -> dict:
    return {
        "kafka_topic": "recsys_events",
        "kafka_partition": 2,
        "kafka_offset": kafka_offset,
        "kafka_timestamp": 1718400000200,
        "raw_value": encoded() if raw_value is None else raw_value,
        "schema_fingerprint": schema_fingerprint,
        "error_code": error_code,
        "error_detail": error_detail,
        "archived_at": 1718400000300,
    }


def commit_batch(
    root: Path, *, batch_id: int, query_namespace: str, kind: str = "dead-letter"
) -> None:
    batch_root = root / "_queries" / query_namespace / "_batches" / str(batch_id)
    inventory = []
    row_count = 0
    for path in sorted(batch_root.rglob("*.parquet")):
        relative = path.relative_to(batch_root).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        inventory.append(f"{relative}\t{path.stat().st_size}\t{digest}")
        row_count += pq.ParquetFile(path).metadata.num_rows
    (batch_root / "_SUCCESS").touch()
    (batch_root / "_COMMITTED").write_text(
        f"version=2\nquery={query_namespace}\nkind={kind}\n"
        f"batch_id={batch_id}\nrow_count={row_count}\n"
        + "".join(f"file={entry}\n" for entry in inventory),
        encoding="utf-8",
    )


def write_dead_letter_archive(
    root: Path,
    rows: list[dict],
    *,
    ingest_date: str = INGEST_DATE,
    batch_id: int = 7,
    query_namespace: str = ARCHIVE_QUERY_NAMESPACE,
    kind: str = "dead-letter",
) -> Path:
    batch_root = root / "_queries" / query_namespace / "_batches" / str(batch_id)
    destination = batch_root / f"date={ingest_date}"
    destination.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(rows, schema=DEAD_LETTER_SCHEMA),
        destination / "part-0.parquet",
    )
    commit_batch(root, batch_id=batch_id, query_namespace=query_namespace, kind=kind)
    return root


def config(root: Path, **overrides: object) -> RedriveConfig:
    values = {
        "archive_path": root,
        "archive_query_namespace": ARCHIVE_QUERY_NAMESPACE,
        "operation_id": "op-1",
        "start_ingest_date": date(2024, 6, 15),
        "end_ingest_date": date(2024, 6, 16),
        "max_rows": 10,
        "override_limit": False,
        "records_per_second": 1000.0,
        "bootstrap_servers": "kafka:9092",
        "manifest_dir": root / "_redrive_manifests",
    }
    values.update(overrides)
    return RedriveConfig(**values)


def test_target_topic_is_fixed_and_not_a_field() -> None:
    assert RedriveConfig.target_topic == "recsys_events.backfill"
    assert "target_topic" not in RedriveConfig.__init__.__annotations__


def test_end_ingest_date_must_be_later_than_start(tmp_path: Path) -> None:
    with pytest.raises(RedriveConfigError, match="end_ingest_date"):
        dead_letter_redrive.validate_config(
            config(
                tmp_path,
                start_ingest_date=date(2024, 6, 15),
                end_ingest_date=date(2024, 6, 15),
            )
        )


def test_max_rows_must_be_a_positive_integer(tmp_path: Path) -> None:
    with pytest.raises(RedriveConfigError, match="max_rows"):
        dead_letter_redrive.validate_config(config(tmp_path, max_rows=0))


def test_operation_id_must_be_path_safe(tmp_path: Path) -> None:
    with pytest.raises(RedriveConfigError, match="operation_id"):
        dead_letter_redrive.validate_config(config(tmp_path, operation_id="../escape"))


def test_selection_reads_committed_dead_letter_batches(tmp_path: Path) -> None:
    write_dead_letter_archive(tmp_path, [dead_letter(), dead_letter(kafka_offset=43)])

    rows = list(select_dead_letters(config(tmp_path)))

    assert len(rows) == 2
    assert {row["error_code"] for row in rows} == {"unknown_fingerprint"}


def test_selection_rejects_a_valid_kind_archive(tmp_path: Path) -> None:
    write_dead_letter_archive(tmp_path, [dead_letter()], kind="valid")

    with pytest.raises(RedriveConfigError, match="commit identity mismatch"):
        list(select_dead_letters(config(tmp_path)))


def test_selection_uses_ingestion_date_not_event_time(tmp_path: Path) -> None:
    """The dead-letter partition is kafka_timestamp, so bounds select when we saw it."""
    write_dead_letter_archive(
        tmp_path, [dead_letter(raw_value=encoded(timestamp_ms=1718400000000))],
        ingest_date="2024-08-02",
    )

    in_ingest_range = list(
        select_dead_letters(
            config(
                tmp_path,
                start_ingest_date=date(2024, 8, 2),
                end_ingest_date=date(2024, 8, 3),
            )
        )
    )
    in_event_time_range = list(
        select_dead_letters(
            config(
                tmp_path,
                start_ingest_date=date(2024, 6, 15),
                end_ingest_date=date(2024, 6, 16),
            )
        )
    )

    assert len(in_ingest_range) == 1
    assert in_event_time_range == []


def test_gate_admits_an_unknown_fingerprint_row_that_now_decodes() -> None:
    eligible, error_code, decoded = dead_letter_redrive.evaluate_row(
        {"raw_value": encoded(), "error_code": "unknown_fingerprint"}
    )

    assert eligible is True
    assert error_code == "unknown_fingerprint"
    assert decoded["event_id"] == "e-1"


@pytest.mark.parametrize(
    "raw_value,error_code",
    [
        (b"not-avro", "invalid_marker"),
        (b"\xc3\x01" + (0).to_bytes(8, "little") + b"junk", "unknown_fingerprint"),
        (b"", "invalid_marker"),
        (None, "corrupt_payload"),
    ],
)
def test_gate_rejects_undecodable_payloads(raw_value, error_code) -> None:
    eligible, reported, decoded = dead_letter_redrive.evaluate_row(
        {"raw_value": raw_value, "error_code": error_code}
    )

    assert eligible is False
    assert decoded is None
    assert reported == error_code


def test_gate_rejects_a_truncated_payload_under_the_local_fingerprint() -> None:
    truncated = encoded()[:12]

    eligible, _, decoded = dead_letter_redrive.evaluate_row(
        {"raw_value": truncated, "error_code": "corrupt_payload"}
    )

    assert eligible is False and decoded is None


def test_gate_rejects_a_record_missing_a_required_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """decode_event does not enforce required fields; the pipeline does, so the gate must.

    Otherwise a `required_field` row is republished only to dead-letter again on arrival.
    """
    incomplete = {key: value for key, value in CANONICAL_EVENT.items() if key != "event_id"}
    monkeypatch.setattr(event_avro, "decode_event", lambda *_args, **_kwargs: incomplete)

    eligible, _, decoded = dead_letter_redrive.evaluate_row(
        {"raw_value": b"any-bytes", "error_code": "required_field"}
    )

    assert eligible is False and decoded is None


def test_gate_never_treats_error_code_as_a_filter() -> None:
    """A row mislabelled corrupt_payload whose bytes are fine is still recoverable."""
    eligible, error_code, decoded = dead_letter_redrive.evaluate_row(
        {"raw_value": encoded(), "error_code": "corrupt_payload"}
    )

    assert eligible is True
    assert error_code == "corrupt_payload"
    assert decoded is not None


def test_selection_accepts_rows_whose_fingerprint_is_unknown_or_null(
    tmp_path: Path,
) -> None:
    """Unknown and absent fingerprints are the recoverable cases, never a rejection."""
    write_dead_letter_archive(
        tmp_path,
        [
            dead_letter(schema_fingerprint=7),
            dead_letter(
                raw_value=b"not-avro",
                schema_fingerprint=None,
                error_code="invalid_marker",
                kafka_offset=43,
            ),
        ],
    )

    assert len(list(select_dead_letters(config(tmp_path)))) == 2
