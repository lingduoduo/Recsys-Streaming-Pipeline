"""Re-drive recoverable dead-lettered events to the backfill topic.

Records land in the dead-letter archive when the Spark decoder could not turn a Kafka value
into a canonical event. Most of those failures are permanent, but `unknown_fingerprint` is
not: it means a producer shipped a schema the consuming catalog did not know yet. Once the
catalog is updated the same bytes decode cleanly, and this command republishes them.

Eligibility is decided by re-decoding each row against the *current* catalog, never by its
recorded `error_code`. That makes the command self-correcting -- it cannot publish anything
that would immediately dead-letter again.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any, ClassVar

import pyarrow.parquet as pq

import event_avro
from archive_replay import (
    _ArchivePosition,
    _relative_physical_path,
    _relative_source_paths,
    _source_signature,
    _timestamp,
    _validate_identity,
    _write_manifest,
    open_archive,
)

ARCHIVE_KIND = "dead-letter"
DEAD_LETTER_COLUMNS = ("raw_value", "error_code")
DELIVERY_TIMEOUT_SECONDS = 10.0


class RedriveConfigError(ValueError):
    """Raised when an operator supplied an unsafe re-drive configuration."""


class RedriveLimitError(RedriveConfigError):
    """Raised when the selected ingestion range exceeds its explicit row cap."""


@dataclass(frozen=True)
class RedriveConfig:
    archive_path: Path
    archive_query_namespace: str
    operation_id: str
    start_ingest_date: date
    end_ingest_date: date
    max_rows: int
    override_limit: bool
    records_per_second: float
    bootstrap_servers: str
    manifest_dir: Path | None = None

    target_topic: ClassVar[str] = "recsys_events.backfill"

    # The shared archive reader selects on `start_date`/`end_date`. Dead-letter partitions
    # derive from kafka_timestamp, so for this command those bounds are ingestion dates --
    # named explicitly everywhere an operator sees them, and adapted here.
    @property
    def start_date(self) -> date:
        return self.start_ingest_date

    @property
    def end_date(self) -> date:
        return self.end_ingest_date


def validate_config(config: RedriveConfig) -> None:
    """Reject unsafe bounds and identities before opening the archive or Kafka."""
    try:
        _validate_identity(config.archive_query_namespace, "archive_query_namespace")
        _validate_identity(config.operation_id, "operation_id")
    except ValueError as exc:
        raise RedriveConfigError(str(exc)) from exc
    if not isinstance(config.start_ingest_date, date) or not isinstance(
        config.end_ingest_date, date
    ):
        raise RedriveConfigError(
            "start_ingest_date and end_ingest_date must be ISO calendar dates"
        )
    if config.end_ingest_date <= config.start_ingest_date:
        raise RedriveConfigError("end_ingest_date must be later than start_ingest_date")
    if (
        isinstance(config.max_rows, bool)
        or not isinstance(config.max_rows, int)
        or config.max_rows <= 0
    ):
        raise RedriveConfigError("max_rows must be a positive integer")
    if (
        isinstance(config.records_per_second, bool)
        or not isinstance(config.records_per_second, (int, float))
        or not math.isfinite(config.records_per_second)
        or config.records_per_second <= 0
    ):
        raise RedriveConfigError("records_per_second must be a finite positive number")
    if not config.bootstrap_servers.strip():
        raise RedriveConfigError("bootstrap_servers must not be blank")


def _open(config: RedriveConfig):
    """Open the dead-letter archive through the shared, fully validated batch reader.

    Schema fingerprints are deliberately not validated here. Unknown and absent
    fingerprints are exactly what this command exists to recover; the decodability gate
    replaces that check.
    """
    try:
        return open_archive(config, ARCHIVE_KIND)
    except ValueError as exc:
        raise RedriveConfigError(str(exc)) from exc


def _iter_rows(archive: Any) -> Iterable[tuple[_ArchivePosition, dict[str, object]]]:
    """Yield explicit file/row-group/row order, independent of Arrow scheduling."""
    for path in sorted(archive.source_paths):
        parquet = pq.ParquetFile(path)
        relative_path = _relative_physical_path(path)
        for row_group in range(parquet.num_row_groups):
            row_index = 0
            for batch in parquet.iter_batches(
                row_groups=[row_group], columns=list(DEAD_LETTER_COLUMNS)
            ):
                for row in batch.to_pylist():
                    yield _ArchivePosition(relative_path, row_group, row_index), row
                    row_index += 1


def _count_rows(archive: Any) -> int:
    return sum(pq.ParquetFile(path).metadata.num_rows for path in archive.source_paths)


def select_dead_letters(config: RedriveConfig) -> Iterable[dict[str, object]]:
    """Yield dead-letter rows from validated committed batches in the ingestion range."""
    validate_config(config)
    return (row for _, row in _iter_rows(_open(config)))


def evaluate_row(
    row: Mapping[str, object]
) -> tuple[bool, str | None, dict[str, object] | None]:
    """Decide eligibility by decoding against the current catalog, never by error_code.

    Returns (eligible, original error_code, decoded record). The decoded record is the only
    source of `event_id`: the dead-letter archive stores raw bytes and Kafka lineage, not
    canonical event columns.
    """
    error_code = row.get("error_code")
    payload = row.get("raw_value")
    if not isinstance(payload, (bytes, bytearray)):
        return False, error_code, None
    try:
        decoded = event_avro.decode_event(bytes(payload))
        # decode_event does not enforce required fields, but the pipeline does. Without
        # this the command could republish a `required_field` row that dead-letters again
        # on arrival -- and event_id, which the key contract needs, could be missing.
        event_avro.validate_required(decoded)
    except (
        event_avro.SchemaFingerprintError,
        event_avro.EventValidationError,
        ValueError,
    ):
        return False, error_code, None
    return True, error_code, decoded


@dataclass(frozen=True)
class RedriveManifest:
    operation_id: str
    status: str
    archive_path: str
    archive_query_namespace: str
    start_ingest_date: str
    end_ingest_date: str
    max_rows: int
    override_limit: bool
    records_per_second: float
    bootstrap_servers: str
    target_topic: str
    selected_rows: int
    examined_rows: int
    published_rows: int
    skipped_rows: int
    skipped_by_error_code: Mapping[str, int]
    acknowledged_position: _ArchivePosition | None
    source_paths: tuple[str, ...]
    source_signature: str
    started_at: str
    updated_at: str
    completed_at: str | None
    error: str | None
    path: Path

    def as_json(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "status": self.status,
            "archive_path": self.archive_path,
            "archive_query_namespace": self.archive_query_namespace,
            "start_ingest_date": self.start_ingest_date,
            "end_ingest_date": self.end_ingest_date,
            "max_rows": self.max_rows,
            "override_limit": self.override_limit,
            "records_per_second": self.records_per_second,
            "bootstrap_servers": self.bootstrap_servers,
            "target_topic": self.target_topic,
            "selected_rows": self.selected_rows,
            "examined_rows": self.examined_rows,
            "published_rows": self.published_rows,
            "skipped_rows": self.skipped_rows,
            "skipped_by_error_code": dict(self.skipped_by_error_code),
            "acknowledged_position": (
                None
                if self.acknowledged_position is None
                else {
                    "path": self.acknowledged_position.path,
                    "row_group": self.acknowledged_position.row_group,
                    "row": self.acknowledged_position.row,
                }
            ),
            "source_paths": list(self.source_paths),
            "source_signature": self.source_signature,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }


def _manifest_path(config: RedriveConfig) -> Path:
    validate_config(config)
    directory = config.manifest_dir or config.archive_path / "_redrive_manifests"
    return directory / f"{config.operation_id}.json"


def _read_manifest(path: Path) -> RedriveManifest:
    values = json.loads(path.read_text(encoding="utf-8"))
    position = values.get("acknowledged_position")
    return RedriveManifest(
        operation_id=values["operation_id"],
        status=values["status"],
        archive_path=values["archive_path"],
        archive_query_namespace=values["archive_query_namespace"],
        start_ingest_date=values["start_ingest_date"],
        end_ingest_date=values["end_ingest_date"],
        max_rows=values["max_rows"],
        override_limit=values["override_limit"],
        records_per_second=values["records_per_second"],
        bootstrap_servers=values["bootstrap_servers"],
        target_topic=values["target_topic"],
        selected_rows=values["selected_rows"],
        examined_rows=values["examined_rows"],
        published_rows=values["published_rows"],
        skipped_rows=values["skipped_rows"],
        skipped_by_error_code=dict(values["skipped_by_error_code"]),
        acknowledged_position=(
            None
            if position is None
            else _ArchivePosition(
                position["path"], position["row_group"], position["row"]
            )
        ),
        source_paths=tuple(values["source_paths"]),
        source_signature=values["source_signature"],
        started_at=values["started_at"],
        updated_at=values["updated_at"],
        completed_at=values.get("completed_at"),
        error=values.get("error"),
        path=path,
    )


def _build_manifest(
    config: RedriveConfig,
    path: Path,
    started_at: str,
    *,
    status: str,
    selected_rows: int,
    examined_rows: int,
    published_rows: int,
    skipped: Mapping[str, int],
    acknowledged_position: _ArchivePosition | None,
    source_paths: tuple[str, ...],
    source_signature: str,
    error: str | None,
) -> RedriveManifest:
    now = _timestamp()
    return RedriveManifest(
        operation_id=config.operation_id,
        status=status,
        archive_path=str(config.archive_path),
        archive_query_namespace=config.archive_query_namespace,
        start_ingest_date=config.start_ingest_date.isoformat(),
        end_ingest_date=config.end_ingest_date.isoformat(),
        max_rows=config.max_rows,
        override_limit=config.override_limit,
        records_per_second=float(config.records_per_second),
        bootstrap_servers=config.bootstrap_servers,
        target_topic=RedriveConfig.target_topic,
        selected_rows=selected_rows,
        examined_rows=examined_rows,
        published_rows=published_rows,
        skipped_rows=sum(skipped.values()),
        skipped_by_error_code=dict(skipped),
        acknowledged_position=acknowledged_position,
        source_paths=source_paths,
        source_signature=source_signature,
        started_at=started_at,
        updated_at=now,
        completed_at=now if status == "completed" else None,
        error=error,
        path=path,
    )


def _make_kafka_producer(config: RedriveConfig):
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        if exc.name == "kafka":
            raise RuntimeError(
                "Missing producer dependencies. Run: "
                "python -m pip install -r services/python-modeling/requirements.txt"
            ) from exc
        raise
    return KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=32 * 1024,
        compression_type="lz4",
        api_version_auto_timeout_ms=5_000,
        request_timeout_ms=10_000,
        max_block_ms=10_000,
    )


def run_redrive(
    config: RedriveConfig,
    producer_factory: Callable[[RedriveConfig], Any] = _make_kafka_producer,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> RedriveManifest:
    """Publish a durable operation, resuming after its last persisted position.

    The cursor advances for skipped rows as well as published ones, so a crash right after
    a skip does not re-examine it. The cursor is persisted after each row; a crash between
    a broker acknowledgement and that write can republish one record, so consumers must
    still deduplicate on event_id.
    """
    import time

    monotonic_clock = clock or time.monotonic
    sleep = sleeper or time.sleep
    manifest_path = _manifest_path(config)
    existing = _read_manifest(manifest_path) if manifest_path.exists() else None
    if existing is not None and existing.status == "completed":
        return existing

    started_at = existing.started_at if existing is not None else _timestamp()
    examined_rows = existing.examined_rows if existing is not None else 0
    published_rows = existing.published_rows if existing is not None else 0
    skipped = dict(existing.skipped_by_error_code) if existing is not None else {}
    acknowledged_position = (
        existing.acknowledged_position if existing is not None else None
    )
    producer = None
    status = "failed"
    error: str | None = None
    selected_rows = 0
    source_paths: tuple[str, ...] = ()
    source_signature = ""
    manifest: RedriveManifest | None = None
    # Never replace durable progress with metadata from a different archive snapshot.
    can_persist_progress = existing is None

    try:
        validate_config(config)
        archive = _open(config)
        selected_rows = _count_rows(archive)
        source_paths = _relative_source_paths(config, archive)
        source_signature = _source_signature(config, archive)
        if selected_rows > config.max_rows and not config.override_limit:
            raise RedriveLimitError(
                f"{selected_rows} rows exceeds max_rows={config.max_rows}"
            )

        if existing is not None:
            if (
                existing.selected_rows != selected_rows
                or existing.source_paths != source_paths
                or existing.source_signature != source_signature
            ):
                raise RedriveConfigError(
                    f"operation_id {config.operation_id} source selection changed "
                    "since it started"
                )
            if not 0 <= examined_rows <= selected_rows:
                raise RedriveConfigError(
                    f"operation_id {config.operation_id} has an invalid examined count"
                )
            if (examined_rows == 0) != (acknowledged_position is None):
                raise RedriveConfigError(
                    f"operation_id {config.operation_id} has an invalid physical cursor"
                )
            can_persist_progress = True

        manifest = _build_manifest(
            config,
            manifest_path,
            started_at,
            status="running",
            selected_rows=selected_rows,
            examined_rows=examined_rows,
            published_rows=published_rows,
            skipped=skipped,
            acknowledged_position=acknowledged_position,
            source_paths=source_paths,
            source_signature=source_signature,
            error=None,
        )
        _write_manifest(manifest)

        if examined_rows < selected_rows:
            producer = producer_factory(config)
            interval = 1.0 / float(config.records_per_second)
            next_deadline = monotonic_clock()
            cursor_found = acknowledged_position is None
            for position, row in _iter_rows(archive):
                if not cursor_found:
                    if position == acknowledged_position:
                        cursor_found = True
                    continue
                eligible, error_code, decoded = evaluate_row(row)
                if eligible:
                    remaining = next_deadline - monotonic_clock()
                    if remaining > 0:
                        sleep(remaining)
                    event_id = str(decoded["event_id"])
                    delivery = producer.send(
                        RedriveConfig.target_topic,
                        # Republish the archived bytes verbatim: re-encoding would stamp
                        # the local fingerprint onto another writer's record.
                        value=bytes(row["raw_value"]),
                        key=f"{config.operation_id}:{event_id}".encode("utf-8"),
                        headers=[
                            ("replay_operation_id", config.operation_id.encode("utf-8")),
                            ("replay_event_id", event_id.encode("utf-8")),
                            (
                                "redrive_error_code",
                                str(error_code or "").encode("utf-8"),
                            ),
                        ],
                    )
                    delivery.get(timeout=DELIVERY_TIMEOUT_SECONDS)
                    published_rows += 1
                    next_deadline += interval
                else:
                    bucket = error_code or "unclassified"
                    skipped[bucket] = skipped.get(bucket, 0) + 1
                examined_rows += 1
                acknowledged_position = position
                manifest = replace(
                    manifest,
                    examined_rows=examined_rows,
                    published_rows=published_rows,
                    skipped_rows=sum(skipped.values()),
                    skipped_by_error_code=dict(skipped),
                    acknowledged_position=acknowledged_position,
                    updated_at=_timestamp(),
                )
                _write_manifest(manifest)
            if not cursor_found:
                raise RedriveConfigError(
                    f"operation_id {config.operation_id} physical cursor is not in "
                    "the source"
                )
            producer.flush()
            producer.close()
            producer = None
        status = "completed"
    except BaseException as exc:
        error = str(exc)
        if producer is not None:
            try:
                producer.close()
            except BaseException:
                pass
        raise
    finally:
        if can_persist_progress:
            manifest = _build_manifest(
                config,
                manifest_path,
                started_at,
                status=status,
                selected_rows=selected_rows,
                examined_rows=examined_rows,
                published_rows=published_rows,
                skipped=skipped,
                acknowledged_position=acknowledged_position,
                source_paths=source_paths,
                source_signature=source_signature,
                error=error,
            )
            _write_manifest(manifest)

    return manifest
