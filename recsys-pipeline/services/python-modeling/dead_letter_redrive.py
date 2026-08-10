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

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, ClassVar

import pyarrow.parquet as pq

import event_avro
from archive_replay import (
    _ArchivePosition,
    _relative_physical_path,
    _validate_identity,
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
        return True, error_code, event_avro.decode_event(bytes(payload))
    except (
        event_avro.SchemaFingerprintError,
        event_avro.EventValidationError,
        ValueError,
    ):
        return False, error_code, None
