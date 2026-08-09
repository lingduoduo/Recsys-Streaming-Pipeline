"""Safely replay committed canonical-event archive batches to the backfill topic."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, ClassVar

import pyarrow.dataset as ds
import pyarrow.parquet as pq

from event_avro import encode_event, load_schema, schema_fingerprint


class ReplayConfigError(ValueError):
    """Raised when an operator supplied an unsafe replay configuration."""


class ReplayLimitError(ReplayConfigError):
    """Raised when the selected archive range exceeds its explicit row cap."""


class ReplaySchemaFingerprintError(ReplayConfigError):
    """Raised when an archive row was written with a non-local schema."""

    def __init__(self, message: str, fingerprints: set[int]) -> None:
        super().__init__(message)
        self.fingerprints = tuple(sorted(fingerprints))


@dataclass(frozen=True)
class ReplayConfig:
    archive_path: Path
    archive_query_namespace: str
    operation_id: str
    start_date: date
    end_date: date
    max_rows: int
    override_limit: bool
    records_per_second: float
    bootstrap_servers: str
    manifest_dir: Path | None = None

    target_topic: ClassVar[str] = "recsys_events.backfill"


@dataclass(frozen=True)
class ReplayManifest:
    operation_id: str
    status: str
    archive_path: str
    archive_query_namespace: str
    start_date: str
    end_date: str
    max_rows: int
    override_limit: bool
    records_per_second: float
    bootstrap_servers: str
    selected_rows: int
    acknowledged_cursor: int
    acknowledged_position: _ArchivePosition | None
    source_paths: tuple[str, ...]
    source_signature: str
    schema_fingerprints: tuple[int, ...]
    target_topic: str
    started_at: str
    updated_at: str
    completed_at: str | None
    error: str | None
    path: Path

    @property
    def run_id(self) -> str:
        """Compatibility alias: durable operations replace random per-run IDs."""
        return self.operation_id

    def as_json(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "run_id": self.operation_id,
            "status": self.status,
            "archive_path": self.archive_path,
            "archive_query_namespace": self.archive_query_namespace,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "max_rows": self.max_rows,
            "override_limit": self.override_limit,
            "records_per_second": self.records_per_second,
            "bootstrap_servers": self.bootstrap_servers,
            "selected_rows": self.selected_rows,
            "acknowledged_cursor": self.acknowledged_cursor,
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
            "schema_fingerprints": list(self.schema_fingerprints),
            "target_topic": self.target_topic,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }


@dataclass(frozen=True)
class _CommittedArchive:
    datasets: tuple[Any, ...]
    source_paths: tuple[Path, ...]


@dataclass(frozen=True, order=True)
class _ArchivePosition:
    path: str
    row_group: int
    row: int


CANONICAL_EVENT_FIELDS = tuple(field["name"] for field in load_schema()["fields"])
ARCHIVE_COLUMNS = (
    *CANONICAL_EVENT_FIELDS,
    "schema_fingerprint",
)
DELIVERY_TIMEOUT_SECONDS = 10.0
_SAFE_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def _validate_identity(value: str, name: str) -> None:
    if not isinstance(value, str) or not _SAFE_IDENTITY.fullmatch(value):
        raise ReplayConfigError(
            f"{name} must be a non-blank, path-safe identifier of at most 128 characters"
        )


def validate_config(config: ReplayConfig) -> None:
    """Reject unsafe bounds and identities before opening the archive or Kafka."""
    _validate_identity(config.archive_query_namespace, "archive_query_namespace")
    _validate_identity(config.operation_id, "operation_id")
    if not isinstance(config.start_date, date) or not isinstance(config.end_date, date):
        raise ReplayConfigError("start_date and end_date must be ISO calendar dates")
    if config.end_date <= config.start_date:
        raise ReplayConfigError("end_date must be later than start_date")
    if (
        isinstance(config.max_rows, bool)
        or not isinstance(config.max_rows, int)
        or config.max_rows <= 0
    ):
        raise ReplayConfigError("max_rows must be a positive integer")
    if (
        isinstance(config.records_per_second, bool)
        or not isinstance(config.records_per_second, (int, float))
        or not math.isfinite(config.records_per_second)
        or config.records_per_second <= 0
    ):
        raise ReplayConfigError("records_per_second must be a finite positive number")
    if not config.bootstrap_servers.strip():
        raise ReplayConfigError("bootstrap_servers must not be blank")


def _parse_batch_manifest(path: Path) -> tuple[dict[str, str], tuple[str, ...]]:
    values: dict[str, str] = {}
    files: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            raise ReplayConfigError(f"commit manifest has malformed line: {path}")
        key, value = line.split("=", 1)
        if key == "file":
            files.append(value)
        elif key in values:
            raise ReplayConfigError(f"commit manifest has duplicate {key}: {path}")
        else:
            values[key] = value
    return values, tuple(files)


def _partition_date(path: Path) -> date:
    values = [part.removeprefix("date=") for part in path.parts if part.startswith("date=")]
    if len(values) != 1:
        raise ReplayConfigError(f"archive parquet path has no unambiguous date partition: {path}")
    try:
        return date.fromisoformat(values[0])
    except ValueError as exc:
        raise ReplayConfigError(f"archive parquet path has invalid date partition: {path}") from exc


def _committed_parquet_files(config: ReplayConfig) -> tuple[Path, ...]:
    query_root = (
        config.archive_path
        / "_queries"
        / config.archive_query_namespace
        / "_batches"
    )
    if not query_root.is_dir():
        raise ReplayConfigError(
            "archive query namespace has no committed batch directory: "
            f"{config.archive_query_namespace}"
        )

    files: list[Path] = []
    batch_directories = sorted(
        (path for path in query_root.iterdir() if path.is_dir()),
        key=lambda path: (not path.name.isdigit(), int(path.name) if path.name.isdigit() else path.name),
    )
    for batch_directory in batch_directories:
        if not batch_directory.name.isdigit():
            raise ReplayConfigError(
                f"archive batch directory is not a numeric batch identity: {batch_directory}"
            )
        batch_id = int(batch_directory.name)
        success = batch_directory / "_SUCCESS"
        committed = batch_directory / "_COMMITTED"
        if not success.is_file() or not committed.is_file():
            raise ReplayConfigError(
                f"uncommitted or incomplete archive batch {batch_directory}"
            )
        manifest, inventory = _parse_batch_manifest(committed)
        expected_identity = {
            "version": "2",
            "query": config.archive_query_namespace,
            "kind": "valid",
            "batch_id": str(batch_id),
        }
        allowed_fields = {*expected_identity, "row_count"}
        if set(manifest) != allowed_fields or any(
            manifest.get(key) != value for key, value in expected_identity.items()
        ):
            raise ReplayConfigError(
                f"commit identity mismatch for archive batch {batch_directory}"
            )
        batch_files = sorted(batch_directory.rglob("*.parquet"))
        actual_inventory = tuple(
            path.relative_to(batch_directory).as_posix() for path in batch_files
        )
        if inventory != actual_inventory:
            raise ReplayConfigError(
                f"commit inventory mismatch for archive batch {batch_directory}"
            )
        try:
            row_count = int(manifest["row_count"])
        except (KeyError, ValueError) as exc:
            raise ReplayConfigError(
                f"commit row_count is missing or invalid for archive batch {batch_directory}"
            ) from exc
        if row_count < 0:
            raise ReplayConfigError(
                f"commit row_count is invalid for archive batch {batch_directory}"
            )
        actual_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in batch_files)
        if actual_rows != row_count or (row_count == 0 and batch_files):
            raise ReplayConfigError(
                f"commit row_count mismatch for archive batch {batch_directory}"
            )
        for parquet_file in batch_files:
            partition_date = _partition_date(parquet_file)
            if config.start_date <= partition_date < config.end_date:
                files.append(parquet_file)
    return tuple(files)


def _open_archive(config: ReplayConfig) -> _CommittedArchive:
    source_paths = _committed_parquet_files(config)
    datasets = tuple(
        ds.dataset(
            str(path),
            format="parquet",
            partitioning="hive",
            partition_base_dir=str(config.archive_path),
        )
        for path in source_paths
    )
    return _CommittedArchive(datasets=datasets, source_paths=source_paths)


def _archive_filter(config: ReplayConfig):
    return (
        (ds.field("date") >= config.start_date.isoformat())
        & (ds.field("date") < config.end_date.isoformat())
    )


def _relative_physical_path(path: Path) -> str:
    parts = path.parts
    try:
        return Path(*parts[parts.index("_queries") :]).as_posix()
    except ValueError as exc:
        raise ReplayConfigError(f"archive path is outside the committed query layout: {path}") from exc


def _iter_archive_rows(
    archive: _CommittedArchive, _date_filter: Any
) -> Iterable[tuple[_ArchivePosition, dict[str, object]]]:
    """Yield explicit file/row-group/row order independent of Arrow scanner scheduling."""
    for path in sorted(archive.source_paths):
        parquet = pq.ParquetFile(path)
        relative_path = _relative_physical_path(path)
        for row_group in range(parquet.num_row_groups):
            row_index = 0
            for batch in parquet.iter_batches(
                row_groups=[row_group], columns=list(ARCHIVE_COLUMNS)
            ):
                for row in batch.to_pylist():
                    yield _ArchivePosition(relative_path, row_group, row_index), row
                    row_index += 1


def _count_archive_rows(archive: _CommittedArchive, date_filter: Any) -> int:
    return sum(dataset.count_rows(filter=date_filter) for dataset in archive.datasets)


def _validate_archive_fingerprints(
    archive: _CommittedArchive, date_filter: Any
) -> tuple[int, ...]:
    expected = schema_fingerprint(load_schema())
    observed: set[int] = set()
    for dataset in archive.datasets:
        scanner = dataset.scanner(columns=["schema_fingerprint"], filter=date_filter)
        for batch in scanner.to_batches():
            for row in batch.to_pylist():
                raw_fingerprint = row.get("schema_fingerprint")
                if raw_fingerprint is None:
                    raise ReplaySchemaFingerprintError(
                        "archive row is missing schema_fingerprint", observed
                    )
                fingerprint = int(raw_fingerprint)
                observed.add(fingerprint)
                if fingerprint != expected:
                    raise ReplaySchemaFingerprintError(
                        "archive schema_fingerprint "
                        f"{fingerprint} does not match local schema_fingerprint {expected}",
                        observed,
                    )
    return tuple(sorted(observed))


def select_archive(config: ReplayConfig) -> Iterable[dict[str, object]]:
    """Yield canonical rows only from validated committed batches for one query."""
    validate_config(config)
    return (
        row
        for _, row in _iter_archive_rows(_open_archive(config), _archive_filter(config))
    )


def _manifest_directory(config: ReplayConfig) -> Path:
    return config.manifest_dir or config.archive_path / "_replay_manifests"


def _manifest_path(config: ReplayConfig) -> Path:
    _validate_identity(config.operation_id, "operation_id")
    return _manifest_directory(config) / f"{config.operation_id}.json"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manifest_date(value: object) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _relative_source_paths(config: ReplayConfig, archive: _CommittedArchive) -> tuple[str, ...]:
    return tuple(
        path.relative_to(config.archive_path).as_posix() for path in archive.source_paths
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_signature(config: ReplayConfig, archive: _CommittedArchive) -> str:
    source = {
        "archive_path": str(config.archive_path.resolve()),
        "archive_query_namespace": config.archive_query_namespace,
        "start_date": config.start_date.isoformat(),
        "end_date": config.end_date.isoformat(),
        "files": [
            {
                "path": path.relative_to(config.archive_path).as_posix(),
                "size": path.stat().st_size,
                "sha256": _file_sha256(path),
            }
            for path in archive.source_paths
        ],
    }
    return hashlib.sha256(
        json.dumps(source, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _write_manifest(manifest: ReplayManifest) -> None:
    manifest.path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = manifest.path.with_suffix(manifest.path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8") as output:
        json.dump(manifest.as_json(), output, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary_path, manifest.path)
    directory_fd = os.open(manifest.path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _read_manifest(path: Path) -> ReplayManifest:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        raw_position = raw.get("acknowledged_position")
        acknowledged_position = (
            None
            if raw_position is None
            else _ArchivePosition(
                path=str(raw_position["path"]),
                row_group=int(raw_position["row_group"]),
                row=int(raw_position["row"]),
            )
        )
        return ReplayManifest(
            operation_id=str(raw["operation_id"]),
            status=str(raw["status"]),
            archive_path=str(raw["archive_path"]),
            archive_query_namespace=str(raw["archive_query_namespace"]),
            start_date=str(raw["start_date"]),
            end_date=str(raw["end_date"]),
            max_rows=int(raw["max_rows"]),
            override_limit=bool(raw["override_limit"]),
            records_per_second=float(raw["records_per_second"]),
            bootstrap_servers=str(raw["bootstrap_servers"]),
            selected_rows=int(raw["selected_rows"]),
            acknowledged_cursor=int(raw["acknowledged_cursor"]),
            acknowledged_position=acknowledged_position,
            source_paths=tuple(str(value) for value in raw["source_paths"]),
            source_signature=str(raw["source_signature"]),
            schema_fingerprints=tuple(int(value) for value in raw["schema_fingerprints"]),
            target_topic=str(raw["target_topic"]),
            started_at=str(raw["started_at"]),
            updated_at=str(raw["updated_at"]),
            completed_at=(
                None if raw.get("completed_at") is None else str(raw["completed_at"])
            ),
            error=None if raw.get("error") is None else str(raw["error"]),
            path=path,
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ReplayConfigError(f"invalid replay operation manifest {path}: {exc}") from exc


def _validate_existing_contract(config: ReplayConfig, manifest: ReplayManifest) -> None:
    expected = {
        "operation_id": config.operation_id,
        "archive_path": str(config.archive_path),
        "archive_query_namespace": config.archive_query_namespace,
        "start_date": _manifest_date(config.start_date),
        "end_date": _manifest_date(config.end_date),
        "max_rows": config.max_rows,
        "override_limit": config.override_limit,
        "records_per_second": float(config.records_per_second),
        "bootstrap_servers": config.bootstrap_servers,
        "target_topic": ReplayConfig.target_topic,
    }
    actual = {name: getattr(manifest, name) for name in expected}
    if actual != expected:
        raise ReplayConfigError(
            f"operation_id {config.operation_id} already exists with a different replay contract"
        )
    if manifest.status not in {"running", "failed", "completed"}:
        raise ReplayConfigError(
            f"operation_id {config.operation_id} has invalid status {manifest.status}"
        )


def _make_kafka_producer(config: ReplayConfig):
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


def _canonical_event(row: Mapping[str, object]) -> dict[str, object]:
    return {field: row.get(field) for field in CANONICAL_EVENT_FIELDS}


def _new_manifest(
    config: ReplayConfig,
    path: Path,
    started_at: str,
    *,
    status: str,
    selected_rows: int,
    acknowledged_cursor: int,
    acknowledged_position: _ArchivePosition | None,
    source_paths: tuple[str, ...],
    source_signature: str,
    fingerprints: tuple[int, ...],
    error: str | None,
) -> ReplayManifest:
    now = _timestamp()
    return ReplayManifest(
        operation_id=config.operation_id,
        status=status,
        archive_path=str(config.archive_path),
        archive_query_namespace=config.archive_query_namespace,
        start_date=_manifest_date(config.start_date),
        end_date=_manifest_date(config.end_date),
        max_rows=config.max_rows,
        override_limit=config.override_limit,
        records_per_second=float(config.records_per_second),
        bootstrap_servers=config.bootstrap_servers,
        selected_rows=selected_rows,
        acknowledged_cursor=acknowledged_cursor,
        acknowledged_position=acknowledged_position,
        source_paths=source_paths,
        source_signature=source_signature,
        schema_fingerprints=fingerprints,
        target_topic=ReplayConfig.target_topic,
        started_at=started_at,
        updated_at=now,
        completed_at=now if status == "completed" else None,
        error=error,
        path=path,
    )


def run_replay(
    config: ReplayConfig,
    producer_factory: Callable[[ReplayConfig], Any] = _make_kafka_producer,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> ReplayManifest:
    """Publish a durable operation, resuming after its last persisted Kafka acknowledgement.

    The cursor is persisted after each broker acknowledgement. A process crash in the small window
    after Kafka acknowledges a record but before cursor persistence can publish that record again;
    consumers must therefore deduplicate the retained event_id (or operation/event key).
    """
    import time

    monotonic_clock = clock or time.monotonic
    sleep = sleeper or time.sleep
    manifest_path = _manifest_path(config)
    existing = _read_manifest(manifest_path) if manifest_path.exists() else None
    if existing is not None:
        _validate_existing_contract(config, existing)
        if existing.status == "completed":
            return existing

    started_at = existing.started_at if existing is not None else _timestamp()
    acknowledged_cursor = existing.acknowledged_cursor if existing is not None else 0
    acknowledged_position = (
        existing.acknowledged_position if existing is not None else None
    )
    producer = None
    status = "failed"
    error: str | None = None
    selected_rows = 0
    fingerprints: tuple[int, ...] = ()
    source_paths: tuple[str, ...] = ()
    source_signature = ""
    manifest: ReplayManifest | None = None
    # Once an operation exists, its original source contract remains immutable until the current
    # source selection has been revalidated. Refusal paths must never replace durable progress
    # with metadata from a different or unreadable archive snapshot.
    can_persist_progress = existing is None

    try:
        validate_config(config)
        archive = _open_archive(config)
        date_filter = _archive_filter(config)
        selected_rows = _count_archive_rows(archive, date_filter)
        source_paths = _relative_source_paths(config, archive)
        source_signature = _source_signature(config, archive)
        if selected_rows > config.max_rows and not config.override_limit:
            raise ReplayLimitError(
                f"{selected_rows} rows exceeds max_rows={config.max_rows}"
            )
        fingerprints = _validate_archive_fingerprints(archive, date_filter)

        if existing is not None:
            if (
                existing.selected_rows != selected_rows
                or existing.source_paths != source_paths
                or existing.source_signature != source_signature
                or existing.schema_fingerprints != fingerprints
            ):
                raise ReplayConfigError(
                    f"operation_id {config.operation_id} source selection changed since it started"
                )
            if not 0 <= acknowledged_cursor <= selected_rows:
                raise ReplayConfigError(
                    f"operation_id {config.operation_id} has invalid acknowledged cursor"
                )
            if (acknowledged_cursor == 0) != (acknowledged_position is None):
                raise ReplayConfigError(
                    f"operation_id {config.operation_id} has invalid physical cursor"
                )
            can_persist_progress = True

        manifest = _new_manifest(
            config,
            manifest_path,
            started_at,
            status="running",
            selected_rows=selected_rows,
            acknowledged_cursor=acknowledged_cursor,
            acknowledged_position=acknowledged_position,
            source_paths=source_paths,
            source_signature=source_signature,
            fingerprints=fingerprints,
            error=None,
        )
        _write_manifest(manifest)

        if acknowledged_cursor < selected_rows:
            producer = producer_factory(config)
            interval = 1.0 / float(config.records_per_second)
            next_deadline = monotonic_clock()
            cursor_found = acknowledged_position is None
            for position, row in _iter_archive_rows(archive, date_filter):
                if not cursor_found:
                    if position == acknowledged_position:
                        cursor_found = True
                    continue
                remaining = next_deadline - monotonic_clock()
                if remaining > 0:
                    sleep(remaining)
                event = _canonical_event(row)
                event_id = str(event["event_id"])
                operation_id = config.operation_id
                delivery = producer.send(
                    ReplayConfig.target_topic,
                    value=encode_event(event),
                    key=f"{operation_id}:{event_id}".encode("utf-8"),
                    headers=[
                        ("replay_operation_id", operation_id.encode("utf-8")),
                        ("replay_event_id", event_id.encode("utf-8")),
                    ],
                )
                delivery.get(timeout=DELIVERY_TIMEOUT_SECONDS)
                acknowledged_cursor += 1
                acknowledged_position = position
                manifest = replace(
                    manifest,
                    acknowledged_cursor=acknowledged_cursor,
                    acknowledged_position=acknowledged_position,
                    updated_at=_timestamp(),
                )
                _write_manifest(manifest)
                next_deadline += interval
            if not cursor_found:
                raise ReplayConfigError(
                    f"operation_id {config.operation_id} physical cursor is not in the source"
                )
            producer.flush()
            producer.close()
            producer = None
        status = "completed"
    except BaseException as exc:
        error = str(exc)
        if isinstance(exc, ReplaySchemaFingerprintError):
            fingerprints = exc.fingerprints
        if producer is not None:
            try:
                producer.close()
            except BaseException:
                pass
        raise
    finally:
        if can_persist_progress:
            manifest = _new_manifest(
                config,
                manifest_path,
                started_at,
                status=status,
                selected_rows=selected_rows,
                acknowledged_cursor=acknowledged_cursor,
                acknowledged_position=acknowledged_position,
                source_paths=source_paths,
                source_signature=source_signature,
                fingerprints=fingerprints,
                error=error,
            )
            _write_manifest(manifest)

    return manifest


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def parse_args(argv: list[str] | None = None) -> ReplayConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-path", type=Path, required=True)
    parser.add_argument("--archive-query-namespace", required=True)
    parser.add_argument("--operation-id", required=True)
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument("--max-rows", type=int, required=True)
    parser.add_argument("--override-limit", action="store_true")
    parser.add_argument("--records-per-second", type=float, required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--manifest-dir", type=Path)
    args = parser.parse_args(argv)
    return ReplayConfig(
        archive_path=args.archive_path,
        archive_query_namespace=args.archive_query_namespace,
        operation_id=args.operation_id,
        start_date=args.start_date,
        end_date=args.end_date,
        max_rows=args.max_rows,
        override_limit=args.override_limit,
        records_per_second=args.records_per_second,
        bootstrap_servers=args.bootstrap_servers,
        manifest_dir=args.manifest_dir,
    )


def main(argv: list[str] | None = None) -> int:
    config = parse_args(argv)
    manifest = run_replay(config)
    print(
        f"replay {manifest.status}: {manifest.selected_rows} rows -> "
        f"{manifest.target_topic} (cursor={manifest.acknowledged_cursor})"
    )
    print(f"manifest: {manifest.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
