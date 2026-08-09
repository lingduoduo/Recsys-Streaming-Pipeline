"""Safely replay bounded canonical-event archive partitions to the backfill topic."""

from __future__ import annotations

import argparse
import json
import math
import os
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, ClassVar

import pyarrow.dataset as ds

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
    run_id: str
    status: str
    archive_path: str
    start_date: str
    end_date: str
    selected_rows: int
    schema_fingerprints: tuple[int, ...]
    target_topic: str
    started_at: str
    completed_at: str
    error: str | None
    path: Path

    def as_json(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "status": self.status,
            "archive_path": self.archive_path,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "selected_rows": self.selected_rows,
            "schema_fingerprints": list(self.schema_fingerprints),
            "target_topic": self.target_topic,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "error": self.error,
        }


CANONICAL_EVENT_FIELDS = tuple(field["name"] for field in load_schema()["fields"])
ARCHIVE_COLUMNS = (*CANONICAL_EVENT_FIELDS, "schema_fingerprint")
DELIVERY_TIMEOUT_SECONDS = 10.0


def validate_config(config: ReplayConfig) -> None:
    """Reject unsafe bounds before opening the archive or a Kafka producer."""
    if not isinstance(config.start_date, date) or not isinstance(config.end_date, date):
        raise ReplayConfigError("start_date and end_date must be ISO calendar dates")
    if config.end_date <= config.start_date:
        raise ReplayConfigError("end_date must be later than start_date")
    if isinstance(config.max_rows, bool) or not isinstance(config.max_rows, int) or config.max_rows <= 0:
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


def _open_archive(config: ReplayConfig):
    return ds.dataset(
        str(config.archive_path),
        format="parquet",
        partitioning="hive",
        # Task 4 deliberately namespaces committed data below `_queries/_batches`.
        # Do not let Arrow's hidden-path default silently skip all archived records.
        ignore_prefixes=[],
        exclude_invalid_files=True,
    )


def _archive_filter(config: ReplayConfig):
    # Hive discovery exposes ``date=YYYY-MM-DD`` path values as strings unless an
    # external Arrow schema is supplied. ISO dates retain chronological ordering.
    return (
        (ds.field("date") >= config.start_date.isoformat())
        & (ds.field("date") < config.end_date.isoformat())
    )


def _iter_archive_rows(dataset: Any, date_filter: Any) -> Iterable[dict[str, object]]:
    """Yield bounded Arrow record batches, never a table-sized Python list."""
    scanner = dataset.scanner(columns=list(ARCHIVE_COLUMNS), filter=date_filter)
    for batch in scanner.to_batches():
        yield from batch.to_pylist()


def _validate_archive_fingerprints(dataset: Any, date_filter: Any) -> tuple[int, ...]:
    """Ensure every selected record uses the sole local writer schema before publishing."""
    expected = schema_fingerprint(load_schema())
    observed: set[int] = set()
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
                    f"archive schema_fingerprint {fingerprint} does not match local schema_fingerprint {expected}",
                    observed,
                )
    return tuple(sorted(observed))


def select_archive(config: ReplayConfig) -> Iterable[dict[str, object]]:
    """Yield canonical records in the inclusive/exclusive UTC date range."""
    validate_config(config)
    return _iter_archive_rows(_open_archive(config), _archive_filter(config))


def _manifest_directory(config: ReplayConfig) -> Path:
    return config.manifest_dir or config.archive_path / "_replay_manifests"


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _manifest_date(value: object) -> str:
    return value.isoformat() if isinstance(value, date) else str(value)


def _write_manifest(manifest: ReplayManifest) -> None:
    manifest.path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = manifest.path.with_suffix(manifest.path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8") as output:
        json.dump(manifest.as_json(), output, sort_keys=True)
        output.write("\n")
    os.replace(temporary_path, manifest.path)


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
    """Drop Kafka/archive lineage before serializing an Avro canonical event."""
    return {field: row.get(field) for field in CANONICAL_EVENT_FIELDS}


def run_replay(
    config: ReplayConfig,
    producer_factory: Callable[[ReplayConfig], Any] = _make_kafka_producer,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> ReplayManifest:
    """Count a bounded selection, then publish it at a monotonic limited rate."""
    import time

    monotonic_clock = clock or time.monotonic
    sleep = sleeper or time.sleep
    run_id = uuid.uuid4().hex
    manifest_path = _manifest_directory(config) / f"{run_id}.json"
    started_at = _timestamp()
    producer = None
    status = "failed"
    error: str | None = None
    selected_rows = 0
    fingerprints: tuple[int, ...] = ()

    try:
        validate_config(config)
        dataset = _open_archive(config)
        date_filter = _archive_filter(config)
        selected_rows = dataset.count_rows(filter=date_filter)
        if selected_rows > config.max_rows and not config.override_limit:
            raise ReplayLimitError(f"{selected_rows} rows exceeds max_rows={config.max_rows}")
        fingerprints = _validate_archive_fingerprints(dataset, date_filter)
        if selected_rows:
            producer = producer_factory(config)
            interval = 1.0 / float(config.records_per_second)
            next_deadline = monotonic_clock()
            for row in _iter_archive_rows(dataset, date_filter):
                remaining = next_deadline - monotonic_clock()
                if remaining > 0:
                    sleep(remaining)
                event = _canonical_event(row)
                key = event.get("request_id") or event["user_id"]
                delivery = producer.send(
                    ReplayConfig.target_topic,
                    value=encode_event(event),
                    key=str(key).encode("utf-8"),
                )
                # Wait per record: delivery failures become run failures while memory remains bounded.
                delivery.get(timeout=DELIVERY_TIMEOUT_SECONDS)
                next_deadline += interval
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
                # The primary publication/flush/close failure is the useful error.
                pass
        raise
    finally:
        manifest = ReplayManifest(
            run_id=run_id,
            status=status,
            archive_path=str(config.archive_path),
            start_date=_manifest_date(config.start_date),
            end_date=_manifest_date(config.end_date),
            selected_rows=selected_rows,
            schema_fingerprints=fingerprints,
            target_topic=ReplayConfig.target_topic,
            started_at=started_at,
            completed_at=_timestamp(),
            error=error,
            path=manifest_path,
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
    print(f"replay {manifest.status}: {manifest.selected_rows} rows -> {manifest.target_topic}")
    print(f"manifest: {manifest.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
