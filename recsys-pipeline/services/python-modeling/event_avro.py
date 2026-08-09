"""Avro single-object encoding for the canonical recommendation event contract."""

from __future__ import annotations

import io
import json
from collections.abc import Mapping
from pathlib import Path

import fastavro


MAGIC = b"\xc3\x01"
REQUIRED_FIELDS = ("event_id", "user_id", "item_id", "event_type", "timestamp_ms")
DEFAULT_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "schemas/recsys-event-v1.avsc"


class SchemaFingerprintError(ValueError):
    """Raised when a single-object payload references no known writer schema."""


class EventValidationError(ValueError):
    """Raised when an event cannot be encoded or decoded as a canonical event."""


def load_schema(path: Path | None = None) -> dict:
    """Load the canonical Avro writer schema, or a supplied schema artifact."""
    with (path or DEFAULT_SCHEMA_PATH).open(encoding="utf-8") as schema_file:
        return json.load(schema_file)


def schema_fingerprint(schema: dict) -> int:
    canonical = fastavro.schema.to_parsing_canonical_form(schema)
    return int(fastavro.schema.fingerprint(canonical, "CRC-64-AVRO"), 16)


def validate_required(event: Mapping[str, object]) -> None:
    for field in REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            raise EventValidationError(f"missing required field {field}")


def encode_event(event: Mapping[str, object], schema: dict | None = None) -> bytes:
    writer_schema = schema or load_schema()
    validate_required(event)
    record = dict(event)
    for field in writer_schema["fields"]:
        if field["name"] not in record and "default" in field:
            record[field["name"]] = field["default"]
    out = io.BytesIO()
    out.write(MAGIC)
    out.write(schema_fingerprint(writer_schema).to_bytes(8, "little"))
    try:
        fastavro.schemaless_writer(out, writer_schema, record, strict=True)
    except (TypeError, ValueError) as exc:
        raise EventValidationError(str(exc)) from exc
    return out.getvalue()


def decode_event(payload: bytes, catalog: Mapping[int, dict] | None = None) -> dict:
    if len(payload) < 10 or payload[:2] != MAGIC:
        raise EventValidationError("invalid Avro single-object marker")
    fingerprint = int.from_bytes(payload[2:10], "little")
    schemas = catalog if catalog is not None else {schema_fingerprint(load_schema()): load_schema()}
    if fingerprint not in schemas:
        raise SchemaFingerprintError(f"unknown schema fingerprint {fingerprint}")
    try:
        return fastavro.schemaless_reader(io.BytesIO(payload[10:]), schemas[fingerprint])
    except (TypeError, ValueError, EOFError) as exc:
        raise EventValidationError(str(exc)) from exc
