"""Avro single-object encoding for the canonical recommendation event contract."""

from __future__ import annotations

import functools
import io
import json
from collections.abc import Mapping
from pathlib import Path
from types import MappingProxyType

import fastavro


MAGIC = b"\xc3\x01"
REQUIRED_FIELDS = ("event_id", "user_id", "item_id", "event_type", "timestamp_ms")
_SCHEMA_DIR = Path(__file__).resolve().parents[2] / "schemas"
DEFAULT_SCHEMA_PATH = _SCHEMA_DIR / "recsys-event-v2.avsc"
LEGACY_SCHEMA_PATHS: tuple[Path, ...] = (_SCHEMA_DIR / "recsys-event-v1.avsc",)


class SchemaFingerprintError(ValueError):
    """Raised when a single-object payload references no known writer schema."""


class EventValidationError(ValueError):
    """Raised when an event cannot be encoded or decoded as a canonical event."""


@functools.lru_cache(maxsize=None)
def load_schema(path: Path | None = None) -> dict:
    """Load the canonical Avro writer schema, or a supplied schema artifact.

    Cached per ``path``: repeated calls return the same dict object rather than re-opening
    and re-parsing the file. Callers must treat the result as read-only — every caller of a
    given path shares this object, and empirically fastavro's schemaless_reader/writer do
    not mutate the schema dicts they are given, but a caller that did would corrupt it for
    everyone else.
    """
    with (path or DEFAULT_SCHEMA_PATH).open(encoding="utf-8") as schema_file:
        return json.load(schema_file)


def schema_fingerprint(schema: dict) -> int:
    canonical = fastavro.schema.to_parsing_canonical_form(schema)
    fingerprint_bytes = bytes.fromhex(
        fastavro.schema.fingerprint(canonical, "CRC-64-AVRO")
    )
    return int.from_bytes(fingerprint_bytes, "little")


@functools.lru_cache(maxsize=None)
def load_catalog() -> Mapping[int, dict]:
    """Every writer schema this decoder accepts, keyed by fingerprint.

    A record written before a schema bump is still a valid record; keeping the older
    writer schemas here is what lets it resolve into the current reader shape instead
    of dead-lettering as an unknown fingerprint.

    Cached: building this catalog canonicalizes and CRC-64-fingerprints every schema, which
    is done once per process rather than on every call (this is used per-message as a Kafka
    deserializer's default catalog). The returned mapping is read-only to stop a caller from
    corrupting the shared cached object; the schema dicts it holds must not be mutated either.
    """
    catalog: dict[int, dict] = {}
    for path in (*LEGACY_SCHEMA_PATHS, DEFAULT_SCHEMA_PATH):
        schema = load_schema(path)
        catalog[schema_fingerprint(schema)] = schema
    return MappingProxyType(catalog)


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
    schemas = catalog if catalog is not None else load_catalog()
    if fingerprint not in schemas:
        raise SchemaFingerprintError(f"unknown schema fingerprint {fingerprint}")
    encoded_record = io.BytesIO(payload[10:])
    try:
        decoded = fastavro.schemaless_reader(
            encoded_record, schemas[fingerprint], load_schema())
    except (TypeError, ValueError, EOFError) as exc:
        raise EventValidationError(str(exc)) from exc
    if encoded_record.read(1):
        raise EventValidationError("trailing bytes after Avro record")
    return decoded
