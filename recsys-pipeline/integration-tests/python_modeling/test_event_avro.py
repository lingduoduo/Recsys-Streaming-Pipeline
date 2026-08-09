import sys
from pathlib import Path

import pytest


PYTHON_MODELING = Path(__file__).resolve().parents[2] / "services/python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

import event_avro


def test_single_object_round_trip():
    """Fails if the wire marker or Avro record serialization changes."""
    event = {
        "event_id": "e-1", "request_id": None, "session_id": None,
        "user_id": "u-1", "item_id": "i-1", "event_type": "click",
        "timestamp_ms": 1718400000000,
    }

    encoded = event_avro.encode_event(event)

    assert encoded[:2] == b"\xc3\x01"
    assert event_avro.decode_event(encoded)["event_id"] == "e-1"


def test_single_object_header_uses_standard_avro_fingerprint_bytes():
    """Fails if the Avro fingerprint bytes are reversed in the wire header."""
    schema = event_avro.load_schema()
    expected_fingerprint = 0x225B275F487979AB
    payload = event_avro.encode_event(
        {
            "event_id": "e-standard-fingerprint",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        },
        schema,
    )

    assert event_avro.schema_fingerprint(schema) == expected_fingerprint
    assert payload[:10] == b"\xc3\x01\xab\x79\x79\x48\x5f\x27\x5b\x22"
    assert event_avro.decode_event(payload, {expected_fingerprint: schema})["event_id"] == "e-standard-fingerprint"


def test_missing_required_field_is_rejected():
    """Fails if required event identity fields are accepted without validation."""
    with pytest.raises(event_avro.EventValidationError, match="event_id"):
        event_avro.encode_event(
            {"user_id": "u", "item_id": "i", "event_type": "click", "timestamp_ms": 1}
        )


def test_unknown_fingerprint_is_rejected():
    """Fails if a payload selects an unregistered writer schema."""
    payload = b"\xc3\x01" + (7).to_bytes(8, "little") + b"bad"

    with pytest.raises(event_avro.SchemaFingerprintError, match="7"):
        event_avro.decode_event(payload)


def test_empty_catalog_rejects_default_fingerprint():
    """Fails if an explicit empty catalog falls back to the default schema."""
    payload = event_avro.encode_event(
        {
            "event_id": "e-empty-catalog",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        }
    )

    with pytest.raises(event_avro.SchemaFingerprintError, match="unknown schema fingerprint"):
        event_avro.decode_event(payload, catalog={})
