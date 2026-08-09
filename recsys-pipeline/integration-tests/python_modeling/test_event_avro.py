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
