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
    schema = event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[0])
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


def test_decode_rejects_trailing_bytes_after_a_valid_record():
    """Fails if Python accepts bytes that the Scala boundary classifies as corrupt."""
    payload = event_avro.encode_event(
        {
            "event_id": "e-trailing",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        }
    )

    with pytest.raises(event_avro.EventValidationError, match="trailing"):
        event_avro.decode_event(payload + b"\x00")


def test_v2_is_the_default_writer_schema():
    """Fails if producers still encode v1 after the bump."""
    schema = event_avro.load_schema()
    names = [field["name"] for field in schema["fields"]]

    assert names[-4:] == ["surface", "locale", "timezone", "device"]
    assert event_avro.schema_fingerprint(schema) == 0xAF86ABE880FE4BB3


def test_v1_payload_decodes_into_the_v2_shape():
    """Fails if a v1 record dead-letters instead of resolving through the catalog."""
    v1 = event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[0])
    payload = event_avro.encode_event(
        {
            "event_id": "e-legacy",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        },
        v1,
    )

    decoded = event_avro.decode_event(payload)

    assert decoded["event_id"] == "e-legacy"
    assert decoded["surface"] is None
    assert decoded["locale"] is None
    assert decoded["timezone"] is None
    assert decoded["device"] is None


def test_v2_round_trips_the_new_context_fields():
    payload = event_avro.encode_event(
        {
            "event_id": "e-v2",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "impression",
            "timestamp_ms": 1718400000000,
            "surface": "home_feed",
            "locale": "en-US",
            "timezone": "America/New_York",
            "device": "ios",
        }
    )

    decoded = event_avro.decode_event(payload)

    assert decoded["surface"] == "home_feed"
    assert decoded["locale"] == "en-US"
    assert decoded["timezone"] == "America/New_York"
    assert decoded["device"] == "ios"


def test_unknown_fingerprint_is_still_rejected():
    """Fails if widening the catalog accidentally accepts any fingerprint."""
    payload = event_avro.encode_event(
        {
            "event_id": "e-1",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        }
    )
    corrupted = payload[:2] + (0xDEADBEEF).to_bytes(8, "little") + payload[10:]

    with pytest.raises(event_avro.SchemaFingerprintError):
        event_avro.decode_event(corrupted)


def test_reader_schema_overrides_the_default_shape():
    """Fails if decode_event ignores an explicit reader schema.

    The default reader is the current writer schema, so a v1 payload normally arrives carrying
    the fields v2 added, set to null. A caller that wants the record in its original v1 shape —
    replaying an archive against the contract that wrote it, say — can now ask for it.
    """
    v1 = event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[0])
    payload = event_avro.encode_event(
        {
            "event_id": "e-legacy",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        },
        v1,
    )

    default_shape = event_avro.decode_event(payload)
    v1_shape = event_avro.decode_event(payload, reader_schema=v1)

    assert "surface" in default_shape and default_shape["surface"] is None
    assert "surface" not in v1_shape
    assert v1_shape["event_id"] == "e-legacy"
