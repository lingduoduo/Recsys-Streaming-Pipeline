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


def test_v3_is_the_default_writer_schema():
    """Fails if new records are written with a schema before v3."""
    schema = event_avro.load_schema()
    names = [field["name"] for field in schema["fields"]]

    assert names[-6:] == [
        "query_id", "query_text", "result_set_id", "referrer", "view_kind", "view_duration_ms",
    ]
    assert {event_avro.schema_fingerprint(known) for known in event_avro.load_catalog().values()} == {
        event_avro.schema_fingerprint(event_avro.load_schema(path))
        for path in (*event_avro.LEGACY_SCHEMA_PATHS, event_avro.DEFAULT_SCHEMA_PATH)
    }


def test_v2_payload_decodes_into_the_v3_shape():
    """Fails if a v2 record dead-letters or misses v3 defaults after resolution."""
    v2 = event_avro.load_schema(event_avro.LEGACY_SCHEMA_PATHS[1])
    payload = event_avro.encode_event(
        {
            "event_id": "e-legacy",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "click",
            "timestamp_ms": 1718400000000,
        },
        v2,
    )

    decoded = event_avro.decode_event(payload)

    assert decoded["event_id"] == "e-legacy"
    assert decoded["surface"] is None
    assert decoded["locale"] is None
    assert decoded["timezone"] is None
    assert decoded["device"] is None
    assert decoded["query_id"] is None
    assert decoded["query_text"] is None
    assert decoded["result_set_id"] is None
    assert decoded["referrer"] is None
    assert decoded["view_kind"] is None
    assert decoded["view_duration_ms"] is None


def test_v3_round_trips_the_behavior_context_fields():
    """Fails if any typed behavioral context field is lost on the Avro round trip."""
    payload = event_avro.encode_event(
        {
            "event_id": "e-v3",
            "user_id": "u-1",
            "item_id": "i-1",
            "event_type": "impression",
            "timestamp_ms": 1718400000000,
            "surface": "home_feed",
            "locale": "en-US",
            "timezone": "America/New_York",
            "device": "ios",
            "query_id": "q-1",
            "query_text": "space opera",
            "result_set_id": "rs-1",
            "referrer": "home_feed",
            "view_kind": "detail",
            "view_duration_ms": 1420,
        }
    )

    decoded = event_avro.decode_event(payload)

    assert decoded["surface"] == "home_feed"
    assert decoded["locale"] == "en-US"
    assert decoded["timezone"] == "America/New_York"
    assert decoded["device"] == "ios"
    assert decoded["query_id"] == "q-1"
    assert decoded["query_text"] == "space opera"
    assert decoded["result_set_id"] == "rs-1"
    assert decoded["referrer"] == "home_feed"
    assert decoded["view_kind"] == "detail"
    assert decoded["view_duration_ms"] == 1420


def test_search_without_item_round_trips_in_v3():
    """Fails if search events are still treated as item-bearing behavior."""
    event = {
        "event_id": "search-1", "user_id": "u1", "item_id": None,
        "event_type": "search", "timestamp_ms": 1000,
        "query_id": "q1", "query_text": "space opera",
    }

    assert event_avro.decode_event(event_avro.encode_event(event))["query_id"] == "q1"


def test_item_bearing_behavior_requires_item():
    """Fails if click events without an item are accepted for encoding."""
    event = {
        "event_id": "click-1", "user_id": "u1", "item_id": None,
        "event_type": "click", "timestamp_ms": 1000,
    }

    with pytest.raises(event_avro.EventValidationError, match="item_id"):
        event_avro.encode_event(event)


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
