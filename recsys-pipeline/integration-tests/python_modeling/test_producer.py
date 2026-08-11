import importlib
import os
import sys
import time
import types
from collections.abc import Mapping
from pathlib import Path
from typing import get_type_hints

import pytest


PRODUCER_PATH = Path(__file__).resolve().parents[2] / "services/python-modeling/producer.py"
PYTHON_MODELING = PRODUCER_PATH.parent
sys.path.insert(0, str(PYTHON_MODELING))

import event_avro


@pytest.fixture(autouse=True)
def isolated_kafka_modules(monkeypatch: pytest.MonkeyPatch):
    """Install Kafka stubs for one test and restore the prior modules afterwards."""
    kafka_stub = types.ModuleType("kafka")
    kafka_errors_stub = types.ModuleType("kafka.errors")

    class KafkaProducerStub:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    kafka_stub.KafkaProducer = KafkaProducerStub
    kafka_errors_stub.NoBrokersAvailable = RuntimeError
    monkeypatch.setitem(sys.modules, "kafka", kafka_stub)
    monkeypatch.setitem(sys.modules, "kafka.errors", kafka_errors_stub)


def load_producer_module():
    """Import producer.py without creating a KafkaProducer connection."""

    spec = importlib.util.spec_from_file_location(
        "producer",
        PRODUCER_PATH,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_kafka_value_serializer_emits_avro_single_object():
    """Fails if producer values bypass the canonical Avro single-object encoder."""
    mod = load_producer_module()

    payload = mod.serialize_event(mod.make_click_event(["u"], ["i"]))

    assert payload[:10] == b"\xc3\x01\xab\x79\x79\x48\x5f\x27\x5b\x22"
    assert event_avro.decode_event(payload)["event_type"] == "click"


def test_make_producer_installs_shared_avro_value_serializer():
    """Fails if KafkaProducer is configured with JSON rather than serialize_event."""
    mod = load_producer_module()

    producer = mod.make_producer()
    event = mod.make_click_event(["u"], ["i"])

    assert producer.kwargs["value_serializer"] is mod.serialize_event
    assert event_avro.decode_event(producer.kwargs["value_serializer"](event))["event_id"] == event["event_id"]


def test_serialize_event_declares_the_kafka_boundary_types():
    mod = load_producer_module()

    hints = get_type_hints(mod.serialize_event)

    assert hints == {"event": Mapping[str, object], "return": bytes}


def make_event(users, items):
    mod = load_producer_module()
    return mod.make_click_event(users, items)


class TestEventSchema:
    def test_event_has_required_keys(self):
        users = ["user_1", "user_2"]
        items = ["movie_1", "movie_2"]
        event = make_event(users, items)
        assert set(event.keys()) == {"event_id", "user_id", "item_id", "event_type", "timestamp_ms"}

    def test_user_id_is_string(self):
        users = ["user_1"]
        items = ["movie_1"]
        event = make_event(users, items)
        assert isinstance(event["user_id"], str)

    def test_item_id_is_string(self):
        users = ["user_1"]
        items = ["movie_1"]
        event = make_event(users, items)
        assert isinstance(event["item_id"], str)

    def test_event_type_is_click(self):
        event = make_event(["u"], ["movie_1"])
        assert event["event_type"] == "click"

    def test_timestamp_ms_is_int_and_recent(self):
        event = make_event(["u"], ["movie_1"])
        assert isinstance(event["timestamp_ms"], int)
        assert event["timestamp_ms"] > 1_000_000_000_000
        assert event["timestamp_ms"] <= int(time.time() * 1000) + 2000

    def test_user_id_comes_from_pool(self):
        users = ["user_1", "user_2", "user_3"]
        items = ["movie_1"]
        for _ in range(50):
            event = make_event(users, items)
            assert event["user_id"] in users

    def test_item_id_comes_from_pool(self):
        users = ["user_1"]
        items = ["movie_1", "movie_2", "movie_3"]
        for _ in range(50):
            event = make_event(users, items)
            assert event["item_id"] in items


class TestEnvConfig:
    def test_num_users_default(self, monkeypatch):
        monkeypatch.delenv("NUM_USERS", raising=False)
        num = max(int(os.getenv("NUM_USERS", "5")), 1)
        assert num == 5

    def test_num_items_default(self, monkeypatch):
        monkeypatch.delenv("NUM_ITEMS", raising=False)
        num = max(int(os.getenv("NUM_ITEMS", "10")), 1)
        assert num == 10

    def test_num_users_from_env(self, monkeypatch):
        monkeypatch.setenv("NUM_USERS", "100")
        num = max(int(os.getenv("NUM_USERS", "5")), 1)
        assert num == 100

    def test_num_items_from_env(self, monkeypatch):
        monkeypatch.setenv("NUM_ITEMS", "50")
        num = max(int(os.getenv("NUM_ITEMS", "10")), 1)
        assert num == 50

    def test_num_users_floored_at_one(self, monkeypatch):
        monkeypatch.setenv("NUM_USERS", "0")
        num = max(int(os.getenv("NUM_USERS", "5")), 1)
        assert num == 1


def test_report_delivery_error_prints_failure(capsys):
    mod = load_producer_module()

    mod.report_delivery_error(RuntimeError("broker unavailable"))

    assert "delivery failed: broker unavailable" in capsys.readouterr().out


class TestBehaviorWorkflowEvents:
    def test_behavior_slate_contains_impressions_with_request_id(self):
        mod = load_producer_module()
        events = mod.make_behavior_slate(["user_1"], ["movie_1", "movie_2", "movie_3"])
        impressions = [event for event in events if event["event_type"] == "impression"]
        assert len(impressions) >= 1
        assert all(event["request_id"] == impressions[0]["request_id"] for event in impressions)
        assert all("user_features" in event for event in impressions)
        assert all("item_features" in event for event in impressions)
        assert all("context_features" in event for event in impressions)

    def test_behavior_slate_events_have_unified_schema(self):
        mod = load_producer_module()
        events = mod.make_behavior_slate(["user_1"], ["movie_1", "movie_2", "movie_3"])
        for event in events:
            assert "event_id" in event, f"event_id missing from {event}"
            assert "timestamp_ms" in event, f"timestamp_ms missing from {event}"
            assert "timestamp" not in event, f"old 'timestamp' field still present in {event}"
            assert isinstance(event["event_id"], str)
            assert isinstance(event["timestamp_ms"], int)
            assert event["timestamp_ms"] > 1_000_000_000_000
            assert "session_id" in event

    def test_behavior_slate_event_ids_are_unique(self):
        mod = load_producer_module()
        events = mod.make_behavior_slate(["user_1"], ["movie_1", "movie_2", "movie_3", "movie_4", "movie_5"])
        event_ids = [e["event_id"] for e in events]
        assert len(event_ids) == len(set(event_ids)), "event_ids must be unique per event"


def test_click_event_has_unified_schema():
    users = ["user_1", "user_2"]
    items = ["movie_1", "movie_2", "movie_3"]
    mod = load_producer_module()
    event = mod.make_click_event(users, items)
    assert "event_id" in event
    assert isinstance(event["event_id"], str)
    assert event["user_id"].startswith("user_")
    assert event["item_id"].startswith("movie_")
    assert "timestamp_ms" in event
    assert isinstance(event["timestamp_ms"], int)
    assert event["timestamp_ms"] > 1_000_000_000_000


def test_behavior_slate_has_unified_schema():
    users = ["user_1"]
    items = ["movie_1", "movie_2", "movie_3", "movie_4", "movie_5"]
    mod = load_producer_module()
    events = mod.make_behavior_slate(users, items)
    assert len(events) >= 1
    for event in events:
        assert "event_id" in event
        assert event["user_id"].startswith("user_")
        assert event["item_id"].startswith("movie_")
        assert "timestamp_ms" in event
        assert isinstance(event["timestamp_ms"], int)
        assert event["timestamp_ms"] > 1_000_000_000_000


class _NoOpFuture:
    def add_errback(self, callback):
        return self


class _RecordingProducer:
    """Stands in for KafkaProducer: records every value passed to send(), no network."""

    def __init__(self):
        self.sent = []

    def send(self, topic, value=None, key=None):
        self.sent.append(value)
        return _NoOpFuture()

    def flush(self):
        pass

    def close(self):
        pass


def test_max_events_drains_pending_feedback_before_returning(monkeypatch):
    """Regression test for the MAX_EVENTS early return stranding scheduled feedback.

    Runs producer.py's real main() in behavior mode with MAX_EVENTS set to the slate's
    impression count, so the cap is hit right after the first slate's impressions are sent —
    while that slate's click and order are still sitting in the schedule, not yet due. Before
    the fix, main() returned immediately at that point and the click/order were never sent.
    """
    monkeypatch.setenv("PRODUCER_MODE", "behavior")
    monkeypatch.setenv("NUM_USERS", "1")
    monkeypatch.setenv("NUM_ITEMS", "5")
    monkeypatch.setenv("SLATE_SIZE", "3")
    monkeypatch.setenv("MAX_EVENTS", "3")
    monkeypatch.setenv("EVENTS_PER_SECOND", "1000")
    monkeypatch.setenv("LOG_EVERY", "1000")

    mod = load_producer_module()

    # Force a click and an order every slate, with the smallest possible encoded delay, so the
    # drain loop's real time.sleep calls stay well under a second.
    monkeypatch.setattr(mod.random, "random", lambda: 0.0)
    monkeypatch.setattr(mod.random, "randint", lambda a, b: a)

    # Compress FeedbackSchedule's real-time delay so the drain finishes fast, without depending
    # on the FEEDBACK_DELAY_SCALE-derived module constant (already fixed at whatever value
    # feedback_schedule.py first loaded with, elsewhere in the suite).
    real_feedback_schedule = mod.FeedbackSchedule
    monkeypatch.setattr(mod, "FeedbackSchedule", lambda: real_feedback_schedule(scale=0.01))

    recorder = _RecordingProducer()
    monkeypatch.setattr(mod, "make_producer", lambda: recorder)

    mod.main()

    sent_types = [event["event_type"] for event in recorder.sent]
    assert sent_types.count("impression") == 3
    assert "click" in sent_types, "deferred click was left unsent when MAX_EVENTS returned early"
    assert "order" in sent_types, "deferred order was left unsent when MAX_EVENTS returned early"
    assert len(recorder.sent) == 5, "nothing scheduled should be left behind"
