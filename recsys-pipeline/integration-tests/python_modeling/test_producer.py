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

    assert payload[:10] == b"\xc3\x01\xb3\x4b\xfe\x80\xe8\xab\x86\xaf"
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


class _FakeFeedbackSchedule:
    """Deterministic stand-in for FeedbackSchedule: due() releases events according to a
    prescribed release plan (how many of the oldest still-scheduled events to release on each
    call), independent of real elapsed time.

    This reproduces the exact bug without any wall-clock race: schedule.due() pops events off
    its heap before main()'s MAX_EVENTS check runs, so an already-popped event stranded past the
    point where a tick's event list gets cut off is gone from the schedule too — the drain loop
    that follows never sees it.
    """

    def __init__(self, release_counts):
        self._release_counts = list(release_counts)
        self._queue: list[dict] = []

    def schedule(self, delay_seconds, event):
        self._queue.append(event)

    def due(self):
        n = self._release_counts.pop(0) if self._release_counts else 0
        released, self._queue = self._queue[:n], self._queue[n:]
        return released

    def pending(self):
        return len(self._queue)

    def next_due_in(self):
        return 0.0


def test_max_events_drains_pending_feedback_before_returning(monkeypatch):
    """Regression test for the MAX_EVENTS early return stranding scheduled feedback.

    Runs producer.py's real main() in behavior mode with a deterministic fake FeedbackSchedule
    (no reliance on wall-clock timing): tick 1's due() call sees nothing due yet; tick 2's due()
    call releases tick 1's click and order, which are already popped off the "heap" by the time
    the tick's event list is built. MAX_EVENTS trips on tick 2's click, mid-list, with tick 1's
    order still unconsumed in that same list and no longer in the schedule. Before the fix,
    main() returned immediately at that point and the order was never sent — nor was the
    following tick's click/order, since draining resumed from an already-mutated queue.
    """
    monkeypatch.setenv("PRODUCER_MODE", "behavior")
    monkeypatch.setenv("NUM_USERS", "1")
    monkeypatch.setenv("NUM_ITEMS", "5")
    monkeypatch.setenv("SLATE_SIZE", "3")
    monkeypatch.setenv("MAX_EVENTS", "7")
    monkeypatch.setenv("EVENTS_PER_SECOND", "1000")
    monkeypatch.setenv("LOG_EVERY", "1000")

    mod = load_producer_module()

    # Force a click and an order every slate.
    monkeypatch.setattr(mod.random, "random", lambda: 0.0)
    monkeypatch.setattr(mod.random, "randint", lambda a, b: a)

    # release_counts: tick 1's due() releases nothing (0); tick 2's due() releases tick 1's
    # click+order (2, already queued ahead of tick 2's own click+order); the drain call after
    # MAX_EVENTS trips releases tick 2's click+order (2).
    fake_schedule = _FakeFeedbackSchedule(release_counts=[0, 2, 2])
    monkeypatch.setattr(mod, "FeedbackSchedule", lambda: fake_schedule)

    recorder = _RecordingProducer()
    monkeypatch.setattr(mod, "make_producer", lambda: recorder)

    mod.main()

    sent_types = [event["event_type"] for event in recorder.sent]
    assert sent_types.count("impression") == 6
    assert sent_types.count("click") == 2, f"expected both clicks sent, got {sent_types}"
    assert sent_types.count("order") == 2, f"expected both orders sent, got {sent_types}"
    assert len(recorder.sent) == 10, "nothing popped from the schedule may be lost"
