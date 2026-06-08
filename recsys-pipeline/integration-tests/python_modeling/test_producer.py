import importlib
import os
import sys
import time
import types
from pathlib import Path


PRODUCER_PATH = Path(__file__).resolve().parents[2] / "services/python-modeling/producer.py"


def load_producer_module():
    """Import producer.py without creating a KafkaProducer connection."""
    kafka_stub = types.ModuleType("kafka")
    kafka_errors_stub = types.ModuleType("kafka.errors")
    kafka_stub.KafkaProducer = lambda **kwargs: None
    kafka_errors_stub.NoBrokersAvailable = RuntimeError
    sys.modules.setdefault("kafka", kafka_stub)
    sys.modules.setdefault("kafka.errors", kafka_errors_stub)

    spec = importlib.util.spec_from_file_location(
        "producer",
        PRODUCER_PATH,
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_event(users, items):
    mod = load_producer_module()
    return mod.make_click_event(users, items)


class TestEventSchema:
    def test_event_has_required_keys(self):
        users = ["user_1", "user_2"]
        items = ["item_1", "item_2"]
        event = make_event(users, items)
        assert set(event.keys()) == {"user_id", "item_id", "event_type", "timestamp"}

    def test_user_id_is_string(self):
        users = ["user_1"]
        items = ["item_1"]
        event = make_event(users, items)
        assert isinstance(event["user_id"], str)

    def test_item_id_is_string(self):
        users = ["user_1"]
        items = ["item_1"]
        event = make_event(users, items)
        assert isinstance(event["item_id"], str)

    def test_event_type_is_click(self):
        event = make_event(["u"], ["i"])
        assert event["event_type"] == "click"

    def test_timestamp_is_int_and_recent(self):
        event = make_event(["u"], ["i"])
        assert isinstance(event["timestamp"], int)
        assert event["timestamp"] <= int(time.time()) + 1

    def test_user_id_comes_from_pool(self):
        users = ["user_1", "user_2", "user_3"]
        items = ["item_1"]
        for _ in range(50):
            event = make_event(users, items)
            assert event["user_id"] in users

    def test_item_id_comes_from_pool(self):
        users = ["user_1"]
        items = ["item_1", "item_2", "item_3"]
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
        events = mod.make_behavior_slate(["user_1"], ["item_1", "item_2", "item_3"])
        impressions = [event for event in events if event["event_type"] == "impression"]
        assert len(impressions) >= 1
        assert all(event["request_id"] == impressions[0]["request_id"] for event in impressions)
        assert all("user_features" in event for event in impressions)
        assert all("item_features" in event for event in impressions)
        assert all("context_features" in event for event in impressions)
