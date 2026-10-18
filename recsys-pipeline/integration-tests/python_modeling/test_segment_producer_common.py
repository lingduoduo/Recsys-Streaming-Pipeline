import importlib

import pytest

from feedback_schedule import FeedbackSchedule


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        assert 0 < seconds <= 1.0
        self.now += seconds


class RecordingProducer:
    def __init__(self):
        self.sent = []
        self.flushed = self.closed = False

    def send(self, topic, *, value, key):
        self.sent.append((topic, key, value))

    def flush(self):
        self.flushed = True

    def close(self):
        self.closed = True


def test_slate_events_keep_context_on_impressions_and_copy_features():
    from segment_producer_common import SlateEvents

    features = {"subscription": "premium"}
    context = {"surface": "home_feed", "device": "ios",
               "locale": "en-US", "timezone": "America/New_York"}
    slate = SlateEvents("user_1", "req_1", "sess_1", features, context)
    impression = slate.event("movie_1", "impression", 1000, 0)
    click = slate.event("movie_1", "click", 2000, 0)

    assert impression["surface"] == "home_feed"
    assert impression["timezone"] == "America/New_York"
    assert not set(context).intersection(click)
    assert click["request_id"] == "req_1" and click["session_id"] == "sess_1"
    assert click["user_id"] == "user_1" and click["item_id"] == "movie_1"
    assert click["timestamp_ms"] == 2000 and click["position"] == 0
    assert impression["event_id"] != click["event_id"]
    impression["user_features"]["subscription"] = "free"
    impression["item_features"]["bucket"] = "b1"
    assert click["user_features"] == features == {"subscription": "premium"}
    assert click["item_features"] == {} and click["context_features"] == {}


def test_emitter_defers_feedback_and_drains_in_due_order(monkeypatch):
    from segment_producer_common import BehaviorEmitter

    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    producer = RecordingProducer()
    emitter = BehaviorEmitter(producer, "events", schedule=schedule)
    impression = {"request_id": "req_1", "event_type": "impression", "timestamp_ms": 1000}
    order = {"request_id": "req_1", "event_type": "order", "timestamp_ms": 4000}
    click = {"request_id": "req_1", "event_type": "click", "timestamp_ms": 2000}
    assert emitter.emit_slate([impression, order, click]) == 1
    assert producer.sent == [("events", "req_1", impression)]
    assert schedule.pending() == 2

    monkeypatch.setattr("segment_producer_common.time.sleep", clock.sleep)
    assert emitter.drain() == 2
    assert producer.sent == [("events", "req_1", impression),
                             ("events", "req_1", click), ("events", "req_1", order)]
    assert schedule.pending() == 0
    assert emitter.drain() == 0


def test_emitter_releases_previous_slate_feedback_during_next_slate():
    from segment_producer_common import BehaviorEmitter

    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    producer = RecordingProducer()
    emitter = BehaviorEmitter(producer, "events", schedule=schedule)
    first = {"request_id": "req_1", "event_type": "impression", "timestamp_ms": 1000}
    click = {"request_id": "req_1", "event_type": "click", "timestamp_ms": 2000}
    second = {"request_id": "req_2", "event_type": "impression", "timestamp_ms": 3000}
    assert emitter.emit_slate([first, click]) == 1
    clock.now = 2.0
    assert emitter.emit_slate([second]) == 2
    assert producer.sent == [("events", "req_1", first), ("events", "req_2", second),
                             ("events", "req_1", click)]


def test_emitter_propagates_send_errors():
    from segment_producer_common import BehaviorEmitter

    class FailingProducer:
        def send(self, topic, *, value, key):
            raise RuntimeError("Kafka send failed")

    with pytest.raises(RuntimeError, match="Kafka send failed"):
        BehaviorEmitter(FailingProducer(), "events").emit_slate([
            {"request_id": "req_1", "event_type": "impression", "timestamp_ms": 1000}])


@pytest.mark.parametrize("module_name", ["movie_segment_producer", "movielens_segment_producer"])
def test_segment_main_sends_context_and_drains_behavior_before_close(module_name, monkeypatch):
    module = importlib.import_module(module_name)
    behavior, context = RecordingProducer(), RecordingProducer()
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    monkeypatch.setattr("segment_producer_common.FeedbackSchedule", lambda: schedule)
    monkeypatch.setattr("segment_producer_common.time.sleep", clock.sleep)
    monkeypatch.setattr(module, "make_producer", lambda: behavior)
    monkeypatch.setattr(module, "make_json_producer", lambda: context)
    monkeypatch.setattr(module, "NUM_ITEMS", 10)
    monkeypatch.setattr(module, "NUM_USERS", 2)
    monkeypatch.setattr(module, "NUM_SLATES", 10)
    monkeypatch.setattr(module, "SLATE_SIZE", 3)
    if module_name == "movielens_segment_producer":
        monkeypatch.setattr(module, "RATINGS_PER_USER", 2)
    monkeypatch.delenv("RATINGS_OUTPUT_PATH", raising=False)

    module.main()

    assert behavior.flushed and behavior.closed and context.flushed and context.closed
    assert schedule.pending() == 0
    assert sum(value["event_type"] == "impression" for _, _, value in behavior.sent) == 30
    assert any(value["event_type"] == "click" for _, _, value in behavior.sent)
    assert all(topic == module.RECSYS_TOPIC and key == value["request_id"]
               for topic, key, value in behavior.sent)
    assert all(topic == module.CONTEXT_TOPIC for topic, _, _ in context.sent)
    assert len(context.sent) == (10 if module_name == "movie_segment_producer" else 6)
