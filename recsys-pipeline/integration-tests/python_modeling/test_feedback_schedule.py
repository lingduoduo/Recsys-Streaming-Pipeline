import sys
from pathlib import Path

import pytest

PYTHON_MODELING = Path(__file__).resolve().parents[2] / "services/python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

from feedback_schedule import FeedbackSchedule, split_slate


def event(event_type: str, timestamp_ms: int, item: str = "item_1") -> dict:
    return {"event_type": event_type, "timestamp_ms": timestamp_ms, "item_id": item}


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_split_slate_defers_feedback_by_its_own_encoded_offset():
    events = [
        event("impression", 1_000_000),
        event("impression", 1_000_000, "item_2"),
        event("click", 1_005_000),
        event("order", 1_030_000),
    ]

    immediate, deferred = split_slate(events)

    assert [e["event_type"] for e in immediate] == ["impression", "impression"]
    assert [(delay, e["event_type"]) for delay, e in deferred] == [(5.0, "click"), (30.0, "order")]


def test_split_slate_passes_through_a_slate_with_no_impression():
    events = [event("click", 1_005_000)]

    immediate, deferred = split_slate(events)

    assert immediate == events and deferred == []


def test_schedule_releases_nothing_before_its_due_time():
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    schedule.schedule(5.0, event("click", 1_005_000))

    assert schedule.due() == []
    assert schedule.pending() == 1
    assert schedule.next_due_in() == 5.0

    clock.now = 4.999
    assert schedule.due() == []

    clock.now = 5.0
    assert [e["event_type"] for e in schedule.due()] == ["click"]
    assert schedule.pending() == 0
    assert schedule.next_due_in() is None


def test_schedule_releases_in_due_order_not_insertion_order():
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    schedule.schedule(30.0, event("order", 1_030_000))
    schedule.schedule(5.0, event("click", 1_005_000))

    clock.now = 40.0

    assert [e["event_type"] for e in schedule.due()] == ["click", "order"]


def test_scale_compresses_delays():
    clock = FakeClock()
    schedule = FeedbackSchedule(scale=0.1, clock=clock)
    schedule.schedule(30.0, event("order", 1_030_000))

    clock.now = 2.9
    assert schedule.due() == []

    clock.now = 3.0
    assert len(schedule.due()) == 1


def test_non_positive_scale_is_rejected():
    with pytest.raises(ValueError, match="FEEDBACK_DELAY_SCALE"):
        FeedbackSchedule(scale=0.0)


def test_producer_emission_pattern_sends_impressions_before_feedback():
    """The pattern each producer main loop follows: immediate now, deferred when due."""
    clock = FakeClock()
    schedule = FeedbackSchedule(clock=clock)
    sent: list[str] = []

    slate = [
        event("impression", 1_000_000),
        event("click", 1_005_000),
        event("order", 1_030_000),
    ]
    immediate, deferred = split_slate(slate)
    for pending in immediate:
        sent.append(pending["event_type"])
    for delay, pending in deferred:
        schedule.schedule(delay, pending)
    for pending in schedule.due():
        sent.append(pending["event_type"])

    assert sent == ["impression"]

    clock.now = 5.0
    sent.extend(e["event_type"] for e in schedule.due())
    assert sent == ["impression", "click"]

    clock.now = 30.0
    sent.extend(e["event_type"] for e in schedule.due())
    assert sent == ["impression", "click", "order"]
    assert schedule.pending() == 0
