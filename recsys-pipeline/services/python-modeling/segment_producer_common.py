"""Shared event envelopes and delayed delivery for the segment simulations."""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass

from feedback_schedule import FeedbackSchedule, split_slate

PLATFORMS = ["ios", "android", "web"]
SURFACES = ("home_feed", "search_results", "detail_page", "continue_watching")
SURFACE_EFF = {"home_feed": 0.02, "search_results": 0.04,
               "detail_page": 0.01, "continue_watching": 0.03}


@dataclass
class SlateEvents:
    """Build independent event payloads, with request context on impressions only."""

    user: str
    request_id: str
    session_id: str
    user_features: dict
    context: dict

    def event(self, item: str, event_type: str, timestamp_ms: int, position: int) -> dict:
        event = {
            "event_id": str(uuid.uuid4()), "request_id": self.request_id,
            "session_id": self.session_id, "user_id": self.user, "item_id": item,
            "event_type": event_type, "timestamp_ms": timestamp_ms, "position": position,
            "user_features": dict(self.user_features), "item_features": {}, "context_features": {},
        }
        if event_type == "impression":
            event.update(self.context)
        return event


class BehaviorEmitter:
    """Send impressions now and release feedback at its encoded offset.

    The caller owns the Kafka producer and must drain before flushing and closing it.
    """

    def __init__(self, producer, topic: str, *, schedule: FeedbackSchedule | None = None):
        self.producer = producer
        self.topic = topic
        self.schedule = schedule if schedule is not None else FeedbackSchedule()

    def _send(self, events: list[dict]) -> int:
        for event in events:
            self.producer.send(self.topic, value=event, key=event["request_id"])
        return len(events)

    def emit_slate(self, events: list[dict]) -> int:
        immediate, deferred = split_slate(events)
        sent = self._send(immediate)
        for delay, event in deferred:
            self.schedule.schedule(delay, event)
        return sent + self._send(self.schedule.due())

    def drain(self) -> int:
        sent = 0
        while self.schedule.pending():
            wait = self.schedule.next_due_in()
            if wait:
                time.sleep(min(wait, 1.0))
            sent += self._send(self.schedule.due())
        return sent
