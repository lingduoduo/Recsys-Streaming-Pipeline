"""Emit a slate's feedback when it is due, instead of all at once.

The producers already encode realistic delay in each event's `timestamp_ms` (clicks 1-20s after
impression, orders 21-120s). They then emit the whole slate in one instant, so a consumer never
sees feedback arrive later than its impression. This releases each feedback event at the offset its
own payload already claims.
"""

from __future__ import annotations

import heapq
import itertools
import os
import time
from collections.abc import Callable

IMPRESSION_TYPES = frozenset({"impression", "exposure"})
FEEDBACK_TYPES = frozenset({"click", "order", "purchase", "thumb_up", "thumb_down", "abandon"})

DELAY_SCALE = float(os.getenv("FEEDBACK_DELAY_SCALE", "1.0"))


def split_slate(events: list[dict]) -> tuple[list[dict], list[tuple[float, dict]]]:
    """Split into events to send now and (delay_seconds, event) pairs to send later.

    The delay is derived from the event's own `timestamp_ms` relative to the slate's earliest
    impression, so no caller has to restate the delay model.
    """
    impressions = [e for e in events if e.get("event_type") in IMPRESSION_TYPES]
    if not impressions:
        return events, []

    base_ms = min(e["timestamp_ms"] for e in impressions)
    immediate: list[dict] = []
    deferred: list[tuple[float, dict]] = []
    for candidate in events:
        if candidate.get("event_type") in FEEDBACK_TYPES:
            delay = max(0.0, (candidate["timestamp_ms"] - base_ms) / 1000.0)
            deferred.append((delay, candidate))
        else:
            immediate.append(candidate)
    return immediate, deferred


class FeedbackSchedule:
    """A due-time heap of events awaiting release."""

    def __init__(
        self, scale: float = DELAY_SCALE, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if not scale > 0:
            raise ValueError("FEEDBACK_DELAY_SCALE must be a positive number")
        self._scale = scale
        self._clock = clock
        self._heap: list[tuple[float, int, dict]] = []
        # Sequence breaks ties so heapq never compares two dicts, which would raise.
        self._sequence = itertools.count()

    def schedule(self, delay_seconds: float, event: dict) -> None:
        due_at = self._clock() + max(0.0, delay_seconds) * self._scale
        heapq.heappush(self._heap, (due_at, next(self._sequence), event))

    def due(self, now: float | None = None) -> list[dict]:
        moment = self._clock() if now is None else now
        released = []
        while self._heap and self._heap[0][0] <= moment:
            released.append(heapq.heappop(self._heap)[2])
        return released

    def pending(self) -> int:
        return len(self._heap)

    def next_due_in(self, now: float | None = None) -> float | None:
        if not self._heap:
            return None
        moment = self._clock() if now is None else now
        return max(0.0, self._heap[0][0] - moment)
