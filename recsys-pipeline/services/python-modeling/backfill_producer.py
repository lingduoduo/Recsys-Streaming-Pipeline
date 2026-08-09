#!/usr/bin/env python3
"""Backfill the recsys_events topic with weeks of *historical* engagement events.

Unlike producer.py (which stamps every event with `now` at a constant click rate),
this generates behavior slates spanning a historical window, emitted in chronological
order, with a click probability that embeds a known ground-truth pattern:

    p(click) = base
             + diurnal   (peaks in the evening)
             + weekly    (weekend boost)
             + trend     (gradual linear drift over the window)
             + changepoint (one sudden step drop after a chosen day)

The chronological order keeps the OnlineJoiner dedup watermark advancing monotonically
so historical events are not dropped as "late" (still run the consumer with a wide
EVENT_WATERMARK_DELAY — see run-engagement-sim.sh). The OnlineJoiner writes the result
to date-partitioned Parquet, which becomes the time-series store the engagement report
reads. The ground-truth knobs below let that report be validated against what was injected.

Env knobs (all optional):
  KAFKA_BOOTSTRAP_SERVERS  default localhost:9092
  KAFKA_TOPIC              default recsys_events
  BACKFILL_DAYS            default 21      window length (ending now)
  SLATES_PER_HOUR          default 12      slate density
  NUM_USERS / NUM_ITEMS / SLATE_SIZE   default 20 / 30 / 5
  SEED                     default 7
  BASE_CTR                 default 0.30    baseline click probability
  DIURNAL_AMP              default 0.10    evening-peak amplitude
  WEEKEND_BOOST            default 0.06    added on Sat/Sun
  TREND_DELTA              default -0.10   linear drift start→end
  CHANGEPOINT_DAY          default 14      day-offset of the sudden drop (<0 disables)
  CHANGEPOINT_DROP         default 0.12    size of the sudden drop
"""
from __future__ import annotations

import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from producer import make_producer  # reuse the tuned KafkaProducer config

TOPIC = os.getenv("KAFKA_TOPIC", "recsys_events")
BACKFILL_DAYS = max(int(os.getenv("BACKFILL_DAYS", "21")), 1)
SLATES_PER_HOUR = max(int(os.getenv("SLATES_PER_HOUR", "12")), 1)
NUM_USERS = max(int(os.getenv("NUM_USERS", "20")), 1)
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "30")), 1)
SLATE_SIZE = max(int(os.getenv("SLATE_SIZE", "5")), 1)
SEED = int(os.getenv("SEED", "7"))

BASE_CTR = float(os.getenv("BASE_CTR", "0.30"))
DIURNAL_AMP = float(os.getenv("DIURNAL_AMP", "0.10"))
WEEKEND_BOOST = float(os.getenv("WEEKEND_BOOST", "0.06"))
TREND_DELTA = float(os.getenv("TREND_DELTA", "-0.10"))
CHANGEPOINT_DAY = int(os.getenv("CHANGEPOINT_DAY", "14"))
CHANGEPOINT_DROP = float(os.getenv("CHANGEPOINT_DROP", "0.12"))


def click_prob(dt: datetime, start: datetime, end: datetime) -> float:
    """Ground-truth click probability at time `dt` within window [start, end]."""
    # Diurnal: cosine peaking at 20:00, trough at 08:00.
    hour = dt.hour + dt.minute / 60.0
    diurnal = DIURNAL_AMP * math.cos((hour - 20.0) / 24.0 * 2 * math.pi)
    # Weekly: weekend boost (Sat=5, Sun=6).
    weekly = WEEKEND_BOOST if dt.weekday() >= 5 else 0.0
    # Trend: linear drift across the window.
    span = max((end - start).total_seconds(), 1.0)
    frac = (dt - start).total_seconds() / span
    trend = TREND_DELTA * frac
    # Changepoint: sudden step drop after CHANGEPOINT_DAY.
    changepoint = 0.0
    if CHANGEPOINT_DAY >= 0 and (dt - start).days >= CHANGEPOINT_DAY:
        changepoint = -CHANGEPOINT_DROP
    return min(0.95, max(0.02, BASE_CTR + diurnal + weekly + trend + changepoint))


def slate_times(start: datetime, end: datetime, rng: random.Random) -> list[datetime]:
    """Chronologically-sorted slate timestamps across the window."""
    total_hours = int((end - start).total_seconds() // 3600)
    times: list[datetime] = []
    for h in range(total_hours):
        hour_start = start + timedelta(hours=h)
        for _ in range(SLATES_PER_HOUR):
            times.append(hour_start + timedelta(seconds=rng.randint(0, 3599)))
    times.sort()
    return times


def make_slate(dt: datetime, users, items, rng: random.Random) -> list[dict]:
    base_ms = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    user = rng.choice(users)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))
    device = rng.choice(["ios", "android", "web"])
    country = rng.choice(["US", "CA", "GB"])
    user_tier = rng.choice(["new", "standard", "vip"])

    events = []
    for position, item in enumerate(slate_items):
        events.append({
            "event_id": str(uuid.uuid4()),
            "request_id": request_id,
            "session_id": session_id,
            "user_id": user,
            "item_id": item,
            "event_type": "impression",
            "timestamp_ms": base_ms,
            "position": position,
            "user_features": {"tier": user_tier},
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": {"device": device, "country": country},
        })

    p = click_prob(dt, make_slate.start, make_slate.end)
    if rng.random() < p:
        clicked_item = rng.choice(slate_items)
        events.append({
            "event_id": str(uuid.uuid4()),
            "request_id": request_id,
            "session_id": session_id,
            "user_id": user,
            "item_id": clicked_item,
            "event_type": "click",
            "timestamp_ms": base_ms + rng.randint(1, 20) * 1000,
            "position": slate_items.index(clicked_item),
            "user_features": {},
            "item_features": {},
            "context_features": {},
        })
        if rng.random() < 0.12:
            events.append({
                "event_id": str(uuid.uuid4()),
                "request_id": request_id,
                "session_id": session_id,
                "user_id": user,
                "item_id": clicked_item,
                "event_type": "order",
                "timestamp_ms": base_ms + rng.randint(21, 120) * 1000,
                "position": slate_items.index(clicked_item),
                "user_features": {},
                "item_features": {},
                "context_features": {},
            })
    return events


def main() -> None:
    rng = random.Random(SEED)
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    start = end - timedelta(days=BACKFILL_DAYS)
    make_slate.start, make_slate.end = start, end  # ground-truth window for click_prob

    users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]
    items = [f"movie_{i}" for i in range(1, NUM_ITEMS + 1)]
    times = slate_times(start, end, rng)

    print(f"backfilling {TOPIC}: {len(times)} slates over {BACKFILL_DAYS}d "
          f"[{start:%Y-%m-%d} → {end:%Y-%m-%d}]", flush=True)
    producer = make_producer()
    sent = slates = 0
    try:
        for dt in times:
            for event in make_slate(dt, users, items, rng):
                producer.send(TOPIC, value=event, key=event["request_id"])
                sent += 1
            slates += 1
            if slates % 500 == 0:
                print(f"  {slates}/{len(times)} slates, {sent} events", flush=True)
    finally:
        producer.flush()
        producer.close()
    print(f"done: {sent} events from {slates} slates", flush=True)


if __name__ == "__main__":
    main()
