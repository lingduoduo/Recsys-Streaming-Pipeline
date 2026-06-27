#!/usr/bin/env python3
"""Synthetic stream for USER-SEGMENT engagement comparison.

Each synthetic user is assigned fixed demographics — cohort (new vs existing), age band,
sex, education — carried in every event's `user_features`. Geo and platform vary per event
and ride in `context_features`. Click (and order-given-click) probability is modulated by a
known per-segment ground truth, so the segment report can be validated against what was
injected. Events go to recsys_events; OnlineJoiner carries user_features/context_features into
the date-partitioned Parquet, where segment_report.py breaks engagement down by segment.

Env knobs:
  KAFKA_BOOTSTRAP_SERVERS / KAFKA_TOPIC   defaults localhost:9092 / recsys_events
  NUM_USERS        default 800
  NUM_SLATES       default 20000      total slates to emit
  NUM_ITEMS / SLATE_SIZE   default 30 / 5
  SEED             default 11
"""
from __future__ import annotations

import os
import random
import time
import uuid

from producer import make_producer  # reuse the tuned KafkaProducer config

TOPIC = os.getenv("KAFKA_TOPIC", "recsys_events")
NUM_USERS = max(int(os.getenv("NUM_USERS", "800")), 1)
NUM_SLATES = max(int(os.getenv("NUM_SLATES", "20000")), 1)
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "30")), 1)
SLATE_SIZE = max(int(os.getenv("SLATE_SIZE", "5")), 1)
SEED = int(os.getenv("SEED", "11"))

AGE_BANDS = ["18-24", "25-34", "35-44", "45-54", "55+"]
SEXES = ["F", "M"]
EDU = ["hs", "college", "grad"]
GEOS = ["US", "CA", "GB", "DE", "IN"]
PLATFORMS = ["ios", "android", "web"]

# Ground-truth additive effects on click probability (base below). Documented so the
# segment report can be checked: existing > new, ios > web, 25-34 top, US top, grad > hs.
BASE_CTR = 0.20
COHORT_EFF = {"new": 0.0, "existing": 0.05}
AGE_EFF = {"18-24": 0.0, "25-34": 0.06, "35-44": 0.03, "45-54": 0.01, "55+": -0.02}
SEX_EFF = {"F": 0.01, "M": 0.0}
EDU_EFF = {"hs": 0.0, "college": 0.02, "grad": 0.03}
GEO_EFF = {"US": 0.03, "CA": 0.02, "GB": 0.01, "DE": 0.0, "IN": -0.01}
PLATFORM_EFF = {"ios": 0.04, "android": 0.01, "web": 0.0}


def assign_demographics(num_users: int, rng: random.Random) -> dict[str, dict]:
    """Stable per-user demographics keyed by user_id."""
    demo = {}
    for i in range(1, num_users + 1):
        demo[f"user_{i}"] = {
            "cohort": "new" if rng.random() < 0.35 else "existing",
            "age_band": rng.choice(AGE_BANDS),
            "sex": rng.choice(SEXES),
            "education": rng.choice(EDU),
        }
    return demo


def click_prob(demo: dict, geo: str, platform: str) -> float:
    p = (BASE_CTR + COHORT_EFF[demo["cohort"]] + AGE_EFF[demo["age_band"]]
         + SEX_EFF[demo["sex"]] + EDU_EFF[demo["education"]]
         + GEO_EFF[geo] + PLATFORM_EFF[platform])
    return min(0.95, max(0.02, p))


def order_prob(demo: dict, platform: str) -> float:
    """Order probability given a click — also segment-modulated."""
    p = 0.20 + (0.06 if demo["cohort"] == "existing" else 0.0) \
        + (0.03 if platform == "ios" else 0.0) \
        + (0.03 if demo["education"] == "grad" else 0.0)
    return min(0.9, max(0.02, p))


def make_slate(user: str, demo: dict, items, rng: random.Random) -> list[dict]:
    now_ms = int(time.time() * 1000)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))
    geo = rng.choice(GEOS)
    platform = rng.choice(PLATFORMS)
    user_features = {
        "cohort": demo["cohort"], "age_band": demo["age_band"],
        "sex": demo["sex"], "education": demo["education"],
    }
    context_features = {"geo": geo, "platform": platform}

    events = []
    for position, item in enumerate(slate_items):
        events.append({
            "event_id": str(uuid.uuid4()),
            "request_id": request_id,
            "session_id": session_id,
            "user_id": user,
            "item_id": item,
            "event_type": "impression",
            "timestamp_ms": now_ms,
            "position": position,
            "user_features": user_features,
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": context_features,
        })

    if rng.random() < click_prob(demo, geo, platform):
        clicked_item = rng.choice(slate_items)
        events.append({
            "event_id": str(uuid.uuid4()),
            "request_id": request_id,
            "session_id": session_id,
            "user_id": user,
            "item_id": clicked_item,
            "event_type": "click",
            "timestamp_ms": now_ms + rng.randint(1, 20) * 1000,
            "position": slate_items.index(clicked_item),
            "user_features": {},
            "item_features": {},
            "context_features": {},
        })
        if rng.random() < order_prob(demo, platform):
            events.append({
                "event_id": str(uuid.uuid4()),
                "request_id": request_id,
                "session_id": session_id,
                "user_id": user,
                "item_id": clicked_item,
                "event_type": "order",
                "timestamp_ms": now_ms + rng.randint(21, 120) * 1000,
                "position": slate_items.index(clicked_item),
                "user_features": {},
                "item_features": {},
                "context_features": {},
            })
    return events


def main() -> None:
    rng = random.Random(SEED)
    demo = assign_demographics(NUM_USERS, rng)
    users = list(demo.keys())
    items = [f"movie_{i}" for i in range(1, NUM_ITEMS + 1)]

    print(f"segment stream → {TOPIC}: {NUM_SLATES} slates, {NUM_USERS} users", flush=True)
    producer = make_producer()
    sent = 0
    try:
        for s in range(NUM_SLATES):
            user = rng.choice(users)
            for event in make_slate(user, demo[user], items, rng):
                producer.send(TOPIC, value=event, key=event["request_id"])
                sent += 1
            if (s + 1) % 2000 == 0:
                print(f"  {s + 1}/{NUM_SLATES} slates, {sent} events", flush=True)
    finally:
        producer.flush()
        producer.close()
    print(f"done: {sent} events from {NUM_SLATES} slates", flush=True)


if __name__ == "__main__":
    main()
