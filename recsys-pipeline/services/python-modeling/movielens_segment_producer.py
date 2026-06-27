#!/usr/bin/env python3
"""Segment simulation aligned to the canonical MovieLens UserEvent demographics.

Unlike segment_producer.py (which embeds invented attributes in user_features), this routes
demographics through the REAL context path and keeps engagement on the behavior path:

  • per user: a UserUpdated-shaped record {user_id, age:int, gender, occupation, zip_code,
    timestamp} → topic `movielens_context` → MovieLensContextCollectorStreamingJob →
    Redis user:{id}:features.
  • behavior slates (impression/click/order, NO embedded demographics) → topic `recsys_events`
    → OnlineJoinerStreamingJob → training_samples Parquet.

Engagement probability is modulated by the user's canonical demographics (ground truth, below),
so the report — which joins Parquet engagement with Redis demographics — recovers the segment
differences. age→age_band and zip_code→geo are derived via segment_features.

Env: KAFKA_BOOTSTRAP_SERVERS, RECSYS_TOPIC (recsys_events), MOVIELENS_CONTEXT_TOPIC
(movielens_context), NUM_USERS (800), NUM_SLATES (20000), NUM_ITEMS (30), SLATE_SIZE (5), SEED (13).
"""
from __future__ import annotations

import os
import random
import time
import uuid

from producer import make_producer
from segment_features import derive_age_band, derive_geo

RECSYS_TOPIC = os.getenv("RECSYS_TOPIC", "recsys_events")
CONTEXT_TOPIC = os.getenv("MOVIELENS_CONTEXT_TOPIC", "movielens_context")
NUM_USERS = max(int(os.getenv("NUM_USERS", "800")), 1)
NUM_SLATES = max(int(os.getenv("NUM_SLATES", "20000")), 1)
RATINGS_PER_USER = max(int(os.getenv("RATINGS_PER_USER", "20")), 0)
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "30")), 1)
SLATE_SIZE = max(int(os.getenv("SLATE_SIZE", "5")), 1)
SEED = int(os.getenv("SEED", "13"))

GENDERS = ["F", "M"]
OCCUPATIONS = ["student", "engineer", "scientist", "educator", "technician",
               "writer", "artist", "administrator", "marketing", "retired", "other"]
PLATFORMS = ["ios", "android", "web"]

# Ground-truth additive effects on click probability (base below), keyed by the *derived*
# buckets so the report can recover them: 25-34 top / 55+ bottom; F>M; student/eng/sci top,
# retired bottom; West/NE top; ios>web.
BASE_CTR = 0.20
AGE_EFF = {"18-24": 0.0, "25-34": 0.06, "35-44": 0.03, "45-54": 0.01, "55+": -0.02}
GENDER_EFF = {"F": 0.01, "M": 0.0}
OCC_EFF = {"student": 0.04, "engineer": 0.03, "scientist": 0.03, "educator": 0.02,
           "technician": 0.02, "writer": 0.01, "artist": 0.01, "administrator": 0.0,
           "marketing": 0.0, "retired": -0.02, "other": -0.01}
REGION_EFF = {"Northeast": 0.02, "Mid-Atlantic": 0.02, "Southeast": 0.0, "Midwest": 0.0,
              "South-Central": -0.01, "Mountain": 0.0, "West": 0.03, "unknown": 0.0}
PLATFORM_EFF = {"ios": 0.04, "android": 0.01, "web": 0.0}


def assign_demographics(num_users: int, rng: random.Random) -> dict[str, dict]:
    """Canonical UserEvent demographics per user: age:int, gender, occupation, zip_code."""
    demo = {}
    for i in range(1, num_users + 1):
        first = str(rng.randint(0, 9))                      # zip region digit
        zip_code = first + "".join(str(rng.randint(0, 9)) for _ in range(4))
        demo[f"user_{i}"] = {
            "age": rng.randint(18, 64),
            "gender": rng.choice(GENDERS),
            "occupation": rng.choice(OCCUPATIONS),
            "zip_code": zip_code,
        }
    return demo


def click_prob(demo: dict, platform: str) -> float:
    p = (BASE_CTR + AGE_EFF[derive_age_band(demo["age"])] + GENDER_EFF[demo["gender"]]
         + OCC_EFF[demo["occupation"]] + REGION_EFF[derive_geo(demo["zip_code"])]
         + PLATFORM_EFF[platform])
    return min(0.95, max(0.02, p))


def order_prob(demo: dict, platform: str) -> float:
    p = 0.20 + (0.04 if demo["occupation"] in ("engineer", "scientist") else 0.0) \
        + (0.03 if platform == "ios" else 0.0)
    return min(0.9, max(0.02, p))


def rating_mean(demo: dict) -> float:
    """Ground-truth mean rating for a user (1-5), correlated with engagement segments so the
    report's avg-rating-per-segment recovers the same orderings as CTR."""
    eff = (AGE_EFF[derive_age_band(demo["age"])] + GENDER_EFF[demo["gender"]]
           + OCC_EFF[demo["occupation"]] + REGION_EFF[derive_geo(demo["zip_code"])])
    return min(5.0, max(1.0, 3.2 + 8.0 * eff))


def rating_value(demo: dict, rng: random.Random) -> int:
    return min(5, max(1, round(rng.gauss(rating_mean(demo), 0.8))))


def rating_event(user: str, item: str, demo: dict, rng: random.Random) -> dict:
    """RatingEvent-shaped record for movielens_context (event_type=rating)."""
    return {
        "user_id": user,
        "item_id": item,
        "event_type": "rating",
        "rating": float(rating_value(demo, rng)),
        "timestamp": int(time.time()),
    }


def demographics_event(user: str, demo: dict) -> dict:
    """UserUpdated-shaped record for movielens_context (matches RecSysEvent.UserUpdated JSON)."""
    return {
        "user_id": user,
        "age": demo["age"],
        "gender": demo["gender"],
        "occupation": demo["occupation"],
        "zip_code": demo["zip_code"],
        "timestamp": int(time.time()),
    }


def make_slate(user: str, demo: dict, items, rng: random.Random) -> list[dict]:
    now_ms = int(time.time() * 1000)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))
    platform = rng.choice(PLATFORMS)
    context_features = {"platform": platform}   # demographics are NOT embedded here

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
            "user_features": {},
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": context_features,
        })

    if rng.random() < click_prob(demo, platform):
        clicked = rng.choice(slate_items)
        events.append({
            "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
            "user_id": user, "item_id": clicked, "event_type": "click",
            "timestamp_ms": now_ms + rng.randint(1, 20) * 1000,
            "position": slate_items.index(clicked),
            "user_features": {}, "item_features": {}, "context_features": {},
        })
        if rng.random() < order_prob(demo, platform):
            events.append({
                "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
                "user_id": user, "item_id": clicked, "event_type": "order",
                "timestamp_ms": now_ms + rng.randint(21, 120) * 1000,
                "position": slate_items.index(clicked),
                "user_features": {}, "item_features": {}, "context_features": {},
            })
    return events


def main() -> None:
    rng = random.Random(SEED)
    demo = assign_demographics(NUM_USERS, rng)
    users = list(demo.keys())
    items = [f"movie_{i}" for i in range(1, NUM_ITEMS + 1)]

    print(f"demographics + {RATINGS_PER_USER} ratings/user → {CONTEXT_TOPIC} ({NUM_USERS} users); "
          f"behavior → {RECSYS_TOPIC} ({NUM_SLATES} slates)", flush=True)
    producer = make_producer()
    sent = 0
    try:
        for user in users:                                  # demographics + ratings per user
            producer.send(CONTEXT_TOPIC, value=demographics_event(user, demo[user]), key=user)
            sent += 1
            for _ in range(RATINGS_PER_USER):
                item = rng.choice(items)
                producer.send(CONTEXT_TOPIC, value=rating_event(user, item, demo[user], rng), key=user)
                sent += 1
        for s in range(NUM_SLATES):                         # behavior slates
            user = rng.choice(users)
            for event in make_slate(user, demo[user], items, rng):
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
            if (s + 1) % 2000 == 0:
                print(f"  {s + 1}/{NUM_SLATES} slates, {sent} events", flush=True)
    finally:
        producer.flush()
        producer.close()
    print(f"done: {sent} messages ({NUM_USERS} demographics + behavior)", flush=True)


if __name__ == "__main__":
    main()
