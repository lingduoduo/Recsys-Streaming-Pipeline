#!/usr/bin/env python3
"""Synthetic stream for MOVIE-CATEGORY engagement comparison (aligned to MovieEvent).

Each movie gets metadata (title, genres, releaseYear); the 3-level category (l1 family /
l2 primary genre / l3 genre×decade) is derived via feature_derivations. Metadata rides the real
context path; engagement rides the behavior path:

  • per movie: a MovieUpdated-shaped record {item_id, title, genres, release_year, timestamp}
    → movielens_context → MovieLensContextCollectorStreamingJob → Redis movie:{id}:features.
  • behavior slates → recsys_events → OnlineJoinerStreamingJob → training_samples Parquet.
    Each impressed item is clicked INDEPENDENTLY with a category-modulated probability, so
    per-item CTR reflects its category (ground truth below). No category embedded in the events.

Env: KAFKA_BOOTSTRAP_SERVERS, RECSYS_TOPIC (recsys_events), MOVIELENS_CONTEXT_TOPIC
(movielens_context), NUM_ITEMS (400), NUM_USERS (200), NUM_SLATES (20000), SLATE_SIZE (5), SEED (17).
"""
from __future__ import annotations

import csv
import os
import random
import time
import uuid

from producer import make_json_producer, make_producer
from feature_derivations import GENRES, l1, decade
from feedback_schedule import FeedbackSchedule, split_slate

RECSYS_TOPIC = os.getenv("RECSYS_TOPIC", "recsys_events")
CONTEXT_TOPIC = os.getenv("MOVIELENS_CONTEXT_TOPIC", "movielens_context")
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "400")), 1)
NUM_USERS = max(int(os.getenv("NUM_USERS", "200")), 1)
NUM_SLATES = max(int(os.getenv("NUM_SLATES", "20000")), 1)
SLATE_SIZE = max(int(os.getenv("SLATE_SIZE", "5")), 1)
SEED = int(os.getenv("SEED", "17"))


def ratings_from_events(events: list[dict]) -> list[dict]:
    by_pair = {}
    for event in events:
        if event["event_type"] not in ("click", "order"):
            continue
        pair = (event["user_id"], event["item_id"])
        row = {"userId": event["user_id"], "movieId": event["item_id"],
               "rating": 5.0 if event["event_type"] == "order" else 4.0,
               "timestamp": event["timestamp_ms"] // 1000}
        if pair not in by_pair or row["rating"] > by_pair[pair]["rating"]:
            by_pair[pair] = row
    return list(by_pair.values())

# Ground-truth additive effects on per-item click prob (base below), keyed by derived buckets so
# the report can recover them: SciFi&Fantasy/Action top, Other bottom; newer releases higher.
BASE_CTR = 0.15
FAMILY_EFF = {"SciFi&Fantasy": 0.06, "Action&Adventure": 0.04, "Crime&Thriller": 0.02,
              "Comedy": 0.01, "Drama&Romance": 0.0, "Other": -0.03}
DECADE_EFF = {"2020s": 0.04, "2010s": 0.03, "2000s": 0.02, "1990s": 0.01, "1980s": 0.0}

# Measurement ground truth (recoverable from the dashboard's measurement sections).
# Catalog availability is deliberately independent of release_year: an old film can
# enter the catalog last week, which is what freshness measures.
FRESHNESS_WINDOW_DAYS = 30
FRESH_SHARE = 0.15                 # share of items published inside the window
MAX_CATALOG_AGE_DAYS = 900         # oldest catalog availability instant
UNSAFE_SHARE = 0.02                # share of items an independent labeler flags unsafe
NEGATIVE_COMPLETION_CUTOFF = 0.10  # clicks below this completion report not_interested
GENDERS = ("female", "male", "unknown")
AGE_BANDS = ("18-24", "25-34", "35-49", "50+")
COUNTRIES = ("us", "ca", "gb", "de")
# Additive per-user click effect, so fairness has one explainable gap to report.
SUBSCRIPTION_EFF = {"premium": 0.03, "basic": 0.0, "free": -0.02}


def assign_movies(num_items: int, rng: random.Random) -> dict[str, dict]:
    """Per-movie metadata: 1-3 genres (primary first), releaseYear 1980-2024, title,
    catalog availability instant, and an independent safety label."""
    movies = {}
    now = int(time.time())
    for i in range(1, num_items + 1):
        primary = rng.choice(GENRES)
        extras = rng.sample([g for g in GENRES if g != primary], rng.randint(0, 2))
        year = rng.randint(1980, 2024)
        fresh = rng.random() < FRESH_SHARE
        age_days = (rng.uniform(0, FRESHNESS_WINDOW_DAYS) if fresh
                    else rng.uniform(FRESHNESS_WINDOW_DAYS + 1, MAX_CATALOG_AGE_DAYS))
        movies[f"movie_{i}"] = {
            "title": f"Movie {i}",
            "genres": [primary] + extras,
            "release_year": year,
            "published_at": now - int(age_days * 86400),
            "new_release": fresh,
            "unsafe": rng.random() < UNSAFE_SHARE,
        }
    return movies


def assign_users(num_users: int, rng: random.Random) -> dict[str, dict]:
    """Per-user demographics, stable across every slate that user appears in.

    Only dimensions in governance_measurements.DEFAULT_DIMENSIONS are emitted, so
    nothing unbounded can reach a published fairness group.
    """
    return {
        f"user_{i}": {
            "gender": rng.choice(GENDERS),
            "age_band": rng.choice(AGE_BANDS),
            "country": rng.choice(COUNTRIES),
            "subscription": rng.choice(tuple(SUBSCRIPTION_EFF)),
        }
        for i in range(1, num_users + 1)
    }


def item_click_prob(meta: dict) -> float:
    p = BASE_CTR + FAMILY_EFF[l1(meta["genres"])] + DECADE_EFF.get(decade(meta["release_year"]), 0.0)
    return min(0.6, max(0.02, p))


def user_click_bias(user_meta: dict) -> float:
    """Documented additive click effect of the user's subscription tier."""
    return SUBSCRIPTION_EFF.get(user_meta.get("subscription"), 0.0)


def click_completion(meta: dict, rng: random.Random) -> float:
    """Completion tracks the item's appeal: higher-CTR items get watched further."""
    center = min(0.95, item_click_prob(meta) * 3.0)
    return min(1.0, max(0.0, rng.gauss(center, 0.15)))


def order_prob(meta: dict) -> float:
    return 0.20 + (0.05 if l1(meta["genres"]) in ("SciFi&Fantasy", "Action&Adventure") else 0.0)


def movie_event(item_id: str, meta: dict) -> dict:
    """MovieUpdated-shaped record for movielens_context (matches RecSysEvent.MovieUpdated JSON)."""
    return {
        "item_id": item_id,
        "title": meta["title"],
        "genres": meta["genres"],
        "release_year": meta["release_year"],
        "timestamp": int(time.time()),
    }


def make_slate(user: str, user_meta: dict, items, movies: dict, rng: random.Random) -> list[dict]:
    now_ms = int(time.time() * 1000)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))

    def base(item: str, event_type: str, timestamp_ms: int, position: int) -> dict:
        return {
            "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
            "user_id": user, "item_id": item, "event_type": event_type,
            "timestamp_ms": timestamp_ms, "position": position,
            "user_features": dict(user_meta), "item_features": {}, "context_features": {},
        }

    events = []
    for position, item in enumerate(slate_items):
        meta = movies[item]
        impression = base(item, "impression", now_ms, position)
        impression["published_at"] = meta["published_at"]
        impression["new_release"] = meta["new_release"]
        impression["unsafe_label"] = meta["unsafe"]
        events.append(impression)

        # independent per-item click decision → per-item CTR reflects the item's category,
        # shifted by the user's documented subscription effect
        click_prob = min(0.6, max(0.02, item_click_prob(meta) + user_click_bias(user_meta)))
        if rng.random() < click_prob:
            completion = click_completion(meta, rng)
            click = base(item, "click", now_ms + rng.randint(1, 20) * 1000, position)
            click["completion_rate"] = round(completion, 4)
            click["dwell_millis"] = int(completion * 120_000)
            click["negative_feedback_reason"] = (
                "not_interested" if click["completion_rate"] < NEGATIVE_COMPLETION_CUTOFF else None)
            events.append(click)

            if rng.random() < order_prob(meta):
                order = base(item, "order", now_ms + rng.randint(21, 120) * 1000, position)
                order["rating"] = round(min(5.0, 3.0 + 2.0 * completion), 1)
                # OnlineJoinerStreamingJob attributes the whole feedback struct to the latest
                # feedback event (one max_by over the event timestamp), so an order carrying
                # only `rating` erases the click's engagement signals. Carry them forward —
                # this is the same shape OnlineJoinerStreamingJobSpec's fixture asserts.
                for field in ("completion_rate", "dwell_millis", "negative_feedback_reason"):
                    order[field] = click[field]
                events.append(order)
    return events


def main() -> None:
    rng = random.Random(SEED)
    movies = assign_movies(NUM_ITEMS, rng)
    items = list(movies.keys())
    user_meta = assign_users(NUM_USERS, rng)
    users = list(user_meta)

    print(f"movie metadata → {CONTEXT_TOPIC} ({NUM_ITEMS} movies); "
          f"behavior → {RECSYS_TOPIC} ({NUM_SLATES} slates)", flush=True)
    producer = make_producer()
    # movielens_context carries MovieUpdated records, which are JSON, not canonical Avro events.
    context_producer = make_json_producer()
    ratings_file = None
    ratings_writer = None
    ratings_path = os.getenv("RATINGS_OUTPUT_PATH")
    if ratings_path:
        os.makedirs(os.path.dirname(ratings_path) or ".", exist_ok=True)
        ratings_file = open(ratings_path, "w", newline="", encoding="utf-8")
        ratings_writer = csv.DictWriter(
            ratings_file, fieldnames=["userId", "movieId", "rating", "timestamp"])
        ratings_writer.writeheader()
    ratings_by_pair: dict[tuple[str, str], dict] = {}
    sent = 0
    try:
        for item in items:
            context_producer.send(CONTEXT_TOPIC, value=movie_event(item, movies[item]), key=item)
            sent += 1
        schedule = FeedbackSchedule()
        for s in range(NUM_SLATES):
            user = rng.choice(users)
            events = make_slate(user, user_meta[user], items, movies, rng)
            if ratings_writer:
                # Accumulate; do not write per slate. ratings_from_events dedupes within one
                # slate, but a user meeting the same movie in a later slate would still emit a
                # second row, and MovieLensDataset rejects the file on a duplicate user/movie
                # pair. Keep the strongest signal per pair, matching that function's own rule.
                for row in ratings_from_events(events):
                    pair = (row["userId"], row["movieId"])
                    if pair not in ratings_by_pair or row["rating"] > ratings_by_pair[pair]["rating"]:
                        ratings_by_pair[pair] = row
            immediate, deferred = split_slate(events)
            for event in immediate:
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
            for delay, event in deferred:
                schedule.schedule(delay, event)
            for event in schedule.due():
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
            if (s + 1) % 2000 == 0:
                print(f"  {s + 1}/{NUM_SLATES} slates, {sent} events", flush=True)
        while schedule.pending():
            wait = schedule.next_due_in()
            if wait:
                time.sleep(min(wait, 1.0))
            for event in schedule.due():
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
    finally:
        if ratings_writer:
            ratings_writer.writerows(ratings_by_pair.values())
        if ratings_file:
            ratings_file.close()
        producer.flush()
        producer.close()
        context_producer.flush()
        context_producer.close()
    print(f"done: {sent} messages ({NUM_ITEMS} movies + behavior)", flush=True)


if __name__ == "__main__":
    main()
