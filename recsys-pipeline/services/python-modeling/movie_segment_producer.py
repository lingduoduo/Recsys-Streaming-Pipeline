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

from producer import make_producer
from feature_derivations import GENRES, l1, decade

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


def assign_movies(num_items: int, rng: random.Random) -> dict[str, dict]:
    """Per-movie metadata: 1-3 genres (primary first), releaseYear 1980-2024, title."""
    movies = {}
    for i in range(1, num_items + 1):
        primary = rng.choice(GENRES)
        extras = rng.sample([g for g in GENRES if g != primary], rng.randint(0, 2))
        year = rng.randint(1980, 2024)
        movies[f"movie_{i}"] = {
            "title": f"Movie {i}",
            "genres": [primary] + extras,
            "release_year": year,
        }
    return movies


def item_click_prob(meta: dict) -> float:
    p = BASE_CTR + FAMILY_EFF[l1(meta["genres"])] + DECADE_EFF.get(decade(meta["release_year"]), 0.0)
    return min(0.6, max(0.02, p))


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


def make_slate(user: str, items, movies: dict, rng: random.Random) -> list[dict]:
    now_ms = int(time.time() * 1000)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    slate_items = rng.sample(items, min(SLATE_SIZE, len(items)))

    events = []
    for position, item in enumerate(slate_items):
        events.append({
            "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
            "user_id": user, "item_id": item, "event_type": "impression",
            "timestamp_ms": now_ms, "position": position,
            "user_features": {}, "item_features": {}, "context_features": {},
        })
        # independent per-item click decision → per-item CTR reflects the item's category
        if rng.random() < item_click_prob(movies[item]):
            events.append({
                "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
                "user_id": user, "item_id": item, "event_type": "click",
                "timestamp_ms": now_ms + rng.randint(1, 20) * 1000, "position": position,
                "user_features": {}, "item_features": {}, "context_features": {},
            })
            if rng.random() < order_prob(movies[item]):
                events.append({
                    "event_id": str(uuid.uuid4()), "request_id": request_id, "session_id": session_id,
                    "user_id": user, "item_id": item, "event_type": "order",
                    "timestamp_ms": now_ms + rng.randint(21, 120) * 1000, "position": position,
                    "user_features": {}, "item_features": {}, "context_features": {},
                })
    return events


def main() -> None:
    rng = random.Random(SEED)
    movies = assign_movies(NUM_ITEMS, rng)
    items = list(movies.keys())
    users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]

    print(f"movie metadata → {CONTEXT_TOPIC} ({NUM_ITEMS} movies); "
          f"behavior → {RECSYS_TOPIC} ({NUM_SLATES} slates)", flush=True)
    producer = make_producer()
    ratings_file = None
    ratings_writer = None
    ratings_path = os.getenv("RATINGS_OUTPUT_PATH")
    if ratings_path:
        os.makedirs(os.path.dirname(ratings_path) or ".", exist_ok=True)
        ratings_file = open(ratings_path, "w", newline="", encoding="utf-8")
        ratings_writer = csv.DictWriter(
            ratings_file, fieldnames=["userId", "movieId", "rating", "timestamp"])
        ratings_writer.writeheader()
    sent = 0
    try:
        for item in items:
            producer.send(CONTEXT_TOPIC, value=movie_event(item, movies[item]), key=item)
            sent += 1
        for s in range(NUM_SLATES):
            user = rng.choice(users)
            events = make_slate(user, items, movies, rng)
            if ratings_writer:
                ratings_writer.writerows(ratings_from_events(events))
            for event in events:
                producer.send(RECSYS_TOPIC, value=event, key=event["request_id"])
                sent += 1
            if (s + 1) % 2000 == 0:
                print(f"  {s + 1}/{NUM_SLATES} slates, {sent} events", flush=True)
    finally:
        if ratings_file:
            ratings_file.close()
        producer.flush()
        producer.close()
    print(f"done: {sent} messages ({NUM_ITEMS} movies + behavior)", flush=True)


if __name__ == "__main__":
    main()
