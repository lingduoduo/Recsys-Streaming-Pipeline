#!/usr/bin/env python3
"""Size AFFINITY_STRENGTH by measurement rather than argument.

Generates slates in-process, assembles the training_samples columns the next-item harness
reads, and reports what each strength buys. The full Kafka+Spark sim is the verification
run; it is far too slow to iterate a constant against, and the joiner does not change which
items a user clicked, so this shortcut is exact for the quantity being calibrated.

  python3 affinity_calibration.py --strengths 0.0 0.10 0.15 0.20 0.30
"""
from __future__ import annotations

import argparse
import random
from typing import Sequence

import pandas as pd

import movie_segment_producer as producer
import next_item_model as nim
from feature_derivations import l1


def slates_to_frame(num_items: int, num_users: int, slates_per_user: int,
                    seed: int) -> pd.DataFrame:
    """One row per impression, carrying the columns next_item_model reads."""
    rng = random.Random(seed)
    movies = producer.assign_movies(num_items, rng)
    users = producer.assign_users(num_users, rng)
    items = list(movies)

    rows = []
    stamp = 1_700_000_000
    for round_index in range(slates_per_user):
        for user in users:
            events = producer.make_slate(user, users[user], items, movies, rng)
            clicked = {e["item_id"] for e in events if e["event_type"] == "click"}
            ordered = {e["item_id"] for e in events if e["event_type"] == "order"}
            for event in events:
                if event["event_type"] != "impression":
                    continue
                rows.append({
                    "user_id": user,
                    "item_id": event["item_id"],
                    "impression_ts": stamp + round_index,
                    "position": event["position"],
                    "clicked": int(event["item_id"] in clicked),
                    "ordered": int(event["item_id"] in ordered),
                })
    return pd.DataFrame(rows)


def preferred_share(strength: float, num_items: int, num_users: int,
                    slates_per_user: int, seed: int) -> float:
    """Share of clicks landing in the clicking user's preferred family.

    The mechanism the harness ultimately reads. With six families, no affinity puts this
    near 1/6.
    """
    original = producer.AFFINITY_STRENGTH
    producer.AFFINITY_STRENGTH = strength
    try:
        rng = random.Random(seed)
        movies = producer.assign_movies(num_items, rng)
        users = producer.assign_users(num_users, rng)
        items = list(movies)
        hits = total = 0
        for _ in range(slates_per_user):
            for user in users:
                preferred = producer.user_preferred_family(user)
                for event in producer.make_slate(user, users[user], items, movies, rng):
                    if event["event_type"] != "click":
                        continue
                    total += 1
                    hits += l1(movies[event["item_id"]]["genres"]) == preferred
        return hits / total if total else 0.0
    finally:
        producer.AFFINITY_STRENGTH = original


def measure(strength: float, num_items: int, num_users: int, slates_per_user: int,
            seed: int, epochs: int) -> dict:
    """Score most_popular and the transformer on data generated at this strength."""
    original = producer.AFFINITY_STRENGTH
    producer.AFFINITY_STRENGTH = strength
    try:
        frame = slates_to_frame(num_items, num_users, slates_per_user, seed)
    finally:
        producer.AFFINITY_STRENGTH = original

    positives = frame[nim.positive_mask(frame)]
    timelines = nim.build_timelines(positives)
    split = nim.split_timelines(timelines, nim.resolve_cutoff(timelines))
    model, item_index = nim.train_next_item(split, epochs=epochs, seed=seed)

    return {
        "strength": strength,
        "test_users": len(split.targets),
        "catalog_size": len(item_index),
        "preferred_share": preferred_share(strength, num_items, num_users, slates_per_user, seed),
        "most_popular": nim.evaluate_system(nim.most_popular(split), split.targets),
        "next_item_transformer": nim.evaluate_system(
            nim.model_rankings(model, item_index, split), split.targets),
    }


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Calibrate AFFINITY_STRENGTH by measurement.")
    parser.add_argument("--strengths", type=float, nargs="+",
                        default=[0.0, 0.10, 0.15, 0.20, 0.30])
    parser.add_argument("--num-items", type=int, default=400)
    parser.add_argument("--num-users", type=int, default=200)
    parser.add_argument("--slates-per-user", type=int, default=100)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--epochs", type=int, default=60)
    args = parser.parse_args(argv)

    print(f"{'strength':>9} {'pref_share':>11} {'pop hit@10':>11} {'model hit@10':>13} {'n':>5}")
    for strength in args.strengths:
        result = measure(strength, args.num_items, args.num_users,
                         args.slates_per_user, args.seed, args.epochs)
        print(f"{result['strength']:>9.2f} {result['preferred_share']:>11.3f} "
              f"{result['most_popular']['hit_rate@10']:>11.4f} "
              f"{result['next_item_transformer']['hit_rate@10']:>13.4f} "
              f"{result['test_users']:>5}")


if __name__ == "__main__":
    main()
