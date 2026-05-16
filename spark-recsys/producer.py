import json
import os
import random
import time
import uuid

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
EVENTS_PER_SECOND = max(float(os.getenv("EVENTS_PER_SECOND", "1")), 0.1)
NUM_USERS = max(int(os.getenv("NUM_USERS", "5")), 1)
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "10")), 1)
SLATE_SIZE = max(int(os.getenv("SLATE_SIZE", "5")), 1)
PRODUCER_MODE = os.getenv("PRODUCER_MODE", "clickstream").lower()
LOG_EVERY = max(int(os.getenv("LOG_EVERY", "100")), 1)


def make_click_event(users, items):
    return {
        "user_id": random.choice(users),
        "item_id": random.choice(items),
        "event_type": "click",
        "timestamp": int(time.time()),
    }


def make_behavior_slate(users, items):
    now = int(time.time())
    user = random.choice(users)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    slate_items = random.sample(items, min(SLATE_SIZE, len(items)))
    device = random.choice(["ios", "android", "web"])
    country = random.choice(["US", "CA", "GB"])
    user_tier = random.choice(["new", "standard", "vip"])

    events = []
    for position, item in enumerate(slate_items):
        events.append({
            "request_id": request_id,
            "user_id": user,
            "item_id": item,
            "event_type": "impression",
            "timestamp": now,
            "position": position,
            "user_features": {"tier": user_tier},
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": {"device": device, "country": country},
        })

    clicked_item = random.choice(slate_items) if random.random() < 0.35 else None
    if clicked_item:
        events.append({
            "request_id": request_id,
            "user_id": user,
            "item_id": clicked_item,
            "event_type": "click",
            "timestamp": now + random.randint(1, 20),
            "position": slate_items.index(clicked_item),
            "user_features": {},
            "item_features": {},
            "context_features": {},
        })

        if random.random() < 0.12:
            events.append({
                "request_id": request_id,
                "user_id": user,
                "item_id": clicked_item,
                "event_type": "order",
                "timestamp": now + random.randint(21, 120),
                "position": slate_items.index(clicked_item),
                "user_features": {},
                "item_features": {},
                "context_features": {},
            })

    return events


def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=32 * 1024,
        compression_type="lz4",
    )


def main():
    producer = make_producer()
    users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]
    items = [f"item_{i}" for i in range(1, NUM_ITEMS + 1)]
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0

    try:
        while True:
            tick = time.monotonic()

            if PRODUCER_MODE == "behavior":
                events = make_behavior_slate(users, items)
            else:
                events = [make_click_event(users, items)]

            for event in events:
                key = event.get("request_id") or event["user_id"]
                producer.send(TOPIC, value=event, key=key)
                sent += 1
                if sent % LOG_EVERY == 0:
                    print(f"sent {sent} events, last: {event}")

            elapsed = time.monotonic() - tick
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("stopping producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
