import os
import random
import time
import uuid
from collections.abc import Mapping

from event_avro import encode_event
from feedback_schedule import FeedbackSchedule, split_slate

try:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
except ModuleNotFoundError as exc:
    if exc.name == "kafka":
        raise SystemExit(
            "Missing producer dependencies. Run: "
            "python -m pip install -r services/python-modeling/requirements.txt"
        ) from exc
    raise

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "recsys_events")
EVENTS_PER_SECOND = max(float(os.getenv("EVENTS_PER_SECOND", "1")), 0.1)
NUM_USERS = max(int(os.getenv("NUM_USERS", "5")), 1)
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "10")), 1)
SLATE_SIZE = max(int(os.getenv("SLATE_SIZE", "5")), 1)
PRODUCER_MODE = os.getenv("PRODUCER_MODE", "clickstream").lower()
LOG_EVERY = max(int(os.getenv("LOG_EVERY", "100")), 1)
MAX_EVENTS = max(int(os.getenv("MAX_EVENTS", "0")), 0)


def make_click_event(users, items):
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "item_id": random.choice(items),
        "event_type": "click",
        "timestamp_ms": int(time.time() * 1000),
    }


def make_behavior_slate(users, items):
    now_ms = int(time.time() * 1000)
    user = random.choice(users)
    request_id = f"req_{uuid.uuid4().hex[:12]}"
    slate_items = random.sample(items, min(SLATE_SIZE, len(items)))
    device = random.choice(["ios", "android", "web"])
    country = random.choice(["US", "CA", "GB"])
    user_tier = random.choice(["new", "standard", "vip"])
    session_id = f"sess_{uuid.uuid4().hex[:8]}"

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
            "user_features": {"tier": user_tier},
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": {"device": device, "country": country},
        })

    clicked_item = random.choice(slate_items) if random.random() < 0.35 else None
    if clicked_item:
        events.append({
            "event_id": str(uuid.uuid4()),
            "request_id": request_id,
            "session_id": session_id,
            "user_id": user,
            "item_id": clicked_item,
            "event_type": "click",
            "timestamp_ms": now_ms + random.randint(1, 20) * 1000,
            "position": slate_items.index(clicked_item),
            "user_features": {},
            "item_features": {},
            "context_features": {},
        })

        if random.random() < 0.12:
            events.append({
                "event_id": str(uuid.uuid4()),
                "request_id": request_id,
                "session_id": session_id,
                "user_id": user,
                "item_id": clicked_item,
                "event_type": "order",
                "timestamp_ms": now_ms + random.randint(21, 120) * 1000,
                "position": slate_items.index(clicked_item),
                "user_features": {},
                "item_features": {},
                "context_features": {},
            })

    return events


def serialize_event(event: Mapping[str, object]) -> bytes:
    return encode_event(event)


def make_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=serialize_event,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=20,
        batch_size=32 * 1024,
        compression_type="lz4",
        api_version_auto_timeout_ms=5_000,
        request_timeout_ms=10_000,
        max_block_ms=10_000,
    )


def report_delivery_error(error):
    print(f"delivery failed: {error}", flush=True)


def main():
    print(
        f"connecting to Kafka at {BOOTSTRAP_SERVERS} "
        f"(topic={TOPIC}, mode={PRODUCER_MODE}, rate={EVENTS_PER_SECOND:g}/s)",
        flush=True,
    )
    try:
        producer = make_producer()
    except NoBrokersAvailable:
        raise SystemExit(
            f"No Kafka broker is available at {BOOTSTRAP_SERVERS}. "
            "Start Kafka with: docker compose up -d"
        ) from None
    print("connected; producing events (press Ctrl-C to stop)", flush=True)
    users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]
    items = [f"movie_{i}" for i in range(1, NUM_ITEMS + 1)]
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    schedule = FeedbackSchedule()

    try:
        while True:
            tick = time.monotonic()

            if PRODUCER_MODE == "behavior":
                events = make_behavior_slate(users, items)
            else:
                events = [make_click_event(users, items)]

            immediate, deferred = split_slate(events)
            for delay, pending in deferred:
                schedule.schedule(delay, pending)
            for event in immediate + schedule.due():
                key = event.get("request_id") or event["user_id"]
                producer.send(TOPIC, value=event, key=key).add_errback(report_delivery_error)
                sent += 1
                if sent == 1 or sent % LOG_EVERY == 0:
                    print(f"sent {sent} events, last: {event}", flush=True)
                if MAX_EVENTS and sent >= MAX_EVENTS:
                    print(f"reached MAX_EVENTS={MAX_EVENTS}; stopping", flush=True)
                    return

            elapsed = time.monotonic() - tick
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("stopping producer", flush=True)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
