import json
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

SURFACES = ("home_feed", "search_results", "detail_page", "continue_watching")
COUNTRIES = ("us", "ca", "gb")
# One locale and zone per country, matching movie_segment_producer's vocabulary.
# Keyed by COUNTRIES above; an entry for a country this producer never draws would be dead data.
COUNTRY_LOCALE = {"us": "en-US", "ca": "en-CA", "gb": "en-GB"}
COUNTRY_TIMEZONE = {"us": "America/New_York", "ca": "America/Toronto",
                    "gb": "Europe/London"}


def user_country(user: str) -> str:
    """The user's country: a pure function of the user id, so it stays stable across every
    slate that user appears in instead of being redrawn per slate (which would make locale
    and timezone, both derived from country, noise no report could attribute to anyone).
    """
    tail = user.rsplit("_", 1)[-1]
    index = int(tail) if tail.isdigit() else 0
    return COUNTRIES[index % len(COUNTRIES)]


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
    surface = random.choice(SURFACES)
    device = random.choice(["ios", "android", "web"])
    country = user_country(user)
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
            "user_features": {"tier": user_tier, "country": country},
            "item_features": {"bucket": f"b{int(item.split('_')[-1]) % 4}"},
            "context_features": {},
            "surface": surface,
            "device": device,
            "locale": COUNTRY_LOCALE[country],
            "timezone": COUNTRY_TIMEZONE[country],
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


SEARCH_TERMS = ("space opera", "heist thriller", "quiet drama", "animated adventure")


def make_search_journey(users, items):
    """One user's search, the results they saw, the one they opened, and the click.

    Every event shares a query id so the whole journey rejoins downstream, and the views
    onward share a result-set id naming the slate they came from. The leading result is
    the detail/click target rather than a random one, so the shape of the trace is a
    property of the generator and not of the RNG.
    """
    now_ms = int(time.time() * 1000)
    user = random.choice(users)
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    query_id = f"q_{uuid.uuid4().hex[:12]}"
    result_set_id = f"rs_{uuid.uuid4().hex[:12]}"
    result_items = items[:SLATE_SIZE]
    country = user_country(user)

    def event(event_type, item_id, offset_ms, **extra):
        return {
            "event_id": str(uuid.uuid4()),
            "session_id": session_id,
            "user_id": user,
            "item_id": item_id,
            "event_type": event_type,
            "timestamp_ms": now_ms + offset_ms,
            "query_id": query_id,
            "surface": "search_results",
            "locale": COUNTRY_LOCALE[country],
            "timezone": COUNTRY_TIMEZONE[country],
            **extra,
        }

    events = [event("search", None, 0, query_text=random.choice(SEARCH_TERMS))]
    for position, item in enumerate(result_items):
        events.append(event(
            "result_view", item, 1_000 + position,
            position=position, result_set_id=result_set_id,
            referrer="search_results", view_kind="result",
        ))
    target = result_items[0]
    events.append(event(
        "detail_view", target, 2_000,
        position=0, result_set_id=result_set_id,
        referrer="search_results", view_kind="detail", view_duration_ms=4_000,
    ))
    events.append(event(
        "click", target, 3_000,
        position=0, result_set_id=result_set_id, referrer="detail_page",
    ))
    return events


def serialize_event(event: Mapping[str, object]) -> bytes:
    return encode_event(event)


def serialize_json(event: Mapping[str, object]) -> bytes:
    """Plain JSON, for topics that are not the canonical Avro event stream.

    `movielens_context` carries MovieUpdated/UserUpdated/rating records, which have none of the
    canonical event's required fields, and its consumer reads them with
    `CAST(value AS STRING)` + `from_json`. Encoding those with `serialize_event` fails validation
    on `event_id` before a byte is sent.
    """
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def _producer(value_serializer):
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=value_serializer,
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


def make_producer():
    """Canonical Avro events (`recsys_events`)."""
    return _producer(serialize_event)


def make_json_producer():
    """JSON topics such as `movielens_context`."""
    return _producer(serialize_json)


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
            elif PRODUCER_MODE == "search":
                events = make_search_journey(users, items)
            else:
                events = [make_click_event(users, items)]

            immediate, deferred = split_slate(events)
            for delay, pending in deferred:
                schedule.schedule(delay, pending)
            pending_now = schedule.due()
            tick_events = immediate + pending_now
            for index, event in enumerate(tick_events):
                key = event.get("request_id") or event["user_id"]
                producer.send(TOPIC, value=event, key=key).add_errback(report_delivery_error)
                sent += 1
                if sent == 1 or sent % LOG_EVERY == 0:
                    print(f"sent {sent} events, last: {event}", flush=True)
                if MAX_EVENTS and sent >= MAX_EVENTS:
                    print(f"reached MAX_EVENTS={MAX_EVENTS}; stopping", flush=True)
                    # Events after this point in tick_events were already popped off the
                    # schedule by the schedule.due() call above, so they must be sent here —
                    # the drain below only sees what is still in the schedule.
                    for remaining_event in tick_events[index + 1 :]:
                        remaining_key = remaining_event.get("request_id") or remaining_event["user_id"]
                        producer.send(TOPIC, value=remaining_event, key=remaining_key).add_errback(
                            report_delivery_error
                        )
                        sent += 1
                    while schedule.pending():
                        wait = schedule.next_due_in()
                        if wait:
                            time.sleep(min(wait, 1.0))
                        for pending_event in schedule.due():
                            key = pending_event.get("request_id") or pending_event["user_id"]
                            producer.send(TOPIC, value=pending_event, key=key).add_errback(
                                report_delivery_error
                            )
                            sent += 1
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
