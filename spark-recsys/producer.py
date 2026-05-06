import json
import os
import random
import time
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
EVENTS_PER_SECOND = max(float(os.getenv("EVENTS_PER_SECOND", "1")), 0.1)
NUM_USERS = max(int(os.getenv("NUM_USERS", "5")), 1)
NUM_ITEMS = max(int(os.getenv("NUM_ITEMS", "10")), 1)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    acks="all",
    retries=5,
    linger_ms=20,
    batch_size=32 * 1024,
    compression_type="gzip",
)

users = [f"user_{i}" for i in range(1, NUM_USERS + 1)]
items = [f"item_{i}" for i in range(1, NUM_ITEMS + 1)]

try:
    while True:
        event = {
            "user_id": random.choice(users),
            "item_id": random.choice(items),
            "event_type": "click",
            "timestamp": int(time.time()),
        }
        producer.send(TOPIC, event)
        print("sent", event)
        time.sleep(1 / EVENTS_PER_SECOND)
except KeyboardInterrupt:
    print("stopping producer")
finally:
    producer.flush()
    producer.close()
