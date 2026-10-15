"""Test doubles shared by test_archive_replay.py and test_dead_letter_redrive.py."""
import hashlib
from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq


CANONICAL_EVENT = {
    "event_id": "e-1",
    "request_id": "request-1",
    "session_id": "session-1",
    "user_id": "u-1",
    "item_id": "i-1",
    "event_type": "click",
    "timestamp_ms": 1718400000000,
    "position": 2,
    "user_features": {"tier": "vip"},
    "item_features": {"genre": "drama"},
    "context_features": {"device": "web"},
    "model_version": "model-1",
    "policy_version": "policy-1",
    "algorithm_version": "algorithm-1",
    "rating": 4.5,
    "negative_feedback_reason": None,
    "dwell_millis": 900,
    "completion_rate": 0.75,
    "published_at": 1718400000100,
    "new_release": False,
    "filter_reason": None,
    "unsafe_label": False,
}


@dataclass(frozen=True)
class Sent:
    topic: str
    value: bytes
    key: bytes
    headers: list | None


class FakeFuture:
    def __init__(self, delivery_error: Exception | None = None) -> None:
        self.delivery_error = delivery_error

    def get(self, timeout: float) -> None:
        if self.delivery_error:
            raise self.delivery_error


class FakeProducer:
    def __init__(
        self,
        *,
        fail_on_send: bool = False,
        fail_on_delivery: bool = False,
        fail_on_close: bool = False,
        fail_on_send_number: int | None = None,
    ) -> None:
        self.fail_on_send = fail_on_send
        self.fail_on_delivery = fail_on_delivery
        self.fail_on_close = fail_on_close
        self.fail_on_send_number = fail_on_send_number
        self.sent: list[Sent] = []
        self.flushes = 0
        self.closes = 0

    def send(self, topic: str, *, value: bytes, key: bytes, headers=None) -> FakeFuture:
        if self.fail_on_send or self.fail_on_send_number == len(self.sent) + 1:
            raise RuntimeError("broker unavailable")
        self.sent.append(Sent(topic, value, key, headers))
        return FakeFuture(RuntimeError("delivery timed out") if self.fail_on_delivery else None)

    def flush(self) -> None:
        self.flushes += 1

    def close(self) -> None:
        self.closes += 1
        if self.fail_on_close:
            raise RuntimeError("close failed")


def write_commit_manifest(
    batch_root: Path, *, query_namespace: str, batch_id: int, kind: str
) -> None:
    """Write the v2 _COMMITTED manifest both services verify: identity, row count, file inventory."""
    inventory = []
    row_count = 0
    for path in sorted(batch_root.rglob("*.parquet")):
        relative = path.relative_to(batch_root).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        inventory.append(f"{relative}\t{path.stat().st_size}\t{digest}")
        row_count += pq.ParquetFile(path).metadata.num_rows
    (batch_root / "_COMMITTED").write_text(
        f"version=2\nquery={query_namespace}\nkind={kind}\n"
        f"batch_id={batch_id}\nrow_count={row_count}\n"
        + "".join(f"file={entry}\n" for entry in inventory),
        encoding="utf-8",
    )
