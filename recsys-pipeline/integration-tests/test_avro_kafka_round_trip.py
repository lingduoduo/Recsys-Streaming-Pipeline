"""Opt-in live Kafka coverage for Avro ingestion, archival, and replay.

This test intentionally touches a real Kafka broker, Redis, Spark, and the
checked-in scripts.  It is excluded from normal developer and CI suites because
the fixture needs the local Compose services plus an assembled Spark job JAR.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid
from datetime import date
from pathlib import Path
from typing import Any

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_MODELING = REPO_ROOT / "services" / "python-modeling"
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LIVE_TOPIC = "recsys_events"
BACKFILL_TOPIC = "recsys_events.backfill"
FIXED_EVENT_ID = "avro-kafka-round-trip-event"
FIXED_EVENT_DATE = date(2026, 1, 1)
FIXED_EVENT = {
    "event_id": FIXED_EVENT_ID,
    "request_id": "avro-kafka-round-trip-request",
    "session_id": "avro-kafka-round-trip-session",
    "user_id": "avro-kafka-round-trip-user",
    "item_id": "avro-kafka-round-trip-item",
    "event_type": "click",
    "timestamp_ms": 1767225600000,
    "position": 1,
    "user_features": {"tier": "integration"},
    "item_features": {"genre": "test"},
    "context_features": {"source": "pytest"},
    "model_version": "integration-v1",
    "policy_version": "integration-v1",
    "algorithm_version": "integration-v1",
    "rating": None,
    "negative_feedback_reason": None,
    "dwell_millis": 1,
    "completion_rate": 1.0,
    "published_at": 1767225600000,
    "new_release": False,
    "filter_reason": None,
    "unsafe_label": False,
}
POLL_TIMEOUT_SECONDS = 90


def event_avro_module():
    """Load the local Avro helper only after enabled-run prerequisites pass."""
    if str(PYTHON_MODELING) not in sys.path:
        sys.path.insert(0, str(PYTHON_MODELING))
    return importlib.import_module("event_avro")


def _first_bootstrap_endpoint() -> tuple[str, int]:
    endpoint = BOOTSTRAP_SERVERS.split(",", 1)[0].strip()
    try:
        host, port = endpoint.rsplit(":", 1)
        return host.strip("[]"), int(port)
    except ValueError as exc:
        pytest.skip(f"Kafka integration prerequisite has invalid KAFKA_BOOTSTRAP_SERVERS={endpoint!r}: {exc}")


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def integration_preflight() -> None:
    """Skip an enabled live test with remediation instead of failing on local setup gaps."""
    missing_packages = [
        package for package in ("fastavro", "kafka", "lz4", "pyarrow")
        if importlib.util.find_spec(package) is None
    ]
    if missing_packages:
        pytest.skip(
            "Kafka integration prerequisite missing Python packages "
            f"{', '.join(missing_packages)}. Run: python -m pip install -r "
            "services/python-modeling/requirements.txt"
        )

    jar = REPO_ROOT / "services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
    if not jar.is_file():
        pytest.skip(
            "Kafka integration prerequisite missing assembled Spark JAR. Run: "
            "(cd services/spark-streaming-job && sbt assembly)"
        )

    kafka_host, kafka_port = _first_bootstrap_endpoint()
    if not _reachable(kafka_host, kafka_port):
        pytest.skip(
            f"Kafka integration prerequisite cannot reach Kafka at {kafka_host}:{kafka_port}. "
            "Start it with: docker compose up -d kafka redis"
        )
    redis_host = os.getenv("REDIS_HOST", "localhost")
    try:
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
    except ValueError:
        pytest.skip("Kafka integration prerequisite has invalid REDIS_PORT; set it to a TCP port number.")
    if not _reachable(redis_host, redis_port):
        pytest.skip(
            f"Kafka integration prerequisite cannot reach Redis at {redis_host}:{redis_port}. "
            "Start it with: docker compose up -d kafka redis"
        )

    command_mode = os.getenv("KAFKA_INTEGRATION_COMMAND_MODE", "host")
    if command_mode == "host":
        missing_clis = [command for command in ("kafka-topics", "kafka-configs") if shutil.which(command) is None]
        if missing_clis:
            pytest.skip(
                "Kafka integration prerequisite missing host Kafka CLI "
                f"{', '.join(missing_clis)}. Install the Kafka CLI tools, or set "
                "KAFKA_INTEGRATION_COMMAND_MODE=docker-compose for the running local Compose stack."
            )
    elif command_mode == "docker-compose":
        if shutil.which("docker") is None or subprocess.run(
            ["docker", "compose", "ps", "--status", "running", "kafka"],
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode != 0:
            pytest.skip(
                "Kafka integration prerequisite needs a running Docker Compose Kafka service. "
                "Run: docker compose up -d kafka redis"
            )
    else:
        pytest.skip(
            "Kafka integration prerequisite has unsupported KAFKA_INTEGRATION_COMMAND_MODE="
            f"{command_mode!r}; use host or docker-compose."
        )


def provision_topics() -> None:
    """Provision the two checked-in topics with their real retention policy."""
    command = [
        sys.executable,
        "scripts/provision-kafka-topics.py",
        "--bootstrap-server",
        BOOTSTRAP_SERVERS,
        "--command-mode",
        os.getenv("KAFKA_INTEGRATION_COMMAND_MODE", "host"),
    ]
    subprocess.run(command, cwd=REPO_ROOT, check=True)


def publish_fixed_avro_event(topic: str) -> str:
    from kafka import KafkaProducer

    event_avro = event_avro_module()
    event_id = f"{FIXED_EVENT_ID}-{uuid.uuid4().hex}"
    event = dict(FIXED_EVENT)
    event["event_id"] = event_id
    event["request_id"] = f"request-{event_id}"
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=event_avro.encode_event,
    )
    try:
        producer.send(topic, key=event["request_id"].encode("utf-8"), value=event).get(timeout=10)
        producer.flush()
    finally:
        producer.close()
    return event_id


def archived_event(archive_root: Path, event_id: str) -> tuple[dict[str, Any], Path] | None:
    """Read only committed Parquet batches and return the archived event lineage."""
    import pyarrow.parquet as pq

    for marker in archive_root.rglob("_COMMITTED"):
        if not (marker.parent / "_SUCCESS").is_file():
            continue
        for parquet_file in marker.parent.rglob("*.parquet"):
            for row in pq.ParquetFile(parquet_file).read().to_pylist():
                if row.get("event_id") == event_id:
                    return row, marker.parent
    return None


def run_bounded_ingestion(tmp_path: Path, event_id: str) -> tuple[dict[str, Any], Path]:
    """Run the real unbounded job only until its committed archive contains ``event_id``."""
    archive_root = tmp_path / "archive"
    log_path = tmp_path / "spark-ingestion.log"
    environment = os.environ.copy()
    environment.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
            "KAFKA_TOPIC": LIVE_TOPIC,
            "RECSYS_EVENT_ARCHIVE_PATH": str(archive_root),
            "RECSYS_EVENT_DEAD_LETTER_PATH": str(tmp_path / "dead-letter"),
            "SPARK_CHECKPOINT_LOCATION": str(tmp_path / "checkpoint"),
            "SPARK_MASTER": "local[2]",
            "TRIGGER_INTERVAL": "1 second",
        }
    )
    with log_path.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            ["bash", "scripts/run-streaming-job.sh"],
            cwd=REPO_ROOT,
            env=environment,
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                archived = archived_event(archive_root, event_id)
                if archived is not None:
                    return archived
                if process.poll() is not None:
                    pytest.fail(
                        f"Spark ingestion exited with {process.returncode}:\n"
                        f"{log_path.read_text(encoding='utf-8')}"
                    )
                time.sleep(1)
            pytest.fail(
                f"Timed out waiting for {event_id} in the committed archive:\n"
                f"{log_path.read_text(encoding='utf-8')}"
            )
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=20)
    raise AssertionError("the bounded ingestion loop must return or fail")


def run_archive_replay(archive_root: Path, manifest_root: Path) -> dict[str, Any]:
    environment = os.environ.copy()
    environment.update(
        {
            "KAFKA_BOOTSTRAP_SERVERS": BOOTSTRAP_SERVERS,
            "REPLAY_ARCHIVE_PATH": str(archive_root),
            "REPLAY_START_DATE": FIXED_EVENT_DATE.isoformat(),
            "REPLAY_END_DATE": date(2026, 1, 2).isoformat(),
            "REPLAY_MAX_ROWS": "10000",
            "REPLAY_RECORDS_PER_SECOND": "1000",
            "REPLAY_MANIFEST_DIR": str(manifest_root),
        }
    )
    subprocess.run(["bash", "scripts/run-archive-replay.sh"], cwd=REPO_ROOT, env=environment, check=True)
    manifests = list(manifest_root.glob("*.json"))
    assert len(manifests) == 1, "replay must produce exactly one audit manifest"
    import json

    return json.loads(manifests[0].read_text(encoding="utf-8"))


def consume_event_id(topic: str, event_id: str) -> str | None:
    from kafka import KafkaConsumer

    event_avro = event_avro_module()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"avro-kafka-round-trip-{time.time_ns()}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=1_000,
        value_deserializer=event_avro.decode_event,
    )
    try:
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            for records in consumer.poll(timeout_ms=1_000).values():
                for record in records:
                    if record.value["event_id"] == event_id:
                        return record.value["event_id"]
    finally:
        consumer.close()
    return None


@pytest.mark.skipif(os.getenv("RUN_KAFKA_INTEGRATION") != "1", reason="opt-in Kafka integration")
def test_avro_archive_and_replay_round_trip(tmp_path: Path) -> None:
    """A live Avro event keeps its lineage through archive and backfill replay."""
    integration_preflight()
    event_avro = event_avro_module()
    provision_topics()
    event_id = publish_fixed_avro_event(LIVE_TOPIC)
    archived, batch_directory = run_bounded_ingestion(tmp_path, event_id)

    assert archived["kafka_topic"] == LIVE_TOPIC
    assert archived["event_id"] == event_id
    assert archived["kafka_partition"] is not None
    assert archived["kafka_offset"] is not None
    assert archived["kafka_timestamp"] is not None
    assert archived["schema_fingerprint"] == event_avro.schema_fingerprint(event_avro.load_schema())
    assert (batch_directory / "_SUCCESS").is_file()
    manifest = (batch_directory / "_COMMITTED").read_text(encoding="utf-8")
    expected_query = hashlib.sha256(str(tmp_path / "checkpoint").encode("utf-8")).hexdigest()
    assert f"query={expected_query}\n" in manifest
    assert "kind=valid\n" in manifest
    assert f"batch_id={batch_directory.name}\n" in manifest

    replay_manifest = run_archive_replay(tmp_path / "archive", tmp_path / "replay-manifests")
    assert replay_manifest["status"] == "completed"
    assert replay_manifest["target_topic"] == BACKFILL_TOPIC
    assert replay_manifest["schema_fingerprints"] == [event_avro.schema_fingerprint(event_avro.load_schema())]
    assert consume_event_id(BACKFILL_TOPIC, event_id) == event_id
