import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "provision-kafka-topics.py"


def load_provisioner():
    spec = importlib.util.spec_from_file_location("provision_kafka_topics", SCRIPT_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def write_catalog(path: Path, topics: list[dict]) -> Path:
    path.write_text(json.dumps({"topics": topics}), encoding="utf-8")
    return path


def topic(name: str, **overrides: object) -> dict:
    result = {
        "name": name,
        "partitions": 1,
        "replication_factor": 1,
        "messages_per_second": 1,
        "average_record_bytes": 100,
        "retention_ms": 86_400_000,
        "retention_bytes": 20_000_000,
        "storage_budget_bytes": 20_000_000,
        "overhead_factor": 1.1,
        "cleanup_policy": "delete",
        "schema_subject": "recsys-event",
        "schema_fingerprint": "abc",
    }
    result.update(overrides)
    return result


def test_provisioner_runs_idempotent_create_then_retention_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = load_provisioner()
    catalog = write_catalog(tmp_path / "topics.json", [topic("recsys_events"), topic("recsys_events.backfill")])
    calls: list[list[str]] = []

    def fake_run(args: list[str], check: bool) -> None:
        assert check is True
        calls.append(args)

    monkeypatch.setattr(provisioner.subprocess, "run", fake_run)

    provisioner.main(["--catalog", str(catalog), "--bootstrap-server", "broker:9092"])

    assert calls == [
        ["kafka-topics", "--bootstrap-server", "broker:9092", "--create", "--if-not-exists", "--topic", "recsys_events", "--partitions", "1", "--replication-factor", "1"],
        ["kafka-configs", "--bootstrap-server", "broker:9092", "--alter", "--entity-type", "topics", "--entity-name", "recsys_events", "--add-config", "cleanup.policy=delete,retention.ms=86400000,retention.bytes=20000000"],
        ["kafka-topics", "--bootstrap-server", "broker:9092", "--create", "--if-not-exists", "--topic", "recsys_events.backfill", "--partitions", "1", "--replication-factor", "1"],
        ["kafka-configs", "--bootstrap-server", "broker:9092", "--alter", "--entity-type", "topics", "--entity-name", "recsys_events.backfill", "--add-config", "cleanup.policy=delete,retention.ms=86400000,retention.bytes=20000000"],
    ]


def test_invalid_policy_stops_before_any_kafka_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = load_provisioner()
    catalog = write_catalog(tmp_path / "topics.json", [topic("valid"), topic("invalid", storage_budget_bytes=1)])
    calls: list[list[str]] = []
    monkeypatch.setattr(provisioner.subprocess, "run", lambda args, check: calls.append(args))

    with pytest.raises(ValueError, match="storage budget"):
        provisioner.main(["--catalog", str(catalog), "--bootstrap-server", "broker:9092"])

    assert calls == []


def test_cleanup_policy_override_is_rejected_before_any_kafka_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = load_provisioner()
    catalog = write_catalog(tmp_path / "topics.json", [topic("recsys_events")])
    calls: list[list[str]] = []
    monkeypatch.setenv("TOPIC_POLICY_RECSYS_EVENTS_CLEANUP_POLICY", "delete,retention.ms=1")
    monkeypatch.setattr(provisioner.subprocess, "run", lambda args, check: calls.append(args))

    with pytest.raises(ValueError, match="cleanup policy"):
        provisioner.main(["--catalog", str(catalog), "--bootstrap-server", "broker:9092"])

    assert calls == []


def test_docker_compose_command_mode_uses_structured_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = load_provisioner()
    catalog = write_catalog(tmp_path / "topics.json", [topic("recsys_events")])
    calls: list[list[str]] = []
    monkeypatch.setattr(provisioner.subprocess, "run", lambda args, check: calls.append(args))

    provisioner.main([
        "--catalog", str(catalog),
        "--bootstrap-server", "localhost:9092",
        "--command-mode", "docker-compose",
    ])

    assert calls == [
        ["docker", "compose", "exec", "-T", "kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--create", "--if-not-exists", "--topic", "recsys_events", "--partitions", "1", "--replication-factor", "1"],
        ["docker", "compose", "exec", "-T", "kafka", "kafka-configs", "--bootstrap-server", "localhost:9092", "--alter", "--entity-type", "topics", "--entity-name", "recsys_events", "--add-config", "cleanup.policy=delete,retention.ms=86400000,retention.bytes=20000000"],
    ]


def test_catalog_has_only_live_and_backfill_topics() -> None:
    catalog = REPO_ROOT / "config" / "kafka-topics.json"
    topics = json.loads(catalog.read_text(encoding="utf-8"))["topics"]

    assert [entry["name"] for entry in topics] == ["recsys_events", "recsys_events.backfill"]


def test_environment_override_changes_only_the_targeted_policy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provisioner = load_provisioner()
    catalog = write_catalog(tmp_path / "topics.json", [topic("recsys_events"), topic("recsys_events.backfill")])
    monkeypatch.setenv("TOPIC_POLICY_RECSYS_EVENTS_RETENTION_MS", "3600000")

    policies = provisioner.load_policies(catalog)

    assert policies[0].retention_ms == 3_600_000
    assert policies[1].retention_ms == 86_400_000
