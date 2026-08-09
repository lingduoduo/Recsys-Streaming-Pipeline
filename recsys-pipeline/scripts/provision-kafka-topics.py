#!/usr/bin/env python3
"""Create and configure the checked-in Kafka topics idempotently."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_MODELING = REPO_ROOT / "services" / "python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

from topic_policy import TopicPolicy, config_args, create_args, validate_policy


DEFAULT_CATALOG = REPO_ROOT / "config" / "kafka-topics.json"
INTEGER_FIELDS = {
    "partitions",
    "replication_factor",
    "messages_per_second",
    "average_record_bytes",
    "retention_ms",
    "retention_bytes",
}
FLOAT_FIELDS = {"overhead_factor"}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    return parser.parse_args(argv)


def policy_from_mapping(values: dict[str, Any]) -> TopicPolicy:
    return TopicPolicy(
        name=str(values["name"]),
        partitions=int(values["partitions"]),
        replication_factor=int(values["replication_factor"]),
        messages_per_second=int(values["messages_per_second"]),
        average_record_bytes=int(values["average_record_bytes"]),
        retention_ms=int(values["retention_ms"]),
        retention_bytes=int(values["retention_bytes"]),
        overhead_factor=float(values["overhead_factor"]),
        cleanup_policy=str(values["cleanup_policy"]),
        schema_subject=str(values["schema_subject"]),
        schema_fingerprint=str(values["schema_fingerprint"]),
    )


def environment_overrides(policy: TopicPolicy) -> TopicPolicy:
    """Apply explicit per-topic overrides without changing the catalog on disk.

    An override uses ``TOPIC_POLICY_<TOPIC>_<FIELD>``, where topic punctuation
    becomes underscores; for example
    ``TOPIC_POLICY_RECSYS_EVENTS_RETENTION_MS=3600000``.
    """
    topic_token = policy.name.upper().replace(".", "_").replace("-", "_")
    values: dict[str, Any] = {}
    for field in fields(TopicPolicy):
        if field.name == "name":
            continue
        raw = os.getenv(f"TOPIC_POLICY_{topic_token}_{field.name.upper()}")
        if raw is None:
            continue
        if field.name in INTEGER_FIELDS:
            values[field.name] = int(raw)
        elif field.name in FLOAT_FIELDS:
            values[field.name] = float(raw)
        else:
            values[field.name] = raw
    return replace(policy, **values)


def load_policies(catalog: Path) -> list[TopicPolicy]:
    data = json.loads(catalog.read_text(encoding="utf-8"))
    return [environment_overrides(policy_from_mapping(entry)) for entry in data["topics"]]


def provision(policies: Sequence[TopicPolicy], bootstrap_server: str) -> None:
    # Validate the complete catalog before creating a single topic.  Commands
    # are argv arrays rather than shell strings so catalog values cannot alter
    # command parsing.
    for policy in policies:
        validate_policy(policy)

    for policy in policies:
        subprocess.run(
            ["kafka-topics", "--bootstrap-server", bootstrap_server, *create_args(policy)],
            check=True,
        )
        subprocess.run(
            ["kafka-configs", "--bootstrap-server", bootstrap_server, *config_args(policy)],
            check=True,
        )


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    provision(load_policies(args.catalog), args.bootstrap_server)


if __name__ == "__main__":
    main()
