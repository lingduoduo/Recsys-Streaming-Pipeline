"""Declarative Kafka topic capacity policies and CLI argument builders."""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class TopicPolicy:
    """Capacity inputs and Kafka retention settings for one topic.

    ``retention_bytes`` is emitted unchanged as Kafka's ``retention.bytes``
    ceiling.  ``required_storage_bytes`` is instead a conservative estimate of
    replicated cluster storage.  Comparing the two therefore requires the
    declared ceiling to also be used as this policy's storage budget; it does
    not claim that Kafka's per-partition retention implementation measures
    cluster storage itself.
    """

    name: str
    partitions: int
    replication_factor: int
    messages_per_second: int
    average_record_bytes: int
    retention_ms: int
    retention_bytes: int
    overhead_factor: float
    cleanup_policy: str
    schema_subject: str
    schema_fingerprint: str


def required_storage_bytes(policy: TopicPolicy) -> int:
    """Return conservative expected replicated storage over the retention window."""
    retention_days = policy.retention_ms / 86_400_000
    return math.ceil(
        policy.messages_per_second
        * policy.average_record_bytes
        * 86_400
        * retention_days
        * policy.replication_factor
        * policy.overhead_factor
    )


def validate_policy(policy: TopicPolicy) -> None:
    """Reject invalid policy inputs before any Kafka command can be invoked."""
    numeric_values = (
        policy.partitions,
        policy.replication_factor,
        policy.messages_per_second,
        policy.average_record_bytes,
        policy.retention_ms,
        policy.retention_bytes,
        policy.overhead_factor,
    )
    if min(numeric_values) <= 0:
        raise ValueError(f"{policy.name}: numeric policy values must be positive")
    if required_storage_bytes(policy) > policy.retention_bytes:
        raise ValueError(f"{policy.name}: required storage exceeds storage budget")


def create_args(policy: TopicPolicy) -> list[str]:
    """Return the stable ``kafka-topics`` create arguments for ``policy``."""
    return [
        "--create",
        "--if-not-exists",
        "--topic",
        policy.name,
        "--partitions",
        str(policy.partitions),
        "--replication-factor",
        str(policy.replication_factor),
    ]


def config_args(policy: TopicPolicy) -> list[str]:
    """Return the stable ``kafka-configs`` retention arguments for ``policy``."""
    config = (
        f"cleanup.policy={policy.cleanup_policy},"
        f"retention.ms={policy.retention_ms},"
        f"retention.bytes={policy.retention_bytes}"
    )
    return [
        "--alter",
        "--entity-type",
        "topics",
        "--entity-name",
        policy.name,
        "--add-config",
        config,
    ]
