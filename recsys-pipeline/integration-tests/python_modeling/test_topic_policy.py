import dataclasses
import math
import sys
from pathlib import Path

import pytest


PYTHON_MODELING = Path(__file__).resolve().parents[2] / "services/python-modeling"
sys.path.insert(0, str(PYTHON_MODELING))

import event_avro
from topic_policy import TopicPolicy, config_args, create_args, required_storage_bytes, validate_policy

REPO_ROOT = Path(__file__).resolve().parents[3]
KAFKA_TOPICS_CONFIG = REPO_ROOT / "recsys-pipeline/config/kafka-topics.json"


PRIMARY = TopicPolicy(
    "recsys_events",
    3,
    1,
    10,
    1024,
    86_400_000,
    1_500_000_000,
    1_500_000_000,
    1.10,
    "delete",
    "recsys-event",
    "afbfcef03d048f09",
)
BACKFILL = TopicPolicy(
    "recsys_events.backfill",
    1,
    1,
    5,
    1024,
    21_600_000,
    250_000_000,
    150_000_000,
    1.10,
    "delete",
    "recsys-event",
    "afbfcef03d048f09",
)


def test_required_storage_includes_replication_and_overhead() -> None:
    policy = TopicPolicy(
        "events", 12, 3, 1_000_000, 500, 86_400_000,
        100_000_000_000_000, 200_000_000_000_000, 1.10, "delete", "recsys-event", "abc",
    )

    assert required_storage_bytes(policy) == 1_000_000 * 500 * 86_400 * 3 * 1.10


def test_budget_overrun_is_rejected() -> None:
    with pytest.raises(ValueError, match="storage budget"):
        validate_policy(dataclasses.replace(PRIMARY, storage_budget_bytes=1))


def test_storage_budget_is_distinct_from_per_partition_retention_ceiling() -> None:
    policy = dataclasses.replace(PRIMARY, retention_bytes=1, storage_budget_bytes=2_000_000_000)

    validate_policy(policy)

    assert config_args(policy)[-1].endswith("retention.bytes=1")


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (PRIMARY, ["--create", "--if-not-exists", "--topic", "recsys_events", "--partitions", "3", "--replication-factor", "1"]),
        (BACKFILL, ["--create", "--if-not-exists", "--topic", "recsys_events.backfill", "--partitions", "1", "--replication-factor", "1"]),
    ],
)
def test_create_args_are_exact_for_each_catalog_topic(policy: TopicPolicy, expected: list[str]) -> None:
    assert create_args(policy) == expected


@pytest.mark.parametrize(
    ("policy", "expected"),
    [
        (PRIMARY, ["--alter", "--entity-type", "topics", "--entity-name", "recsys_events", "--add-config", "cleanup.policy=delete,retention.ms=86400000,retention.bytes=1500000000"]),
        (BACKFILL, ["--alter", "--entity-type", "topics", "--entity-name", "recsys_events.backfill", "--add-config", "cleanup.policy=delete,retention.ms=21600000,retention.bytes=250000000"]),
    ],
)
def test_config_args_are_exact_for_each_catalog_topic(policy: TopicPolicy, expected: list[str]) -> None:
    assert config_args(policy) == expected


@pytest.mark.parametrize("field", ["partitions", "replication_factor", "messages_per_second", "average_record_bytes", "retention_ms", "retention_bytes", "storage_budget_bytes"])
def test_nonpositive_numeric_policy_values_are_rejected(field: str) -> None:
    with pytest.raises(ValueError, match="numeric policy values must be positive"):
        validate_policy(dataclasses.replace(PRIMARY, **{field: 0}))


@pytest.mark.parametrize("overhead_factor", [math.nan, math.inf, -math.inf])
def test_nonfinite_overhead_factor_is_rejected(overhead_factor: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        validate_policy(dataclasses.replace(PRIMARY, overhead_factor=overhead_factor))


def test_unknown_or_delimited_cleanup_policy_is_rejected() -> None:
    with pytest.raises(ValueError, match="cleanup policy"):
        validate_policy(dataclasses.replace(PRIMARY, cleanup_policy="delete,retention.ms=1"))


def test_configured_fingerprint_matches_the_current_writer_schema() -> None:
    """Fails if a schema bump forgets kafka-topics.json.

    The fingerprint in that config is what an operator reconciles a live topic against, and
    nothing else in the suite reads it — every other test builds its own TopicPolicy fixtures,
    so a stale value here passed unnoticed through the v1 to v2 bump.
    """
    import json

    expected = format(event_avro.schema_fingerprint(event_avro.load_schema()), "016x")
    configured = json.loads(KAFKA_TOPICS_CONFIG.read_text())

    for topic in configured["topics"]:
        assert topic["schema_fingerprint"] == expected, (
            f"{topic['name']} pins {topic['schema_fingerprint']}, "
            f"but the current writer schema is {expected}"
        )
