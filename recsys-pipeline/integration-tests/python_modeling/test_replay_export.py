import csv
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import replay_export


SAMPLE_ENTRIES = [
    {"userId": "u1", "itemId": "m001", "score": 0.8, "reward": 0.9, "timestamp": 1718300000000},
    {"userId": "u1", "itemId": "m002", "score": 0.5, "reward": 0.2, "timestamp": 1718300001000},
    {"userId": "u2", "itemId": "m001", "score": 0.6, "reward": 1.0, "timestamp": 1718300002000},
]


def test_convert_entries_to_rows():
    rows = replay_export.entries_to_rows(SAMPLE_ENTRIES)
    assert len(rows) == 3
    assert rows[0]["userId"] == "u1"
    assert rows[0]["movieId"] == "m001"
    assert float(rows[0]["rating"]) == pytest.approx(4.5, abs=0.01)  # 0.9 * 5
    assert int(rows[0]["timestamp"]) == 1718300000000


def test_convert_entries_reward_clipped_to_five():
    entries = [{"userId": "u1", "itemId": "m1", "score": 0.1, "reward": 1.5, "timestamp": 0}]
    rows = replay_export.entries_to_rows(entries)
    assert float(rows[0]["rating"]) == 5.0


def test_write_csv(tmp_path):
    output = tmp_path / "out.csv"
    replay_export.write_csv(SAMPLE_ENTRIES, output)
    with open(output, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 3
    assert set(rows[0].keys()) == {"userId", "movieId", "rating", "timestamp"}


def test_main_reads_from_redis_mock(tmp_path):
    output = tmp_path / "replay_training.csv"
    raw = [json.dumps(e).encode() for e in SAMPLE_ENTRIES]

    mock_client = MagicMock()
    mock_client.lrange.return_value = raw

    with patch("replay_export.redis.Redis", return_value=mock_client):
        replay_export.main([
            "--output", str(output),
            "--redis-host", "localhost",
        ])

    mock_client.lrange.assert_called_once_with("replay:recommendations", 0, -1)
    assert output.is_file()
    with open(output, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 3
