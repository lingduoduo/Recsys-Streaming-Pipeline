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
    {"user": "u1", "action": "m001", "banditScore": 0.8, "reward": 0.9, "timestamp": 1718300000000},
    {"user": "u1", "action": "m002", "banditScore": 0.5, "reward": 0.2, "timestamp": 1718300001000},
    {"user": "u2", "action": "m001", "banditScore": 0.6, "reward": 1.0, "timestamp": 1718300002000},
]


def test_convert_entries_to_rows():
    rows = replay_export.entries_to_rows(SAMPLE_ENTRIES)
    assert len(rows) == 3
    assert rows[0]["userId"] == "u1"
    assert rows[0]["movieId"] == "m001"
    assert float(rows[0]["rating"]) == pytest.approx(4.5, abs=0.01)  # 0.9 * 5
    assert int(rows[0]["timestamp"]) == 1718300000000


def test_convert_entries_reward_clipped_to_five():
    entries = [{"user": "u1", "action": "m1", "reward": 1.5, "timestamp": 0}]
    rows = replay_export.entries_to_rows(entries)
    assert float(rows[0]["rating"]) == 5.0


def test_write_csv(tmp_path):
    output = tmp_path / "out.csv"
    replay_export.write_csv(SAMPLE_ENTRIES, output)
    with open(output, newline="") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 3
    assert set(rows[0].keys()) == {"userId", "movieId", "rating", "timestamp"}


def test_write_parquet_roundtrips_raw_tuples(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    output = tmp_path / "replay.parquet"
    replay_export.write_parquet(SAMPLE_ENTRIES, output)
    assert output.is_file()
    df = pd.read_parquet(output)
    assert len(df) == 3
    # raw experience fields preserved (not the CSV rating mapping)
    assert {"user", "action", "banditScore", "reward", "timestamp"}.issubset(df.columns)
    assert df.iloc[0]["reward"] == pytest.approx(0.9)


def test_main_writes_parquet_when_requested(tmp_path):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    csv_out = tmp_path / "replay_training.csv"
    parquet_out = tmp_path / "replay.parquet"
    raw = [json.dumps(e).encode() for e in SAMPLE_ENTRIES]

    mock_client = MagicMock()
    mock_client.lrange.return_value = raw

    with patch("replay_export.redis.Redis", return_value=mock_client):
        replay_export.main([
            "--output", str(csv_out),
            "--parquet", str(parquet_out),
            "--redis-host", "localhost",
        ])

    assert csv_out.is_file()
    assert parquet_out.is_file()
    assert len(pd.read_parquet(parquet_out)) == 3


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
