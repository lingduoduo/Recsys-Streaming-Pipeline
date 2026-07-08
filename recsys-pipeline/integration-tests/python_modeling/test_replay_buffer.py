import json, sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).parents[2] / "services" / "python-modeling"))
import replay_buffer


def test_load_from_redis_parses_bytes_and_str():
    entries = [{"user": "u1", "action": "m1", "reward": 0.9},
               {"user": "u2", "action": "m2", "reward": 0.0}]
    client = MagicMock()
    client.lrange.return_value = [json.dumps(entries[0]).encode(), json.dumps(entries[1])]
    out = replay_buffer.load_from_redis(client, limit=-1)
    client.lrange.assert_called_once_with("replay:recommendations", 0, -1)
    assert out == entries


def test_load_from_redis_empty():
    client = MagicMock()
    client.lrange.return_value = []
    assert replay_buffer.load_from_redis(client) == []
