# Phase 4+5 — Automated Retraining Loop & Feedback Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the training loop — streaming feedback events trigger offline retraining, the freshly trained ONNX models are hot-reloaded into the Java service without downtime, and the orphaned RL replay buffer is consumed by a fine-tuning pass in the Python pipeline.

**Architecture:** A `run-retrain.sh` shell script orchestrates (1) Spark offline jobs, (2) Python two-tower fine-tuning, and (3) a `POST /actuator/model-reload` call to the running Java service. A `replay_export.py` script reads the Redis `replay:recommendations` list, converts it to training format, and writes it to `sampledata/replay_training.csv` for the Python pipeline to consume.

**Depends on:**
- Plan A complete (runner scripts exist, `deep-learning-weight=0.15` active)
- Plan B complete (`TwoTowerPredictionService` loaded, `POST /actuator/model-reload` endpoint live)
- Java service running on `localhost:8080` during integration tests (or `RECSYS_SERVICE_URL` env var)

**Tech Stack:** bash, Python 3, redis-py, PyTorch, ONNX Runtime, curl, Spark (Scala/sbt), Maven.

---

## File Map

| Action | File |
|--------|------|
| Create | `recsys-pipeline/run-retrain.sh` |
| Create | `recsys-pipeline/services/python-modeling/replay_export.py` |
| Modify | `recsys-pipeline/services/python-modeling/movielens_pipeline.py` |
| Create | `recsys-pipeline/integration-tests/python_modeling/test_replay_export.py` |
| Create | `recsys-pipeline/integration-tests/test_retrain_pipeline.sh` |

---

## Task 11: replay_export.py — Redis Replay Buffer → CSV

**Files:**
- Create: `recsys-pipeline/services/python-modeling/replay_export.py`
- Create: `recsys-pipeline/integration-tests/python_modeling/test_replay_export.py`

Reads the `replay:recommendations` Redis list (each entry is a JSON string matching the `ExperienceCollectorStreamingJob` output format), converts to a ratings-CSV-compatible format, and writes `sampledata/replay_training.csv`.

- [ ] **Step 1: Understand the replay entry format**

Read `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/process/ExperienceCollectorStreamingJob.scala` to confirm the JSON schema of each replay entry. The expected schema is:
```json
{
  "userId": "alice",
  "itemId": "item1",
  "score": 0.73,
  "reward": 1.0,
  "timestamp": 1718300000000
}
```
The `reward` field maps to `rating` in the output CSV (clipped to [0.0, 5.0] by multiplying by 5.0).

- [ ] **Step 2: Write the failing test**

Create `recsys-pipeline/integration-tests/python_modeling/test_replay_export.py`:

```python
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
```

- [ ] **Step 3: Run to confirm failure**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/test_replay_export.py -v
```

Expected: `FAILED — ModuleNotFoundError: No module named 'replay_export'`

- [ ] **Step 4: Create `replay_export.py`**

Create `recsys-pipeline/services/python-modeling/replay_export.py`:

```python
#!/usr/bin/env python3
"""Export the Redis replay buffer to a ratings-CSV file for offline retraining.

Usage:
    python replay_export.py [--output PATH] [--redis-host HOST] [--redis-port PORT]
                            [--key replay:recommendations] [--limit N]

Output CSV columns: userId, movieId, rating, timestamp
rating = min(reward * 5.0, 5.0)  — maps [0, 1] reward to MovieLens 0–5 scale
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Sequence

import redis

_DEFAULT_KEY = "replay:recommendations"
_DEFAULT_OUTPUT = Path(__file__).parents[2] / "sampledata" / "replay_training.csv"


def entries_to_rows(entries: list[dict]) -> list[dict]:
    rows = []
    for e in entries:
        rating = min(float(e.get("reward", 0.0)) * 5.0, 5.0)
        rows.append({
            "userId": str(e["userId"]),
            "movieId": str(e["itemId"]),
            "rating": f"{rating:.1f}",
            "timestamp": str(int(e.get("timestamp", 0))),
        })
    return rows


def write_csv(entries: list[dict], output: Path) -> None:
    rows = entries_to_rows(entries)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["userId", "movieId", "rating", "timestamp"])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {output}")


def main(args: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=_DEFAULT_OUTPUT)
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "localhost"))
    parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", "6379")))
    parser.add_argument("--key", default=_DEFAULT_KEY)
    parser.add_argument("--limit", type=int, default=-1,
                        help="Max entries to export (-1 = all, default).")
    cfg = parser.parse_args(args)

    client = redis.Redis(host=cfg.redis_host, port=cfg.redis_port, decode_responses=False)
    raw = client.lrange(cfg.key, 0, cfg.limit)
    if not raw:
        print(f"No entries found at {cfg.key}")
        return
    entries = [json.loads(b) for b in raw]
    write_csv(entries, cfg.output)


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run the tests**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/test_replay_export.py -v
```

Expected: `4 passed`

- [ ] **Step 6: Run all Python tests**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/ -v
```

Expected: all pass

- [ ] **Step 7: Commit**

```bash
git add recsys-pipeline/services/python-modeling/replay_export.py \
        recsys-pipeline/integration-tests/python_modeling/test_replay_export.py
git commit -m "feat: add replay_export.py to convert Redis replay buffer to training CSV"
```

---

## Task 12: Fine-Tuning Mode in Python Pipeline

**Files:**
- Modify: `recsys-pipeline/services/python-modeling/movielens_pipeline.py`
- Test: `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`

When `--fine-tune-csv` is provided alongside `--ratings-csv`, the pipeline merges both datasets before training. This allows the freshly exported `replay_training.csv` to augment the base ratings without replacing it.

- [ ] **Step 1: Write the failing test**

Add to `recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py`:

```python
REPLAY_RATINGS = [
    ("u1", "m005", "4.0", "2000"),
    ("u3", "m001", "5.0", "2001"),
]


def test_fine_tune_merges_ratings(tmp_path):
    base_csv = tmp_path / "ratings.csv"
    replay_csv = tmp_path / "replay.csv"
    _write_csv(SAMPLE_RATINGS, base_csv)
    _write_csv(REPLAY_RATINGS, replay_csv)
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    mp.main([
        "--ratings-csv", str(base_csv),
        "--fine-tune-csv", str(replay_csv),
        "--model-dir", str(model_dir),
        "--retrieval-epochs", "1",
        "--ranking-epochs", "1",
        "--force-train",
    ])
    assert (model_dir / "movielens_user_tower.onnx").is_file()


def test_parse_args_has_fine_tune_csv():
    config = mp.parse_args(["--user", "alice"])
    assert hasattr(config, "fine_tune_csv")
    assert config.fine_tune_csv is None
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest integration-tests/python_modeling/test_movielens_pipeline.py::test_parse_args_has_fine_tune_csv -v
```

Expected: `FAILED — AttributeError`

- [ ] **Step 3: Add `--fine-tune-csv` to `parse_args`**

```python
parser.add_argument(
    "--fine-tune-csv",
    type=Path,
    default=None,
    help=(
        "Path to an additional ratings CSV to merge with --ratings-csv before training. "
        "Use to fine-tune on replay buffer data without discarding base ratings."
    ),
)
```

- [ ] **Step 4: Merge datasets in `load_user_history` or in `main()`**

In `main()`, after loading `USER_HISTORY` from `--ratings-csv`, add:

```python
    if config.fine_tune_csv is not None:
        fine_tune_history = load_user_history(config.fine_tune_csv, min_rating=config.min_rating)
        # Merge: extend watched/rated_high/favorites/rewatched for each user
        for uid, h in fine_tune_history.items():
            if uid in USER_HISTORY:
                USER_HISTORY[uid]["watched"].extend(h["watched"])
                USER_HISTORY[uid]["rated_high"].extend(h["rated_high"])
                USER_HISTORY[uid]["favorites"].extend(h["favorites"])
                USER_HISTORY[uid]["rewatched"].extend(h["rewatched"])
            else:
                USER_HISTORY[uid] = h
        # Extend MOVIES/USER lists with any new IDs from fine-tune CSV
        fine_tune_movies = load_movies_from_ratings(config.fine_tune_csv)
        existing_ids = {m[0] for m in MOVIES}
        new_movies = [m for m in fine_tune_movies if m[0] not in existing_ids]
        MOVIES.extend(new_movies)
        MOVIE_TO_IDX = {m[0]: i for i, m in enumerate(MOVIES)}
        N_MOVIES = len(MOVIES)
        MOVIE_GENRE_FEATS = np.zeros((N_MOVIES, GENRE_DIM), dtype=np.float32)
        USERS = list(USER_HISTORY.keys())
        N_USERS = len(USERS)
        USER_TO_IDX = {u: i for i, u in enumerate(USERS)}
```

- [ ] **Step 5: Run tests**

```bash
cd recsys-pipeline
pytest integration-tests/python_modeling/ -v
```

Expected: all pass

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/services/python-modeling/movielens_pipeline.py \
        recsys-pipeline/integration-tests/python_modeling/test_movielens_pipeline.py
git commit -m "feat: add --fine-tune-csv flag to merge replay data into training pipeline"
```

---

## Task 13: run-retrain.sh — Orchestration Script

**Files:**
- Create: `recsys-pipeline/run-retrain.sh`
- Create: `recsys-pipeline/integration-tests/test_retrain_pipeline.sh`

A single script that (1) exports the replay buffer, (2) runs Spark offline jobs to regenerate embeddings, (3) runs the Python two-tower pipeline with fine-tuning, and (4) calls the hot-reload endpoint. Each step is guarded by an exit-code check so failures abort early.

- [ ] **Step 1: Write the integration test**

Create `recsys-pipeline/integration-tests/test_retrain_pipeline.sh`:

```bash
#!/usr/bin/env bash
# Integration test for run-retrain.sh
# Runs in dry-run mode (DRY_RUN=1) to verify the script's step sequencing without
# actually running Spark jobs or calling a live service.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== run-retrain.sh integration test (dry-run) ==="

DRY_RUN=1 bash "${REPO_ROOT}/run-retrain.sh" 2>&1 | tee /tmp/retrain-test-output.txt

if grep -q "DRY RUN: skip" /tmp/retrain-test-output.txt; then
  echo "PASS: dry-run mode activated"
else
  echo "FAIL: dry-run output not detected"
  exit 1
fi

for step in "Step 1" "Step 2" "Step 3" "Step 4" "Step 5"; do
  if grep -q "${step}" /tmp/retrain-test-output.txt; then
    echo "PASS: ${step} found in output"
  else
    echo "FAIL: ${step} not found in output"
    exit 1
  fi
done

echo "=== All integration tests passed ==="
```

- [ ] **Step 2: Run to confirm failure**

```bash
chmod +x recsys-pipeline/integration-tests/test_retrain_pipeline.sh
bash recsys-pipeline/integration-tests/test_retrain_pipeline.sh
```

Expected: `bash: run-retrain.sh: No such file or directory`

- [ ] **Step 3: Create `run-retrain.sh`**

Create `recsys-pipeline/run-retrain.sh`:

```bash
#!/usr/bin/env bash
# run-retrain.sh — Orchestrate a full retraining pass:
#   1. Export replay buffer from Redis to CSV
#   2. Regenerate Spark ALS embeddings (writes to Redis)
#   3. Regenerate Spark UserEmbedding vectors (writes to Redis)
#   4. Re-train Python two-tower with fine-tuning on replay data
#   5. Hot-reload ONNX model in running Java service
#
# Usage:
#   ./run-retrain.sh [--skip-spark] [--skip-python] [--skip-reload]
#
# Environment Variables:
#   REDIS_HOST             Redis host (default: localhost)
#   REDIS_PORT             Redis port (default: 6379)
#   RECSYS_SERVICE_URL     Java retrieval service base URL (default: http://localhost:8080)
#   RATINGS_CSV            Path to base ratings CSV (default: sampledata/ratings.csv)
#   MODEL_DIR              ONNX output directory (default: sampledata)
#   DRY_RUN                Set to 1 to print steps without executing (default: 0)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_MODELING="${SCRIPT_DIR}/services/python-modeling"
SPARK_JOB="${SCRIPT_DIR}/services/spark-streaming-job"

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
RECSYS_SERVICE_URL="${RECSYS_SERVICE_URL:-http://localhost:8080}"
RATINGS_CSV="${RATINGS_CSV:-${SCRIPT_DIR}/sampledata/ratings.csv}"
MODEL_DIR="${MODEL_DIR:-${SCRIPT_DIR}/sampledata}"
DRY_RUN="${DRY_RUN:-0}"
REPLAY_CSV="${SCRIPT_DIR}/sampledata/replay_training.csv"

SKIP_SPARK=0
SKIP_PYTHON=0
SKIP_RELOAD=0

for arg in "$@"; do
  case $arg in
    --skip-spark)  SKIP_SPARK=1 ;;
    --skip-python) SKIP_PYTHON=1 ;;
    --skip-reload) SKIP_RELOAD=1 ;;
  esac
done

run() {
  if [[ "${DRY_RUN}" == "1" ]]; then
    echo "DRY RUN: skip → $*"
  else
    "$@"
  fi
}

echo "=== Recsys Retraining Pipeline ==="
echo "  Redis:      ${REDIS_HOST}:${REDIS_PORT}"
echo "  Service:    ${RECSYS_SERVICE_URL}"
echo "  Ratings:    ${RATINGS_CSV}"
echo "  Model dir:  ${MODEL_DIR}"
echo ""

# Step 1: Export replay buffer
echo "Step 1: Exporting replay buffer → ${REPLAY_CSV}"
run python3 "${PYTHON_MODELING}/replay_export.py" \
    --output "${REPLAY_CSV}" \
    --redis-host "${REDIS_HOST}" \
    --redis-port "${REDIS_PORT}"

# Step 2: Spark ALS embeddings
if [[ "${SKIP_SPARK}" == "0" ]]; then
  echo "Step 2: Running ALS embedding job"
  run bash "${SCRIPT_DIR}/run-als-pipeline.sh"
else
  echo "Step 2: Skipped (--skip-spark)"
fi

# Step 3: Spark UserEmbedding vectors
if [[ "${SKIP_SPARK}" == "0" ]]; then
  echo "Step 3: Running UserEmbedding job"
  run bash "${SCRIPT_DIR}/run-user-embedding-pipeline.sh"
else
  echo "Step 3: Skipped (--skip-spark)"
fi

# Step 4: Python two-tower fine-tuning
if [[ "${SKIP_PYTHON}" == "0" ]]; then
  echo "Step 4: Running Python two-tower pipeline with fine-tuning"
  FINE_TUNE_ARGS=""
  if [[ -f "${REPLAY_CSV}" ]]; then
    FINE_TUNE_ARGS="--fine-tune-csv ${REPLAY_CSV}"
  fi
  run python3 "${PYTHON_MODELING}/movielens_pipeline.py" \
      --ratings-csv "${RATINGS_CSV}" \
      ${FINE_TUNE_ARGS} \
      --model-dir "${MODEL_DIR}" \
      --force-train \
      --save-embeddings-to-redis \
      --redis-host "${REDIS_HOST}" \
      --redis-port "${REDIS_PORT}"
else
  echo "Step 4: Skipped (--skip-python)"
fi

# Step 5: Hot-reload ONNX model in running service
if [[ "${SKIP_RELOAD}" == "0" ]]; then
  echo "Step 5: Hot-reloading ONNX model in Java service at ${RECSYS_SERVICE_URL}"
  HTTP_CODE=$(run curl -s -o /dev/null -w "%{http_code}" \
      -X POST "${RECSYS_SERVICE_URL}/actuator/model-reload" 2>/dev/null || echo "000")
  if [[ "${DRY_RUN}" != "1" && "${HTTP_CODE}" != "200" ]]; then
    echo "  WARN: model-reload returned HTTP ${HTTP_CODE} — service may need manual restart"
  else
    echo "  model-reload → HTTP ${HTTP_CODE}"
  fi
else
  echo "Step 5: Skipped (--skip-reload)"
fi

echo ""
echo "=== Retraining pipeline complete ==="
```

- [ ] **Step 4: Make executable**

```bash
chmod +x recsys-pipeline/run-retrain.sh
```

- [ ] **Step 5: Run the integration test**

```bash
bash recsys-pipeline/integration-tests/test_retrain_pipeline.sh
```

Expected:
```
=== run-retrain.sh integration test (dry-run) ===
...
PASS: dry-run mode activated
PASS: Step 1 found in output
PASS: Step 2 found in output
PASS: Step 3 found in output
PASS: Step 4 found in output
PASS: Step 5 found in output
=== All integration tests passed ===
```

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/run-retrain.sh \
        recsys-pipeline/integration-tests/test_retrain_pipeline.sh
git commit -m "feat: add run-retrain.sh orchestration script for full retraining pass"
```

---

## Task 14: Cron/README Documentation

**Files:**
- Modify: `recsys-pipeline/README.md`

Document the automated retraining workflow so the pipeline can be operated without reading the code.

- [ ] **Step 1: Add "Automated Retraining" section to README.md**

Find the existing "Quick Start" section in `recsys-pipeline/README.md` and add a new section after it:

```markdown
## Automated Retraining

The `run-retrain.sh` script runs a full retraining pass and hot-reloads the ONNX
model into the running Java service. It chains four stages:

1. **Replay export** — reads `replay:recommendations` from Redis and writes
   `sampledata/replay_training.csv`
2. **Spark ALS** — regenerates ALS item/user embeddings and writes them to Redis
3. **Spark UserEmbedding** — regenerates weighted-average user vectors in Redis
4. **Python two-tower** — fine-tunes the two-tower model on the merged dataset and
   writes item embeddings to Redis under the `twoTowerItemEmb:*` prefix
5. **Model hot-reload** — calls `POST /actuator/model-reload` to swap the ONNX model
   in the running Java service without restart

### One-Off Run

```bash
# From the recsys-pipeline/ directory
./run-retrain.sh
```

### Scheduled Run (cron)

To retrain every 6 hours:

```cron
0 */6 * * * cd /path/to/recsys-pipeline && ./run-retrain.sh >> /var/log/recsys-retrain.log 2>&1
```

### Skip Individual Stages

```bash
./run-retrain.sh --skip-spark          # Only Python + reload
./run-retrain.sh --skip-python         # Only Spark + reload
./run-retrain.sh --skip-reload         # Train without hot-reload (offline mode)
./run-retrain.sh --skip-spark --skip-python  # Only hot-reload (for manual model swap)
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | `localhost` | Redis server hostname |
| `REDIS_PORT` | `6379` | Redis server port |
| `RECSYS_SERVICE_URL` | `http://localhost:8080` | Java retrieval service URL |
| `RATINGS_CSV` | `sampledata/ratings.csv` | Base training data |
| `MODEL_DIR` | `sampledata` | ONNX output directory |
| `DRY_RUN` | `0` | Set to `1` to print steps without executing |

### Replay Buffer

The `replay:recommendations` Redis list is populated by `ExperienceCollectorStreamingJob`.
Run `replay_export.py` standalone if you want to inspect or back up the buffer:

```bash
python3 services/python-modeling/replay_export.py \
    --output /tmp/replay_backup.csv \
    --redis-host localhost
```
```

- [ ] **Step 2: Commit**

```bash
git add recsys-pipeline/README.md
git commit -m "docs: document run-retrain.sh automated retraining workflow in README"
```

---

## Verification Checklist

### Python Tests

- [ ] `pytest recsys-pipeline/integration-tests/python_modeling/ -q` — all pass
  - test_replay_export: 4 tests
  - test_movielens_pipeline: includes `test_fine_tune_merges_ratings`, `test_parse_args_has_fine_tune_csv`

### Shell Integration Test

- [ ] `bash recsys-pipeline/integration-tests/test_retrain_pipeline.sh` — 5 PASS lines

### Manual Dry-Run

- [ ] `DRY_RUN=1 bash recsys-pipeline/run-retrain.sh` — prints all 5 steps with "DRY RUN: skip" prefix

### End-to-End (requires live environment)

When Redis and the Java service are running:

```bash
# 1. Prime the replay buffer (simulate ExperienceCollector output)
redis-cli RPUSH replay:recommendations \
  '{"userId":"alice","itemId":"item1","score":0.8,"reward":0.9,"timestamp":1718300000000}'

# 2. Run the retrain pipeline
cd recsys-pipeline
REDIS_HOST=localhost RECSYS_SERVICE_URL=http://localhost:8080 ./run-retrain.sh --skip-spark

# 3. Verify replay_training.csv written
wc -l sampledata/replay_training.csv  # should be 2 (header + 1 row)

# 4. Verify model-reload response
curl -X POST http://localhost:8080/actuator/model-reload
# Expected: {"status":"ok"}

# 5. Verify recommendations still served
curl http://localhost:8080/recommend?user=alice
# Expected: JSON list of recommended items
```
