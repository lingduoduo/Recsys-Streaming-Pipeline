# Phase 1 — Quick Wins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable the three offline training jobs to actually be runnable and wire the deep learning score into the live ranking pipeline — zero architectural changes, config and scripts only.

**Architecture:** ALS and UserEmbedding training jobs already support all needed env vars; they just lack shell runner scripts. The deep learning weight is already wired in the scoring formula but defaults to 0.0. These are config-only gaps.

**Tech Stack:** Bash, YAML (Spring Boot), Markdown. No code compilation required.

---

## File Map

| Action | File |
|--------|------|
| Create | `recsys-pipeline/run-als-pipeline.sh` |
| Create | `recsys-pipeline/run-user-embedding-pipeline.sh` |
| Modify | `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml` (line 45: `deep-learning-weight`) |
| Modify | `recsys-pipeline/README.md` (Quick Start section) |
| Modify | `recsys-pipeline/integration-tests/test_service_scripts.py` (add script presence tests) |

---

## Task 1: ALS Runner Script

**Files:**
- Create: `recsys-pipeline/run-als-pipeline.sh`
- Test: `recsys-pipeline/integration-tests/test_service_scripts.py`

- [ ] **Step 1: Write a failing test for script presence and error behavior**

Add to `recsys-pipeline/integration-tests/test_service_scripts.py`:

```python
import os
import subprocess
import pytest

PIPELINE_DIR = os.path.join(os.path.dirname(__file__), "..")


def test_als_pipeline_script_exists():
    script = os.path.join(PIPELINE_DIR, "run-als-pipeline.sh")
    assert os.path.isfile(script), "run-als-pipeline.sh not found"


def test_als_pipeline_script_is_executable():
    script = os.path.join(PIPELINE_DIR, "run-als-pipeline.sh")
    assert os.access(script, os.X_OK), "run-als-pipeline.sh is not executable"


def test_als_pipeline_requires_ratings_input():
    script = os.path.join(PIPELINE_DIR, "run-als-pipeline.sh")
    result = subprocess.run(
        ["bash", script],
        capture_output=True,
        text=True,
        env={**os.environ, "RATINGS_INPUT_PATH": ""},
    )
    assert result.returncode != 0
    assert "RATINGS_INPUT_PATH" in result.stderr
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd recsys-pipeline
pytest integration-tests/test_service_scripts.py::test_als_pipeline_script_exists -v
```

Expected: `FAILED — AssertionError: run-als-pipeline.sh not found`

- [ ] **Step 3: Create `run-als-pipeline.sh`**

```bash
#!/usr/bin/env bash
# ALS embedding pipeline
#
# Flow: ratings.csv ──► AlsEmbeddingTrainingJob
#                            ├──► sampledata/als/userFactors/
#                            ├──► sampledata/als/itemFactors/
#                            ├──► Redis alsUserEmb:{userId}    (if ALS_SAVE_TO_REDIS=true)
#                            └──► Redis alsItemEmb:{movieId}   (if ALS_SAVE_TO_REDIS=true)
#
# Required:
#   RATINGS_INPUT_PATH   Path to ratings CSV (userId,movieId,rating,timestamp)
#
# Optional (model):
#   ALS_EMBEDDING_OUTPUT_PATH   Output directory  (default: sampledata/als)
#   ALS_RANK                    Latent factor dimension (default: 16)
#   ALS_MAX_ITER                ALS iterations (default: 10)
#   ALS_REG_PARAM               Regularisation lambda (default: 0.1)
#
# Optional (Redis — set ALS_SAVE_TO_REDIS=true to enable):
#   ALS_SAVE_TO_REDIS           true | false (default: false)
#   REDIS_HOST                  Redis hostname (default: localhost)
#   REDIS_PORT                  Redis port (default: 6379)
#   ALS_USER_REDIS_KEY_PREFIX   Key prefix for user factors (default: alsUserEmb)
#   ALS_ITEM_REDIS_KEY_PREFIX   Key prefix for item factors (default: alsItemEmb)
#   ALS_REDIS_TTL_SECONDS       TTL in seconds (default: 86400)
#
# Optional (Spark):
#   SPARK_HOME              Path to Spark installation
#   SPARK_MASTER            Spark master URL (default: local[*])
#   SPARK_DRIVER_MEMORY     Driver heap (default: 1g)
#   SPARK_EXECUTOR_MEMORY   Executor heap (default: 2g)
#   SPARK_SQL_SHUFFLE_PARTITIONS  (default: 4)
#
# Example — write to file only:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv ./run-als-pipeline.sh
#
# Example — write to file + Redis:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv \
#   ALS_SAVE_TO_REDIS=true \
#   REDIS_HOST=localhost \
#   ./run-als-pipeline.sh

set -euo pipefail

cd "$(dirname "$0")"

if [[ -z "${RATINGS_INPUT_PATH:-}" ]]; then
  echo "Error: RATINGS_INPUT_PATH is required." >&2
  echo "Usage: RATINGS_INPUT_PATH=<path/to/ratings.csv> $0" >&2
  exit 1
fi

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"

SPARK_SUBMIT=""
if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
  exit 127
fi

JAR="services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Missing Spark job jar. Run: cd services/spark-streaming-job && sbt assembly" >&2
  exit 127
fi

OUTPUT_PATH="${ALS_EMBEDDING_OUTPUT_PATH:-sampledata/als}"

echo "=== ALS embedding pipeline ==="
echo "  Ratings input : $RATINGS_INPUT_PATH"
echo "  Output path   : $OUTPUT_PATH"
echo "  Rank          : ${ALS_RANK:-16}"
echo "  Max iter      : ${ALS_MAX_ITER:-10}"
echo "  Reg param     : ${ALS_REG_PARAM:-0.1}"
echo "  Save to Redis : ${ALS_SAVE_TO_REDIS:-false}"
if [[ "${ALS_SAVE_TO_REDIS:-false}" == "true" ]]; then
  echo "  Redis host    : ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
  echo "  User key      : ${ALS_USER_REDIS_KEY_PREFIX:-alsUserEmb}:{userId}"
  echo "  Item key      : ${ALS_ITEM_REDIS_KEY_PREFIX:-alsItemEmb}:{movieId}"
fi
echo ""

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.task.AlsEmbeddingTrainingJob \
  "$JAR" \
  "$RATINGS_INPUT_PATH" \
  "$OUTPUT_PATH"
```

- [ ] **Step 4: Make the script executable**

```bash
chmod +x recsys-pipeline/run-als-pipeline.sh
```

- [ ] **Step 5: Run the tests**

```bash
cd recsys-pipeline
pytest integration-tests/test_service_scripts.py::test_als_pipeline_script_exists \
       integration-tests/test_service_scripts.py::test_als_pipeline_script_is_executable \
       integration-tests/test_service_scripts.py::test_als_pipeline_requires_ratings_input -v
```

Expected: `3 passed`

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/run-als-pipeline.sh recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "feat: add ALS embedding pipeline runner script"
```

---

## Task 2: UserEmbedding Runner Script

**Files:**
- Create: `recsys-pipeline/run-user-embedding-pipeline.sh`
- Test: `recsys-pipeline/integration-tests/test_service_scripts.py`

- [ ] **Step 1: Write failing tests**

Add to `recsys-pipeline/integration-tests/test_service_scripts.py`:

```python
def test_user_embedding_script_exists():
    script = os.path.join(PIPELINE_DIR, "run-user-embedding-pipeline.sh")
    assert os.path.isfile(script), "run-user-embedding-pipeline.sh not found"


def test_user_embedding_script_is_executable():
    script = os.path.join(PIPELINE_DIR, "run-user-embedding-pipeline.sh")
    assert os.access(script, os.X_OK), "run-user-embedding-pipeline.sh is not executable"


def test_user_embedding_requires_ratings_input():
    script = os.path.join(PIPELINE_DIR, "run-user-embedding-pipeline.sh")
    result = subprocess.run(
        ["bash", script],
        capture_output=True,
        text=True,
        env={**os.environ, "RATINGS_INPUT_PATH": "", "ITEM2VEC_EMBEDDING_PATH": "some.txt"},
    )
    assert result.returncode != 0
    assert "RATINGS_INPUT_PATH" in result.stderr


def test_user_embedding_requires_item_embedding():
    script = os.path.join(PIPELINE_DIR, "run-user-embedding-pipeline.sh")
    result = subprocess.run(
        ["bash", script],
        capture_output=True,
        text=True,
        env={**os.environ, "RATINGS_INPUT_PATH": "ratings.csv", "ITEM2VEC_EMBEDDING_PATH": ""},
    )
    assert result.returncode != 0
    assert "ITEM2VEC_EMBEDDING_PATH" in result.stderr
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
cd recsys-pipeline
pytest integration-tests/test_service_scripts.py::test_user_embedding_script_exists -v
```

Expected: `FAILED — AssertionError: run-user-embedding-pipeline.sh not found`

- [ ] **Step 3: Create `run-user-embedding-pipeline.sh`**

```bash
#!/usr/bin/env bash
# User embedding pipeline
#
# Flow: ratings.csv + item_embedding.txt ──► UserEmbeddingTrainingJob
#                                                ├──► sampledata/user_embedding.txt
#                                                └──► Redis uEmb:{userId}   (if USER_EMBEDDING_SAVE_TO_REDIS=true)
#
# Requires item embeddings from a prior Item2Vec run:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv ./run-offline-pipeline.sh
#
# Required:
#   RATINGS_INPUT_PATH        Path to ratings CSV (userId,movieId,rating,timestamp)
#   ITEM2VEC_EMBEDDING_PATH   Path to item embedding file produced by Item2VecTrainingJob
#
# Optional (model):
#   USER_EMBEDDING_OUTPUT_PATH   Output text file (default: sampledata/user_embedding.txt)
#   USER_EMBEDDING_MIN_RATING    Minimum rating to include (default: 3.5)
#
# Optional (Redis — set USER_EMBEDDING_SAVE_TO_REDIS=true to enable):
#   USER_EMBEDDING_SAVE_TO_REDIS     true | false (default: false)
#   REDIS_HOST                       Redis hostname (default: localhost)
#   REDIS_PORT                       Redis port (default: 6379)
#   USER_EMBEDDING_REDIS_KEY_PREFIX  Key prefix written as {prefix}:{userId} (default: uEmb)
#   USER_EMBEDDING_REDIS_TTL_SECONDS TTL in seconds (default: 86400)
#
# Optional (Spark):
#   SPARK_HOME              Path to Spark installation
#   SPARK_MASTER            Spark master URL (default: local[*])
#   SPARK_DRIVER_MEMORY     Driver heap (default: 1g)
#   SPARK_EXECUTOR_MEMORY   Executor heap (default: 2g)
#   SPARK_SQL_SHUFFLE_PARTITIONS  (default: 4)
#
# Example — write to file only:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv \
#   ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
#   ./run-user-embedding-pipeline.sh
#
# Example — write to file + Redis:
#   RATINGS_INPUT_PATH=sampledata/ratings.csv \
#   ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
#   USER_EMBEDDING_SAVE_TO_REDIS=true \
#   REDIS_HOST=localhost \
#   ./run-user-embedding-pipeline.sh

set -euo pipefail

cd "$(dirname "$0")"

if [[ -z "${RATINGS_INPUT_PATH:-}" ]]; then
  echo "Error: RATINGS_INPUT_PATH is required." >&2
  echo "Usage: RATINGS_INPUT_PATH=<path> ITEM2VEC_EMBEDDING_PATH=<path> $0" >&2
  exit 1
fi

if [[ -z "${ITEM2VEC_EMBEDDING_PATH:-}" ]]; then
  echo "Error: ITEM2VEC_EMBEDDING_PATH is required." >&2
  echo "Tip: Run ./run-offline-pipeline.sh first to generate item embeddings." >&2
  exit 1
fi

if [[ ! -f "$ITEM2VEC_EMBEDDING_PATH" ]]; then
  echo "Error: Item embedding file not found: $ITEM2VEC_EMBEDDING_PATH" >&2
  echo "Tip: Run ./run-offline-pipeline.sh first to generate item embeddings." >&2
  exit 1
fi

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"

SPARK_SUBMIT=""
if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
  exit 127
fi

JAR="services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
if [[ ! -f "$JAR" ]]; then
  echo "Missing Spark job jar. Run: cd services/spark-streaming-job && sbt assembly" >&2
  exit 127
fi

OUTPUT_PATH="${USER_EMBEDDING_OUTPUT_PATH:-sampledata/user_embedding.txt}"

echo "=== User embedding pipeline ==="
echo "  Ratings input    : $RATINGS_INPUT_PATH"
echo "  Item embeddings  : $ITEM2VEC_EMBEDDING_PATH"
echo "  Output file      : $OUTPUT_PATH"
echo "  Min rating       : ${USER_EMBEDDING_MIN_RATING:-3.5}"
echo "  Save to Redis    : ${USER_EMBEDDING_SAVE_TO_REDIS:-false}"
if [[ "${USER_EMBEDDING_SAVE_TO_REDIS:-false}" == "true" ]]; then
  echo "  Redis host       : ${REDIS_HOST:-localhost}:${REDIS_PORT:-6379}"
  echo "  Redis key        : ${USER_EMBEDDING_REDIS_KEY_PREFIX:-uEmb}:{userId}"
fi
echo ""

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class com.demo.task.UserEmbeddingTrainingJob \
  "$JAR" \
  "$RATINGS_INPUT_PATH" \
  "$ITEM2VEC_EMBEDDING_PATH" \
  "$OUTPUT_PATH"
```

- [ ] **Step 4: Make executable**

```bash
chmod +x recsys-pipeline/run-user-embedding-pipeline.sh
```

- [ ] **Step 5: Run the tests**

```bash
cd recsys-pipeline
pytest integration-tests/test_service_scripts.py -k "user_embedding" -v
```

Expected: `4 passed`

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/run-user-embedding-pipeline.sh recsys-pipeline/integration-tests/test_service_scripts.py
git commit -m "feat: add user embedding pipeline runner script"
```

---

## Task 3: Enable Deep Learning Weight

**Files:**
- Modify: `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml`

Context: `offlineScore = relevanceWeight*cosine + contentWeight*genre + popularityWeight*pop + deepLearningWeight*onnxScore`. The last term is multiplied by `RECSYS_DEEP_LEARNING_WEIGHT` which defaults to `0.0`, making the ONNX model a no-op.

The MLP model (`mlp_embedding_model.onnx`) is already loaded at startup. Changing the default to `0.15` means it contributes without requiring any env-var changes on existing deployments.

- [ ] **Step 1: Write a test that verifies the default weight is non-zero**

Add to a new file `recsys-pipeline/integration-tests/test_application_config.py`:

```python
import os
import yaml
import pytest

CONFIG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "services",
    "java-retrieval-service",
    "src",
    "main",
    "resources",
    "application.yml",
)


def test_deep_learning_weight_default_is_nonzero():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    raw = config["recsys"]["bandit"]["deep-learning-weight"]
    # Extract the default value from the Spring placeholder ${VAR:default}
    default_str = str(raw).split(":")[-1].rstrip("}")
    default_val = float(default_str)
    assert default_val > 0.0, (
        f"deep-learning-weight default is {default_val}; must be > 0.0 "
        "or the ONNX model score is ignored"
    )
```

- [ ] **Step 2: Run the test to confirm it fails**

```bash
cd recsys-pipeline
pip install pyyaml -q
pytest integration-tests/test_application_config.py::test_deep_learning_weight_default_is_nonzero -v
```

Expected: `FAILED — AssertionError: deep-learning-weight default is 0.0; must be > 0.0`

- [ ] **Step 3: Change the default in `application.yml`**

In `recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml`, change line 45 from:

```yaml
    deep-learning-weight: ${RECSYS_DEEP_LEARNING_WEIGHT:0.0}
```

to:

```yaml
    deep-learning-weight: ${RECSYS_DEEP_LEARNING_WEIGHT:0.15}
```

The surrounding context for reference:

```yaml
  bandit:
    algorithm: ${RECSYS_BANDIT_ALGORITHM:ucb}
    exploration-alpha: ${RECSYS_EXPLORATION_ALPHA:0.75}
    max-exploration-bonus: ${RECSYS_MAX_EXPLORATION_BONUS:0.25}
    cold-start-exposure-threshold: ${RECSYS_COLD_START_THRESHOLD:5}
    cold-start-boost: ${RECSYS_COLD_START_BOOST:1.35}
    relevance-weight: ${RECSYS_RELEVANCE_WEIGHT:0.6}
    content-weight: ${RECSYS_CONTENT_WEIGHT:0.25}
    popularity-weight: ${RECSYS_POPULARITY_WEIGHT:0.15}
    deep-learning-weight: ${RECSYS_DEEP_LEARNING_WEIGHT:0.15}   # ← changed from 0.0
```

Note: weights don't need to sum to 1.0 — each signal is independently clamped to [0,1]. Setting this to 0.15 adds ONNX score without reducing other signals.

- [ ] **Step 4: Run the test to confirm it passes**

```bash
cd recsys-pipeline
pytest integration-tests/test_application_config.py::test_deep_learning_weight_default_is_nonzero -v
```

Expected: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml \
        recsys-pipeline/integration-tests/test_application_config.py
git commit -m "feat: enable ONNX deep-learning-weight default (0.0 → 0.15)"
```

---

## Task 4: Document End-to-End Startup Sequence

**Files:**
- Modify: `recsys-pipeline/README.md`

- [ ] **Step 1: Write a test that checks key steps exist in the README**

Add to `recsys-pipeline/integration-tests/test_application_config.py`:

```python
README_PATH = os.path.join(os.path.dirname(__file__), "..", "README.md")


def test_readme_documents_item2vec_redis():
    with open(README_PATH) as f:
        content = f.read()
    assert "ITEM2VEC_SAVE_TO_REDIS=true" in content, (
        "README must show how to run Item2Vec with Redis output"
    )


def test_readme_documents_user_embedding_pipeline():
    with open(README_PATH) as f:
        content = f.read()
    assert "run-user-embedding-pipeline.sh" in content, (
        "README must mention run-user-embedding-pipeline.sh"
    )


def test_readme_documents_als_pipeline():
    with open(README_PATH) as f:
        content = f.read()
    assert "run-als-pipeline.sh" in content, (
        "README must mention run-als-pipeline.sh"
    )
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
cd recsys-pipeline
pytest integration-tests/test_application_config.py -k "readme" -v
```

Expected: `3 failed`

- [ ] **Step 3: Add the startup sequence to `recsys-pipeline/README.md`**

Find the "Quick Start" section and replace it with the following (keep the existing content above and below — just replace the inner steps between the prerequisite list and the "Query the API" section):

```markdown
## Quick Start

Prerequisites:

- Java 17
- Apache Spark 3.5.x with Scala 2.12
- `sbt`
- Maven 3.8+
- Docker / Docker Compose
- Python 3

Build the Spark job jar:

```bash
cd services/spark-streaming-job
sbt assembly
cd ../..
```

Start Kafka, Zookeeper, and Redis:

```bash
docker compose up -d
```

### Step 1 — Offline embeddings (run once before starting the retrieval service)

Train Item2Vec embeddings and write them to Redis (the retrieval service reads `i2vEmb:{itemId}` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ITEM2VEC_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./run-offline-pipeline.sh
```

Train user embeddings from the item vectors produced above and write to Redis (`uEmb:{userId}` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ITEM2VEC_EMBEDDING_PATH=sampledata/item_embedding.txt \
USER_EMBEDDING_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./run-user-embedding-pipeline.sh
```

Optionally, train ALS collaborative-filtering embeddings (writes `alsItemEmb:*` and `alsUserEmb:*` keys):

```bash
RATINGS_INPUT_PATH=sampledata/ratings.csv \
ALS_SAVE_TO_REDIS=true \
REDIS_HOST=localhost \
./run-als-pipeline.sh
```

To switch the retrieval service to ALS embeddings set these env vars before starting it:

```bash
export ITEM_EMBEDDING_PREFIX=alsItemEmb
export USER_EMBEDDING_PREFIX=alsUserEmb
```

### Step 2 — Start the retrieval service

```bash
cd services/java-retrieval-service
mvn spring-boot:run
```

The service loads `mlp_embedding_model.onnx` from the classpath at startup. To use a model trained
outside the JAR, set `ONNX_MODEL_PATH` and `ONNX_LOOKUPS_PATH` environment variables before starting.

### Step 3 — Start the clickstream producer

```bash
python -m pip install -r services/python-modeling/requirements.txt
python services/python-modeling/producer.py
```

### Step 4 — Run the streaming job

```bash
./run-streaming-job.sh
```

This populates `user:{id}:recent` and `global:item_popularity` in Redis in real time, which the retrieval service uses for recency and popularity signals.

### Step 5 — Query the API

```bash
curl http://localhost:8080/recommend/user_1
curl http://localhost:8080/recommend/user_1?limit=10
curl http://localhost:8080/metrics
```
```

- [ ] **Step 4: Run the tests**

```bash
cd recsys-pipeline
pytest integration-tests/test_application_config.py -k "readme" -v
```

Expected: `3 passed`

- [ ] **Step 5: Run the full integration test suite**

```bash
cd recsys-pipeline
pytest -q
```

Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/README.md recsys-pipeline/integration-tests/test_application_config.py
git commit -m "docs: add complete startup sequence with embedding Redis writes"
```

---

## Verification Checklist

After all tasks complete:

- [ ] `ls -la recsys-pipeline/run-als-pipeline.sh recsys-pipeline/run-user-embedding-pipeline.sh` — both files exist and are executable (`-rwxr-xr-x`)
- [ ] `grep "RATINGS_INPUT_PATH" recsys-pipeline/run-als-pipeline.sh | grep -c "required"` — output is `1`
- [ ] `grep "deep-learning-weight" recsys-pipeline/services/java-retrieval-service/src/main/resources/application.yml` — shows `0.15`, not `0.0`
- [ ] `cd recsys-pipeline && pytest -q` — all tests pass
