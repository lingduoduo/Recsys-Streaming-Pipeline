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

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_MODELING="${SCRIPT_DIR}/services/python-modeling"

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
