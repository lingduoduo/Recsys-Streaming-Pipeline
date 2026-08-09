#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

LAUNCHER="${RUN_STREAMING_JOB:-./scripts/run-streaming-job.sh}"
EVENT_PRODUCER="${EVENT_PRODUCER:-}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
EVENT_PRODUCER_SCRIPT="${EVENT_PRODUCER_SCRIPT:-services/python-modeling/producer.py}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark-recsys}"
KAFKA_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

CLASSES=(
  "com.demo.task.UserEventStreamingJob:user-event-streaming-job"
)

# Keep topic creation an explicit, single pipeline-start boundary.  The
# individual launcher repeats this idempotently for the default consumer, but
# doing it here means no producer/consumer is started before the complete
# catalog has passed policy validation and been provisioned.
if [[ "${DRY_RUN:-0}" != "1" ]]; then
  kafka_reachable() {
    local hostport="${1%%,*}"  # first entry when a bootstrap list is supplied
    (exec 3<>"/dev/tcp/${hostport%:*}/${hostport##*:}") 2>/dev/null
  }

  if ! kafka_reachable "$KAFKA_SERVERS"; then
    echo "Kafka unreachable at $KAFKA_SERVERS." >&2
    echo "Start the local stack:  docker compose up -d" >&2
    exit 1
  fi

  PROVISION_COMMAND_MODE="host"
  PROVISION_BOOTSTRAP_SERVER="$KAFKA_SERVERS"
  if ! command -v kafka-topics >/dev/null 2>&1 || ! command -v kafka-configs >/dev/null 2>&1; then
    if [[ "$KAFKA_SERVERS" != "localhost:9092" && "$KAFKA_SERVERS" != "127.0.0.1:9092" ]]; then
      echo "Kafka CLIs not found. Docker Compose fallback is only available for localhost:9092 or 127.0.0.1:9092; install kafka-topics and kafka-configs for $KAFKA_SERVERS." >&2
      exit 127
    fi
    if command -v docker >/dev/null 2>&1 && docker compose ps --status running kafka >/dev/null 2>&1; then
      PROVISION_COMMAND_MODE="docker-compose"
      PROVISION_BOOTSTRAP_SERVER="localhost:9092"
    else
      echo "Kafka CLIs not found and the local Docker Kafka service is unavailable." >&2
      exit 127
    fi
  fi

  python3 scripts/provision-kafka-topics.py \
    --bootstrap-server "$PROVISION_BOOTSTRAP_SERVER" \
    --command-mode "$PROVISION_COMMAND_MODE"
fi

pids=()
for entry in "${CLASSES[@]}"; do
  class="${entry%%:*}"
  ckpt="${entry##*:}"
  echo "Starting ${class} (checkpoint ${CHECKPOINT_ROOT}/${ckpt})"
  SPARK_MAIN_CLASS="$class" \
    SPARK_CHECKPOINT_LOCATION="${CHECKPOINT_ROOT}/${ckpt}" \
    "$LAUNCHER" &
  pids+=("$!")
done

echo "Starting clickstream producer for recsys_events"
if [[ -n "$EVENT_PRODUCER" ]]; then
  "$EVENT_PRODUCER" &
else
  "$PYTHON_BIN" "$EVENT_PRODUCER_SCRIPT" &
fi
pids+=("$!")

echo "Launched PIDs: ${pids[*]}"

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  wait "${pids[@]}" 2>/dev/null || true
  exit 0
fi

shutdown() { echo "Stopping ${pids[*]}"; kill "${pids[@]}" 2>/dev/null || true; }
trap shutdown INT TERM
wait
