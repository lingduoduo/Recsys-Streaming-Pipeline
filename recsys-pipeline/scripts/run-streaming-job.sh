#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"
SPARK_HOME="${SPARK_HOME:-}"
SPARK_SUBMIT=""
if [[ -n "$SPARK_HOME" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi

if [[ -z "$SPARK_SUBMIT" ]]; then
  if command -v spark-submit >/dev/null 2>&1; then
    SPARK_SUBMIT="$(command -v spark-submit)"
  else
    echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
    exit 127
  fi
fi

if [[ ! -f services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar ]]; then
  echo "Missing Spark job jar. Run: cd services/spark-streaming-job && sbt assembly" >&2
  exit 127
fi

MAIN_CLASS="${SPARK_MAIN_CLASS:-com.demo.task.UserEventStreamingJob}"

# Spark's Kafka AdminClient does not trigger broker-side topic auto-creation when it
# asks for initial offsets. Provision the checked-in catalog before starting the
# default consumer so topics and retention policy are explicit and repeatable.
if [[ "$MAIN_CLASS" == "com.demo.task.UserEventStreamingJob" ]]; then
  KAFKA_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

  # Bash's /dev/tcp builtin is the only bounded check available here: nc -w does not
  # bound connect on macOS, curl telnet:// never returns on a live port, and timeout
  # is not installed. A host that silently drops SYNs still waits on the OS TCP
  # timeout; a refused connection returns in about 6ms.
  kafka_reachable() {
    local hostport="${1%%,*}"                       # first entry if a list
    (exec 3<>"/dev/tcp/${hostport%:*}/${hostport##*:}") 2>/dev/null
  }

  if ! kafka_reachable "$KAFKA_SERVERS"; then
    echo "Kafka unreachable at $KAFKA_SERVERS." >&2
    echo "Start the local stack:  docker compose up -d" >&2
    echo "If the stack looks up, the broker may be crash-looping:  docker compose ps kafka" >&2
    echo "Or set KAFKA_BOOTSTRAP_SERVERS to your broker." >&2
    exit 1
  fi

  PROVISION_COMMAND_MODE="host"
  PROVISION_BOOTSTRAP_SERVER="$KAFKA_SERVERS"
  if ! command -v kafka-topics >/dev/null 2>&1 || ! command -v kafka-configs >/dev/null 2>&1; then
    case "${KAFKA_SERVERS%%,*}" in
      localhost:*|127.0.0.1:*)
        if command -v docker >/dev/null 2>&1 && docker compose ps --status running kafka >/dev/null 2>&1; then
          PROVISION_COMMAND_MODE="docker-compose"
          # The CLI runs inside the Kafka container, where the broker listens on 9092.
          PROVISION_BOOTSTRAP_SERVER="localhost:9092"
        else
          echo "Kafka CLIs not found and the local Docker Kafka service is unavailable." >&2
          exit 127
        fi
        ;;
      *)
        echo "Kafka CLIs not found. Install kafka-topics and kafka-configs for $KAFKA_SERVERS." >&2
        exit 127
        ;;
    esac
  fi

  python3 scripts/provision-kafka-topics.py \
    --bootstrap-server "$PROVISION_BOOTSTRAP_SERVER" \
    --command-mode "$PROVISION_COMMAND_MODE"
fi

exec "$SPARK_SUBMIT" \
  --master "${SPARK_MASTER:-local[*]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}" \
  --class "$MAIN_CLASS" \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
