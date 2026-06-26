#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

LAUNCHER="${RUN_STREAMING_JOB:-./run-streaming-job.sh}"
CHECKPOINT_ROOT="${CHECKPOINT_ROOT:-/tmp/spark-recsys}"

CLASSES=(
  "com.demo.task.UserEventStreamingJob:user-event-streaming-job"
  "com.demo.process.OnlineJoinerStreamingJob:online-joiner"
  "com.demo.process.ExperienceCollectorStreamingJob:experience-collector"
)

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

echo "Launched PIDs: ${pids[*]}"

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  wait "${pids[@]}" 2>/dev/null || true
  exit 0
fi

shutdown() { echo "Stopping ${pids[*]}"; kill "${pids[@]}" 2>/dev/null || true; }
trap shutdown INT TERM
wait
