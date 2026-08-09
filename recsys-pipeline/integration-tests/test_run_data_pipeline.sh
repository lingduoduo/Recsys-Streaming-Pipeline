#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."   # recsys-pipeline/

# Stub run-streaming-job.sh on PATH: record the class it would launch, then exit.
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
cat > "$tmp/run-streaming-job.sh" <<'STUB'
#!/usr/bin/env bash
echo "launched ${SPARK_MAIN_CLASS}" >> "$RECORD"
STUB
chmod +x "$tmp/run-streaming-job.sh"

cat > "$tmp/producer.sh" <<'STUB'
#!/usr/bin/env bash
echo "producer" >> "$RECORD"
STUB
chmod +x "$tmp/producer.sh"

export RECORD="$tmp/record.txt"
: > "$RECORD"

# DRY_RUN makes the script use the stubbed launcher and not wait/trap.
DRY_RUN=1 RUN_STREAMING_JOB="$tmp/run-streaming-job.sh" EVENT_PRODUCER="$tmp/producer.sh" \
  bash scripts/run-data-pipeline.sh

grep -q "com.demo.task.UserEventStreamingJob" "$RECORD"          || { echo "FAIL: UserEvent not launched"; exit 1; }
grep -q "^producer$" "$RECORD" || { echo "FAIL: direct event producer not launched"; exit 1; }
if grep -q "com.demo.process.OnlineJoinerStreamingJob" "$RECORD"; then
  echo "FAIL: derived-topic OnlineJoiner must not be part of the Avro vertical-slice wrapper"
  exit 1
fi
if grep -q "com.demo.process.ExperienceCollectorStreamingJob" "$RECORD"; then
  echo "FAIL: derived-topic ExperienceCollector must not be part of the Avro vertical-slice wrapper"
  exit 1
fi
[[ "$(wc -l < "$RECORD" | tr -d ' ')" == "2" ]] || { echo "FAIL: expected producer plus exactly one launched job"; exit 1; }
echo "PASS"
