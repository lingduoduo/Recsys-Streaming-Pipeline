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

export RECORD="$tmp/record.txt"
: > "$RECORD"

# DRY_RUN makes the script use the stubbed launcher and not wait/trap.
DRY_RUN=1 RUN_STREAMING_JOB="$tmp/run-streaming-job.sh" bash scripts/run-data-pipeline.sh

grep -q "com.demo.task.UserEventStreamingJob" "$RECORD"          || { echo "FAIL: UserEvent not launched"; exit 1; }
grep -q "com.demo.process.OnlineJoinerStreamingJob" "$RECORD"    || { echo "FAIL: OnlineJoiner not launched"; exit 1; }
grep -q "com.demo.process.ExperienceCollectorStreamingJob" "$RECORD" || { echo "FAIL: ExperienceCollector not launched"; exit 1; }
echo "PASS"
