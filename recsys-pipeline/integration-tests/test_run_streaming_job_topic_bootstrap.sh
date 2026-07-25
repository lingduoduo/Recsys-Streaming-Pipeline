#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

mkdir -p "$tmp/spark/bin"
cat > "$tmp/spark/bin/spark-submit" <<'STUB'
#!/usr/bin/env bash
echo spark-submit >> "$RECORD"
STUB
chmod +x "$tmp/spark/bin/spark-submit"

cat > "$tmp/docker" <<'STUB'
#!/usr/bin/env bash
if [[ "$*" == *"kafka-topics"* ]]; then
  echo "docker $*" >> "$RECORD"
fi
STUB
chmod +x "$tmp/docker"

cat > "$tmp/kafka-topics" <<'STUB'
#!/usr/bin/env bash
echo "kafka-topics $*" >> "$RECORD"
STUB
chmod +x "$tmp/kafka-topics"

export RECORD="$tmp/record.txt"
: > "$RECORD"

PATH="$tmp:$PATH" \
SPARK_HOME="$tmp/spark" \
KAFKA_TOPIC="events_for_test" \
./run-streaming-job.sh

first_line="$(sed -n '1p' "$RECORD")"
[[ "$first_line" == "kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic events_for_test" ]] ||
  { echo "FAIL: expected topic bootstrap before Spark, got: $first_line"; exit 1; }

[[ "$(sed -n '2p' "$RECORD")" == "spark-submit" ]] ||
  { echo "FAIL: Spark was not launched after topic bootstrap"; exit 1; }

echo "PASS"
