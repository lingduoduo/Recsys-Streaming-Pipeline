#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"

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

# The preflight uses bash /dev/tcp and cannot be stubbed on PATH, so give it a
# real throwaway listener rather than depending on a running broker.
port_file="$tmp/port"
python3 -c '
import socket, sys, time
s = socket.socket()
s.bind(("127.0.0.1", 0))
s.listen(1)
open(sys.argv[1], "w").write(str(s.getsockname()[1]))
time.sleep(60)
' "$port_file" &
listener=$!
trap 'kill "$listener" 2>/dev/null; rm -rf "$tmp"' EXIT
for _ in $(seq 1 50); do [[ -s "$port_file" ]] && break; sleep 0.1; done
bootstrap="127.0.0.1:$(cat "$port_file")"

PATH="$tmp:$PATH" \
SPARK_HOME="$tmp/spark" \
KAFKA_BOOTSTRAP_SERVERS="$bootstrap" \
KAFKA_TOPIC="events_for_test" \
./scripts/run-streaming-job.sh

first_line="$(sed -n '1p' "$RECORD")"
[[ "$first_line" == "kafka-topics --bootstrap-server $bootstrap --create --if-not-exists --topic events_for_test" ]] ||
  { echo "FAIL: expected topic bootstrap before Spark, got: $first_line"; exit 1; }

[[ "$(sed -n '2p' "$RECORD")" == "spark-submit" ]] ||
  { echo "FAIL: Spark was not launched after topic bootstrap"; exit 1; }

echo "PASS"
