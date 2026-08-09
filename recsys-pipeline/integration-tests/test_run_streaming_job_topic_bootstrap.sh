#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

tmp="$(mktemp -d)"

mkdir -p "$tmp/spark/bin"
mkdir -p services/spark-streaming-job/target/scala-2.12
jar_path="services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar"
created_jar=0
if [[ ! -e "$jar_path" ]]; then
  : > "$jar_path"
  created_jar=1
fi
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

cat > "$tmp/kafka-configs" <<'STUB'
#!/usr/bin/env bash
echo "kafka-configs $*" >> "$RECORD"
STUB
chmod +x "$tmp/kafka-configs"

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
trap 'kill "$listener" 2>/dev/null; wait "$listener" 2>/dev/null || true; if [[ "$created_jar" -eq 1 ]]; then rm -f "$jar_path"; fi; rm -rf "$tmp"' EXIT
for _ in $(seq 1 50); do [[ -s "$port_file" ]] && break; sleep 0.1; done
bootstrap="127.0.0.1:$(cat "$port_file")"

PATH="$tmp:$PATH" \
SPARK_HOME="$tmp/spark" \
KAFKA_BOOTSTRAP_SERVERS="$bootstrap" \
./scripts/run-streaming-job.sh

first_line="$(sed -n '1p' "$RECORD")"
[[ "$first_line" == "kafka-topics --bootstrap-server $bootstrap --create --if-not-exists --topic recsys_events --partitions 3 --replication-factor 1" ]] ||
  { echo "FAIL: expected declarative provisioning before Spark, got: $first_line"; exit 1; }

last_line="$(tail -n 1 "$RECORD")"
[[ "$last_line" == "spark-submit" ]] ||
  { echo "FAIL: Spark was not launched after topic provisioning"; exit 1; }

echo "PASS"
