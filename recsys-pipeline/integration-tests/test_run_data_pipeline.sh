#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."   # recsys-pipeline/

tmp="$(mktemp -d)"
trap 'kill "${listener_pid:-}" 2>/dev/null || true; wait "${listener_pid:-}" 2>/dev/null || true; rm -rf "$tmp"' EXIT
export RECORD="$tmp/record.txt"

cat > "$tmp/run-streaming-job.sh" <<'STUB'
#!/usr/bin/env bash
echo "consumer-start" >> "$RECORD"
if [[ "${FAIL_ROLE:-}" == "consumer" ]]; then
  sleep 0.2
  exit 41
fi
trap 'echo "consumer-stopped" >> "$RECORD"; exit 0' TERM INT
sleep 0.5
exit 0
STUB
chmod +x "$tmp/run-streaming-job.sh"

cat > "$tmp/python-bin" <<'STUB'
#!/usr/bin/env bash
if [[ "$1" == "scripts/provision-kafka-topics.py" ]]; then
  echo "provision:$*" >> "$RECORD"
  exit 0
fi
echo "producer-start" >> "$RECORD"
if [[ "${FAIL_ROLE:-}" == "producer" ]]; then
  sleep 0.2
  exit 42
fi
trap 'echo "producer-stopped" >> "$RECORD"; exit 0' TERM INT
sleep 0.5
exit 0
STUB
chmod +x "$tmp/python-bin"

for command in kafka-topics kafka-configs; do
  cat > "$tmp/$command" <<'STUB'
#!/usr/bin/env bash
exit 0
STUB
  chmod +x "$tmp/$command"
done

python3 -c '
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
sock.listen()
print(sock.getsockname()[1], flush=True)
while True:
    connection, _ = sock.accept()
    connection.close()
' > "$tmp/kafka-port" &
listener_pid="$!"
for _ in $(seq 1 50); do
  [[ -s "$tmp/kafka-port" ]] && break
  sleep 0.1
done
bootstrap="127.0.0.1:$(< "$tmp/kafka-port")"

run_failure_case() {
  local failed_role="$1"
  local expected_status="$2"
  : > "$RECORD"

  if PATH="$tmp:$PATH" KAFKA_BOOTSTRAP_SERVERS="$bootstrap" \
    RUN_STREAMING_JOB="$tmp/run-streaming-job.sh" PYTHON_BIN="$tmp/python-bin" \
    FAIL_ROLE="$failed_role" bash scripts/run-data-pipeline.sh; then
    echo "FAIL: wrapper returned success after $failed_role exited" >&2
    exit 1
  else
    actual_status="$?"
  fi

  [[ "$actual_status" == "$expected_status" ]] || {
    echo "FAIL: expected $failed_role status $expected_status, got $actual_status" >&2
    exit 1
  }
  grep -q '^provision:scripts/provision-kafka-topics.py ' "$RECORD" || {
    echo "FAIL: PYTHON_BIN did not run topic provisioning" >&2
    exit 1
  }
  grep -q '^consumer-start$' "$RECORD" || { echo "FAIL: consumer did not start" >&2; exit 1; }
  grep -q '^producer-start$' "$RECORD" || { echo "FAIL: producer did not start" >&2; exit 1; }
  if [[ "$failed_role" == "consumer" ]]; then
    grep -q '^producer-stopped$' "$RECORD" || { echo "FAIL: producer was not stopped" >&2; exit 1; }
  else
    grep -q '^consumer-stopped$' "$RECORD" || { echo "FAIL: consumer was not stopped" >&2; exit 1; }
  fi
}

run_failure_case consumer 41
run_failure_case producer 42
echo "PASS"
