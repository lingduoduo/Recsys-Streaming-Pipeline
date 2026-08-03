# Kafka Dependency Resilience Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the local Kafka dependency fail visibly and fast — the broker restarts itself after a crash, and a launch with no reachable broker aborts in under a second with an actionable message instead of 64 seconds and a Java stack trace.

**Architecture:** Two independent changes. `docker-compose.yml` gains `restart: unless-stopped` so a broker that loses the ZooKeeper broker-registration race comes back on its own. `run-streaming-job.sh` gains a `/dev/tcp` reachability preflight ahead of its topic bootstrap, so the unbounded `kafka-topics` AdminClient retry loop can never be entered.

**Tech Stack:** Bash (with the `/dev/tcp` builtin), Docker Compose, pytest, PyYAML 6.0.3.

**Spec:** [.superpowers/docs/specs/2026-08-03-kafka-dependency-resilience-design.md](../specs/2026-08-03-kafka-dependency-resilience-design.md)

## Global Constraints

- Work on branch `design/kafka-dependency-resilience`. Never commit to `master`.
- Run all commands from `recsys-pipeline/`.
- Test interpreter: `/Users/linghuang/miniconda3/envs/llm/bin/python`. The full suite takes about 4.5 minutes in this environment; that is pre-existing and unrelated to this work.
- The reachability check MUST use the bash `/dev/tcp` builtin. Measured on this macOS host: `nc -z -w2` does **not** bound connect time (75s on an unroutable host), `curl --connect-timeout 2 telnet://` hangs forever on a *live* port, and neither `timeout` nor `gtimeout` is installed. `/dev/tcp` is 0.006s on a refused port and 0.005s on a live one.
- No test may require a live Kafka broker on the developer's machine.
- Leave `test_run_streaming_job_topic_bootstrap.sh` unwired from any runner. Wiring it in is explicitly out of scope.

---

### Task 1: Restart policy for the local stack

**Files:**
- Modify: `recsys-pipeline/docker-compose.yml`
- Create: `recsys-pipeline/integration-tests/test_docker_compose_resilience.py`

**Interfaces:**
- Consumes: nothing.
- Produces: nothing consumed by Task 2. The two tasks are independent.

- [ ] **Step 1: Write the failing test**

Create `recsys-pipeline/integration-tests/test_docker_compose_resilience.py`:

```python
from pathlib import Path

import yaml


COMPOSE_FILE = Path(__file__).resolve().parents[1] / "docker-compose.yml"


def test_every_service_declares_a_restart_policy() -> None:
    services = yaml.safe_load(COMPOSE_FILE.read_text(encoding="utf-8"))["services"]
    missing = [
        name
        for name, config in services.items()
        if config.get("restart") != "unless-stopped"
    ]
    assert not missing, f"services without 'restart: unless-stopped': {missing}"
```

This iterates every service rather than naming the current three, so a service added later is covered automatically.

- [ ] **Step 2: Run the test to verify it fails**

Run: `/Users/linghuang/miniconda3/envs/llm/bin/python -m pytest integration-tests/test_docker_compose_resilience.py -v`

Expected: FAIL with `services without 'restart: unless-stopped': ['zookeeper', 'kafka', 'redis']`

- [ ] **Step 3: Add the restart policy**

In `recsys-pipeline/docker-compose.yml`, add one line to each of the three services. Place it directly after the `image:` line so all three read the same way.

For `zookeeper`:

```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    restart: unless-stopped
```

For `kafka`:

```yaml
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    restart: unless-stopped
```

For `redis`:

```yaml
  redis:
    image: redis:7
    restart: unless-stopped
```

Change nothing else in the file.

- [ ] **Step 4: Run the test to verify it passes**

Run: `/Users/linghuang/miniconda3/envs/llm/bin/python -m pytest integration-tests/test_docker_compose_resilience.py -v`

Expected: PASS

- [ ] **Step 5: Verify the policy against a Docker daemon restart**

The unit test only proves the file says the right thing. Prove the behavior against the scenario that actually caused the incident: a VM restart.

**Do NOT use `docker kill` to test this.** Docker treats a user-initiated kill as a manual stop and deliberately skips the restart policy. Measured on this host, `docker kill` leaves the container `Exited` with `RestartCount=0` for 60s+. That is correct Docker behavior, not a failure of the policy.

```bash
cd /Users/linghuang/Git/Recsys-Streaming-Pipeline/recsys-pipeline
docker compose up -d          # ensure containers are recreated with the new policy
docker compose ps --format '{{.Service}} {{.Status}}'

colima stop
colima start

# wait for the daemon, then check WITHOUT running docker compose up
for i in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 5; done
docker compose ps --format '{{.Service}} {{.Status}}'
```

Expected: all three services report `Up`, with no `docker compose up` run after the restart.

If Kafka shows `Restarting`, wait up to 90s and re-check. That state reproduces the original incident (a stale `/brokers/ids/1` znode), and the restart policy is expected to heal it once the ZooKeeper session expires — that outcome is a PASS, and is the clearest possible proof the policy works. Only a container still not `Up` after 90s is a failure; report DONE_WITH_CONCERNS with the output rather than trying to fix the broker by hand.

- [ ] **Step 6: Commit**

```bash
git add recsys-pipeline/docker-compose.yml recsys-pipeline/integration-tests/test_docker_compose_resilience.py
git commit -m "fix: restart the local stack services automatically

Kafka exited 1 on a VM restart when its pre-restart ephemeral znode
/brokers/ids/1 had not yet expired, and nothing brought it back because
no service declared a restart policy. Redis and ZooKeeper stayed healthy,
masking the outage.

restart: unless-stopped lets Docker retry with backoff; the stale znode
expires within the 18-40s ZooKeeper session timeout, so a later attempt
registers cleanly. It also starts the containers when the Docker daemon
starts after a VM restart."
```

---

### Task 2: Fail-fast broker preflight in the launch script

**Files:**
- Modify: `recsys-pipeline/run-streaming-job.sh:29-45`
- Modify: `recsys-pipeline/integration-tests/test_service_scripts.py`
- Modify: `recsys-pipeline/integration-tests/test_run_streaming_job_topic_bootstrap.sh`

**Interfaces:**
- Consumes: `copy_pipeline_scripts(tmp_path) -> Path`, `add_spark_job_jar(pipeline) -> Path`, `base_env(tmp_path) -> dict[str, str]`, and `run_script(script, env) -> CompletedProcess[str]`, all already in `test_service_scripts.py`. `base_env` already puts a no-op `kafka-topics` stub on PATH.
- Produces: a `kafka_reachable <host:port[,host:port...]>` shell function in `run-streaming-job.sh` returning 0 when a TCP connection succeeds and non-zero otherwise; on failure the script writes an actionable message to stderr and exits 1. Also a `listening_socket()` context manager in `test_service_scripts.py` yielding a `"127.0.0.1:<port>"` string.

- [ ] **Step 1: Write the failing test**

Add to `recsys-pipeline/integration-tests/test_service_scripts.py`. Add one import with the existing imports at the top of the file — this step needs only `time`; Step 5 adds the rest when it uses them:

```python
import time
```

Then add the test after `test_streaming_script_reports_missing_consolidated_jar`:

```python
def test_streaming_script_fails_fast_when_broker_unreachable(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    # The preflight runs after the spark-submit and jar checks, so without a jar
    # the script would exit 127 and this test would pass for the wrong reason.
    add_spark_job_jar(pipeline)
    env = base_env(tmp_path)
    env["KAFKA_BOOTSTRAP_SERVERS"] = "127.0.0.1:1"

    start = time.monotonic()
    result = run_script(pipeline / "run-streaming-job.sh", env)
    elapsed = time.monotonic() - start

    assert result.returncode == 1
    # Guards the 64s AdminClient retry loop this replaced; loose enough not to flake.
    assert elapsed < 5
    assert "127.0.0.1:1" in result.stderr
    assert "docker compose up -d" in result.stderr
    assert "Exception" not in result.stderr
    assert not Path(env["SPARK_SUBMIT_LOG"]).exists()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `/Users/linghuang/miniconda3/envs/llm/bin/python -m pytest integration-tests/test_service_scripts.py::test_streaming_script_fails_fast_when_broker_unreachable -v`

Expected: FAIL on `assert result.returncode == 1`, actual `0`. The `kafka-topics` stub from `base_env` exits 0 immediately, so today the script sails past the unreachable broker and runs `spark-submit`.

- [ ] **Step 3: Add the preflight to the launch script**

In `recsys-pipeline/run-streaming-job.sh`, replace the block that currently starts at the `if [[ "$MAIN_CLASS" == "com.demo.task.UserEventStreamingJob" ]]` line and ends at its closing `fi` with:

```bash
# Spark's Kafka AdminClient does not trigger broker-side topic auto-creation when it
# asks for initial offsets. Bootstrap the default job's input topic when using the
# local Docker stack so a consumer can be started before the first producer.
if [[ "$MAIN_CLASS" == "com.demo.task.UserEventStreamingJob" ]]; then
  KAFKA_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
  INPUT_TOPIC="${KAFKA_TOPIC:-recsys_events}"

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
    echo "Or set KAFKA_BOOTSTRAP_SERVERS to your broker." >&2
    exit 1
  fi

  if command -v kafka-topics >/dev/null 2>&1; then
    kafka-topics --bootstrap-server "$KAFKA_SERVERS" \
      --create --if-not-exists --topic "$INPUT_TOPIC"
  elif [[ "$KAFKA_SERVERS" == "localhost:9092" ]] &&
       command -v docker >/dev/null 2>&1 &&
       docker compose ps --status running kafka >/dev/null 2>&1; then
    docker compose exec -T kafka kafka-topics \
      --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$INPUT_TOPIC"
  fi
fi
```

The two bootstrap branches are unchanged. Only the preflight is new.

- [ ] **Step 4: Run the new test to verify it passes**

Run: `/Users/linghuang/miniconda3/envs/llm/bin/python -m pytest integration-tests/test_service_scripts.py::test_streaming_script_fails_fast_when_broker_unreachable -v`

Expected: PASS in well under a second.

- [ ] **Step 5: Make the spark-path test independent of a real broker**

`test_streaming_script_uses_consolidated_spark_service_path` currently relies on the default `localhost:9092`. With the preflight in place it would pass only on a machine that happens to be running Kafka. Give it a real throwaway listener instead.

Add these imports alongside the existing ones:

```python
import contextlib
import socket
from typing import Iterator
```

Add this helper next to the other helpers in `test_service_scripts.py`:

```python
@contextlib.contextmanager
def listening_socket() -> Iterator[str]:
    """Yield 'host:port' for a real socket that accepts connections.

    The preflight uses a bash builtin, so it cannot be stubbed on PATH. A real
    throwaway listener keeps the test hermetic without needing a broker.
    """
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    try:
        yield f"127.0.0.1:{sock.getsockname()[1]}"
    finally:
        sock.close()
```

Then change the body of `test_streaming_script_uses_consolidated_spark_service_path` to:

```python
def test_streaming_script_uses_consolidated_spark_service_path(tmp_path: Path) -> None:
    pipeline = copy_pipeline_scripts(tmp_path)
    jar = add_spark_job_jar(pipeline)
    env = base_env(tmp_path)

    with listening_socket() as bootstrap:
        env["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
        result = run_script(pipeline / "run-streaming-job.sh", env)

    assert result.returncode == 0
    args = Path(env["SPARK_SUBMIT_LOG"]).read_text(encoding="utf-8").splitlines()
    assert "--class" in args
    assert "com.demo.task.UserEventStreamingJob" in args
    assert str(jar.relative_to(pipeline)) in args
```

Because `KAFKA_BOOTSTRAP_SERVERS` now points at the test's own socket, the test never contacts `localhost:9092` at all — it is hermetic by construction, whether or not the local stack is running.

- [ ] **Step 6: Run the whole pytest file to verify it passes**

Run: `/Users/linghuang/miniconda3/envs/llm/bin/python -m pytest integration-tests/test_service_scripts.py -v`

Expected: PASS, 14 tests.

- [ ] **Step 7: Repair the shell bootstrap test that this change breaks**

`test_run_streaming_job_topic_bootstrap.sh` drives the real script with the default `localhost:9092`, so the new preflight would abort it whenever no broker is listening. Give it a listener too.

In `recsys-pipeline/integration-tests/test_run_streaming_job_topic_bootstrap.sh`, after the `export RECORD=...` and `: > "$RECORD"` lines and before the invocation, insert:

```bash
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
```

Then change the invocation to pass it:

```bash
PATH="$tmp:$PATH" \
SPARK_HOME="$tmp/spark" \
KAFKA_BOOTSTRAP_SERVERS="$bootstrap" \
KAFKA_TOPIC="events_for_test" \
./run-streaming-job.sh
```

And update the first-line assertion, which hardcodes the old address:

```bash
[[ "$first_line" == "kafka-topics --bootstrap-server $bootstrap --create --if-not-exists --topic events_for_test" ]] ||
  { echo "FAIL: expected topic bootstrap before Spark, got: $first_line"; exit 1; }
```

Note the existing `trap` on the line after `tmp=` is replaced by the new one above, which also kills the listener. Do not leave two traps — the second would override the first and leak the temp directory.

- [ ] **Step 8: Run the shell test to verify it passes**

Run: `bash integration-tests/test_run_streaming_job_topic_bootstrap.sh`

Expected: `PASS`

- [ ] **Step 9: Run the full integration suite**

Run: `/Users/linghuang/miniconda3/envs/llm/bin/python -m pytest integration-tests -q`

Expected: all tests pass. Baseline before this work was 235 passed in about 4.5 minutes; expect 237 now (one new test in each of Tasks 1 and 2).

- [ ] **Step 10: Commit**

```bash
git add recsys-pipeline/run-streaming-job.sh \
        recsys-pipeline/integration-tests/test_service_scripts.py \
        recsys-pipeline/integration-tests/test_run_streaming_job_topic_bootstrap.sh
git commit -m "fix: fail fast when the Kafka broker is unreachable

The topic bootstrap ran under set -e before spark-submit, so an
unreachable broker blocked ~64s in the AdminClient retry loop and then
aborted the launch with a raw Java stack trace.

Preflight the broker with the bash /dev/tcp builtin and exit 1 with an
actionable message instead. /dev/tcp is the only bounded option here:
nc -w does not bound connect on macOS, curl telnet:// never returns on a
live port, and timeout is not installed.

The preflight cannot be stubbed on PATH, so both script tests now open a
real throwaway listener, which is hermetic without needing a broker."
```

---

## Verification against success criteria

After both tasks, confirm each criterion from the spec:

1. **Stack-down launch is fast and clear.** `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1 ./run-streaming-job.sh` exits 1 in under a second, names the address, suggests `docker compose up -d`, and prints no Java stack trace. Covered by `test_streaming_script_fails_fast_when_broker_unreachable`.
2. **Full integration suite passes.** Task 2, Step 9.
3. **The stack returns after a daemon restart.** Task 1, Step 5. Note `docker kill` is not a valid probe for this — Docker skips the restart policy for user-initiated stops.

## Known limitation carried forward

A remote host that silently drops SYN packets still waits on the OS TCP connect timeout, about 75 seconds. Bounding it needs `timeout`, which macOS does not ship. The motivating case — `localhost` with the stack down — returns in 6ms. This is recorded in the script comment and in the spec; do not attempt to fix it in this plan.
