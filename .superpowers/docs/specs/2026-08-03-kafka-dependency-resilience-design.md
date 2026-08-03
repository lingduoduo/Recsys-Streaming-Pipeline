# Kafka dependency resilience

Date: 2026-08-03

## Problem

Two independent failures share one theme: the local stack depends on Kafka, and
neither the stack nor the launch script handles Kafka being absent.

### 1. Kafka dies silently and stays dead

After a Docker VM restart, `docker compose up -d` reported success but Kafka had
already exited:

```
kafka       Exited (1) 33 minutes ago
redis       Up 33 minutes (healthy)
zookeeper   Up 33 minutes (healthy)
```

```
ERROR Exiting Kafka due to fatal exception during startup. (kafka.Kafka$)
org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists
	at kafka.zk.KafkaZkClient.registerBroker(KafkaZkClient.scala:106)
```

ZooKeeper kept its data directory across the restart, so the ephemeral znode
`/brokers/ids/1` from Kafka's pre-restart session was still present. Kafka could
not re-register `KAFKA_BROKER_ID: 1` and exited 1. `docker-compose.yml` declares
no `restart:` policy, so nothing brought it back. Redis and ZooKeeper both stayed
healthy, which masked the outage.

The stack stayed broken for 33 minutes. Downstream, every recommendation was
cold-start and `run-retrain.sh` found no training data.

### 2. `run-streaming-job.sh` hangs 64s, then aborts

`run-streaming-job.sh` bootstraps its Kafka input topic before `spark-submit`.
With no broker reachable, `kafka-topics --create` blocks about 64 seconds, fails,
and `set -euo pipefail` aborts the script before Spark ever launches. The user
sees a raw Java stack trace.

Measured directly, varying only whether `kafka-topics` was on PATH:

| PATH | Result |
| --- | --- |
| without `kafka-topics` | exit 0 in 0.26s |
| with `kafka-topics`, broker down | exit 1 in 64s, `TimeoutException` |

## Goals

- Kafka returns on its own after a crash or VM restart.
- A launch with no reachable broker fails in under a second with an actionable
  message instead of 64 seconds and a stack trace.
- No test depends on the developer's machine having a broker.

## Non-goals

- Wiring `test_run_streaming_job_topic_bootstrap.sh` into a runner. It is
  invoked by no CI config, script, or pytest collection today. That stays open.
- Seeding the empty pipeline with events or training data. Operational, not a
  code defect.
- Any change to how the recommendation service scores or falls back.

## Design

### Component 1: restart policy

Add `restart: unless-stopped` to `zookeeper`, `kafka`, and `redis` in
`recsys-pipeline/docker-compose.yml`.

This fixes the observed failure directly. ZooKeeper negotiated an 18–40s session
timeout (both values appear in the broker logs), so the stale ephemeral znode
expires shortly after the failed start. Docker restarts with exponential backoff,
so a later attempt registers cleanly — which is exactly why manually starting the
container 33 minutes later worked with no other intervention. The policy also
starts containers when the Docker daemon starts after a VM restart.

Rejected alternatives:

- **Fix the registration race at its source** (dynamic broker id, or a preflight
  that deletes the stale znode). More moving parts, and it addresses only this
  one crash cause. A restart policy covers any transient startup failure.
- **Rely on the healthcheck and an operator noticing.** This is the status quo
  that produced a 33-minute silent outage.

Accepted trade-off: a genuinely misconfigured broker now restart-loops instead of
failing once. Docker's backoff makes this cheap, and a `Restarting` status is
more visible than an `Exited` line that has scrolled out of view.

### Component 2: fail-fast preflight

Inside the existing `MAIN_CLASS == com.demo.task.UserEventStreamingJob` guard in
`recsys-pipeline/run-streaming-job.sh`, check reachability before bootstrapping:

```bash
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
```

The existing `kafka-topics` and `docker compose exec` bootstrap branches then run
unchanged, now guaranteed a live broker, so the 64s AdminClient retry loop cannot
occur. The check stays inside the existing guard because only that job's path
needs Kafka.

Bash's built-in `/dev/tcp` was chosen after measuring the alternatives on macOS:

| Method | Refused port | Live port | Unroutable host |
| --- | --- | --- | --- |
| `bash /dev/tcp` | 0.006s, exit 1 | 0.005s, exit 0 | OS TCP timeout |
| `nc -z -w2` | 0.006s, exit 1 | 0.009s, exit 0 | **75s** (`-w` does not bound connect) |
| `curl --connect-timeout 2 telnet://` | 0.018s, exit 7 | **hangs forever** | n/a |
| `timeout` / `gtimeout` | not installed on macOS | | |

`/dev/tcp` is the only option that is both fast on a refused port and correct on a
live one, and it needs no external dependency.

**Known limitation:** a remote host that silently drops SYN packets still waits on
the OS TCP connect timeout (about 75s). Bounding that requires `timeout`, which
macOS does not ship. The case that motivated this work — `localhost` with the
stack down — returns in 6ms. This limitation is documented rather than hidden.

## Testing

The preflight uses a bash builtin, so it cannot be stubbed on PATH. Tests use
real throwaway TCP sockets instead, which is both more honest than a stub and
still hermetic.

1. **New test — unreachable broker.** Run with
   `KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:1`. Assert exit 1, under one second, and
   that stderr names the address. Asserts no Java stack trace.
   The test must add the job jar first: the preflight runs after the
   `spark-submit` and jar checks, so without a jar the script exits 127 and the
   test would pass for the wrong reason.
2. **`test_streaming_script_uses_consolidated_spark_service_path`.** The test
   opens a throwaway TCP listener and passes its `host:port` as
   `KAFKA_BOOTSTRAP_SERVERS`, so the preflight passes. The no-op `kafka-topics`
   stub already added in PR #159 still covers the bootstrap call itself.
3. **`test_run_streaming_job_topic_bootstrap.sh`.** Update it to point at a
   throwaway listener as well. This change would otherwise silently break it.
   It remains unwired from any runner, per non-goals.

## Success criteria

- `./run-streaming-job.sh` with the stack down exits 1 in under one second with a
  message naming the address and the fix, and no Java stack trace.
- The full integration suite passes.
- After a Docker daemon restart (`colima stop && colima start`), all three
  services return to `Up` with no manual `docker compose up`.

  `docker kill` is **not** a valid probe for this. Docker deliberately skips the
  restart policy for user-initiated stops; measured on this host, a killed
  container stays `Exited` with `RestartCount=0` for 60s+. The policy covers
  container-initiated exits (such as the broker registration failure that caused
  the incident) and daemon start.

## Branching

Component 2 modifies `integration-tests/test_service_scripts.py`, which PR #159
also touched. #159 merged on 2026-08-03, so this work branches from `master`
(`design/kafka-dependency-resilience`) with no conflict to manage.
