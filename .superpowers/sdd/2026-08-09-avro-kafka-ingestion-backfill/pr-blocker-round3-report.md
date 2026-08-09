# PR Blocker Round 3 Report

## Status

The three final PR blockers are implemented. Redis sequence effects now validate the entire retained
namespace before mutation and atomically renew the target plus state, index, current ledger, and
every indexed retained ledger. A fixed-shape state carries an explicit monotonic watermark and
retained count, so missing/corrupt control state fails closed. The control namespace has a one-second
margin beyond target expiry and remains bounded to the configured retained batches after completion.

Durable Parquet consumers now use `DurableParquetCommit.readIdentity`, which resolves the exact
visible `query=<hash>/sink=<hash>` directory before Spark opens files, sets `basePath` to the
configured root, and applies an explicit expected payload schema. Repository durable-root readers
and documentation use this contract, and the shared-root regression uses two incompatible sink
schemas.

The optional archive checksum optimization was intentionally not attempted: it was not necessary to
close a correctness blocker, and retry validation remains unchanged.

## TDD Evidence

| Contract | RED evidence | GREEN evidence |
| --- | --- | --- |
| Identity-safe Parquet reader | Focused compilation failed because `DurableParquetCommit.readIdentity` did not exist. | `SinkSpec` passed 6/6 after adding the exact-path/basePath/explicit-schema contract; final combined focus passed 38/38. |
| Sequence expiry lifecycle | The four new real-Redis tests produced 4 intended failures: retained N-1/state expired across the old clock boundary, missing state did not throw, and a wrong-type retained ledger did not throw. | Final Redis focus preserves markers across N/N-1 and pre-completion clock boundaries and rejects both corrupt namespaces before target mutation. |
| Conservative retry horizon | The one-second-margin test failed after 1.2 seconds because state/index/ledger had expired with the target. | Target expires first; control state remains for the retry margin and subsequently expires. |
| Missing watermark corruption | Deleting `committed_batch` allowed an older effect and the expected exception was not thrown. | Effects create sentinel watermark `-1`; every live state requires an integer watermark before mutation. |

## Commit

This report is committed with the implementation. The exact immutable commit SHA is supplied in the
parent handoff because a commit cannot contain its own SHA.

## Independent Review

The required read-only completion review examined all seven changed repository files plus the
surrounding Redis, sequence-sink, execution-engine, archive, and Parquet reader call sites. Verdict:
**APPROVE / ready to merge**, with **0 critical, 0 important, and 0 minor findings**. The reviewer
confirmed validation precedes Lua mutation, duplicate effects renew without replay, completion
retains bounded N/N-1 state, identity selection precedes Parquet file indexing, and no mixed
durable-root reader remains.

## Exact Verification

| Command | Exit | Exact result |
| --- | ---: | --- |
| `cd recsys-pipeline/services/spark-streaming-job && sbt 'testOnly com.demo.engine.RedisSinkSpec com.demo.sequence.SequenceRedisSinkSpec com.demo.engine.SinkSpec com.demo.sequence.SequenceParquetSinkSpec'` | 0 | **38 succeeded, 0 failed, 0 canceled, 0 ignored, 0 pending**. Uses a real standalone `redis-server` and real Spark Parquet reads. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error test` | 0 | Fresh JUnit aggregate: **226 tests, 0 failures, 0 errors, 0 skipped**. |
| `cd recsys-pipeline && pytest -q integration-tests` | 0 | **311 passed, 1 skipped, 20 warnings** in 7.79s. The skip is the opt-in live Kafka round trip. |
| `cd recsys-pipeline && bash integration-tests/test_install_cron.sh` | 0 | **7 passed, 0 failed**. |
| `cd recsys-pipeline && bash integration-tests/test_retrain_pipeline.sh` | 0 | **PASS**; all dry-run pipeline steps verified. |
| `cd recsys-pipeline && bash integration-tests/test_run_data_pipeline.sh` | 1 sandbox / elevated unavailable | Sandbox stopped at `sock.bind(("127.0.0.1", 0))` with `PermissionError: [Errno 1] Operation not permitted`, then reported `FAIL: expected consumer status 41, got 1`. The required elevated rerun was rejected before execution because the approval service reported its usage limit exhausted until 2026-08-16. The parent agent will perform this root verification after the commit. |
| `cd recsys-pipeline && bash integration-tests/test_run_streaming_job_topic_bootstrap.sh` | 1 sandbox / elevated unavailable | Sandbox stopped at the same prohibited loopback bind and then could not read the uncreated temporary port file. The parent agent will perform the required root verification after the commit. |
| `cd recsys-pipeline && bash -n scripts/*.sh integration-tests/*.sh` | 0 | No output. |
| `git diff --check` and `git diff --cached --check` | 0 | No output. |
| Task 7 forbidden-pattern `rg` | 1 | No matches, the expected no-match status. |
| Mixed-root unsafe-reader `rg` | 1 | No configured-root-first Spark read or obsolete root-filter guidance matched. |
| Tracked-artifact scan | 1 | No tracked `.ua`, `kafka.png`, Parquet, replay-manifest, target, cache, environment, or credential-like artifact matched. |
| Independent read-only code review | approve | **0 critical, 0 important, 0 minor findings**; ready to merge. |

## Residual Concerns

- Redis guarantees remain for standalone Redis only. Dynamic retained-ledger validation is not a
  Redis Cluster cross-slot contract.
- Control hashes created by the immediately preceding, unreleased layout lack the new fixed-shape
  marker/count. They deliberately fail closed rather than silently reopening old effects; an
  operator upgrading a deployed intermediate build would need to let that namespace expire or
  perform an explicit reconciliation/reset.
- Kafka business delivery and archive replay remain at-least-once; consumers must deduplicate stable
  keys/event identities.
- Durable Parquet and archive commits still require atomic, non-overwriting directory rename
  semantics such as local filesystem or HDFS. Object-store rename is not claimed.
- The two loopback shell contracts need the parent agent's post-commit root verification because the
  approval service rejected this agent's required elevated rerun before execution.
