# PR Blocker Round 2 Report

## Status

All five requested PR blockers are resolved. Redis ledger key/field cardinality is bounded by the
retained batches and their effects; its committed-batch watermark is monotonic and prunes only
after completion. Archive protocol v1 is a documented hard cutover with explicit regeneration
errors in both replay and Spark retry validation. Parquet control fields are reserved and shared
roots require three-field identity filtering. Version-2 archive inventories carry and validate
path, size, and SHA-256, and archive count/write uses one reliably unpersisted materialization.

## Commit

`HEAD` — the implementation, tests, documentation, and this report are committed together; the
exact immutable SHA is supplied in the parent handoff.

## Exact Verification

| Command | Exit | Exact result |
| --- | ---: | --- |
| `cd recsys-pipeline && pytest -q integration-tests/python_modeling/test_archive_replay.py` | 0 | **32 passed** in 0.89s. |
| `cd recsys-pipeline && pytest -q integration-tests` | 0 | **311 passed, 1 skipped, 20 warnings** in 8.36s. The skip is the opt-in real Kafka round trip. |
| `cd recsys-pipeline && bash integration-tests/test_install_cron.sh` | 0 | **7 passed, 0 failed**. |
| `cd recsys-pipeline && bash integration-tests/test_retrain_pipeline.sh` | 0 | **PASS**, all five dry-run steps verified. |
| `cd recsys-pipeline && bash integration-tests/test_run_data_pipeline.sh` | 0 | **PASS** after the required loopback-permission rerun. |
| `cd recsys-pipeline && bash integration-tests/test_run_streaming_job_topic_bootstrap.sh` | 0 | **PASS** after the required loopback-permission rerun. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error "testOnly com.demo.engine.RedisSinkSpec com.demo.sequence.SequenceRedisSinkSpec"` | 0 | Focused standalone-Redis contracts GREEN. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error 'testOnly com.demo.engine.RedisSinkSpec -- -z "complete sequence ledger namespace"'` | 0 | Full sequence target/ledger/index/watermark expiry contract GREEN. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error 'testOnly com.demo.engine.RawArchiveSinkSpec -- -z "archive lineage" -z "materialize a nondeterministic source once"'` | 0 | Enhanced inventory and single-materialization contracts GREEN. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error 'testOnly com.demo.engine.RawArchiveSinkSpec -- -z "pre-release v1 commits"'` | 0 | Spark-side v1 hard-cutover error contract GREEN. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error 'testOnly com.demo.engine.SinkSpec -- -z "visible identity partition columns" -z "shared-root reads"'` | 0 | Reserved-column and shared-root filter contracts GREEN. |
| `cd recsys-pipeline/services/spark-streaming-job && sbt -error test` | 0 | **221 succeeded, 0 failed, 0 canceled, 0 ignored, 0 pending**. |
| `git diff --check` | 0 | No output. |
| Task 7 forbidden-pattern `rg` | 1 | No matches, the expected no-match status. |
| `bash -n scripts/*.sh integration-tests/*.sh` | 0 | No output. |
| Tracked-artifact scan | 1 | No tracked `.ua`, `kafka.png`, Parquet, replay-manifest, target, cache, environment, or credential-like artifact matched. |

## Residual Concerns

- Kafka business output and archive replay remain at-least-once. Consumers must deduplicate the
  stable business key or retained event identity.
- Redis guarantees cover standalone Redis only. Redis Cluster slotting is not supported. A
  deliberate checkpoint rewind below the retained recovery horizon is fenced and skipped rather
  than reapplying effects; operators must reconcile such an exceptional rewind separately.
- Archive and durable Parquet commits require atomic, non-overwriting directory rename semantics
  such as local filesystem or HDFS. Object-store rename is not supported.
- Pre-release archive protocol v1 has no migration or compatibility path; its data must be
  regenerated as v2 before replay or retry validation.
- The opt-in live Kafka round trip was not run because `RUN_KAFKA_INTEGRATION=1` was not enabled;
  its test skipped cleanly in the full Python suite.
