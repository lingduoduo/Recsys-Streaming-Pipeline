# PR Blocker Fix Report

## Status

All six requested PR blockers are resolved and the required Python, shell, Scala, static, syntax,
and tracked-artifact checks are green.

## Corrections

1. Replay now traverses sorted committed Parquet paths, numeric row groups, and physical row
   indexes. Its manifest retains both the acknowledgement count and the durable
   `acknowledged_position` identity. Resume locates that physical cursor rather than treating an
   Arrow scanner ordinal as stable.
2. Raw archive data/state commits use a version-2 manifest with an exact Parquet inventory and row
   count. A zero-row batch is committed as `_SUCCESS`, `row_count=0`, and no file entries. Replay
   validates identity, inventory, and Parquet metadata, skips a valid empty batch, and rejects
   absent or contradictory zero-row metadata.
3. Both Redis Lua scripts validate ledger and target key types plus numeric arguments before any
   write, run the complete business mutation first, and write the batch ledger marker last. Real
   loopback Redis tests prove wrong-type popularity/sequence targets and wrong-type ledgers cannot
   leave a ledger-only success marker or a partial business effect.
4. Redis uses one deterministic hash per item/sequence effect, with batch IDs as fields. Applying a
   later batch conservatively prunes fields outside `REDIS_LEDGER_RETENTION_BATCHES`; the setting is
   configurable, floored at two, and therefore retains both N and N-1 retry state. No TTL shorter
   than the recovery horizon is used.
5. Durable Parquet commits use visible `query=<hash>/sink=<hash>/batch=<id>` partitions. Regression
   tests read the configured sink root directly with Spark and assert the committed schema/rows.
   The README and data-pipeline guide document this reader contract.
6. Replay manifests use `updated_at` on every write. `completed_at` is null for running/failed
   states and populated only for completed operations.

## TDD evidence

| Finding | RED | GREEN |
| --- | --- | --- |
| Physical replay cursor | Resume after reversed dataset order sent `first-1` again and skipped unsent `second-1`. | Replay focused suite: **30 passed**; manifest cursor is exact path/row-group/row. |
| Zero-row archive commits | Version-2 zero-row batch was rejected as a commit-identity mismatch. | Scala empty-archive regression and Python replay/negative metadata cases pass. |
| Manifest timestamps | Running/failed JSON raised `KeyError: updated_at`; `completed_at` was populated. | Running/failed/completed timestamp regressions pass in the 30-test replay suite. |
| Redis lifecycle/contracts | Focused Scala RED failed compilation with **6 errors** because per-effect keys/window APIs did not exist. | Real Redis and lifecycle specs pass: **6 tests**, including both scripts' wrong-type paths and N/N-1 retention. |
| Root-readable Parquet | Tests requested root-visible rows/partition columns before the visible layout existed. | Sink and sequence Parquet focused specs pass from both committed leaf and configured root. |
| Combined focused Scala | New contracts initially failed compilation as above. | Five focused specs: **43 passed**, followed by the expanded 6-test Redis contract rerun. |

Initial focused Python RED was **4 failed, 3 passed, 23 deselected**. The failures were the intended
reversed-order cursor, missing timestamps, and zero-row protocol behaviors.

## Fresh verification

| Command | Exit | Exact result |
| --- | ---: | --- |
| Required Task 7 Python pytest list | 0 | **104 passed, 1 skipped, 0 failed** in 2.01s. The skip is the opt-in real Kafka integration because `RUN_KAFKA_INTEGRATION` was not enabled. |
| `bash integration-tests/test_run_streaming_job_topic_bootstrap.sh` | 0 | **PASS**. |
| `sbt -error test` | 0 | Fresh JUnit totals: **214 tests, 0 failures, 0 errors, 0 skipped**. |
| `git diff --check` | 0 | No output. |
| Task 7 forbidden-pattern `rg` | 1 | No matches (expected no-match status). |
| `bash -n` on replay/bootstrap scripts | 0 | No output. |
| Tracked artifact scan | 0 wrapper / no matches | No tracked `.ua`, `kafka.png`, environment, Parquet, replay-manifest, target, cache, or credential-like artifact matched. |

The real opt-in Kafka round trip was not run, as required when the environment flag is absent.

## Residual guarantees and limitations

- Kafka business delivery and archive replay remain at-least-once. Broker acknowledgement can
  precede filesystem completion/cursor persistence; downstream stable-key/event-ID deduplication
  remains required.
- Archive and Parquet directory commits still require a filesystem with atomic, non-overwriting
  rename semantics (local filesystem/HDFS contract). Object-store rename is not claimed.
- Redis guarantees cover standalone Redis only. Redis Cluster slotting is not supported.
- The bounded Redis window assumes normal forward recovery from the configured Structured
  Streaming checkpoint. Deliberately rewinding/replacing a checkpoint beyond the retained window
  can reapply old effects and requires an operator-chosen larger window or separate reconciliation.
