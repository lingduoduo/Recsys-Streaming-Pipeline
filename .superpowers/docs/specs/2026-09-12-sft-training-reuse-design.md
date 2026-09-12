# SFT training and evaluation reuse

**Date:** 2026-09-12
**Status:** Implemented and verified; preparing PR

## Problem and scope

`CtrRankingModelTrainingJob` is the repository's consolidated supervised ranker (SFT), as
established by [the consolidation design](2026-08-25-consolidate-sft-design.md). It reads
date-partitioned training-sample Parquet, hashes categorical and text features, fits logistic
regression or gradient-boosted trees, and writes a Spark ML model and metrics JSON. The model
is an offline artifact; this change does not add serving integration.

The implementation repeats work across Spark actions:

- ROC-AUC and PR-AUC use separate evaluators, rebuilding the same cumulative ranking statistics.
- Logloss and positive rate use separate aggregations over uncached predictions.
- Training features are not persisted between the job's preparation, fitting, and reporting actions.
- The temporal split collects every distinct date and sorts it on the driver. A null date mixed
  with valid dates throws `NullPointerException` during that sort.

Investigation reproduced the evaluation cost with a two-partition, four-row fixture: an
accumulator in the upstream prediction computation counted **16 row computations**. The
optimized path counts **4**, while preserving all four metric values. This measures redundant
computation on a local fixture, not end-to-end throughput or production speedup.

This document records the implementation investigated and developed in this session. It is
being added with the implementation plan and code at the user's request.

## Global constraints

- Use Java 17, Scala 2.12.18, and Spark 3.5.1; add no dependencies.
- Keep feature definitions, hash dimensions, label semantics, and algorithm hyperparameters unchanged.
- Keep the model artifact format, metrics JSON keys, environment variables, and offline-only behavior unchanged.
- Hold out the latest `max(1, holdoutDays)` distinct observed dates, not a calendar-day interval.
- Exclude null dates from both split outputs.
- Retain Spark evaluator default binning and the existing probability clipping and non-finite JSON handling.
- Release caches owned by this job on success and failure; retain caller-owned prediction caches.

## Alternatives and decision

| Approach | Tradeoff |
|---|---|
| Cache the entire featured input | Simple reuse, but retains unused maps, identifiers, text arrays, and intermediate vectors during fitting and evaluation. |
| Cache narrow training and prediction frames; share metric statistics | Selected: reuses expensive work while limiting retained columns and giving each cache a short, explicit lifetime. |
| Change algorithms, hashing, or feature engineering | Could affect cost and quality, but requires a separate quality study and would change the training contract. |

## Design

### Temporal split

Project `date` as a string, remove nulls, take distinct dates, sort descending in Spark, and
limit to `max(1, holdoutDays)` before collecting. The driver receives at most the requested
holdout size. The distributed distinct operation still considers the available dates; this is
not a claim that partition discovery or source scanning becomes constant-time.

Use the collected dates in the existing membership filters. Duplicate rows on a holdout date
remain together. Gaps between dates do not change the number of observed dates held out.
All-null input produces empty training and validation outputs. `run` continues to reject
an empty training or validation split with its existing diagnostic.

### Training lifetime

Split the raw filtered input before feature assembly. Count the raw validation frame once.
Assemble training features, project only `ctr_label` and `features`, and persist with
`MEMORY_AND_DISK`. Count the persisted training frame once to materialize it, validate that
both splits contain rows, and reuse the count in `train_rows`.

Fit the unchanged classifier inside `try/finally`. Return the fitted model and count, then
unpersist the training frame before evaluation starts. This avoids deliberately retaining
training features throughout evaluation; Spark's unpersist cleanup is asynchronous by default.

### Evaluation lifetime

Project only `ctr_label` and `probability`. If this projection is already cached, retain the
existing ownership; otherwise persist it with `MEMORY_AND_DISK` for the duration of evaluation.

Use one `BinaryClassificationEvaluator.getMetrics` instance for ROC-AUC and PR-AUC. This
preserves the evaluator's defaults while allowing both curves to reuse cumulative counts.
Release its intermediate cache in `finally`.

Compute logloss and positive rate in one aggregate over the cached projection, preserving
the current `1e-15` clipping UDF. Release the projection in an outer `finally` only if this
invocation created its cache. Schema validation errors must also take this cleanup path.

Spark 3.5.1 exposes the shared metric object through its
[evaluator implementation](https://github.com/apache/spark/blob/v3.5.1/mllib/src/main/scala/org/apache/spark/ml/evaluation/BinaryClassificationEvaluator.scala).

## Files

Paths below are relative to the repository root.

| File | Responsibility |
|---|---|
| `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala` | Temporal split, cache lifetimes, shared evaluation, reused row counts. |
| `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala` | Computation-count regression, metric parity, date edges, both classifiers, and cache ownership/cleanup. |
| `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md` | SFT identity, observed-date semantics, and execution/cache behavior. |
| `.superpowers/docs/plans/2026-09-12-sft-training-reuse.md` | Reproducible implementation and verification steps. |

## Acceptance criteria

1. The four-row evaluation fixture performs four upstream row computations and returns
   ROC-AUC `1.0`, PR-AUC `1.0`, logloss `0.2990011586691898`, and positive rate `0.5` within `1e-12`.
2. ROC/PR results match separate Spark evaluators for tied scores and an all-positive holdout.
3. Evaluation leaves no owned cache after success or invalid probability-schema failure,
   and retains a prediction cache supplied by the caller.
4. Temporal tests cover the latest date, multiple observed dates with gaps and duplicates,
   mixed null dates, and all-null dates. Undated rows do not enter either output.
5. Both `logreg` and `gbt` write a model and metrics; the integration fixture reports two
   training rows and two validation rows. An oversized holdout fails without leaving a cache.
6. Existing label, feature fallback, feature-size, and non-finite metrics JSON tests pass.
7. Run the full Spark module suite before publishing the PR; report any unrelated failures
   with baseline evidence rather than claiming a green suite.

## Risks and limits

Persistence trades memory and possible disk I/O for less recomputation. Only narrow frames are
retained, and training/evaluation lifetimes are separated, but production-scale memory and
throughput have not been benchmarked. Spark retries or cache eviction can legitimately cause
recomputation; the accumulator assertion describes the deterministic local regression fixture.

The source store is assumed stable during a batch run, as before. This change does not add
snapshot isolation. It also does not introduce new input validation, model selection, or a
feature-transform serving pipeline. Rollback consists of reverting this change; no data or
artifact migration is required.

## Verification record

- `sbt test`: 423 tests passed, 0 failures, on 2026-09-12 with Java 17 and Spark 3.5.1.
- Independent code/spec review: no substantive findings.
- Regression evidence: four prediction rows caused 16 upstream computations before the change
  and 4 afterward; the mixed-null-date fixture failed before the fix and passed afterward.
