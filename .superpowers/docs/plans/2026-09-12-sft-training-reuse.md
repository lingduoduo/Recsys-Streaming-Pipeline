# SFT Training Reuse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove redundant Spark SFT computation and the null-date split crash while preserving training and artifact contracts.

**Architecture:** Bound holdout-date collection in Spark. Persist only training labels/features and evaluation labels/probabilities, reuse metric statistics and row counts, and release each owned cache through `finally`.

**Tech Stack:** Java 17, Scala 2.12.18, Spark ML/SQL 3.5.1, sbt, ScalaTest 3.2.18.

**Spec:** [SFT training and evaluation reuse](../specs/2026-09-12-sft-training-reuse-design.md)

## Global Constraints

- Use Java 17, Scala 2.12.18, and Spark 3.5.1; add no dependencies.
- Keep feature definitions, hash dimensions, label semantics, and algorithm hyperparameters unchanged.
- Keep the model artifact format, metrics JSON keys, environment variables, and offline-only behavior unchanged.
- Hold out the latest `max(1, holdoutDays)` distinct observed dates, not a calendar-day interval.
- Exclude null dates from both split outputs.
- Retain Spark evaluator default binning and the existing probability clipping and non-finite JSON handling.
- Release caches owned by this job on success and failure; retain caller-owned prediction caches.

## Execution state

Tasks 1 and 2 describe code and tests already developed during the investigation. Their checked
steps record observed work, including the failing baseline. Task 3 packages the result into the
requested spec, plan, and PR. Do not discard the existing implementation to replay completed steps.

All source paths are repository-relative. Run sbt commands from
`recsys-pipeline/services/spark-streaming-job/`. This work is on `perf/sft-training-reuse`, based on
`origin/master`; the existing checkout holds the implementation. Publish a PR against `master`.

### Task 1: Bound the temporal split and handle null dates

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: `splitByDate(df: DataFrame, holdoutDays: Int)` with a `date` column.
- Produces: `(DataFrame, DataFrame)` for training and validation; public signature unchanged.

- [x] **Step 1: Add the mixed-date regression fixture.**

```scala
val s = spark; import s.implicits._
val df = Seq(
  ("a", "2026-06-01"), ("b", "2026-06-10"), ("c", "2026-06-20"),
  ("d", "2026-06-20"), ("undated", null.asInstanceOf[String])
).toDF("id", "date")
val (train, valid) = CtrRankingModelTrainingJob.splitByDate(df, 2)
train.select("id").as[String].collect().toSet shouldBe Set("a")
valid.select("id").as[String].collect().toSet shouldBe Set("b", "c", "d")
```

- [x] **Step 2: Observe the baseline failure.**

Run `sbt 'testOnly com.demo.task.CtrRankingModelTrainingJobSpec'`.
Observed: the new mixed-null-date test threw `NullPointerException` in driver-side string sorting.
The eight pre-existing tests passed.

- [x] **Step 3: Replace driver sorting with a limited distributed date query.**

```scala
def splitByDate(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame) = {
  val holdout = df.select(col("date").cast("string").as("date"))
    .where(col("date").isNotNull).distinct()
    .orderBy(col("date").desc).limit(math.max(1, holdoutDays))
    .collect().map(_.getString(0)).toSeq
  val train = df.where(!col("date").cast("string").isin(holdout: _*))
  val valid = df.where(col("date").cast("string").isin(holdout: _*))
  (train, valid)
}
```

- [x] **Step 4: Verify mixed and all-null input.**

Keep the latest-date test and add this all-null characterization:

```scala
val s = spark; import s.implicits._
val df = Seq(("undated", null.asInstanceOf[String])).toDF("id", "date")
val (train, valid) = CtrRankingModelTrainingJob.splitByDate(df, 1)
train.count() shouldBe 0L
valid.count() shouldBe 0L
```

Run `sbt 'testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z "splitByDate"'`.
Expected: all split tests pass. The all-null case already passed after Step 3 and required no
additional production change.

### Task 2: Reuse training features and evaluation work

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`

**Interfaces:**
- Consumes: `evaluate(predictions: DataFrame)` with `ctr_label` and vector `probability` columns.
- Consumes: `trainModel(training: DataFrame, algorithm: String): Model[_]` and the split from Task 1.
- Produces: the existing `Map[String, Double]` from `evaluate` and `run`; public signatures unchanged.

- [x] **Step 1: Instrument real upstream prediction computation.**

```scala
val s = spark; import s.implicits._
val computed = spark.sparkContext.longAccumulator("ctr-predictions")
val preds = spark.sparkContext.parallelize(Seq(
  (1.0, Vectors.dense(0.2, 0.8)),
  (0.0, Vectors.dense(0.7, 0.3)),
  (1.0, Vectors.dense(0.4, 0.6)),
  (0.0, Vectors.dense(0.9, 0.1))
), 2).map { row => computed.add(1L); row }.toDF("ctr_label", "probability")
val cachedBefore = spark.sparkContext.getPersistentRDDs.keySet
val m = CtrRankingModelTrainingJob.evaluate(preds)
m("auc_roc") shouldBe (1.0 +- 1e-12)
m("pr_auc") shouldBe (1.0 +- 1e-12)
m("logloss") shouldBe (0.2990011586691898 +- 1e-12)
m("positive_rate") shouldBe (0.5 +- 1e-12)
computed.value shouldBe 4L
spark.sparkContext.getPersistentRDDs.keySet shouldBe cachedBefore
```

- [x] **Step 2: Observe the computation-count failure.**

Run `sbt 'testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z "compute each prediction"'`.
Observed on the original implementation: `16 was not equal to 4`; numerical metric assertions
passed before the accumulator assertion failed.

- [x] **Step 3: Persist the narrow prediction frame and share metrics.**

Add `import org.apache.spark.storage.StorageLevel`. Replace `evaluate` with:

```scala
def evaluate(predictions: DataFrame): Map[String, Double] = {
  val scores = predictions.select("ctr_label", "probability")
  val ownsCache = scores.storageLevel == StorageLevel.NONE
  if (ownsCache) scores.persist(StorageLevel.MEMORY_AND_DISK)
  try {
    val metrics = new BinaryClassificationEvaluator()
      .setLabelCol("ctr_label").setRawPredictionCol("probability").getMetrics(scores)
    val (auc, prauc) = try {
      (metrics.areaUnderROC(), metrics.areaUnderPR())
    } finally {
      metrics.unpersist()
    }
    val eps = 1e-15
    val posProb = udf { v: Vector => math.min(1.0 - eps, math.max(eps, v(1))) }
    val summary = scores.withColumn("p", posProb(col("probability")))
      .select(
        mean(-(col("ctr_label") * log(col("p")) +
          (lit(1.0) - col("ctr_label")) * log(lit(1.0) - col("p")))).as("ll"),
        mean(col("ctr_label")).as("positive_rate"))
      .first()
    Map("auc_roc" -> auc, "pr_auc" -> prauc,
      "logloss" -> summary.getDouble(0), "positive_rate" -> summary.getDouble(1))
  } finally {
    if (ownsCache) scores.unpersist()
  }
}
```

- [x] **Step 4: Give training features a bounded lifetime.**

Add `import scala.language.existentials` for the fitted-model/count tuple. After constructing
`raw` in `run`, replace preparation through metric calculation with:

```scala
val (trainRaw, validRaw) = splitByDate(raw, holdoutDays)
val validationRows = validRaw.count()
val training = assembleFeatures(labelColumn(trainRaw, labelMode), numFeatures)
  .select("ctr_label", "features").persist(StorageLevel.MEMORY_AND_DISK)
val (model, trainingRows): (Model[_], Long) = try {
  val trainingRows = training.count()
  require(trainingRows > 0 && validationRows > 0,
    s"Not enough distinct dates in $inputPath to form a train/validation split with holdoutDays=$holdoutDays")
  (trainModel(training, algorithm), trainingRows)
} finally {
  training.unpersist()
}
val validation = assembleFeatures(labelColumn(validRaw, labelMode), numFeatures)
  .select("ctr_label", "features")
val metrics = evaluate(model.transform(validation)) ++ Map(
  "train_rows" -> trainingRows.toDouble,
  "val_rows" -> validationRows.toDouble
)
```

Keep the existing model save, `writeMetrics`, logging, and returned map after this block.

- [x] **Step 5: Verify metric parity and cache ownership.**

The checked-in test file contains executable fixtures for these boundaries:

| Fixture | Required assertions |
|---|---|
| Labels `1,0,1,0` and probabilities `0.9,0.5,0.5,0.1` | ROC/PR equal separate Spark evaluators within `1e-12`. |
| Labels `1,1,1,1` with the same probabilities | ROC/PR equal separate Spark evaluators within `1e-12`. |
| String `probability = "invalid"` | `IllegalArgumentException`; no projection cache or new persistent RDD remains. |
| A pre-cached two-row prediction frame | Its cache remains registered after evaluation; the test releases it in `finally`. |
| Existing four-row Parquet integration fixture, run for both `logreg` and `gbt` | Model and metrics exist; `train_rows == 2.0`, `val_rows == 2.0`; persistent RDD IDs return to the baseline. |
| Same Parquet with `holdoutDays = 2` | Empty training split fails; cache manager is empty afterward. |

For example, the malformed-schema boundary executes the real evaluator:

```scala
val s = spark; import s.implicits._
val preds = Seq((1.0, "invalid")).toDF("ctr_label", "probability")
val cachedBefore = spark.sparkContext.getPersistentRDDs.keySet
intercept[IllegalArgumentException] {
  CtrRankingModelTrainingJob.evaluate(preds)
}
spark.sharedState.cacheManager.lookupCachedData(preds.select("ctr_label", "probability")) shouldBe None
spark.sparkContext.getPersistentRDDs.keySet shouldBe cachedBefore
```

Run `sbt 'testOnly com.demo.task.CtrRankingModelTrainingJobSpec'`.
Expected: 14 tests pass, including the unchanged non-finite JSON regression.

- [x] **Step 6: Document the execution behavior.**

In `Data_Pipeline.md`, under `CtrRankingModelTrainingJob`, describe the SFT role, latest observed
dates, null exclusion, limited driver collection, narrow memory/disk caches, shared ROC/PR
statistics, combined scalar aggregation, and cache ownership. Keep the offline-only statement.

### Task 3: Verify and publish the complete change

**Files:**
- Create: `.superpowers/docs/specs/2026-09-12-sft-training-reuse-design.md`
- Create: `.superpowers/docs/plans/2026-09-12-sft-training-reuse.md`
- Review: the three implementation/test/documentation files listed in Tasks 1 and 2.

**Interfaces:**
- Consumes: the final implementation and regression tests from Tasks 1 and 2.
- Produces: reviewed commits on `perf/sft-training-reuse` and a PR targeting `master`.

- [x] **Step 1: Write and self-review the spec and plan.**

Check that every acceptance criterion maps to a test or verification step, public signatures
match the implementation, and the performance claim is computation count rather than throughput.

- [x] **Step 2: Verify the full Spark module.**

```bash
cd recsys-pipeline/services/spark-streaming-job
sbt test
```

Expected: all module tests pass. If unrelated failures appear, reproduce them against the PR
base in an isolated location, record the evidence, and keep the SFT changes scoped.

Observed on 2026-09-12: **423 tests passed, 0 failures** with `sbt test`.

- [x] **Step 3: Review the final change and check whitespace.**

Use `superpowers:requesting-code-review` for code/spec consistency review. Run `git diff --check`
and inspect the staged paths. Include only the five files named in this plan.

Observed: independent code/spec review returned no substantive findings; `git diff --check` passed.

- [ ] **Step 4: Commit the design record and implementation.**

From the repository root:

```bash
git add .superpowers/docs/specs/2026-09-12-sft-training-reuse-design.md .superpowers/docs/plans/2026-09-12-sft-training-reuse.md
git commit -m "docs: specify SFT training reuse and verification plan"
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "perf: reuse SFT training features and evaluation statistics"
```

- [ ] **Step 5: Push and create the requested PR.**

Push `perf/sft-training-reuse` to `origin` and use `gh pr create --base master --head perf/sft-training-reuse`.
Write the PR body to a temporary file and use `--body-file`; include the problem, changes,
16-to-4 fixture result, actual test results, and links to the spec and plan. Verify the remote
PR contains both documentation and code, then report its URL. Leave the branch available for review.

## Coverage self-review

- Temporal semantics, bounded driver collection, and null handling: Task 1.
- Metric equivalence, preserved binning/clipping, and inference reuse: Task 2 Steps 1–3 and 5.
- Training persistence, reused counts, both classifiers, and cleanup: Task 2 Steps 4–5.
- Existing feature/label behavior and non-finite JSON: the existing tests retained in Task 2 Step 5.
- User-facing documentation and benchmark limitations: Task 2 Step 6 and the spec.
- Full module verification, review, commits, and PR delivery: Task 3.
