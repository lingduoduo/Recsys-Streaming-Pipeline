# Optimize SFT Training Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the standalone validation-count scan from SFT training while preserving model behavior.

**Architecture:** Aggregate row counts during observed-date discovery, collect only selected dates and counts, and return the holdout count alongside the lazy splits. Keep the public two-result split method as a compatibility wrapper.

**Tech Stack:** Scala 2.12.18, Spark 3.5.1, ScalaTest 3.2.18, sbt.

**Spec:** [Optimize SFT training design](../specs/2026-09-14-optimize-sft-training-design.md)

## Global Constraints

- Use Scala 2.12.18 and Spark 3.5.1 with no new dependencies.
- Preserve the latest `max(1, holdoutDays)` observed date strings as the holdout; gaps between dates do not change selection.
- Exclude null dates from both splits and from the holdout count.
- Preserve `splitByDate(DataFrame, Int): (DataFrame, DataFrame)` for existing callers.
- Keep row counts as `Long` until the existing metrics conversion to `Double`.
- Preserve input eligibility filters, feature assembly, labels, model hyperparameters, metric names, model serialization, and cache ownership.
- Collect at most `max(1, holdoutDays)` date/count records to the driver.

## Execution context

The implementation and focused red/green verification were completed earlier in this session. Checked steps below record that evidence; remaining steps govern review and PR delivery. Work is on `optimize/sft-holdout-count`, based on `origin/master`, in the existing checkout.

### Task 1: Combine holdout discovery and counting

**Files:**
- Modify: `recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`

**Interfaces:**
- Consumes: eligible input `DataFrame` with a date column and `holdoutDays: Int`.
- Produces: package-visible `splitByDateWithCount(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame, Long)`; existing `splitByDate` remains compatible.

- [x] **Step 1: Add regression tests that detect a second input pass and incorrect boundary counts.** Add these cases to `CtrRankingModelTrainingJobSpec`:

```scala
  it should "count the holdout while selecting dates in a single input pass" in {
    val s = spark; import s.implicits._
    val scanned = spark.sparkContext.longAccumulator("ctr-split-rows")
    val df = spark.sparkContext.parallelize(Seq(
      ("a", "2026-06-01"), ("b", "2026-06-10"),
      ("c", "2026-06-20"), ("d", "2026-06-20"),
      ("undated", null.asInstanceOf[String])
    ), 2).map { row => scanned.add(1L); row }.toDF("id", "date")

    val (train, valid, validationRows) =
      CtrRankingModelTrainingJob.splitByDateWithCount(df, holdoutDays = 2)

    validationRows shouldBe 3L
    scanned.value shouldBe 5L
    train.select("id").as[String].collect().toSet shouldBe Set("a")
    valid.select("id").as[String].collect().toSet shouldBe Set("b", "c", "d")
  }

  it should "count empty and minimum-sized holdouts consistently" in {
    val s = spark; import s.implicits._
    val undated = Seq(("a", null.asInstanceOf[String])).toDF("id", "date")
    CtrRankingModelTrainingJob.splitByDateWithCount(undated, 1)._3 shouldBe 0L
    CtrRankingModelTrainingJob.splitByDateWithCount(undated.limit(0), 1)._3 shouldBe 0L
    val dated = Seq(("a", "2026-06-01"), ("b", "2026-06-02"), ("c", "2026-06-02"))
      .toDF("id", "date")
    CtrRankingModelTrainingJob.splitByDateWithCount(dated, 0)._3 shouldBe 2L
    CtrRankingModelTrainingJob.splitByDateWithCount(dated, 10)._3 shouldBe 3L
  }
```

- [x] **Step 2: Establish the failing performance baseline.** Temporarily implement the new helper by calling the original `splitByDate` and returning `(train, valid, valid.count())`. Run from `recsys-pipeline/services/spark-streaming-job`:

```bash
sbt 'testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z "single input pass"'
```

Observed: the test failed with `10 was not equal to 5`, demonstrating the repeated scan. An earlier compile attempt reported the missing helper; the runtime failure is the regression evidence.

- [x] **Step 3: Implement grouped date counts and integrate the result into `run`.** Replace date splitting with:

```scala
  def splitByDate(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame) = {
    val (train, valid, _) = splitByDateWithCount(df, holdoutDays)
    (train, valid)
  }

  private[task] def splitByDateWithCount(
      df: DataFrame, holdoutDays: Int
  ): (DataFrame, DataFrame, Long) = {
    // Count during date discovery to avoid scanning the holdout again just for its size.
    // Only the latest observed dates and their counts reach the driver.
    val holdoutCounts = df.select(col("date").cast("string").as("date"))
      .where(col("date").isNotNull).groupBy("date").count()
      .orderBy(col("date").desc).limit(math.max(1, holdoutDays))
      .collect()
    val holdout = holdoutCounts.map(_.getString(0)).toSeq
    val validationRows = holdoutCounts.map(_.getLong(1)).sum
    val train = df.where(!col("date").cast("string").isin(holdout: _*))
    val valid = df.where(col("date").cast("string").isin(holdout: _*))
    (train, valid, validationRows)
  }
```

In `run`, replace the split and standalone `validRaw.count()` with:

```scala
val (trainRaw, validRaw, validationRows) = splitByDateWithCount(raw, holdoutDays)
```

- [x] **Step 4: Update the SFT architecture paragraph.** Explain that date discovery counts rows per date, only selected dates/counts reach the driver, and no separate holdout-count scan is needed.

- [x] **Step 5: Verify the focused suite.**

```bash
cd recsys-pipeline/services/spark-streaming-job
sbt 'testOnly com.demo.task.CtrRankingModelTrainingJobSpec'
```

Observed: 16 tests passed, including logistic regression, GBT, split boundaries, metrics, and cache cleanup.

### Task 2: Review and publish the implementation

**Files:** All Task 1 files plus this plan and its linked spec.

**Interfaces:** Consumes the tested Task 1 diff; produces a reviewed commit and a PR against `master`.

- [x] **Step 1: Request a read-only code review.** Check the diff against every spec constraint; resolve substantive findings before publishing.
- [ ] **Step 2: Run the Spark module suite and whitespace validation.** From the Spark module run `sbt test`; from the repository root run `git diff --check`. Record the outcome below and in the PR.
- [ ] **Step 3: Publish the design for review.** Commit the spec and plan with `docs: specify SFT holdout counting optimization`, push `optimize/sft-holdout-count` to `origin`, and create a draft PR against `master` titled `Optimize SFT holdout counting`. Supply the prepared PR description with `gh pr create --draft --body-file`.
- [ ] **Step 4: Add verified code to the PR.** Commit the implementation, tests, architecture documentation, and final plan evidence with `perf: reuse date counts in SFT training`. Push the branch, update the PR description with test evidence, and mark it ready for review.

## Delivery evidence

Focused regression baseline: 1 expected runtime failure (`10 was not equal to 5`). Focused final suite: 16 passed. Independent read-only review: no actionable findings; spec and implementation are aligned. Module-suite outcome will be recorded after completion.
