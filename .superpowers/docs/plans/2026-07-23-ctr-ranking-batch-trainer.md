# CTR/Ranking Batch Trainer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an offline-only Spark ML batch job that reads the Parquet training-samples store and trains + evaluates a CTR/ranking model.

**Architecture:** A new Scala object `com.demo.task.CtrRankingModelTrainingJob` with small pure methods (`labelColumn`, `assembleFeatures`, `splitByDate`, `evaluate`, `trainModel`, `run`) so each is unit-testable on an in-memory DataFrame, matching the existing `OnlineJoinerStreamingJob.buildTrainingSamples` pattern. `main` parses env config and delegates to `run`.

**Tech Stack:** Scala 2.12, Spark 3.5.x ML (`FeatureHasher`, `HashingTF`, `VectorAssembler`, `LogisticRegression`/`GBTClassifier`, `BinaryClassificationEvaluator`), sbt, ScalaTest (`AnyFlatSpec`).

## Global Constraints

- Module: `recsys-pipeline/services/spark-streaming-job` (sbt). All Scala paths below are relative to it.
- Test pattern: `AnyFlatSpec with Matchers with BeforeAndAfterAll`, a `local[1]` SparkSession built in `beforeAll` with `spark.sql.shuffle.partitions=1` and `spark.ui.enabled=false`, stopped in `afterAll`.
- Config via `com.demo.util.Env` and `sys.env.getOrElse`; SparkSession via `com.demo.util.SparkSessions.create(...)`.
- Out of scope: no serving/Java-API changes, no ONNX export, no new Redis keys, no changes to existing jobs/tests.
- Every commit message ends with:
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- Run tests from the module dir: `cd recsys-pipeline/services/spark-streaming-job`.

## File Structure

- Create: `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala` — the job (built up across Tasks 1–5).
- Create: `src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala` — unit + end-to-end tests (built up across Tasks 1–5).
- Create: `recsys-pipeline/run-ctr-training.sh` — thin spark-submit wrapper (Task 6).
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md` — document the new job under the Offline Path (Task 6).

---

### Task 1: Label column

**Files:**
- Create: `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: nothing.
- Produces: `CtrRankingModelTrainingJob.labelColumn(df: DataFrame, mode: String): DataFrame` — adds a `ctr_label: Double` column. `mode="click"` → `clicked == 1`; any other value → `label > 0.0`.

- [ ] **Step 1: Write the failing test**

Create the test file with the harness and the first two tests:

```scala
package com.demo.task

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CtrRankingModelTrainingJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("CtrRankingModelTrainingJobSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  "labelColumn" should "map label>0 to 1.0 in positive mode" in {
    val s = spark; import s.implicits._
    val df = Seq((1, 0.0), (1, 1.0), (0, 2.0)).toDF("clicked", "label")
    val out = CtrRankingModelTrainingJob.labelColumn(df, "positive")
      .select("ctr_label").collect().map(_.getDouble(0))
    out shouldBe Array(0.0, 1.0, 1.0)
  }

  it should "map clicked==1 to 1.0 in click mode" in {
    val s = spark; import s.implicits._
    val df = Seq((1, 0.0), (0, 2.0)).toDF("clicked", "label")
    val out = CtrRankingModelTrainingJob.labelColumn(df, "click")
      .select("ctr_label").collect().map(_.getDouble(0))
    out shouldBe Array(1.0, 0.0)
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: FAIL — compilation error, `CtrRankingModelTrainingJob` not found.

- [ ] **Step 3: Write minimal implementation**

Create `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`:

```scala
package com.demo.task

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CtrRankingModelTrainingJob {

  def labelColumn(df: DataFrame, mode: String): DataFrame = {
    val label = mode match {
      case "click" => when(col("clicked") === 1, 1.0).otherwise(0.0)
      case _       => when(col("label") > 0.0, 1.0).otherwise(0.0)
    }
    df.withColumn("ctr_label", label)
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala
git commit -m "feat: CtrRankingModelTrainingJob.labelColumn

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Feature assembly

**Files:**
- Modify: `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces:
  - `CtrRankingModelTrainingJob.HashTfSize: Int` — the `HashingTF` size used for genres and tags (256).
  - `CtrRankingModelTrainingJob.assembleFeatures(df: DataFrame, numFeatures: Int): DataFrame` — adds a `features` vector column of size `numFeatures + 2 * HashTfSize`. Expects columns `user_features`/`item_features`/`context_features` (map<string,string>), `genres`/`tags` (array<string>), `item_id` (string), `position`.

- [ ] **Step 1: Write the failing test**

Append inside the spec class:

```scala
  "assembleFeatures" should "produce a features vector of the expected size" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("item_1", 0,
        Map("tier" -> "gold"),
        Map("bucket" -> "b1"),
        Map("device" -> "ios", "country" -> "US"),
        Seq("drama"), Seq("classic"))
    ).toDF("item_id", "position", "user_features", "item_features",
           "context_features", "genres", "tags")

    val out = CtrRankingModelTrainingJob.assembleFeatures(df, numFeatures = 1024)
    out.columns should contain ("features")
    val v = out.select("features").first().getAs[Vector](0)
    v.size shouldBe (1024 + 2 * CtrRankingModelTrainingJob.HashTfSize)
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z assembleFeatures"`
Expected: FAIL — `assembleFeatures` / `HashTfSize` not found.

- [ ] **Step 3: Write minimal implementation**

Add imports at the top of `CtrRankingModelTrainingJob.scala` (replace the existing import block):

```scala
import org.apache.spark.ml.feature.{FeatureHasher, HashingTF, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
```

Add inside the object:

```scala
  val HashTfSize = 256

  def assembleFeatures(df: DataFrame, numFeatures: Int): DataFrame = {
    val withCols = df
      .withColumn("uf_tier",    coalesce(element_at(col("user_features"), "tier"), lit("NA")))
      .withColumn("if_bucket",  coalesce(element_at(col("item_features"), "bucket"), lit("NA")))
      .withColumn("cf_device",  coalesce(element_at(col("context_features"), "device"), lit("NA")))
      .withColumn("cf_country", coalesce(element_at(col("context_features"), "country"), lit("NA")))
      .withColumn("position_d", coalesce(col("position").cast("double"), lit(0.0)))
      .withColumn("genres_arr", coalesce(col("genres"), array().cast("array<string>")))
      .withColumn("tags_arr",   coalesce(col("tags"),   array().cast("array<string>")))

    val hasher = new FeatureHasher()
      .setInputCols(Array("uf_tier", "if_bucket", "cf_device", "cf_country", "item_id", "position_d"))
      .setOutputCol("cat_features")
      .setNumFeatures(numFeatures)

    val genresTf = new HashingTF()
      .setInputCol("genres_arr").setOutputCol("genres_features").setNumFeatures(HashTfSize)
    val tagsTf = new HashingTF()
      .setInputCol("tags_arr").setOutputCol("tags_features").setNumFeatures(HashTfSize)

    val assembler = new VectorAssembler()
      .setInputCols(Array("cat_features", "genres_features", "tags_features"))
      .setOutputCol("features")

    val hashed = hasher.transform(withCols)
    val g = genresTf.transform(hashed)
    val t = tagsTf.transform(g)
    assembler.transform(t)
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala
git commit -m "feat: CtrRankingModelTrainingJob.assembleFeatures

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Temporal split

**Files:**
- Modify: `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces: `CtrRankingModelTrainingJob.splitByDate(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame)` — returns `(train, validation)`. Validation = rows whose `date` (cast to string) is among the latest `max(1, holdoutDays)` distinct dates; train = the rest.

- [ ] **Step 1: Write the failing test**

Append inside the spec class:

```scala
  "splitByDate" should "hold out the latest date" in {
    val s = spark; import s.implicits._
    val df = Seq(
      ("a", "2026-06-01"), ("b", "2026-06-02"), ("c", "2026-06-03")
    ).toDF("id", "date")

    val (train, valid) = CtrRankingModelTrainingJob.splitByDate(df, holdoutDays = 1)
    train.select("date").distinct().collect().map(_.getString(0)).sorted shouldBe
      Array("2026-06-01", "2026-06-02")
    valid.select("date").distinct().collect().map(_.getString(0)) shouldBe
      Array("2026-06-03")
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z splitByDate"`
Expected: FAIL — `splitByDate` not found.

- [ ] **Step 3: Write minimal implementation**

Add inside the object:

```scala
  def splitByDate(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame) = {
    val dates = df.select(col("date").cast("string")).distinct()
      .collect().map(_.getString(0)).sorted
    val holdout = dates.takeRight(math.max(1, holdoutDays)).toSeq
    val train = df.where(!col("date").cast("string").isin(holdout: _*))
    val valid = df.where(col("date").cast("string").isin(holdout: _*))
    (train, valid)
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala
git commit -m "feat: CtrRankingModelTrainingJob.splitByDate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Evaluation metrics

**Files:**
- Modify: `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: nothing from prior tasks.
- Produces: `CtrRankingModelTrainingJob.evaluate(predictions: DataFrame): Map[String, Double]` — expects columns `ctr_label: Double` and `probability: Vector` (positive-class prob at index 1). Returns keys `auc_roc`, `pr_auc`, `logloss`, `positive_rate`.

- [ ] **Step 1: Write the failing test**

Append inside the spec class:

```scala
  "evaluate" should "return auc/pr/logloss/positive_rate in valid ranges" in {
    val s = spark; import s.implicits._
    val preds = Seq(
      (1.0, Vectors.dense(0.2, 0.8)),
      (0.0, Vectors.dense(0.7, 0.3)),
      (1.0, Vectors.dense(0.4, 0.6)),
      (0.0, Vectors.dense(0.9, 0.1))
    ).toDF("ctr_label", "probability")

    val m = CtrRankingModelTrainingJob.evaluate(preds)
    m.keySet should contain allOf ("auc_roc", "pr_auc", "logloss", "positive_rate")
    m("auc_roc") should (be >= 0.0 and be <= 1.0)
    m("pr_auc") should (be >= 0.0 and be <= 1.0)
    m("positive_rate") shouldBe (0.5 +- 1e-9)
    m("logloss") should be >= 0.0
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z evaluate"`
Expected: FAIL — `evaluate` not found.

- [ ] **Step 3: Write minimal implementation**

Add these imports to `CtrRankingModelTrainingJob.scala`:

```scala
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.linalg.Vector
```

Add inside the object:

```scala
  def evaluate(predictions: DataFrame): Map[String, Double] = {
    val auc = new BinaryClassificationEvaluator()
      .setLabelCol("ctr_label").setRawPredictionCol("probability").setMetricName("areaUnderROC")
      .evaluate(predictions)
    val prauc = new BinaryClassificationEvaluator()
      .setLabelCol("ctr_label").setRawPredictionCol("probability").setMetricName("areaUnderPR")
      .evaluate(predictions)

    val eps = 1e-15
    val posProb = udf { v: Vector => math.min(1.0 - eps, math.max(eps, v(1))) }
    val logloss = predictions
      .withColumn("p", posProb(col("probability")))
      .select(mean(-(col("ctr_label") * log(col("p")) +
        (lit(1.0) - col("ctr_label")) * log(lit(1.0) - col("p")))).as("ll"))
      .first().getDouble(0)
    val posRate = predictions.select(mean(col("ctr_label"))).first().getDouble(0)

    Map("auc_roc" -> auc, "pr_auc" -> prauc, "logloss" -> logloss, "positive_rate" -> posRate)
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala
git commit -m "feat: CtrRankingModelTrainingJob.evaluate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Train, run, and main

**Files:**
- Modify: `src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala`
- Test: `src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala`

**Interfaces:**
- Consumes: `labelColumn`, `assembleFeatures`, `splitByDate`, `evaluate` from Tasks 1–4.
- Produces:
  - `CtrRankingModelTrainingJob.trainModel(training: DataFrame, algorithm: String): org.apache.spark.ml.Model[_]` — `algorithm="gbt"` → `GBTClassifier`, else `LogisticRegression`; both use `featuresCol="features"`, `labelCol="ctr_label"`.
  - `CtrRankingModelTrainingJob.run(spark, inputPath, modelPath, metricsPath, holdoutDays, algorithm, labelMode, numFeatures): Map[String, Double]` — reads Parquet, trains, evaluates, saves the model dir and `metrics.json`, returns the metrics map (including `train_rows`/`val_rows`).
  - `CtrRankingModelTrainingJob.main(args)` — parses env, calls `run`.

- [ ] **Step 1: Write the failing test**

Append inside the spec class:

```scala
  "run" should "train a model and write metrics.json" in {
    import java.nio.file.{Files, Paths}
    val s = spark; import s.implicits._

    val dir = Files.createTempDirectory("ctr").toFile
    val input = new java.io.File(dir, "samples").getAbsolutePath
    val modelP = new java.io.File(dir, "model").getAbsolutePath
    val metricsP = new java.io.File(dir, "metrics.json").getAbsolutePath

    val rows = Seq(
      ("user_1", "item_1", 0, 1, 1.0, Map("tier" -> "gold"), Map("bucket" -> "b1"),
        Map("device" -> "ios", "country" -> "US"), Seq("drama"),  Seq("a"), "2026-06-01"),
      ("user_2", "item_2", 1, 0, 0.0, Map("tier" -> "free"), Map("bucket" -> "b2"),
        Map("device" -> "web", "country" -> "GB"), Seq("comedy"), Seq("b"), "2026-06-01"),
      ("user_3", "item_1", 0, 1, 1.0, Map("tier" -> "gold"), Map("bucket" -> "b1"),
        Map("device" -> "ios", "country" -> "US"), Seq("drama"),  Seq("a"), "2026-06-02"),
      ("user_4", "item_2", 2, 0, 0.0, Map("tier" -> "free"), Map("bucket" -> "b2"),
        Map("device" -> "web", "country" -> "GB"), Seq("comedy"), Seq("b"), "2026-06-02")
    ).toDF("user_id", "item_id", "position", "clicked", "label", "user_features",
           "item_features", "context_features", "genres", "tags", "date")
      .withColumn("impression_time", current_timestamp())

    rows.write.mode("overwrite").partitionBy("date").parquet(input)

    val m = CtrRankingModelTrainingJob.run(
      spark, input, modelP, metricsP,
      holdoutDays = 1, algorithm = "logreg", labelMode = "positive", numFeatures = 1024)

    m("auc_roc") should (be >= 0.0 and be <= 1.0)
    Files.exists(Paths.get(metricsP)) shouldBe true
    new java.io.File(modelP).exists() shouldBe true
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec -- -z \"train a model\""`
Expected: FAIL — `run` not found.

- [ ] **Step 3: Write minimal implementation**

Add these imports to `CtrRankingModelTrainingJob.scala`:

```scala
import com.demo.util.{Env, SparkSessions}
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.SparkSession
```

Add inside the object:

```scala
  def trainModel(training: DataFrame, algorithm: String): Model[_] = algorithm match {
    case "gbt" =>
      new GBTClassifier().setLabelCol("ctr_label").setFeaturesCol("features").setMaxIter(10).fit(training)
    case _ =>
      new LogisticRegression().setLabelCol("ctr_label").setFeaturesCol("features").setMaxIter(20).fit(training)
  }

  def run(
      spark: SparkSession,
      inputPath: String,
      modelPath: String,
      metricsPath: String,
      holdoutDays: Int,
      algorithm: String,
      labelMode: String,
      numFeatures: Int
  ): Map[String, Double] = {
    val raw = spark.read.parquet(inputPath)
      .where(col("user_id").isNotNull && col("item_id").isNotNull && col("impression_time").isNotNull)
    val labeled  = labelColumn(raw, labelMode)
    val featured = assembleFeatures(labeled, numFeatures)
    val (training, validation) = splitByDate(featured, holdoutDays)

    val model = trainModel(training, algorithm)
    val metrics = evaluate(model.transform(validation)) ++ Map(
      "train_rows" -> training.count().toDouble,
      "val_rows"   -> validation.count().toDouble
    )

    model match { case w: MLWritable => w.write.overwrite().save(modelPath) }
    writeMetrics(metricsPath, metrics, algorithm, labelMode, holdoutDays)
    println(s"[CTR] algorithm=$algorithm " + metrics.map { case (k, v) => s"$k=$v" }.mkString(" "))
    metrics
  }

  private def writeMetrics(
      path: String, metrics: Map[String, Double],
      algorithm: String, labelMode: String, holdoutDays: Int
  ): Unit = {
    val numeric = metrics.map { case (k, v) => s""""$k": $v""" }
    val meta = Seq(
      s""""algorithm": "$algorithm"""",
      s""""label_mode": "$labelMode"""",
      s""""holdout_days": $holdoutDays"""
    )
    val json = (numeric.toSeq ++ meta).mkString("{\n  ", ",\n  ", "\n}\n")
    val p = java.nio.file.Paths.get(path)
    Option(p.getParent).foreach(java.nio.file.Files.createDirectories(_))
    java.nio.file.Files.write(p, json.getBytes(java.nio.charset.StandardCharsets.UTF_8))
  }

  def main(args: Array[String]): Unit = {
    val inputPath   = sys.env.getOrElse("CTR_INPUT_PATH", "/tmp/spark-recsys/training-samples")
    val modelPath   = sys.env.getOrElse("CTR_MODEL_OUTPUT_PATH", "/tmp/spark-recsys/ctr-model")
    val metricsPath = sys.env.getOrElse("CTR_METRICS_OUTPUT_PATH", s"$modelPath/metrics.json")
    val holdoutDays = Env.int("CTR_HOLDOUT_DAYS", 1)
    val algorithm   = sys.env.getOrElse("CTR_ALGORITHM", "logreg")
    val labelMode   = sys.env.getOrElse("CTR_LABEL_MODE", "positive")
    val numFeatures = Env.int("CTR_NUM_FEATURES", 262144)

    val spark = SparkSessions.create("CtrRankingModelTrainingJob")
    try {
      run(spark, inputPath, modelPath, metricsPath, holdoutDays, algorithm, labelMode, numFeatures)
    } finally {
      spark.stop()
    }
  }
```

- [ ] **Step 4: Run the full spec to verify it passes**

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt "testOnly com.demo.task.CtrRankingModelTrainingJobSpec"`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/services/spark-streaming-job/src/main/scala/com/demo/task/CtrRankingModelTrainingJob.scala \
        recsys-pipeline/services/spark-streaming-job/src/test/scala/com/demo/task/CtrRankingModelTrainingJobSpec.scala
git commit -m "feat: CtrRankingModelTrainingJob run + main

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Run script and docs

**Files:**
- Create: `recsys-pipeline/run-ctr-training.sh`
- Modify: `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`

**Interfaces:**
- Consumes: `com.demo.task.CtrRankingModelTrainingJob.main` from Task 5.
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Create the run script**

Create `recsys-pipeline/run-ctr-training.sh` (mirrors `run-als-pipeline.sh` structure):

```bash
#!/usr/bin/env bash
# CTR/ranking batch trainer
#
# Reads the OnlineJoiner Parquet training-samples store and trains a
# click-probability model (offline only; no serving changes).
#
# Flow: training-samples/date=*/  ──► CtrRankingModelTrainingJob
#                                        ├──► Spark ML model dir
#                                        └──► metrics.json (AUC / PR-AUC / logloss)
#
# Env vars:
#   CTR_INPUT_PATH           default /tmp/spark-recsys/training-samples
#   CTR_MODEL_OUTPUT_PATH    default /tmp/spark-recsys/ctr-model
#   CTR_METRICS_OUTPUT_PATH  default <model>/metrics.json
#   CTR_HOLDOUT_DAYS         default 1
#   CTR_ALGORITHM            logreg | gbt   (default logreg)
#   CTR_LABEL_MODE           positive | click  (default positive)
#   CTR_NUM_FEATURES         default 262144
#   SPARK_HOME, SPARK_MASTER (default local[*])
#
# Example:
#   CTR_INPUT_PATH=/tmp/spark-recsys/training-samples ./run-ctr-training.sh
set -euo pipefail

cd "$(dirname "$0")"

export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 17 2>/dev/null || echo "")}"

SPARK_SUBMIT=""
if [[ -n "${SPARK_HOME:-}" && -x "$SPARK_HOME/bin/spark-submit" ]]; then
  SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
else
  echo "Error: spark-submit not found. Set SPARK_HOME or add spark-submit to PATH." >&2
  exit 1
fi

"$SPARK_SUBMIT" \
  --class com.demo.task.CtrRankingModelTrainingJob \
  --master "${SPARK_MASTER:-local[*]}" \
  services/spark-streaming-job/target/scala-2.12/spark-recsys-job.jar
```

- [ ] **Step 2: Verify the script parses**

Run: `bash -n recsys-pipeline/run-ctr-training.sh && chmod +x recsys-pipeline/run-ctr-training.sh && echo OK`
Expected: prints `OK` (no syntax errors).

- [ ] **Step 3: Document the job in Data_Pipeline.md**

In `recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`, find the `### EmbeddingCandidateGenerationJob` section (the last subsection of `## Offline Path`) and append this new subsection immediately after it (at end of file):

```markdown
### `CtrRankingModelTrainingJob`

Offline batch trainer over the Parquet training-samples store (the
`OnlineJoinerStreamingJob` output). Reads the date-partitioned Parquet, assembles
features (hashed user/item/context map fields + `item_id` via `FeatureHasher`,
`genres`/`tags` via `HashingTF`, numeric `position`), does a temporal train/val
split by `date`, trains a click-probability classifier, and writes the Spark ML
model plus a `metrics.json` (AUC-ROC, PR-AUC, logloss). Offline only — no serving,
Redis, or ONNX changes.

```bash
CTR_INPUT_PATH=/tmp/spark-recsys/training-samples ./run-ctr-training.sh
```

Key environment variables:

| Env var | Default |
|---|---|
| `CTR_INPUT_PATH` | `/tmp/spark-recsys/training-samples` |
| `CTR_MODEL_OUTPUT_PATH` | `/tmp/spark-recsys/ctr-model` |
| `CTR_METRICS_OUTPUT_PATH` | `<model>/metrics.json` |
| `CTR_HOLDOUT_DAYS` | `1` |
| `CTR_ALGORITHM` | `logreg` (`logreg` \| `gbt`) |
| `CTR_LABEL_MODE` | `positive` (`positive` \| `click`) |
| `CTR_NUM_FEATURES` | `262144` |
```

- [ ] **Step 4: Verify the docs edit**

Run: `grep -c "CtrRankingModelTrainingJob" recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md`
Expected: `2` or more.

- [ ] **Step 5: Commit**

```bash
git add recsys-pipeline/run-ctr-training.sh recsys-pipeline/docs/recommendation_architecture/Data_Pipeline.md
git commit -m "feat: run-ctr-training.sh + Data_Pipeline docs for CTR trainer

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Final verification

- [ ] Run the whole module test suite to confirm nothing else broke:

Run: `cd recsys-pipeline/services/spark-streaming-job && sbt test`
Expected: all specs PASS, including `CtrRankingModelTrainingJobSpec` (6 tests).

## Self-Review (completed by plan author)

- **Spec coverage:** input+filter (Task 5 `run`), label modes (Task 1), feature assembly incl. maps/genres/tags/position (Task 2), temporal split (Task 3), model + AUC/PR-AUC/logloss (Tasks 4–5), model + metrics.json outputs (Task 5), config env vars (Task 5 `main` + Task 6 script/docs), testing (Tasks 1–5), run script + docs (Task 6). All spec sections map to a task.
- **Placeholder scan:** none — every code and command step is concrete.
- **Type consistency:** `ctr_label`, `features`, `probability` column names and `HashTfSize`/`assembleFeatures`/`splitByDate`/`evaluate`/`trainModel`/`run` signatures are consistent across tasks and the Interfaces blocks.
- **Note:** `evaluate` uses the `probability` column; `LogisticRegression` (the default) provides it. The `gbt` toggle relies on Spark 3.5 `GBTClassificationModel` also emitting `probability`; tests cover only `logreg`.
