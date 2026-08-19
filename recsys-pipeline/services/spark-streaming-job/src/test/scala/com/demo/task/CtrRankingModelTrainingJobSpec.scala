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

  it should "read device and country from their new homes, falling back to the legacy map" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // genres and tags must be present: assembleFeatures reads them with coalesce, which
    // raises AnalysisException on a missing column rather than yielding null.
    val typed = Seq(
      ("u1", "i1", Map("country" -> "us"), Map.empty[String, String],
        Map.empty[String, String], "ios", 0, 1, Seq("drama"), Seq("classic"))
    ).toDF("user_id", "item_id", "user_features", "item_features",
      "context_features", "device", "position", "clicked", "genres", "tags")

    val typedRow = CtrRankingModelTrainingJob.assembleFeatures(
      CtrRankingModelTrainingJob.labelColumn(typed, "click"), 64).first()

    typedRow.getAs[String]("cf_device") shouldBe "ios"
    typedRow.getAs[String]("cf_country") shouldBe "us"

    val legacy = Seq(
      ("u1", "i1", Map.empty[String, String], Map.empty[String, String],
        Map("device" -> "ios", "country" -> "us"), 0, 1, Seq("drama"), Seq("classic"))
    ).toDF("user_id", "item_id", "user_features", "item_features",
      "context_features", "position", "clicked", "genres", "tags")

    val legacyRow = CtrRankingModelTrainingJob.assembleFeatures(
      CtrRankingModelTrainingJob.labelColumn(legacy, "click"), 64).first()

    legacyRow.getAs[String]("cf_device") shouldBe "ios"
    legacyRow.getAs[String]("cf_country") shouldBe "us"
  }

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

  it should "run writes parseable metrics.json when a metric is non-finite" in {
    import java.nio.file.{Files, Paths}
    val s = spark; import s.implicits._

    // NB: with Spark 3.5.1, BinaryClassificationEvaluator hard-guards away NaN for
    // single-class holdouts (Precision/Recall/FalsePositiveRate all special-case
    // zero-denominator to 0.0/1.0 -- see BinaryClassificationMetricComputers.scala),
    // so a single-class holdout alone no longer reproduces a NaN metric on this
    // Spark version. A NaN `position` value is a realistic way non-finite doubles
    // still reach `evaluate`: it flows through `assembleFeatures`'s numeric
    // `position_d` column into the model's raw prediction, so `probability` (and
    // therefore this job's own `logloss` computation) comes out NaN.
    val dir = Files.createTempDirectory("ctr-nan-metric").toFile
    val input = new java.io.File(dir, "samples").getAbsolutePath
    val modelP = new java.io.File(dir, "model").getAbsolutePath
    val metricsP = new java.io.File(dir, "metrics.json").getAbsolutePath

    val rows = Seq(
      ("user_1", "item_1", 0.0, 1, 1.0, Map("tier" -> "gold"), Map("bucket" -> "b1"),
        Map("device" -> "ios", "country" -> "US"), Seq("drama"),  Seq("a"), "2026-06-01"),
      ("user_2", "item_2", 1.0, 0, 0.0, Map("tier" -> "free"), Map("bucket" -> "b2"),
        Map("device" -> "web", "country" -> "GB"), Seq("comedy"), Seq("b"), "2026-06-01"),
      ("user_3", "item_1", Double.NaN, 1, 1.0, Map("tier" -> "gold"), Map("bucket" -> "b1"),
        Map("device" -> "ios", "country" -> "US"), Seq("drama"),  Seq("a"), "2026-06-02"),
      ("user_4", "item_2", Double.NaN, 0, 0.0, Map("tier" -> "free"), Map("bucket" -> "b2"),
        Map("device" -> "web", "country" -> "GB"), Seq("comedy"), Seq("b"), "2026-06-02")
    ).toDF("user_id", "item_id", "position", "clicked", "label", "user_features",
           "item_features", "context_features", "genres", "tags", "date")
      .withColumn("impression_time", current_timestamp())

    rows.write.mode("overwrite").partitionBy("date").parquet(input)

    CtrRankingModelTrainingJob.run(
      spark, input, modelP, metricsP,
      holdoutDays = 1, algorithm = "logreg", labelMode = "positive", numFeatures = 1024)

    val metricsJson = new String(Files.readAllBytes(Paths.get(metricsP)), java.nio.charset.StandardCharsets.UTF_8)
    metricsJson should not include "NaN"
    metricsJson should include (""""logloss": null""")
  }
}
