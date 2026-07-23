package com.demo.task

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{FeatureHasher, HashingTF, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CtrRankingModelTrainingJob {

  def labelColumn(df: DataFrame, mode: String): DataFrame = {
    val label = mode match {
      case "click" => when(col("clicked") === 1, 1.0).otherwise(0.0)
      case _       => when(col("label") > 0.0, 1.0).otherwise(0.0)
    }
    df.withColumn("ctr_label", label)
  }

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

  def splitByDate(df: DataFrame, holdoutDays: Int): (DataFrame, DataFrame) = {
    val dates = df.select(col("date").cast("string")).distinct()
      .collect().map(_.getString(0)).sorted
    val holdout = dates.takeRight(math.max(1, holdoutDays)).toSeq
    val train = df.where(!col("date").cast("string").isin(holdout: _*))
    val valid = df.where(col("date").cast("string").isin(holdout: _*))
    (train, valid)
  }

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
}
