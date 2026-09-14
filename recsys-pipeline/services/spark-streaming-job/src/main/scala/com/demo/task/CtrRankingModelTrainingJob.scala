package com.demo.task

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.ml.Model
import org.apache.spark.ml.classification.{GBTClassifier, LogisticRegression}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{FeatureHasher, HashingTF, VectorAssembler}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.language.existentials

object CtrRankingModelTrainingJob {

  def labelColumn(df: DataFrame, mode: String): DataFrame = {
    val label = mode match {
      case "click" => when(col("clicked") === 1, 1.0).otherwise(0.0)
      case _       => when(col("label") > 0.0, 1.0).otherwise(0.0)
    }
    df.withColumn("ctr_label", label)
  }

  val HashTfSize = 256

  /** Reads `map(key)` if `map` exists on `df`, else null. */
  private def mapValue(df: DataFrame, map: String, key: String): Column =
    if (df.columns.contains(map)) element_at(col(map), key) else lit(null).cast("string")

  /** The first source that has the value: the typed column, then the named map key.
    *
    * `device` moved from `context_features` to a typed field in schema v2. Reading both
    * keeps Parquet written before that bump trainable, and keeps a producer that has not
    * been updated yet from silently degrading the model to the "NA" default. */
  private def firstAvailable(df: DataFrame, column: String, map: String, key: String): Column = {
    val fromColumn = if (df.columns.contains(column)) col(column) else lit(null).cast("string")
    coalesce(fromColumn, mapValue(df, map, key), lit("NA"))
  }

  def assembleFeatures(df: DataFrame, numFeatures: Int): DataFrame = {
    val withCols = df
      .withColumn("uf_tier",    coalesce(element_at(col("user_features"), "tier"), lit("NA")))
      .withColumn("if_bucket",  coalesce(element_at(col("item_features"), "bucket"), lit("NA")))
      .withColumn("cf_device",  firstAvailable(df, "device", "context_features", "device"))
      .withColumn("cf_country", coalesce(
        mapValue(df, "user_features", "country"),
        mapValue(df, "context_features", "country"),
        lit("NA")))
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

  def evaluate(predictions: DataFrame): Map[String, Double] = {
    // Reuse inference across metric actions without retaining the wide feature frame.
    val scores = predictions.select("ctr_label", "probability")
    val ownsCache = scores.storageLevel == StorageLevel.NONE
    if (ownsCache) scores.persist(StorageLevel.MEMORY_AND_DISK)
    try {
      val metrics = new BinaryClassificationEvaluator()
        .setLabelCol("ctr_label").setRawPredictionCol("probability").getMetrics(scores)
      val (auc, prauc) = try {
        // Both curves share the same sorted cumulative counts and default binning.
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
    val (trainRaw, validRaw, validationRows) = splitByDateWithCount(raw, holdoutDays)
    val training = assembleFeatures(labelColumn(trainRaw, labelMode), numFeatures)
      .select("ctr_label", "features").persist(StorageLevel.MEMORY_AND_DISK)
    val (model, trainingRows): (Model[_], Long) = try {
      // Materialize once: reuse the count for validation/reporting and features for fitting.
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
      "val_rows"   -> validationRows.toDouble
    )

    model match {
      case w: MLWritable => w.write.overwrite().save(modelPath)
      case other => sys.error(s"Model ${other.getClass.getName} is not MLWritable; cannot save")
    }
    writeMetrics(metricsPath, metrics, algorithm, labelMode, holdoutDays)
    println(s"[CTR] algorithm=$algorithm " + metrics.map { case (k, v) => s"$k=$v" }.mkString(" "))
    metrics
  }

  private def writeMetrics(
      path: String, metrics: Map[String, Double],
      algorithm: String, labelMode: String, holdoutDays: Int
  ): Unit = {
    val numeric = metrics.map { case (k, v) =>
      val jv = if (v.isNaN || v.isInfinite) "null" else v.toString
      s""""$k": $jv"""
    }
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
