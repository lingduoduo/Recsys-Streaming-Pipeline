package com.demo.task

import org.apache.spark.ml.feature.{FeatureHasher, HashingTF, VectorAssembler}
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
}
