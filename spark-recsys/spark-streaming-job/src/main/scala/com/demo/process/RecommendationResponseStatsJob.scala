package com.demo.process

import com.demo.util.SparkSessions
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object RecommendationResponseStatsJob {

  val ResponseMetric = "RecommendationFeed.response"

  val ItemSchema: StructType = StructType(Seq(
    StructField("position", IntegerType, nullable = true),
    StructField("item_id", StringType, nullable = false),
    StructField("clicked", IntegerType, nullable = true),
    StructField("ordered", IntegerType, nullable = true),
    StructField("label", DoubleType, nullable = true),
    StructField("item_features", MapType(StringType, StringType), nullable = true)
  ))

  val SlateSchema: StructType = StructType(Seq(
    StructField("slate_id", StringType, nullable = false),
    StructField("request_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("request_ts", LongType, nullable = false),
    StructField("slate_clicked", IntegerType, nullable = true),
    StructField("slate_ordered", IntegerType, nullable = true),
    StructField("slate_reward", DoubleType, nullable = true),
    StructField("slate_size", IntegerType, nullable = true),
    StructField("user_features", MapType(StringType, StringType), nullable = true),
    StructField("context_features", MapType(StringType, StringType), nullable = true),
    StructField("items", ArrayType(ItemSchema), nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("RESPONSE_STATS_INPUT_TOPIC", "training_experiences")
    val outputTopic = sys.env.getOrElse("RESPONSE_STATS_OUTPUT_TOPIC", "recommendation_metrics")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/response-stats"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")

    val spark = SparkSessions.create("RecommendationResponseStatsJob")

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    buildMetricEvents(parseSlates(raw))
      .select(
        col("metric_name").as("key"),
        to_json(struct(col("metric_name"), col("tags"), col("value"))).as("value")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }

  def parseSlates(rawKafka: DataFrame): DataFrame =
    rawKafka.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), SlateSchema).as("data"))
      .select("data.*")
      .filter(col("slate_id").isNotNull && col("request_id").isNotNull && col("user_id").isNotNull)

  def buildMetricEvents(slates: DataFrame): DataFrame = {
    val safeItems = coalesce(col("safe_items"), array().cast(ArrayType(ItemSchema)))
    val adItems = filter(safeItems, item =>
      lower(coalesce(
        item.getField("item_features").getItem("type"),
        item.getField("item_features").getItem("product_type"),
        lit("")
      )).isin("ad", "ads", "sponsored")
    )

    val enriched = slates
      .withColumn("safe_items", coalesce(col("items"), array().cast(ArrayType(ItemSchema))))
      .withColumn("safe_user_features", coalesce(col("user_features"), typedLit(Map.empty[String, String])))
      .withColumn("safe_context_features", coalesce(col("context_features"), typedLit(Map.empty[String, String])))
      .withColumn("selected_ads", size(adItems).cast("long"))
      .withColumn("selected_items", (size(safeItems) - size(adItems)).cast("long"))
      .withColumn(
        "country",
        bucketCountry(coalesce(
          col("safe_context_features").getItem("country_code"),
          col("safe_user_features").getItem("country_code"),
          col("safe_user_features").getItem("country")
        ))
      )
      .withColumn(
        "subscription",
        coalesce(
          col("safe_user_features").getItem("subscription_level"),
          col("safe_user_features").getItem("subscription"),
          lit("none")
        )
      )
      .withColumn(
        "blender",
        coalesce(
          col("safe_context_features").getItem("AdsBlenderType"),
          col("safe_context_features").getItem("ads_blender_type"),
          lit("default")
        )
      )

    enriched
      .select(explode(metricArray).as("metric"))
      .select(
        lit(ResponseMetric).as("metric_name"),
        col("metric.tags").as("tags"),
        col("metric.value").as("value")
      )
      .filter(col("value") > 0L)
  }

  private def metricArray: Column =
    array(
      metric("total", col("subscription"), lit(null).cast(StringType), lit(null).cast(StringType), lit(1L)),
      metric("total", col("subscription"), col("country"), lit(null).cast(StringType), lit(1L)),
      metric("items", col("subscription"), lit(null).cast(StringType), col("blender"), col("selected_items")),
      metric("ads", col("subscription"), lit(null).cast(StringType), col("blender"), col("selected_ads")),
      stageMetric("empty_ads", "response", col("subscription"), col("blender"), when(col("selected_ads") === 0L, lit(1L)).otherwise(lit(0L))),
      stageMetric("empty_items", "response", col("subscription"), col("blender"), when(col("selected_items") === 0L, lit(1L)).otherwise(lit(0L))),
      stageMetric("sufficient_ads", "response", col("subscription"), col("blender"), when(col("selected_ads") >= 5L, lit(1L)).otherwise(lit(0L))),
      stageMetric("sufficient_items", "response", col("subscription"), col("blender"), when(col("selected_items") >= 20L, lit(1L)).otherwise(lit(0L)))
    )

  private def metric(metricType: String, subscription: Column, country: Column, blender: Column, value: Column): Column =
    struct(
      cleanTags(map(
        lit("type"), lit(metricType),
        lit("subscription"), subscription,
        lit("country"), country,
        lit("blender"), blender
      )).as("tags"),
      value.cast("long").as("value")
    )

  private def stageMetric(metricType: String, stage: String, subscription: Column, blender: Column, value: Column): Column =
    struct(
      cleanTags(map(
        lit("type"), lit(metricType),
        lit("stage"), lit(stage),
        lit("subscription"), subscription,
        lit("blender"), blender
      )).as("tags"),
      value.cast("long").as("value")
    )

  private def cleanTags(tags: Column): Column =
    map_filter(tags, (_, value) => value.isNotNull && length(trim(value)) > 0)

  private def bucketCountry(country: Column): Column =
    when(country.isNull || length(trim(country)) === 0, lit("unknown"))
      .when(upper(trim(country)).isin("US", "CA", "GB", "AU", "DE", "FR", "JP", "IN", "BR"), lower(trim(country)))
      .otherwise(lit("other"))

}
