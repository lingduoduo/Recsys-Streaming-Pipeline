package com.demo.process

import com.demo.task.RedisPool
import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object MovieLensContextCollectorStreamingJob {
  private val log = LoggerFactory.getLogger(getClass)

  val ContextEventSchema: StructType = StructType(Seq(
    StructField("event_type", StringType, nullable = true),
    StructField("user_id", StringType, nullable = true),
    StructField("item_id", StringType, nullable = true),
    StructField("rating", DoubleType, nullable = true),
    StructField("timestamp", LongType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("occupation", StringType, nullable = true),
    StructField("zip_code", StringType, nullable = true),
    StructField("title", StringType, nullable = true),
    StructField("genres", ArrayType(StringType), nullable = true),
    StructField("release_year", IntegerType, nullable = true)
  ))

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("MOVIELENS_CONTEXT_INPUT_TOPIC", "movielens_context")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = Env.int("REDIS_PORT", 6379)
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/movielens-context-collector"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")
    val redisPipelineSize = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val redisPoolMaxTotal = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val contextTtlSeconds = Env.int("MOVIELENS_CONTEXT_TTL_SECONDS", 30 * 24 * 3600)
    val recentRatingsLimit = math.max(1, Env.int("MOVIELENS_RECENT_RATINGS_LIMIT", 50))

    val spark = SparkSessions.create("MovieLensContextCollectorStreamingJob", defaultShufflePartitions = 4)

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "latest"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    parseEvents(raw).writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val userUpdates = buildUserFeatureUpdates(batch, recentRatingsLimit)
        val movieUpdates = buildMovieFeatureUpdates(batch)
        writeUserUpdates(userUpdates, redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize, contextTtlSeconds, recentRatingsLimit)
        writeMovieUpdates(movieUpdates, redisHost, redisPort, redisPoolMaxTotal, redisPipelineSize, contextTtlSeconds)
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }

  def parseEvents(rawKafka: DataFrame): DataFrame =
    rawKafka.selectExpr("CAST(value AS STRING) AS json")
      .select(from_json(col("json"), ContextEventSchema).as("data"))
      .select("data.*")
      .withColumn("event_type", lower(trim(coalesce(col("event_type"), lit("")))))
      .withColumn(
        "event_kind",
        when(col("event_type") === "rating" && col("user_id").isNotNull && col("item_id").isNotNull, lit("rating"))
          .when(col("user_id").isNotNull && (
            col("age").isNotNull || col("gender").isNotNull || col("occupation").isNotNull || col("zip_code").isNotNull
          ), lit("user_update"))
          .when(col("item_id").isNotNull && (
            col("title").isNotNull || size(coalesce(col("genres"), array().cast(ArrayType(StringType)))) > 0 || col("release_year").isNotNull
          ), lit("movie_update"))
      )
      .filter(col("event_kind").isNotNull)

  def buildUserFeatureUpdates(events: DataFrame, recentRatingsLimit: Int = 50): DataFrame = {
    val userUpdates = events
      .filter(col("event_kind") === "user_update")
      .groupBy("user_id")
      .agg(
        max(col("age")).as("age"),
        first(col("gender"), ignoreNulls = true).as("gender"),
        first(col("occupation"), ignoreNulls = true).as("occupation"),
        first(col("zip_code"), ignoreNulls = true).as("zipCode"),
        max(col("timestamp")).as("lastUpdatedTs")
      )

    val ratingUpdates = events
      .filter(col("event_kind") === "rating")
      .groupBy("user_id")
      .agg(
        count(lit(1)).cast("long").as("ratingCountDelta"),
        sum(col("rating")).as("ratingSumDelta"),
        max(col("timestamp")).as("lastRatingTs"),
        collect_list(struct(
          (-coalesce(col("timestamp"), lit(0L))).as("sort_ts"),
          col("item_id").as("item_id")
        )).as("ratedItems")
      )
      .select(
        col("user_id"),
        col("ratingCountDelta"),
        coalesce(col("ratingSumDelta"), lit(0.0)).as("ratingSumDelta"),
        col("lastRatingTs"),
        expr(s"transform(slice(array_sort(ratedItems), 1, $recentRatingsLimit), x -> x.item_id)").as("recentlyRatedMovieIds")
      )

    userUpdates.join(ratingUpdates, Seq("user_id"), "full_outer")
      .select(
        col("user_id"),
        col("age"),
        col("gender"),
        col("occupation"),
        col("zipCode"),
        coalesce(col("ratingCountDelta"), lit(0L)).as("ratingCountDelta"),
        coalesce(col("ratingSumDelta"), lit(0.0)).as("ratingSumDelta"),
        coalesce(col("recentlyRatedMovieIds"), array().cast(ArrayType(StringType))).as("recentlyRatedMovieIds"),
        greatest(col("lastUpdatedTs"), col("lastRatingTs")).as("lastUpdatedTs")
      )
  }

  def buildMovieFeatureUpdates(events: DataFrame): DataFrame =
    events
      .filter(col("event_kind") === "movie_update")
      .groupBy("item_id")
      .agg(
        first(col("title"), ignoreNulls = true).as("title"),
        first(col("genres"), ignoreNulls = true).as("genres"),
        max(col("release_year")).as("releaseYear"),
        max(col("timestamp")).as("lastUpdatedTs")
      )

  def writeUserUpdates(
      updates: DataFrame,
      redisHost: String,
      redisPort: Int,
      redisPoolMaxTotal: Int,
      redisPipelineSize: Int,
      contextTtlSeconds: Int,
      recentRatingsLimit: Int
  ): Unit = {
    updates.foreachPartition { rows: Iterator[Row] =>
      val pool = RedisPool.get(redisHost, redisPort, redisPoolMaxTotal)
      val jedis = pool.getResource
      try {
        // Phase 1: build each key's fields, doing any existing-state reads with DIRECT (non-pipelined)
        // jedis calls. Reads must not interleave with an open pipeline on the same connection — that
        // makes hget read buffered pipeline replies and corrupts the protocol (drops most writes).
        val writes = scala.collection.mutable.ArrayBuffer.empty[(String, java.util.Map[String, String])]
        rows.foreach { row =>
          try {
            val userId = row.getAs[String]("user_id")
            if (userId != null && userId.nonEmpty) {
              val key = s"user:$userId:features"
              val ratingCountDelta = row.getAs[Long]("ratingCountDelta")
              val ratingSumDelta = row.getAs[Double]("ratingSumDelta")
              val fields = scala.collection.mutable.Map.empty[String, String]

              putIfPresent(fields, "age", row.getAs[Integer]("age"))
              putIfPresent(fields, "gender", row.getAs[String]("gender"))
              putIfPresent(fields, "occupation", row.getAs[String]("occupation"))
              putIfPresent(fields, "zipCode", row.getAs[String]("zipCode"))
              putIfPresent(fields, "lastContextTimestamp", row.getAs[java.lang.Long]("lastUpdatedTs"))

              if (ratingCountDelta > 0L) {
                val existingCount = parseLong(jedis.hget(key, "ratingCount"))
                val existingAvg = parseDouble(jedis.hget(key, "avgRating"))
                val newCount = existingCount + ratingCountDelta
                val newAvg = if (newCount == 0L) 0.0 else ((existingAvg * existingCount) + ratingSumDelta) / newCount
                val recent = mergeRecent(row.getSeq[String](row.fieldIndex("recentlyRatedMovieIds")), jedis.hget(key, "recentlyRatedMovieIds"), recentRatingsLimit)

                fields += "ratingCount" -> newCount.toString
                fields += "avgRating" -> f"$newAvg%.6f"
                fields += "recentlyRatedMovieIds" -> recent.mkString(",")
                fields += "actionSequenceMovieIds" -> recent.mkString(",")
              }

              if (fields.nonEmpty) writes += ((key, fields.asJava))
            }
          } catch {
            case e: Exception =>
              log.warn("Skipping MovieLens user context row: {}", e.getMessage)
          }
        }

        // Phase 2: flush all writes through a single pipeline (no reads interleaved).
        if (writes.nonEmpty) {
          val pipeline = jedis.pipelined()
          var pendingCommands = 0
          writes.foreach { case (key, f) =>
            pipeline.hset(key, f)
            pipeline.expire(key, contextTtlSeconds)
            pendingCommands += 2
            if (pendingCommands >= redisPipelineSize) { pipeline.sync(); pendingCommands = 0 }
          }
          if (pendingCommands > 0) pipeline.sync()
        }
      } finally {
        jedis.close()
      }
    }
  }

  def writeMovieUpdates(
      updates: DataFrame,
      redisHost: String,
      redisPort: Int,
      redisPoolMaxTotal: Int,
      redisPipelineSize: Int,
      contextTtlSeconds: Int
  ): Unit = {
    updates.foreachPartition { rows: Iterator[Row] =>
      val pool = RedisPool.get(redisHost, redisPort, redisPoolMaxTotal)
      val jedis = pool.getResource
      try {
        val pipeline = jedis.pipelined()
        var pendingCommands = 0

        def flushIfNeeded(): Unit =
          if (pendingCommands >= redisPipelineSize) { pipeline.sync(); pendingCommands = 0 }

        rows.foreach { row =>
          try {
            val itemId = row.getAs[String]("item_id")
            if (itemId != null && itemId.nonEmpty) {
              val fields = scala.collection.mutable.Map.empty[String, String]
              putIfPresent(fields, "title", row.getAs[String]("title"))
              putIfPresent(fields, "releaseYear", row.getAs[Integer]("releaseYear"))
              putIfPresent(fields, "lastContextTimestamp", row.getAs[java.lang.Long]("lastUpdatedTs"))
              val genres = Option(row.getAs[Seq[String]]("genres")).getOrElse(Seq.empty)
                .map(_.trim)
                .filter(_.nonEmpty)
              if (genres.nonEmpty) fields += "genres" -> genres.mkString(",")

              if (fields.nonEmpty) {
                pipeline.hset(s"movie:$itemId:features", fields.asJava)
                pipeline.expire(s"movie:$itemId:features", contextTtlSeconds)
                pendingCommands += 2
                flushIfNeeded()
              }
            }
          } catch {
            case e: Exception =>
              log.warn("Skipping MovieLens movie context row: {}", e.getMessage)
          }
        }

        if (pendingCommands > 0) pipeline.sync()
      } finally {
        jedis.close()
      }
    }
  }

  private def putIfPresent(fields: scala.collection.mutable.Map[String, String], name: String, value: Any): Unit =
    Option(value).map(_.toString.trim).filter(_.nonEmpty).foreach(fields += name -> _)

  private def parseLong(raw: String): Long =
    Option(raw).flatMap(value => scala.util.Try(value.toLong).toOption).getOrElse(0L)

  private def parseDouble(raw: String): Double =
    Option(raw).flatMap(value => scala.util.Try(value.toDouble).toOption).getOrElse(0.0)

  private def mergeRecent(batchRecent: Seq[String], existingRaw: String, limit: Int): Seq[String] = {
    val existing = Option(existingRaw).toSeq
      .flatMap(_.split(","))
      .map(_.trim)
      .filter(_.nonEmpty)

    (batchRecent ++ existing)
      .map(_.trim)
      .filter(_.nonEmpty)
      .foldLeft(Vector.empty[String]) { (acc, item) =>
        if (acc.contains(item)) acc else acc :+ item
      }
      .take(limit)
  }
}
