package com.demo.process

import com.demo.engine.RedisPool
import com.demo.event.{EventParsing, FieldGate, Gated}
import com.demo.util.{BatchMetricsListener, DropMetrics, Env, SparkSessions}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

/** Derives per-impression **relevance records** (query → document, with a score) from
  * `training_samples`, joined with movie text metadata from Redis, and emits them to the
  * `relevance_samples` Kafka topic:
  *   query (user_id:session_id, or user_id:request_id when sessionless), event_ts (time),
  *   recommended_movie_id,
  *   title, genres, release_year (from Redis movie:{id}:features), score (the click/order label).
  *
  * Personalized-retrieval framing: the (user, session) is the query, the recommended movie is the
  * document, and the engagement label is the graded relevance score.
  */
object RelevanceSampleStreamingJob {

  private val JobName = "RelevanceSampleStreamingJob"

  /** The relevance query key: one per session, falling back to one per slate.
    *
    * The joiner emits `coalesce(session_id, "")`, so a sessionless sample carries an empty string
    * rather than a null and every such impression for a user would collapse into a single query —
    * inflating that query's candidate set and distorting nDCG. `request_id` is one-per-slate and
    * non-null in `TrainingSampleSchema`, which is the right unit when no session groups them. */
  private[process] def relevanceQuery: Column =
    when(length(coalesce(col("session_id"), lit(""))) > 0,
      concat_ws(":", col("user_id"), col("session_id")))
      .otherwise(concat_ws(":", col("user_id"), col("request_id")))

  /** Reshape training samples into relevance rows, attaching movie metadata from `features`
    * (schema: item_id, title, genres, release_year — see `fetchMovieFeaturesDf`).
    *
    * Must be a LEFT join: `fetchMovieFeaturesDf` omits ids with a missing/empty Redis hash, so an
    * inner join here would silently drop every impression for an item Redis doesn't know about —
    * exactly the items least likely to be noticed missing downstream. `genres` is coalesced to an
    * empty array because a genuinely missing feature row must still read as "no genres", not null,
    * matching what a present-but-genre-less hash already produces. */
  def buildRelevanceSamples(samples: DataFrame, features: DataFrame): DataFrame =
    samples.join(features, Seq("item_id"), "left")
      .select(
        relevanceQuery.as("query"),
        col("impression_ts").as("event_ts"),
        col("item_id").as("recommended_movie_id"),
        col("title"),
        coalesce(col("genres"), typedLit(Seq.empty[String])).as("genres"),
        col("release_year"),
        col("label").as("score")  // graded relevance: click -> 1.0, order -> 2.0, else 0.0
      )

  def parseSamples(rawKafka: DataFrame): Gated =
    FieldGate(
      EventParsing.fromJson(rawKafka, ExperienceCollectorStreamingJob.TrainingSampleSchema),
      Seq(
        "null_user_id" -> col("user_id").isNull,
        "null_item_id" -> col("item_id").isNull
      ))

  private val FeaturesSchema: StructType =
    StructType(Seq(
      StructField("item_id", StringType),
      StructField("title", StringType),
      StructField("genres", ArrayType(StringType)),
      StructField("release_year", IntegerType)
    ))

  /** Pure: one already-fetched Redis hash → one (item_id, title, genres, release_year) row. Single
    * source of truth for the per-row derivation, used by `fetchMovieFeaturesDf` below. `genres`
    * defaults to an empty (not null) sequence, and a non-numeric `releaseYear` yields a null year
    * rather than throwing — both match the UDF-based behaviour this replaces. */
  def featuresRow(id: String, h: Map[String, String]): Row = {
    val genres = h.get("genres").map(_.split(",").filter(_.nonEmpty).toSeq).getOrElse(Seq.empty[String])
    val year: java.lang.Integer =
      h.get("releaseYear").flatMap(s => scala.util.Try(s.toInt).toOption).map(Int.box).orNull
    Row(id, h.get("title").orNull, genres, year)
  }

  /** Pure: a raw (possibly null/empty) Redis hash → at most one row. "Missing keys omitted" lives
    * here so it is testable without Redis, and is enforced identically to `MovieCategoryReportJob`. */
  def featuresRowOrNone(id: String, h: java.util.Map[String, String]): Option[Row] =
    if (h == null || h.isEmpty) None else Some(featuresRow(id, h.asScala.toMap))

  /** Executor-side, pipelined replacement for the driver-side fetch-then-collect pair previously
    * used inside `foreachBatch`: that old path ran a collect() plus one serial HGETALL per item on
    * the driver every micro-batch, forever. This reads `movie:{id}:features` in parallel across
    * partitions, one pooled Jedis connection per partition (`RedisPool` — one JedisPool per
    * executor JVM; REDIS_POOL_MAX_TOTAL should be at least the executor core count, since it now
    * bounds per-executor concurrency rather than only a driver-side pool), batching HGETALLs
    * through `jedis.pipelined()` so N items cost O(partitions) round trips instead of N sequential
    * ones on the driver, on every batch. Missing/empty hashes are omitted; `buildRelevanceSamples`
    * LEFT joins against this so such items still emit a row.
    */
  def fetchMovieFeaturesDf(ids: DataFrame, host: String, port: Int, poolMax: Int,
                            pipelineSize: Int): DataFrame = {
    val rowRdd = ids.rdd.mapPartitions { partitionRows =>
      val partitionIds = partitionRows.map(_.getString(0)).toList
      if (partitionIds.isEmpty) Iterator.empty
      else {
        val jedis = RedisPool.get(host, port, poolMax).getResource
        try {
          // Eagerly build the full result before returning: mapPartitions hands Spark a lazy
          // iterator, and closing `jedis` in `finally` would run before Spark ever consumes a
          // lazy iterator, killing the connection mid-read. Materializing into `results` here
          // means every Redis call happens inside this try, before close() runs — so it is safe
          // to close right after.
          val results = scala.collection.mutable.ArrayBuffer.empty[Row]
          partitionIds.grouped(pipelineSize).foreach { chunk =>
            val pipeline = jedis.pipelined()
            val pending = chunk.map(id => id -> pipeline.hgetAll(s"movie:$id:features"))
            pipeline.sync()
            pending.foreach { case (id, response) =>
              featuresRowOrNone(id, response.get()).foreach(results += _)
            }
          }
          results.iterator
        } finally jedis.close()
      }
    }
    ids.sparkSession.createDataFrame(rowRdd, FeaturesSchema)
  }

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("RELEVANCE_INPUT_TOPIC", "training_samples")
    val outputTopic = sys.env.getOrElse("RELEVANCE_OUTPUT_TOPIC", "relevance_samples")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/relevance-samples"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = Env.int("REDIS_PORT", 6379)
    val redisPoolMax = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val redisPipelineSize = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))

    val spark: SparkSession = SparkSessions.create("RelevanceSampleStreamingJob")
    BatchMetricsListener.register(spark)

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"))
      .option("kafka.group.id", sys.env.getOrElse("KAFKA_GROUP_ID", "relevance-samples"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (raw: DataFrame, batchId: Long) =>
        val gated = parseSamples(raw)
        val tagged = gated.tagged.persist(StorageLevel.MEMORY_AND_DISK_SER)
        try {
        val batch = DropMetrics.report(gated.copy(tagged = tagged), JobName, batchId)
        val ids = batch.select("item_id").distinct()
        val features = fetchMovieFeaturesDf(ids, redisHost, redisPort, redisPoolMax, redisPipelineSize)

        val relevance = buildRelevanceSamples(batch, features)
        relevance
          .select(
            concat_ws(":", col("query"), col("recommended_movie_id")).as("key"),
            to_json(struct(relevance.columns.map(col): _*)).as("value")
          )
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBootstrapServers)
          .option("topic", outputTopic)
          .save()
        } finally tagged.unpersist()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
