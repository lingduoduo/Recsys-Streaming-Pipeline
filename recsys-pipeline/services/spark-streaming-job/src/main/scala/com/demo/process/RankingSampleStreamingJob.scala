package com.demo.process

import com.demo.engine.RedisPool
import com.demo.event.{EventParsing, FieldGate, Gated}
import com.demo.util.{BatchMetricsListener, DropMetrics, Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

/** Derives per-impression **ranking records** from `training_samples`, enriched with the
  * user/item embedding vectors from Redis, and emits them to the `ranking_samples` Kafka topic:
  *   user_id, user_features (map), user_embedding (vector),
  *   session_id, event_ts (time),
  *   recommended_movie_id, item_features (map), item_embedding (vector),
  *   is_click (boolean), rating (the click/order label).
  *
  * Source: the OnlineJoiner output `training_samples` (already joins a slate's impressions +
  * feedback per (request, user, item) and carries session_id, label, feature maps). Embeddings
  * are looked up per batch from Redis (`uEmb:{user}`, `i2vEmb:{item}`, space-separated floats,
  * written by the offline embedding jobs).
  */
object RankingSampleStreamingJob {

  private val JobName = "RankingSampleStreamingJob"

  /** Reshape training samples into ranking rows, attaching embeddings from the given lookup
    * DataFrames (schema: id, embedding — see `fetchEmbeddingsDf`).
    *
    * Must be LEFT joins: `fetchEmbeddingsDf` omits ids with a missing/empty Redis value, so an
    * inner join here would silently drop every impression for a user/item Redis doesn't know
    * about — exactly the rows least likely to be noticed. `*_embedding` is coalesced to an empty
    * array because a genuinely missing embedding must still read as "no embedding", not null,
    * matching what the old map-lookup's `getOrElse(Seq.empty)` already produced. */
  def buildRankingSamples(
      samples: DataFrame,
      userEmbeddings: DataFrame,
      itemEmbeddings: DataFrame
  ): DataFrame = {
    val users = userEmbeddings.withColumnRenamed("id", "user_id")
      .withColumnRenamed("embedding", "user_embedding")
    val items = itemEmbeddings.withColumnRenamed("id", "item_id")
      .withColumnRenamed("embedding", "item_embedding")
    samples
      .join(users, Seq("user_id"), "left")
      .join(items, Seq("item_id"), "left")
      .select(
        col("user_id"),
        coalesce(col("user_features"), typedLit(Map.empty[String, String])).as("user_features"),
        coalesce(col("user_embedding"), typedLit(Seq.empty[Double])).as("user_embedding"),
        col("session_id"),
        col("impression_ts").as("event_ts"),
        col("item_id").as("recommended_movie_id"),
        coalesce(col("item_features"), typedLit(Map.empty[String, String])).as("item_features"),
        coalesce(col("item_embedding"), typedLit(Seq.empty[Double])).as("item_embedding"),
        (col("clicked") === 1).as("is_click"),
        col("label").as("rating")  // implicit label: click -> 1.0, order -> 2.0, else 0.0
      )
  }

  def parseSamples(rawKafka: DataFrame): Gated =
    FieldGate(
      EventParsing.fromJson(rawKafka, ExperienceCollectorStreamingJob.TrainingSampleSchema),
      Seq(
        "null_user_id" -> col("user_id").isNull,
        "null_item_id" -> col("item_id").isNull
      ))

  private val EmbeddingSchema: StructType =
    StructType(Seq(
      StructField("id", StringType),
      StructField("embedding", ArrayType(DoubleType))
    ))

  /** Pure: one already-fetched Redis value (space-separated floats) -> one (id, embedding) row.
    * Single source of truth for the per-row derivation, used by `fetchEmbeddingsDf` below.
    * Non-numeric tokens are dropped rather than throwing, matching the driver-side path this
    * replaces. */
  def embeddingRow(id: String, raw: String): Row =
    Row(id, raw.trim.split("\\s+").flatMap(t => scala.util.Try(t.toDouble).toOption).toSeq)

  /** Pure: a raw (possibly null/blank) Redis value -> at most one row. "Missing keys omitted"
    * lives here so it is testable without Redis. */
  def embeddingRowOrNone(id: String, raw: String): Option[Row] =
    if (raw == null || raw.trim.isEmpty) None else Some(embeddingRow(id, raw))

  /** Executor-side, pipelined replacement for the driver-side fetch-then-collect pair previously
    * used inside `foreachBatch`: that old path ran two collect()s plus one serial GET per id on
    * the driver every micro-batch (once for users, once for items). This reads `prefix:{id}` in
    * parallel across partitions, one pooled Jedis connection per partition (`RedisPool` — one
    * JedisPool per executor JVM; REDIS_POOL_MAX_TOTAL should be at least the executor core count,
    * since it now bounds per-executor concurrency rather than only a driver-side pool), batching
    * GETs through `jedis.pipelined()` so N ids cost O(partitions) round trips instead of N
    * sequential ones on the driver, on every batch. Missing ids are omitted; `buildRankingSamples`
    * LEFT joins against this so such rows still emit with an empty embedding.
    */
  def fetchEmbeddingsDf(ids: DataFrame, prefix: String, host: String, port: Int, poolMax: Int,
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
            val pending = chunk.map(id => id -> pipeline.get(s"$prefix:$id"))
            pipeline.sync()
            pending.foreach { case (id, response) =>
              embeddingRowOrNone(id, response.get()).foreach(results += _)
            }
          }
          results.iterator
        } finally jedis.close()
      }
    }
    ids.sparkSession.createDataFrame(rowRdd, EmbeddingSchema)
  }

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("RANKING_INPUT_TOPIC", "training_samples")
    val outputTopic = sys.env.getOrElse("RANKING_OUTPUT_TOPIC", "ranking_samples")
    val checkpointLocation = sys.env.getOrElse(
      "SPARK_CHECKPOINT_LOCATION",
      "/tmp/spark-recsys/ranking-samples"
    )
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")
    val redisHost = sys.env.getOrElse("REDIS_HOST", "localhost")
    val redisPort = Env.int("REDIS_PORT", 6379)
    val redisPoolMax = math.max(1, Env.int("REDIS_POOL_MAX_TOTAL", 8))
    val redisPipelineSize = math.max(3, Env.int("REDIS_PIPELINE_SIZE", 500))
    val userPrefix = sys.env.getOrElse("USER_EMBEDDING_PREFIX", "uEmb")
    val itemPrefix = sys.env.getOrElse("ITEM_EMBEDDING_PREFIX", "i2vEmb")

    val spark: SparkSession = SparkSessions.create("RankingSampleStreamingJob")
    BatchMetricsListener.register(spark)

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", sys.env.getOrElse("KAFKA_STARTING_OFFSETS", "earliest"))
      .option("kafka.group.id", sys.env.getOrElse("KAFKA_GROUP_ID", "ranking-samples"))
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (raw: DataFrame, batchId: Long) =>
        val gated = parseSamples(raw)
        val tagged = gated.tagged.persist(StorageLevel.MEMORY_AND_DISK_SER)
        try {
        val batch = DropMetrics.report(gated.copy(tagged = tagged), JobName, batchId)
        val users = batch.select("user_id").distinct()
        val items = batch.select("item_id").distinct()
        val userEmb = fetchEmbeddingsDf(users, userPrefix, redisHost, redisPort, redisPoolMax, redisPipelineSize)
        val itemEmb = fetchEmbeddingsDf(items, itemPrefix, redisHost, redisPort, redisPoolMax, redisPipelineSize)

        val ranking = buildRankingSamples(batch, userEmb, itemEmb)
        ranking
          .select(
            concat_ws(":", col("user_id"), coalesce(col("session_id"), lit("")),
              col("recommended_movie_id")).as("key"),
            to_json(struct(ranking.columns.map(col): _*)).as("value")
          )
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaBootstrapServers)
          .option("topic", outputTopic)
          .save()
        } finally tagged.unpersist()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
