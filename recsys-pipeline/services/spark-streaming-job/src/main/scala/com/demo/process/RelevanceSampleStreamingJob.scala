package com.demo.process

import com.demo.event.EventParsing
import com.demo.engine.RedisPool
import com.demo.util.{BatchMetricsListener, Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

/** Derives per-impression **relevance records** (query → document, with a score) from
  * `training_samples`, joined with movie text metadata from Redis, and emits them to the
  * `relevance_samples` Kafka topic:
  *   query (user_id:session_id), event_ts (time), recommended_movie_id,
  *   title, genres, release_year (from Redis movie:{id}:features), score (the click/order label).
  *
  * Personalized-retrieval framing: the (user, session) is the query, the recommended movie is the
  * document, and the engagement label is the graded relevance score.
  */
object RelevanceSampleStreamingJob {

  /** Reshape training samples into relevance rows, attaching movie metadata from the lookups. */
  def buildRelevanceSamples(
      samples: DataFrame,
      titleMap: Map[String, String],
      genresMap: Map[String, String],
      yearMap: Map[String, String]
  ): DataFrame = {
    val titleUdf  = udf((i: String) => titleMap.get(i).orNull)
    val genresUdf = udf((i: String) =>
      genresMap.get(i).map(_.split(",").filter(_.nonEmpty).toSeq).getOrElse(Seq.empty[String]))
    val yearUdf   = udf((i: String) => yearMap.get(i).flatMap(s => scala.util.Try(s.toInt).toOption))
    samples.select(
      concat_ws(":", col("user_id"), coalesce(col("session_id"), lit(""))).as("query"),
      col("impression_ts").as("event_ts"),
      col("item_id").as("recommended_movie_id"),
      titleUdf(col("item_id")).as("title"),
      genresUdf(col("item_id")).as("genres"),
      yearUdf(col("item_id")).as("release_year"),
      col("label").as("score")  // graded relevance: click -> 1.0, order -> 2.0, else 0.0
    )
  }

  def parseSamples(rawKafka: DataFrame): DataFrame =
    EventParsing.fromJson(rawKafka, ExperienceCollectorStreamingJob.TrainingSampleSchema)
      .filter(col("user_id").isNotNull && col("item_id").isNotNull)

  /** Driver-side Redis HGETALL of `movie:{id}:features`; missing keys omitted. */
  def fetchMovieFeatures(ids: Array[String], host: String, port: Int, poolMax: Int): Map[String, Map[String, String]] = {
    if (ids.isEmpty) return Map.empty
    val jedis = RedisPool.get(host, port, poolMax).getResource
    try {
      ids.flatMap { id =>
        val h = jedis.hgetAll(s"movie:$id:features")
        if (h == null || h.isEmpty) None else Some(id -> h.asScala.toMap)
      }.toMap
    } finally jedis.close()
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

    parseSamples(raw).writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val items = batch.select("item_id").distinct().collect().flatMap(r => Option(r.getString(0)))
        val feats = fetchMovieFeatures(items, redisHost, redisPort, redisPoolMax)
        val titleMap  = feats.flatMap { case (id, h) => h.get("title").map(id -> _) }
        val genresMap = feats.flatMap { case (id, h) => h.get("genres").map(id -> _) }
        val yearMap   = feats.flatMap { case (id, h) => h.get("releaseYear").map(id -> _) }

        val relevance = buildRelevanceSamples(batch, titleMap, genresMap, yearMap)
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
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
