package com.demo.process

import com.demo.event.EventParsing
import com.demo.engine.RedisPool
import com.demo.util.{BatchMetricsListener, Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

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

  /** Reshape training samples into ranking rows, attaching embeddings from the given lookups. */
  def buildRankingSamples(
      samples: DataFrame,
      userEmb: Map[String, Seq[Double]],
      itemEmb: Map[String, Seq[Double]]
  ): DataFrame = {
    val uEmbUdf = udf((u: String) => userEmb.getOrElse(u, Seq.empty[Double]))
    val iEmbUdf = udf((i: String) => itemEmb.getOrElse(i, Seq.empty[Double]))
    samples.select(
      col("user_id"),
      coalesce(col("user_features"), typedLit(Map.empty[String, String])).as("user_features"),
      uEmbUdf(col("user_id")).as("user_embedding"),
      col("session_id"),
      col("impression_ts").as("event_ts"),
      col("item_id").as("recommended_movie_id"),
      coalesce(col("item_features"), typedLit(Map.empty[String, String])).as("item_features"),
      iEmbUdf(col("item_id")).as("item_embedding"),
      (col("clicked") === 1).as("is_click"),
      col("label").as("rating")  // implicit label: click -> 1.0, order -> 2.0, else 0.0
    )
  }

  def parseSamples(rawKafka: DataFrame): DataFrame =
    EventParsing.fromJson(rawKafka, ExperienceCollectorStreamingJob.TrainingSampleSchema)
      .filter(col("user_id").isNotNull && col("item_id").isNotNull)

  /** Driver-side Redis lookup of `prefix:id` embeddings (space-separated floats). Missing keys are
    * omitted (buildRankingSamples then emits an empty vector for them). */
  def fetchEmbeddings(ids: Array[String], prefix: String,
                      host: String, port: Int, poolMax: Int): Map[String, Seq[Double]] = {
    if (ids.isEmpty) return Map.empty
    val jedis = RedisPool.get(host, port, poolMax).getResource
    try {
      ids.flatMap { id =>
        Option(jedis.get(s"$prefix:$id")).filter(_.trim.nonEmpty).map { raw =>
          id -> raw.trim.split("\\s+").flatMap(t => scala.util.Try(t.toDouble).toOption).toSeq
        }
      }.toMap
    } finally jedis.close()
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

    parseSamples(raw).writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
        val users = batch.select("user_id").distinct().collect().flatMap(r => Option(r.getString(0)))
        val items = batch.select("item_id").distinct().collect().flatMap(r => Option(r.getString(0)))
        val userEmb = fetchEmbeddings(users, userPrefix, redisHost, redisPort, redisPoolMax)
        val itemEmb = fetchEmbeddings(items, itemPrefix, redisHost, redisPort, redisPoolMax)

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
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
