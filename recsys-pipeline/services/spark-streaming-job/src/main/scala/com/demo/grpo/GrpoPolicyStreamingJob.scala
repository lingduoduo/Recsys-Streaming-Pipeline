package com.demo.grpo

import com.demo.process.RecommendationResponseStatsJob
import com.demo.engine.RedisPool
import com.demo.event.EventParsing
import com.demo.util.{DropMetrics, SparkSessions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/** GRPO trained continuously off the slate stream.
  *
  * Each micro-batch is a minibatch. Within it the ratio is measured against a snapshot of the
  * weights taken at the batch's start, NOT against the logged serving policy -- see applyBatch.
  */
object GrpoPolicyStreamingJob {

  private val JobName = "GrpoPolicyStreamingJob"
  private val log = LoggerFactory.getLogger(getClass)

  /** One micro-batch of learning.
    *
    * The inner-epoch loop is what makes clipping meaningful. The ratio is pi_theta / pi_snapshot,
    * where the snapshot is `current` frozen before the first step. Anchoring the ratio to the
    * LOGGED policy instead would be a silent failure in shadow mode: serving never changes there,
    * so the ratio would grow without bound as training proceeds, clipping would latch permanently
    * active, and the gradient would go to zero -- while every batch still looked healthy. The KL
    * term still anchors to the logged policy, which is what keeps the drift bounded.
    */
  def applyBatch(current: GrpoWeights, groups: Seq[GrpoGroup], cfg: GrpoJobConfig,
                 batchId: Long): GrpoWeights = {
    if (groups.isEmpty) return current.copy(batchId = batchId)

    val snapshot = current.weights.clone()
    var w = current.weights.clone()
    (1 to cfg.hyper.innerEpochs).foreach { _ =>
      val total = Array.fill(cfg.dim)(0.0)
      groups.foreach { g =>
        GrpoMath.advantages(g.rewards).foreach { adv =>
          // Ratio against the snapshot; KL against what actually served. GrpoMath.gradient takes
          // both references, so the two cannot be conflated here.
          val snapshotLogits =
            g.x.map(row => row.indices.foldLeft(0.0)((a, i) => a + row(i) * snapshot(i)))
          val grad = GrpoMath.gradient(g.x, snapshotLogits, g.logged, w, adv, cfg.hyper)
          (0 until cfg.dim).foreach(d => total(d) += grad(d))
        }
      }
      (0 until cfg.dim).foreach(d => w(d) -= cfg.hyper.learningRate * total(d) / groups.size)
    }
    GrpoWeights(w, cfg.featureVersion, batchId, current.slatesApplied + groups.size)
  }

  def main(args: Array[String]): Unit = {
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val inputTopic = sys.env.getOrElse("GRPO_INPUT_TOPIC", "training_experiences")
    val checkpointLocation =
      sys.env.getOrElse("SPARK_CHECKPOINT_LOCATION", "/tmp/spark-recsys/grpo-policy")
    val maxOffsetsPerTrigger = sys.env.getOrElse("MAX_OFFSETS_PER_TRIGGER", "5000")
    val triggerInterval = sys.env.getOrElse("TRIGGER_INTERVAL", "10 seconds")
    val cfg = GrpoJobConfig.fromEnv()

    val spark = SparkSessions.create(JobName)
    val pool = RedisPool.get(cfg.redisHost, cfg.redisPort, 8)

    // Refuse to start on a layout mismatch rather than applying stale weights to new features.
    var weights: GrpoWeights = {
      val jedis = pool.getResource
      try GrpoWeightStore.decode(jedis.hgetAll(cfg.weightsKey).asScala.toMap, cfg) match {
        case Right(w) => w
        case Left(reason) => throw new IllegalStateException(s"$JobName refusing to start: $reason")
      } finally jedis.close()
    }

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
      .load()

    raw.writeStream
      .foreachBatch { (batch: DataFrame, batchId: Long) =>
        val slates = EventParsing.fromJson(batch, RecommendationResponseStatsJob.SlateSchema)
        val (groups, counts) = GrpoSlates.toGroups(slates, cfg)
        log.info(DropMetrics.format(JobName, batchId, counts.kept, counts.reasons))

        weights = applyBatch(weights, groups, cfg, batchId)
        val jedis = pool.getResource
        // jedis.hset returns the field-count Long; discard it so the lambda's inferred type stays
        // (DataFrame, Long) => Unit, which is the only overload foreachBatch accepts.
        try { jedis.hset(cfg.weightsKey, GrpoWeightStore.encode(weights, System.currentTimeMillis())); () }
        finally jedis.close()
      }
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
      .awaitTermination()
  }
}
