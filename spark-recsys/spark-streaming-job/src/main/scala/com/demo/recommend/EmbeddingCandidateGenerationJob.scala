package com.demo.recommend

import com.demo.util.{Env, SparkSessions}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.util.Try

/**
 * Batch embedding-based candidate generation: three steps.
 *
 *   Step 1 — load pre-computed user embeddings (output of UserEmbeddingTrainingJob or
 *             AlsEmbeddingTrainingJob) in "id:v1 v2 ..." text format.
 *   Step 2 — broadcast the full item embedding catalog to every executor; each user partition
 *             computes cosine similarity against every item locally with no cross join or shuffle.
 *   Step 3 — sort by similarity descending, take topK, emit (userId, candidateItems, scores).
 *             Optionally write candidate lists to Redis as `{prefix}:{userId}:candidates`.
 */
object EmbeddingCandidateGenerationJob {
  private val DefaultTopK            = 100
  private val DefaultRedisTtlSeconds = 60 * 60 * 24
  private val DefaultPipelineSize    = 500

  def main(args: Array[String]): Unit = {
    val userEmbPath = Env.requiredArgOrEnv(args, 0, "USER_EMBEDDING_PATH", "user embedding path")
    val itemEmbPath = Env.requiredArgOrEnv(args, 1, "ITEM_EMBEDDING_PATH", "item embedding path")
    val topK        = Env.int("CANDIDATE_TOP_K", DefaultTopK)
    val outputPath  = Env.argOrEnv(args, 2, "CANDIDATE_OUTPUT_PATH")
    val saveToRedis = Env.boolean("CANDIDATE_SAVE_TO_REDIS", default = false)

    val spark = SparkSessions.create("EmbeddingCandidateGenerationJob")
    try {
      val candidates = topKCandidates(
        spark,
        readEmbeddings(spark, userEmbPath),
        readEmbeddings(spark, itemEmbPath),
        topK
      )

      outputPath.foreach(path => candidates.write.mode("overwrite").parquet(path))

      if (saveToRedis) {
        writeCandidatesToRedis(
          candidates = candidates,
          redisHost  = sys.env.getOrElse("REDIS_HOST", "localhost"),
          redisPort  = Env.int("REDIS_PORT", 6379),
          keyPrefix  = sys.env.getOrElse("CANDIDATE_REDIS_KEY_PREFIX", "user"),
          ttlSeconds = Env.int("CANDIDATE_REDIS_TTL_SECONDS", DefaultRedisTtlSeconds)
        )
      }
    } finally {
      spark.stop()
    }
  }

  def topKCandidates(
      spark: SparkSession,
      userEmbeddings: DataFrame,
      itemEmbeddings: DataFrame,
      topK: Int
  ): DataFrame = {
    import spark.implicits._

    // Step 2: collect item catalog into a broadcast map — safe when items × dim fits in memory
    // (100k items × 16-dim float = ~6 MB; typical catalog is well within executor heap).
    val itemVecMap: Map[String, Array[Double]] = itemEmbeddings
      .collect()
      .map(r => r.getAs[String]("id") -> r.getAs[Seq[Double]]("vector").toArray)
      .toMap
    val broadcast = spark.sparkContext.broadcast(itemVecMap)

    // Step 1 + 3: for each user partition, score every item via cosine similarity and collect
    // the top-K results using a min-heap — O(n log k) time, allocates only k tuples instead of n.
    userEmbeddings.mapPartitions { rows =>
      val items = broadcast.value
      rows.flatMap { row =>
        val userId = row.getAs[String]("id")
        val uVec   = row.getAs[Seq[Double]]("vector").toArray
        if (uVec.isEmpty || items.isEmpty) None
        else {
          val heap = mutable.PriorityQueue.empty[(String, Double)](Ordering.by(_._2).reverse)
          items.foreach { case (itemId, iVec) =>
            val score = cosine(uVec, iVec)
            if (heap.size < topK) heap.enqueue((itemId, score))
            else if (score > heap.head._2) { heap.dequeue(); heap.enqueue((itemId, score)) }
          }
          val topItems = heap.toSeq.sortBy(-_._2)
          Some((userId, topItems.map(_._1), topItems.map(_._2)))
        }
      }
    }.toDF("userId", "candidateItems", "scores")
  }

  /** Read embeddings written by Item2VecTrainingJob / UserEmbeddingTrainingJob / AlsEmbeddingTrainingJob.
   *  Expected line format: `id:v1 v2 v3 ...`
   */
  def readEmbeddings(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._
    spark.read.textFile(path).flatMap { line =>
      val sep = line.indexOf(':')
      if (sep <= 0) None
      else Try {
        val id  = line.substring(0, sep).trim
        val vec = line.substring(sep + 1).trim.split("\\s+").map(_.toDouble).toSeq
        (id, vec)
      }.toOption
    }.toDF("id", "vector")
  }

  private[recommend] def cosine(u: Array[Double], v: Array[Double]): Double = {
    var dot = 0.0; var nu = 0.0; var nv = 0.0
    var i = 0; val len = math.min(u.length, v.length)
    while (i < len) {
      dot += u(i) * v(i); nu += u(i) * u(i); nv += v(i) * v(i)
      i += 1
    }
    if (nu == 0.0 || nv == 0.0) 0.0 else dot / (math.sqrt(nu) * math.sqrt(nv))
  }

  private def writeCandidatesToRedis(
      candidates: DataFrame,
      redisHost: String,
      redisPort: Int,
      keyPrefix: String,
      ttlSeconds: Int,
      pipelineSize: Int = DefaultPipelineSize
  ): Unit = {
    candidates.foreachPartition { rows: Iterator[Row] =>
      val jedis = new Jedis(redisHost, redisPort)
      try {
        val pipeline = jedis.pipelined()
        var count = 0
        rows.foreach { row =>
          val userId = row.getAs[String]("userId")
          val items  = row.getAs[Seq[String]]("candidateItems")
          val key    = s"$keyPrefix:$userId:candidates"
          pipeline.del(key)
          if (items.nonEmpty) pipeline.rpush(key, items.toArray: _*)
          pipeline.expire(key, ttlSeconds)
          count += 3
          if (count >= pipelineSize) { pipeline.sync(); count = 0 }
        }
        if (count > 0) pipeline.sync()
      } finally {
        jedis.close()
      }
    }
  }
}
