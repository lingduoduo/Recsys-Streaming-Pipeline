package com.demo.recsys

import java.io.{BufferedWriter, File, FileWriter}

import com.demo.common.{Env, SparkSessions}
import org.apache.spark.ml.feature.{Word2Vec => MLWord2Vec}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

object Item2VecTrainingJob {
  private val DefaultVectorSize = 10
  private val DefaultWindowSize = 5
  private val DefaultNumIterations = 10
  private val DefaultMinCount = 1
  private val DefaultQueryItem = "592"
  private val DefaultNumSynonyms = 20
  private val DefaultRedisKeyPrefix = "i2vEmb"
  private val DefaultRedisTtlSeconds = 60 * 60 * 24
  private val DefaultRedisPipelineSize = 500

  def main(args: Array[String]): Unit = {
    val ratingsPath = Env.requiredArgOrEnv(args, 0, "RATINGS_INPUT_PATH", "ratings input path")
    val embeddingPath = Env.argOrEnv(args, 1, "ITEM2VEC_EMBEDDING_PATH")
      .getOrElse("spark-recsys/sampledata/embedding.txt")
    val queryItem = Env.argOrEnv(args, 2, "ITEM2VEC_QUERY_ITEM").getOrElse(DefaultQueryItem)

    val spark = SparkSessions.create("Item2VecTrainingJob")

    try {
      val samples = ItemSequencePreprocessingJob.processItemSequenceDataFrame(spark, ratingsPath)
      trainItem2vec(
        samples = samples,
        embeddingPath = embeddingPath,
        queryItem = queryItem,
        redisHost = sys.env.getOrElse("REDIS_HOST", "localhost"),
        redisPort = Env.int("REDIS_PORT", 6379),
        redisKeyPrefix = sys.env.getOrElse("ITEM2VEC_REDIS_KEY_PREFIX", DefaultRedisKeyPrefix),
        redisTtlSeconds = Env.int("ITEM2VEC_REDIS_TTL_SECONDS", DefaultRedisTtlSeconds),
        minCount = Env.int("ITEM2VEC_MIN_COUNT", DefaultMinCount),
        saveToRedis = Env.boolean("ITEM2VEC_SAVE_TO_REDIS", default = false)
      )
    } finally {
      spark.stop()
    }
  }

  def trainItem2vec(
      samples: DataFrame,
      embeddingPath: String,
      queryItem: String = DefaultQueryItem,
      vectorSize: Int = DefaultVectorSize,
      windowSize: Int = DefaultWindowSize,
      numIterations: Int = DefaultNumIterations,
      numSynonyms: Int = DefaultNumSynonyms,
      minCount: Int = DefaultMinCount,
      redisHost: String = "localhost",
      redisPort: Int = 6379,
      redisKeyPrefix: String = DefaultRedisKeyPrefix,
      redisTtlSeconds: Int = DefaultRedisTtlSeconds,
      saveToRedis: Boolean = false
  ): Unit = {
    val word2vec = new MLWord2Vec()
      .setInputCol("movieIds")
      .setOutputCol("_ignored")
      .setVectorSize(vectorSize)
      .setWindowSize(windowSize)
      .setMaxIter(numIterations)
      .setMinCount(minCount)

    val model = word2vec.fit(samples)
    val vectors: Map[String, Array[Float]] = model.getVectors
      .collect()
      .map { row => row.getString(0) -> row.getAs[MLVector](1).toDense.values.map(_.toFloat) }
      .toMap

    if (vectors.contains(queryItem)) {
      vectors.toSeq
        .map { case (word, vec) =>
          val dot = vec.zip(vectors(queryItem)).map { case (a, b) => a * b }.sum
          word -> dot
        }
        .filter(_._1 != queryItem)
        .sortBy(-_._2)
        .take(numSynonyms)
        .foreach { case (synonym, score) => println(s"$synonym $score") }
    } else {
      println(s"Query item '$queryItem' was not found in the trained Item2Vec vocabulary.")
    }

    writeEmbeddings(embeddingPath, vectors)

    if (saveToRedis) {
      writeEmbeddingsToRedis(redisHost, redisPort, redisKeyPrefix, redisTtlSeconds, vectors)
    }
  }

  private def writeEmbeddings(embeddingPath: String, vectors: Map[String, Array[Float]]): Unit = {
    val file = new File(embeddingPath)
    Option(file.getParentFile).foreach(_.mkdirs())

    val bw = new BufferedWriter(new FileWriter(file))
    try {
      for (itemId <- vectors.keys.toSeq.sorted) {
        bw.write(s"$itemId:${vectors(itemId).mkString(" ")}\n")
      }
    } finally {
      bw.close()
    }
  }

  // Writes embeddings to Redis as  i2vEmb:{itemId} → "0.123 -0.456 ..."  with a TTL.
  // Uses pipelining to avoid one round-trip per item.
  private def writeEmbeddingsToRedis(
      redisHost: String,
      redisPort: Int,
      keyPrefix: String,
      ttlSeconds: Int,
      vectors: Map[String, Array[Float]],
      pipelineSize: Int = DefaultRedisPipelineSize
  ): Unit = {
    val jedis = new Jedis(redisHost, redisPort)
    try {

      val pipeline = jedis.pipelined()
      val params = SetParams.setParams().ex(ttlSeconds)
      var count = 0
      for ((itemId, vector) <- vectors) {
        pipeline.set(s"$keyPrefix:$itemId", vector.mkString(" "), params)
        count += 1
        if (count % pipelineSize == 0) pipeline.sync()
      }
      if (count % pipelineSize != 0) pipeline.sync()
    } finally {
      jedis.close()
    }
  }
}
