package com.demo.task

import java.io.{BufferedWriter, File, FileWriter}

import com.demo.process.ItemSequencePreprocessingJob
import com.demo.sink.RedisWriter
import com.demo.util.{Env, SparkSessions}
import org.apache.spark.ml.feature.{Word2Vec => MLWord2Vec}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Item2VecTrainingJob {
  private val DefaultVectorSize      = 10
  private val DefaultWindowSize      = 5
  private val DefaultNumIterations   = 10
  private val DefaultMinCount        = 1
  private val DefaultQueryItem       = "592"
  private val DefaultNumSynonyms     = 20
  private val DefaultRedisKeyPrefix  = "i2vEmb"
  private val DefaultRedisTtlSeconds = 60 * 60 * 24

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
      .setOutputCol("embedding")
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
      model.findSynonyms(queryItem, numSynonyms)
        .collect()
        .foreach { row => println(s"${row.getString(0)} ${row.getDouble(1)}") }
    } else {
      println(s"Query item '$queryItem' was not found in the trained Item2Vec vocabulary.")
    }

    writeEmbeddings(embeddingPath, vectors)

    if (saveToRedis) {
      RedisWriter.writeWithPipeline(
        redisHost, redisPort,
        vectors.iterator.map { case (id, vec) => id -> vec.mkString(" ") },
        redisKeyPrefix, redisTtlSeconds
      )
    }
  }

  private def writeEmbeddings(embeddingPath: String, vectors: Map[String, Array[Float]]): Unit = {
    val file = new File(embeddingPath)
    Option(file.getParentFile).foreach(_.mkdirs())
    val bw = new BufferedWriter(new FileWriter(file))
    try {
      vectors.foreach { case (itemId, vec) =>
        bw.write(s"$itemId:${vec.mkString(" ")}\n")
      }
    } finally {
      bw.close()
    }
  }
}
