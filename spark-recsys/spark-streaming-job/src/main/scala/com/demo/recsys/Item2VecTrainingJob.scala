package com.demo.recsys

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

object Item2VecTrainingJob {
  private val DefaultVectorSize = 10
  private val DefaultWindowSize = 5
  private val DefaultNumIterations = 10
  private val DefaultQueryItem = "592"
  private val DefaultNumSynonyms = 20
  private val DefaultRedisKeyPrefix = "i2vEmb"
  private val DefaultRedisTtlSeconds = 60 * 60 * 24
  private val DefaultRedisPipelineSize = 500

  def main(args: Array[String]): Unit = {
    val ratingsPath = args.headOption.orElse(sys.env.get("RATINGS_INPUT_PATH")).getOrElse {
      throw new IllegalArgumentException(
        "Missing ratings input path. Pass it as the first argument or set RATINGS_INPUT_PATH."
      )
    }
    val embeddingPath = args
      .lift(1)
      .orElse(sys.env.get("ITEM2VEC_EMBEDDING_PATH"))
      .getOrElse("spark-recsys/sampledata/embedding.txt")
    val queryItem = args.lift(2).orElse(sys.env.get("ITEM2VEC_QUERY_ITEM")).getOrElse(DefaultQueryItem)

    val spark = SparkSession.builder()
      .appName(sys.env.getOrElse("SPARK_APP_NAME", "Item2VecTrainingJob"))
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.shuffle.partitions", sys.env.getOrElse("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
      .getOrCreate()

    try {
      val samples = ItemSequencePreprocessingJob.processItemSequence(spark, ratingsPath)
      trainItem2vec(
        samples = samples,
        embeddingPath = embeddingPath,
        queryItem = queryItem,
        redisHost = sys.env.getOrElse("REDIS_HOST", "localhost"),
        redisPort = sys.env.get("REDIS_PORT").flatMap(toIntOption).getOrElse(6379),
        redisKeyPrefix = sys.env.getOrElse("ITEM2VEC_REDIS_KEY_PREFIX", DefaultRedisKeyPrefix),
        redisTtlSeconds = sys.env.get("ITEM2VEC_REDIS_TTL_SECONDS").flatMap(toIntOption).getOrElse(DefaultRedisTtlSeconds),
        saveToRedis = sys.env.getOrElse("ITEM2VEC_SAVE_TO_REDIS", "false").toBoolean
      )
    } finally {
      spark.stop()
    }
  }

  def trainItem2vec(samples: RDD[Seq[String]]): Unit = {
    // Mirror ItemSequencePreprocessingJob: try classpath resource dirs before falling back to a relative path.
    val defaultEmbeddingPath = sys.env
      .get("ITEM2VEC_EMBEDDING_PATH")
      .orElse(Option(getClass.getResource("/webroot/sampledata/")).map(_.getPath + "embedding.txt"))
      .orElse(Option(getClass.getResource("/sampledata/")).map(_.getPath + "embedding.txt"))
      .getOrElse("spark-recsys/sampledata/embedding.txt")

    trainItem2vec(
      samples = samples,
      embeddingPath = defaultEmbeddingPath,
      queryItem = sys.env.getOrElse("ITEM2VEC_QUERY_ITEM", DefaultQueryItem),
      redisHost = sys.env.getOrElse("REDIS_HOST", "localhost"),
      redisPort = sys.env.get("REDIS_PORT").flatMap(toIntOption).getOrElse(6379),
      redisKeyPrefix = sys.env.getOrElse("ITEM2VEC_REDIS_KEY_PREFIX", DefaultRedisKeyPrefix),
      redisTtlSeconds = sys.env.get("ITEM2VEC_REDIS_TTL_SECONDS").flatMap(toIntOption).getOrElse(DefaultRedisTtlSeconds),
      saveToRedis = sys.env.getOrElse("ITEM2VEC_SAVE_TO_REDIS", "false").toBoolean
    )
  }

  def trainItem2vec(
      samples: RDD[Seq[String]],
      embeddingPath: String,
      queryItem: String = DefaultQueryItem,
      vectorSize: Int = DefaultVectorSize,
      windowSize: Int = DefaultWindowSize,
      numIterations: Int = DefaultNumIterations,
      numSynonyms: Int = DefaultNumSynonyms,
      redisHost: String = "localhost",
      redisPort: Int = 6379,
      redisKeyPrefix: String = DefaultRedisKeyPrefix,
      redisTtlSeconds: Int = DefaultRedisTtlSeconds,
      saveToRedis: Boolean = false
  ): Unit = {
    val word2vec = new Word2Vec()
      .setVectorSize(vectorSize)
      .setWindowSize(windowSize)
      .setNumIterations(numIterations)

    val model = word2vec.fit(samples)
    val vectors = model.getVectors

    if (vectors.contains(queryItem)) {
      val synonyms = model.findSynonyms(queryItem, numSynonyms)
      for ((synonym, cosineSimilarity) <- synonyms) {
        println(s"$synonym $cosineSimilarity")
      }
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

  private def toIntOption(value: String): Option[Int] =
    try Some(value.toInt)
    catch { case _: NumberFormatException => None }
}
