package com.demo.sink

import redis.clients.jedis.Jedis
import redis.clients.jedis.params.SetParams

object RedisWriter {
  val DefaultPipelineSize: Int = 500

  /** Write key-value pairs to Redis with a TTL, using pipelining to batch round-trips. */
  def writeWithPipeline(
      redisHost: String,
      redisPort: Int,
      pairs: Iterator[(String, String)],
      keyPrefix: String,
      ttlSeconds: Int,
      pipelineSize: Int = DefaultPipelineSize
  ): Unit = {
    val jedis = new Jedis(redisHost, redisPort)
    try {
      val pipeline = jedis.pipelined()
      val params   = SetParams.setParams().ex(ttlSeconds)
      var count    = 0
      pairs.foreach { case (id, value) =>
        pipeline.set(s"$keyPrefix:$id", value, params)
        count += 1
        if (count % pipelineSize == 0) pipeline.sync()
      }
      if (count % pipelineSize != 0) pipeline.sync()
    } finally {
      jedis.close()
    }
  }
}
