package com.demo.profile

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import redis.clients.jedis.{Jedis, Response}
import redis.clients.jedis.params.SetParams

/** Narrow Redis boundary that keeps publication-order tests independent of Redis. */
trait RedisProfileStore extends Serializable {
  def writeProfiles(runId: String, values: Iterator[(String, String)], ttlSeconds: Int): Unit
  def activate(runId: String): Unit
}

/** Internal seam for exercising deferred Redis command responses without a Redis server. */
private[profile] trait RedisProfileCommandResponse {
  def requireSuccess(): Unit
}

private[profile] trait RedisProfilePipeline {
  def set(key: String, value: String, ttlSeconds: Int): RedisProfileCommandResponse
  def sync(): Unit
}

final case class RedisProfileConfig(
    host: String = "localhost",
    port: Int = 6379,
    ttlSeconds: Int = 86400,
    keyPrefix: String = "user-profile:v1"
) {
  require(host.nonEmpty, "Redis host must be non-empty")
  require(port > 0 && port <= 65535, "Redis port must be between 1 and 65535")
  require(ttlSeconds > 0, "Redis profile TTL must be positive")
  require(keyPrefix.nonEmpty, "Redis profile key prefix must be non-empty")
}

/** Jedis implementation: each Spark partition owns one client and one pipeline. */
final class JedisRedisProfileStore(config: RedisProfileConfig) extends RedisProfileStore {
  override def writeProfiles(runId: String, values: Iterator[(String, String)], ttlSeconds: Int): Unit = {
    val jedis = new Jedis(config.host, config.port)
    try {
      val expiration = SetParams.setParams().ex(ttlSeconds)
      val pipeline = jedis.pipelined()
      JedisRedisProfileStore.writeQueuedProfiles(values, ttlSeconds, new RedisProfilePipeline {
        override def set(key: String, value: String, ignoredTtlSeconds: Int): RedisProfileCommandResponse = {
          val response: Response[String] = pipeline.set(key, value, expiration)
          new RedisProfileCommandResponse {
            override def requireSuccess(): Unit = {
              response.get()
              ()
            }
          }
        }

        override def sync(): Unit = pipeline.sync()
      })
    } finally {
      jedis.close()
    }
  }

  /** The active pointer intentionally has no TTL; profile versions expire independently. */
  override def activate(runId: String): Unit = {
    val jedis = new Jedis(config.host, config.port)
    try jedis.set(s"${config.keyPrefix}:active-run", runId)
    finally jedis.close()
  }
}

object JedisRedisProfileStore {
  /** `Pipeline.sync` sends commands but leaves Redis command errors inside deferred responses. */
  private[profile] def writeQueuedProfiles(
      values: Iterator[(String, String)],
      ttlSeconds: Int,
      pipeline: RedisProfilePipeline
  ): Unit = {
    val responses = values.map { case (key, value) => pipeline.set(key, value, ttlSeconds) }.toVector
    pipeline.sync()
    responses.foreach(_.requireSuccess())
  }
}

object UserProfileRedisPublisher {

  def publish(rows: DataFrame, config: RedisProfileConfig): Unit =
    publish(rows, config, new JedisRedisProfileStore(config))

  /**
    * Publishes every versioned profile before changing the single active-run pointer.
    * `foreachPartition` is an action, so a failed partition prevents driver-side activation.
    */
  def publish(rows: DataFrame, config: RedisProfileConfig, store: RedisProfileStore): Unit = {
    val runIds = rows.select(col("run_id")).distinct().collect().map(_.getString(0))
    require(runIds.length == 1, "Profile publication requires exactly one run_id")
    val runId = runIds.head
    val keyPrefix = config.keyPrefix
    val ttlSeconds = config.ttlSeconds

    rows.select(col("user_id"), col("profile_json")).foreachPartition { partition: Iterator[Row] =>
      store.writeProfiles(runId, partition.map { row =>
        val userId = row.getString(0)
        s"$keyPrefix:$runId:$userId" -> row.getString(1)
      }, ttlSeconds)
    }
    store.activate(runId)
  }
}
