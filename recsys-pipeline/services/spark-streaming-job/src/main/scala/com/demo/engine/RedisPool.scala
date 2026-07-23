package com.demo.engine

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

// One JedisPool per executor JVM — avoids a new TCP connection per partition per micro-batch.
object RedisPool {
  @volatile private var pool: JedisPool = _

  def get(host: String, port: Int, maxTotal: Int): JedisPool = {
    if (pool == null) synchronized {
      if (pool == null) {
        val cfg = new JedisPoolConfig()
        cfg.setMaxTotal(maxTotal)
        cfg.setMaxIdle(maxTotal)
        cfg.setMinIdle(1)
        pool = new JedisPool(cfg, host, port)
      }
    }
    pool
  }
}
