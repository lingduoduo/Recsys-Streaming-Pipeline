package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSessionsSpec extends AnyFlatSpec with Matchers {

  "SparkSessions.adaptiveConfigs" should "enable AQE and partition coalescing" in {
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.enabled") shouldBe "true"
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.coalescePartitions.enabled") shouldBe "true"
  }

  // Asserts the constant, not the wiring: `create` uses `getOrCreate`, which returns whatever
  // session another suite already built in this JVM. Correctness of the date partitions is pinned
  // by TimePartitionsSpec, which exercises the expression directly.
  "SparkSessions.defaultTimeZone" should "be UTC so formatting does not vary by deploy host" in {
    SparkSessions.defaultTimeZone shouldBe "UTC"
  }
}
