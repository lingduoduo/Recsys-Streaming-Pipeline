package com.demo.util

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SparkSessionsSpec extends AnyFlatSpec with Matchers {

  "SparkSessions.adaptiveConfigs" should "enable AQE and partition coalescing" in {
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.enabled") shouldBe "true"
    SparkSessions.adaptiveConfigs("spark.sql.adaptive.coalescePartitions.enabled") shouldBe "true"
  }
}
