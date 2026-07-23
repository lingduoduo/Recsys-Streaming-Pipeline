package com.demo.engine

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EngineConfigSpec extends AnyFlatSpec with Matchers {

  private def good = EngineConfig(
    bootstrapServers = "localhost:9092", inputTopic = "in", startingOffsets = "earliest",
    groupId = "g", maxOffsetsPerTrigger = 5000, triggerInterval = "10 seconds",
    checkpointLocation = "/tmp/ck", watermarkDelay = "10 minutes", sinkMaxRetries = 0)

  "validate" should "accept a well-formed config" in {
    EngineConfig.validate(good) shouldBe Right(good)
  }

  it should "accumulate all errors for an invalid config" in {
    val bad = good.copy(inputTopic = "  ", maxOffsetsPerTrigger = 0, sinkMaxRetries = -1)
    val errs = EngineConfig.validate(bad).left.getOrElse(Nil)
    errs should have size 3
    errs.exists(_.contains("inputTopic")) shouldBe true
    errs.exists(_.contains("maxOffsetsPerTrigger")) shouldBe true
    errs.exists(_.contains("sinkMaxRetries")) shouldBe true
  }
}
