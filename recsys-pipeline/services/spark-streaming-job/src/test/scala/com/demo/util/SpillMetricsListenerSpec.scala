package com.demo.util

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpillMetricsListenerSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "register" should "attach without disturbing the session" in {
    // The listener's arithmetic is covered by SpillMetricsSpec and FailureTallySpec, which need no
    // session. Spark's TaskMetrics has no public constructor, so a unit test cannot synthesise a
    // realistic stage-completed event; what is worth asserting here is that registering the
    // listener does not break a job that then runs.
    SpillMetricsListener.register(spark, "SpillMetricsListenerSpec") shouldBe true
    val s = spark; import s.implicits._
    val counted = Seq(("a", 1), ("b", 2), ("a", 3)).toDF("k", "v")
      .groupBy("k").count().collect()
    counted.length shouldBe 2
  }

  it should "be safe to register twice, reporting false on the second attach" in {
    // ExecutionEngine registers BatchMetricsListener per query; a job that opens two queries would
    // otherwise double-register and double-log. The session is already registered by the previous
    // test, so both calls here return false.
    SpillMetricsListener.register(spark, "SpillMetricsListenerSpec") shouldBe false
    SpillMetricsListener.register(spark, "SpillMetricsListenerSpec") shouldBe false
    val s = spark; import s.implicits._
    Seq(1, 2, 3).toDF("v").count() shouldBe 3L
  }
}
