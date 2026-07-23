package com.demo.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExecutionEngineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var spark: SparkSession = _
  override def beforeAll(): Unit =
    spark = SparkSession.builder().master("local[1]").appName("ExecutionEngineSpec")
      .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").getOrCreate()
  override def afterAll(): Unit = spark.stop()

  "withRetry" should "retry until success within the budget" in {
    var calls = 0
    ExecutionEngine.withRetry(2) { calls += 1; if (calls <= 2) throw new RuntimeException("boom") }
    calls shouldBe 3
  }

  it should "rethrow after exhausting retries" in {
    var calls = 0
    an[RuntimeException] should be thrownBy
      ExecutionEngine.withRetry(1) { calls += 1; throw new RuntimeException("boom") }
    calls shouldBe 2
  }

  "processBatch" should "apply batch stages and write to every sink with the batchId" in {
    val s = spark; import s.implicits._
    val received = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    val collecting: Sink = (b, id) =>
      b.collect().foreach(r => received += ((r.getAs[String]("x"), r.getAs[Long]("bid"))))
    val addBid: BatchStage = (df, id) => df.withColumn("bid", lit(id))

    ExecutionEngine.processBatch(Seq("a", "b").toDF("x"), 5L, Seq(addBid), Seq(collecting), maxRetries = 0)

    received.toSet shouldBe Set(("a", 5L), ("b", 5L))
  }

  it should "write to every sink in the list" in {
    val s = spark; import s.implicits._
    val a = scala.collection.mutable.ArrayBuffer[String]()
    val b = scala.collection.mutable.ArrayBuffer[String]()
    val sinkA: Sink = (batch, _) => batch.collect().foreach(r => a += r.getAs[String]("x"))
    val sinkB: Sink = (batch, _) => batch.collect().foreach(r => b += r.getAs[String]("x"))
    ExecutionEngine.processBatch(Seq("a", "b").toDF("x"), 0L, Seq.empty, Seq(sinkA, sinkB), maxRetries = 0)
    a.toSet shouldBe Set("a", "b")
    b.toSet shouldBe Set("a", "b")
  }
}
