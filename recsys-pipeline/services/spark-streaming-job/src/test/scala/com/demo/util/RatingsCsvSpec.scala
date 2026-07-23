package com.demo.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RatingsCsvSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit =
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("RatingsCsvSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  "RatingsCsv.read" should "read the ratings CSV with the shared typed schema" in {
    import java.nio.file.Files
    val dir = Files.createTempDirectory("ratings").toFile
    val file = new java.io.File(dir, "ratings.csv")
    Files.write(file.toPath, "userId,movieId,rating,timestamp\nu1,m1,4.5,1000\nu2,m2,3.0,2000\n".getBytes)

    val df = RatingsCsv.read(spark, file.getAbsolutePath)

    df.schema.fieldNames shouldBe Array("userId", "movieId", "rating", "timestamp")
    df.schema("userId").dataType shouldBe StringType
    df.schema("rating").dataType shouldBe DoubleType
    df.schema("timestamp").dataType shouldBe LongType

    val rows = df.orderBy("userId").collect()
    rows.map(_.getString(0)) shouldBe Array("u1", "u2")
    rows(0).getAs[Double]("rating") shouldBe 4.5
    rows(0).getAs[Long]("timestamp") shouldBe 1000L
  }
}
