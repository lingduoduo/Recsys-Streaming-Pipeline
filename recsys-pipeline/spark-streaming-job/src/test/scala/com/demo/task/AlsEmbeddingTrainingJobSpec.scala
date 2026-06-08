package com.demo.task

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.io.Source

class AlsEmbeddingTrainingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private def sampleRatings() = {
    val s = spark; import s.implicits._
    Seq(
      ("u1", "m1", 4.0), ("u1", "m2", 3.5), ("u1", "m3", 5.0),
      ("u2", "m1", 3.0), ("u2", "m2", 4.5), ("u2", "m4", 4.0),
      ("u3", "m1", 5.0), ("u3", "m3", 4.0), ("u3", "m4", 3.5),
      ("u4", "m2", 4.0), ("u4", "m3", 3.5), ("u4", "m4", 5.0)
    ).toDF("userId", "movieId", "rating")
  }

  "trainAlsEmbeddings" should "return DataFrames with correct column names" in {
    val (userFactors, itemFactors) = AlsEmbeddingTrainingJob.trainAlsEmbeddings(
      sparkSession = spark, ratings = sampleRatings(), rank = 4, maxIter = 3, regParam = 0.1
    )
    userFactors.columns should contain allOf ("userId", "userEmbedding")
    itemFactors.columns should contain allOf ("movieId", "itemEmbedding")
  }

  it should "return non-empty factor DataFrames" in {
    val (userFactors, itemFactors) = AlsEmbeddingTrainingJob.trainAlsEmbeddings(
      sparkSession = spark, ratings = sampleRatings(), rank = 4, maxIter = 3, regParam = 0.1
    )
    userFactors.count() should be > 0L
    itemFactors.count() should be > 0L
  }

  it should "write factors in id:f1 f2 f3 format readable by downstream consumers" in {
    val tmpDir = Files.createTempDirectory("als-test")
    tmpDir.toFile.deleteOnExit()
    val outputPath = tmpDir.resolve("factors").toString

    val (userFactors, _) = AlsEmbeddingTrainingJob.trainAlsEmbeddings(
      sparkSession = spark, ratings = sampleRatings(), rank = 4, maxIter = 3, regParam = 0.1
    )
    AlsEmbeddingTrainingJob.writeFactors(userFactors, outputPath, "userId", "userEmbedding")

    val lines = new java.io.File(outputPath)
      .listFiles()
      .filter(_.getName.endsWith(".txt"))
      .flatMap(f => Source.fromFile(f).getLines().toSeq)

    lines.length should be > 0
    lines.foreach { line =>
      val parts = line.split(":", 2)
      parts.length shouldBe 2
      parts(0).trim shouldNot be(empty)
      parts(1).trim.split("\\s+").foreach { token =>
        noException should be thrownBy token.toDouble
      }
    }
  }
}
