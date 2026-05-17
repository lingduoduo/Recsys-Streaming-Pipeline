package com.demo.recsys

import java.io.File
import java.nio.file.Files

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class Item2VecTrainingJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val VectorSize = 4

  private def sequences() = {
    val s = spark; import s.implicits._
    Seq(
      ("u1", Seq("a", "b", "c", "d")),
      ("u2", Seq("b", "c", "d", "e")),
      ("u3", Seq("c", "d", "e", "a")),
      ("u4", Seq("a", "c", "e", "b")),
      ("u5", Seq("b", "d", "a", "c"))
    ).toDF("userId", "movieIds")
  }

  private def tmpFile(): File = {
    val f = Files.createTempFile("i2v-test", ".txt").toFile
    f.deleteOnExit()
    f
  }

  // Trained once; the format and vocabulary tests both assert on this result.
  private lazy val trainedLines: Seq[String] = {
    val out = tmpFile()
    Item2VecTrainingJob.trainItem2vec(
      samples = sequences(),
      embeddingPath = out.getAbsolutePath,
      queryItem = "a",
      vectorSize = VectorSize,
      windowSize = 2,
      numIterations = 2,
      minCount = 1,
      saveToRedis = false
    )
    scala.io.Source.fromFile(out).getLines().toSeq
  }

  "trainItem2vec" should "write embeddings in id:v1 v2 ... format" in {
    trainedLines should not be empty
    trainedLines.foreach { line =>
      val sep = line.indexOf(':')
      withClue(s"line '$line' must have id:vector format") { sep should be > 0 }
      val values = line.substring(sep + 1).trim.split(' ')
      withClue(s"vector in '$line' must have $VectorSize components") {
        values.length shouldBe VectorSize
      }
      withClue(s"all components in '$line' must be valid floats") {
        values.foreach(v => noException should be thrownBy v.toFloat)
      }
    }
  }

  it should "include all vocabulary items in the embedding file" in {
    val ids = trainedLines.map(l => l.substring(0, l.indexOf(':'))).toSet
    ids should contain allOf ("a", "b", "c", "d", "e")
  }

  it should "not throw when queryItem is absent from vocabulary" in {
    val out = tmpFile()
    noException should be thrownBy {
      Item2VecTrainingJob.trainItem2vec(
        samples = sequences(),
        embeddingPath = out.getAbsolutePath,
        queryItem = "nonexistent",
        vectorSize = VectorSize,
        windowSize = 2,
        numIterations = 2,
        minCount = 1,
        saveToRedis = false
      )
    }
  }
}
