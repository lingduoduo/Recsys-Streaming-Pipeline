package com.demo.engine

import java.nio.file.{Files, Paths}

import com.demo.SparkTestSupport
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CommitProtocolSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val Manifest = "version=2\nquery=test-namespace\nkind=pending\nbatch_id=7\n"

  private def frame(values: Seq[(String, Long)]) = {
    val session = spark
    import session.implicits._
    values.toDF("event_id", "timestamp_ms")
  }

  private def commit(root: java.nio.file.Path, values: Seq[(String, Long)]): Path = {
    val finalPath = new Path(new Path(root.toString), "7")
    CommitProtocol.writeDirectory(
      frame(values),
      new Path(new Path(root.toString), "_attempts/7"),
      finalPath,
      partitionByDate = false,
      "pending snapshot 7",
      Manifest)
    finalPath
  }

  private def fileSystem(path: Path) =
    path.getFileSystem(spark.sparkContext.hadoopConfiguration)

  "CommitProtocol.writeDirectory" should "commit a readable, validating directory" in {
    val root = Files.createTempDirectory("commit-protocol")
    val committed = commit(root, Seq("a" -> 1L, "b" -> 2L))

    CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(2L))
    CommitProtocol.hasParquetData(fileSystem(committed), committed) shouldBe true
    spark.read.parquet(committed.toString).count() shouldBe 2L
  }

  it should "commit an empty directory without parquet data" in {
    val root = Files.createTempDirectory("commit-protocol-empty")
    val committed = commit(root, Seq.empty)

    CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(0L))
    CommitProtocol.hasParquetData(fileSystem(committed), committed) shouldBe false
  }

  it should "be a no-op when the same content is committed twice" in {
    val root = Files.createTempDirectory("commit-protocol-retry")
    commit(root, Seq("a" -> 1L, "b" -> 2L))
    commit(root, Seq("a" -> 1L, "b" -> 2L))

    val committed = new Path(new Path(root.toString), "7")
    CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(2L))
  }

  "CommitProtocol.validateDataCommitted" should "reject a directory whose inventory grew after the commit" in {
    val root = Files.createTempDirectory("commit-protocol-extra")
    val committed = commit(root, Seq("a" -> 1L))
    val extra = new Path(committed, "part-99-not-in-the-manifest.parquet")
    val output = fileSystem(extra).create(extra, false)
    try output.write(Array[Byte](1, 2, 3)) finally output.close()

    an[IllegalStateException] should be thrownBy
      CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(1L))
  }

  it should "never silently accept a tampered data file" in {
    val root = Files.createTempDirectory("commit-protocol-tamper")
    val committed = commit(root, Seq("a" -> 1L))
    val parquet = Files.list(Paths.get(committed.toString)).filter(_.toString.endsWith(".parquet"))
      .findFirst().orElseThrow(() => new AssertionError("no parquet file written"))
    Files.write(parquet, Files.readAllBytes(parquet) ++ Array[Byte](0))

    // LocalFileSystem keeps a .crc sidecar, so re-reading the corrupted file raises Hadoop's own
    // ChecksumException before the SHA-256 comparison runs. The exception type is Hadoop's, not
    // ours; what this pins is that validation never returns normally on a corrupted file.
    an[Exception] should be thrownBy
      CommitProtocol.validateDataCommitted(fileSystem(committed), committed, Manifest, Some(1L))
  }

  it should "reject a directory with no commit marker" in {
    val root = Files.createTempDirectory("commit-protocol-missing")
    val bare = new Path(new Path(root.toString), "7")
    fileSystem(bare).mkdirs(bare)

    an[IllegalStateException] should be thrownBy
      CommitProtocol.validateDataCommitted(fileSystem(bare), bare, Manifest, None)
  }
}
