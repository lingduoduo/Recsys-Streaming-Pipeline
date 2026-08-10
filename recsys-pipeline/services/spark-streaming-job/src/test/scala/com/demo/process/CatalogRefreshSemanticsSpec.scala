package com.demo.process

import java.nio.file.{Files, Path}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Characterization tests for how a loaded catalog reacts to changes on disk.
  *
  * `OnlineJoinerStreamingJob` builds the catalog DataFrame once, before the stream starts, and
  * never caches it. Because `foreachBatch` re-evaluates that plan for every micro-batch, catalog
  * enrichment is *not* simply frozen at startup -- but what it observes depends on the shape of
  * the configured path. These tests pin that behaviour so a Spark upgrade cannot change it
  * silently; they document what is, not what is ideal.
  */
class CatalogRefreshSemanticsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[1]")
      .appName("CatalogRefreshSemanticsSpec")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = spark.stop()

  private def write(file: Path, contents: String): Unit =
    Files.write(file, contents.getBytes("UTF-8"))

  private def itemIds(catalog: DataFrame): Set[String] =
    catalog.collect().map(_.getAs[String]("item_id")).toSet

  "a catalog loaded from a single file" should "observe an in-place rewrite without reloading" in {
    val directory = Files.createTempDirectory("catalog-file")
    val file = directory.resolve("catalog.json")
    write(file, """{"i1":{"genres":["a"],"tags":[]}}""")
    val catalog = OnlineJoinerStreamingJob.loadCatalog(spark, file.toString)
    itemIds(catalog) shouldBe Set("i1")

    write(file, """{"i2":{"genres":["b"],"tags":["t"]}}""")

    // The plan is lazy and uncached, so the next micro-batch scans the new contents. Editing a
    // live catalog therefore changes enrichment mid-run, with no atomicity around the write.
    itemIds(catalog) shouldBe Set("i2")
  }

  "a catalog loaded from a directory" should "ignore files added after it was built" in {
    val directory = Files.createTempDirectory("catalog-directory")
    write(directory.resolve("part-0.json"), """{"i1":{"genres":["a"],"tags":[]}}""")
    val catalog = OnlineJoinerStreamingJob.loadCatalog(spark, directory.toString)
    itemIds(catalog) shouldBe Set("i1")

    write(directory.resolve("part-1.json"), """{"i2":{"genres":["b"],"tags":[]}}""")

    // The file listing was resolved when the DataFrame was built, so new siblings are invisible
    // until the job restarts -- the opposite of the single-file case above.
    itemIds(catalog) shouldBe Set("i1")
  }

  "a truncated catalog file" should "yield null profiles rather than failing the batch" in {
    val directory = Files.createTempDirectory("catalog-truncated")
    val file = directory.resolve("catalog.json")
    write(file, """{"i1":{"genres":["a"],"tags":[]}}""")
    val catalog = OnlineJoinerStreamingJob.loadCatalog(spark, file.toString)
    itemIds(catalog) shouldBe Set("i1")

    // A partially written file is what a reader sees mid-rewrite when the writer is not atomic.
    write(file, """{"i1":{"genres":["a"],"ta""")

    // from_json returns null for unparseable input, so the catalog silently empties instead of
    // raising. Downstream, enrichWithCatalog coalesces the miss to empty genres/tags, which is
    // indistinguishable from a legitimately untagged item.
    itemIds(catalog) shouldBe empty
  }
}
