package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceJobConfigSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  "from" should "apply the documented defaults when the map is empty" in {
    val cfg = SequenceJobConfig.from(Map.empty)
    cfg.bucketWidth shouldBe "day"
    cfg.lookbackDays shouldBe 90
    cfg.maxRowsPerBucket shouldBe 500
    cfg.parquetPath shouldBe None
  }

  it should "use overrides when all four keys are set to valid values" in {
    val cfg = SequenceJobConfig.from(Map(
      "SEQ_BUCKET_WIDTH" -> "hour",
      "SEQ_LOOKBACK_DAYS" -> "7",
      "SEQ_MAX_ROWS_PER_BUCKET" -> "50",
      "SEQ_PARQUET_PATH" -> "/tmp/x"
    ))
    cfg.bucketWidth shouldBe "hour"
    cfg.lookbackDays shouldBe 7
    cfg.maxRowsPerBucket shouldBe 50
    cfg.parquetPath shouldBe Some("/tmp/x")
  }

  it should "floor lookbackDays and maxRowsPerBucket at 1" in {
    val cfg = SequenceJobConfig.from(Map(
      "SEQ_LOOKBACK_DAYS" -> "0",
      "SEQ_MAX_ROWS_PER_BUCKET" -> "0"
    ))
    cfg.lookbackDays shouldBe 1
    cfg.maxRowsPerBucket shouldBe 1
  }

  it should "fall back to the default when an int key is non-numeric" in {
    val cfg = SequenceJobConfig.from(Map("SEQ_LOOKBACK_DAYS" -> "abc"))
    cfg.lookbackDays shouldBe 90
  }

  it should "treat an empty SEQ_PARQUET_PATH as None" in {
    SequenceJobConfig.from(Map("SEQ_PARQUET_PATH" -> "")).parquetPath shouldBe None
  }

  "ttlSeconds" should "convert the lookback window into seconds" in {
    SequenceJobConfig("day", 90, 500, None).ttlSeconds shouldBe 90 * 24 * 3600
    SequenceJobConfig("day", 1, 500, None).ttlSeconds shouldBe 86400
  }

  "SequenceSinks.write" should "write Parquet when a path is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val chunks = Seq(("u1", "rating", "20260723", "m1,m2", "1000,2000", "rate,rate", ",4.0", ",", "1995,", 2L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
    val path = java.nio.file.Files.createTempDirectory("seq-sinks").toString + "/out"

    SequenceSinks.write(
      chunks, SequenceJobConfig("day", 90, 500, Some(path)),
      redisHost = "unused", redisPort = 0, poolMax = 1, pipelineSize = 10,
      mode = SequenceWriteMode.Overwrite, batchId = 0L, writeRedis = false
    )

    spark.read.parquet(path).count() shouldBe 2L
  }

  it should "skip Parquet entirely when no path is configured" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val chunks = Seq(("u1", "rating", "20260723", "m1", "1000", "rate", "4.0", "", "1995", 1L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")

    // No Redis, no Parquet path — must be a clean no-op rather than an NPE or a
    // write to some default location.
    noException should be thrownBy SequenceSinks.write(
      chunks, SequenceJobConfig("day", 90, 500, None),
      redisHost = "unused", redisPort = 0, poolMax = 1, pipelineSize = 10,
      mode = SequenceWriteMode.Append, batchId = 0L, writeRedis = false
    )
  }
}
