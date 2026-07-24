package com.demo.sequence

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceRedisSinkSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val existing = Map(
    SequenceSchema.ColItemId -> "a,b",
    SequenceSchema.ColTs     -> "1,2",
    SequenceSchema.ColCount  -> "2"
  )
  private val fresh = Map(
    SequenceSchema.ColItemId -> "c",
    SequenceSchema.ColTs     -> "3",
    SequenceSchema.ColCount  -> "1"
  )

  // NOTE: all arguments positional — in Scala 2.12 a positional argument may not follow
  // a named one, so `resolve(existing, fresh, maxRows = 10, Append)` would not compile.
  "resolve" should "merge existing and fresh rows in Append mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 10, SequenceWriteMode.Append)
    resolved(SequenceSchema.ColItemId) shouldBe "a,b,c"
    resolved(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "ignore existing rows entirely in Overwrite mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 10, SequenceWriteMode.Overwrite)
    resolved(SequenceSchema.ColItemId) shouldBe "c"
    resolved(SequenceSchema.ColCount) shouldBe "1"
  }

  it should "cap a bucket at maxRows in Append mode" in {
    val resolved = SequenceRedisSink.resolve(existing, fresh, 2, SequenceWriteMode.Append)
    resolved(SequenceSchema.ColItemId) shouldBe "b,c"
    resolved(SequenceSchema.ColCount) shouldBe "2"
  }

  "chunkFields" should "extract every schema column plus n from a chunk Row" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val row = Seq(("u1", "rating", "20260723", "m1,m2", "1,2", "rate,rate", ",4.0", "Drama,", "1995,", 2L))
      .toDF("user_id", "kind", "bucket", "item_id", "ts", "action", "rating", "genres", "release_year", "n")
      .collect()
      .head

    val fields = SequenceRedisSink.chunkFields(row)

    fields(SequenceSchema.ColItemId) shouldBe "m1,m2"
    fields(SequenceSchema.ColRating) shouldBe ",4.0"
    fields(SequenceSchema.ColCount) shouldBe "2"
    fields.keySet shouldBe (SequenceSchema.Columns.toSet + SequenceSchema.ColCount)
  }
}
