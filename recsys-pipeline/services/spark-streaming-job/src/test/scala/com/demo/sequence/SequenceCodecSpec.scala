package com.demo.sequence

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SequenceCodecSpec extends AnyFlatSpec with Matchers {

  private def chunk(itemIds: String, ts: String, n: Int): Map[String, String] = Map(
    SequenceSchema.ColItemId -> itemIds,
    SequenceSchema.ColTs     -> ts,
    SequenceSchema.ColCount  -> n.toString
  )

  "pack/unpack" should "round-trip a simple chunk" in {
    val values = Seq("31", "1029", "1061")
    SequenceCodec.unpack(SequenceCodec.pack(values), 3) shouldBe values
  }

  it should "preserve leading, interior and trailing nulls as empty elements" in {
    val values = Seq("", "4.0", "")
    val packed = SequenceCodec.pack(values)
    packed shouldBe ",4.0,"
    // The whole reason this test exists: String.split(",") without limit -1 would
    // return Array("", "4.0") and silently shift every later column by one row.
    SequenceCodec.unpack(packed, 3) shouldBe values
  }

  it should "round-trip an all-null column" in {
    SequenceCodec.unpack(SequenceCodec.pack(Seq("", "", "")), 3) shouldBe Seq("", "", "")
  }

  it should "treat an empty chunk as zero rows, not one blank row" in {
    SequenceCodec.pack(Seq.empty) shouldBe ""
    SequenceCodec.unpack("", 0) shouldBe Seq.empty
  }

  it should "pad a column shorter than n rather than mis-aligning" in {
    SequenceCodec.unpack("a,b", 4) shouldBe Seq("a", "b", "", "")
  }

  it should "truncate a column longer than n" in {
    SequenceCodec.unpack("a,b,c,d", 2) shouldBe Seq("a", "b")
  }

  "sanitize" should "strip both separators so a value cannot break the layout" in {
    SequenceCodec.sanitize("Sci-Fi, Drama|Comedy") shouldBe "Sci-Fi Drama Comedy"
    SequenceCodec.sanitize(null) shouldBe ""
  }

  "merge" should "append fresh rows after existing rows" in {
    val merged = SequenceCodec.merge(chunk("a,b", "1,2", 2), chunk("c", "3", 1), maxRows = 10)
    merged(SequenceSchema.ColItemId) shouldBe "a,b,c"
    merged(SequenceSchema.ColTs) shouldBe "1,2,3"
    merged(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "return fresh unchanged when existing is empty" in {
    val merged = SequenceCodec.merge(Map.empty, chunk("c", "3", 1), maxRows = 10)
    merged(SequenceSchema.ColItemId) shouldBe "c"
    merged(SequenceSchema.ColCount) shouldBe "1"
  }

  it should "drop the oldest rows on overflow, keeping every column aligned" in {
    val merged = SequenceCodec.merge(chunk("a,b,c", "1,2,3", 3), chunk("d,e", "4,5", 2), maxRows = 3)
    merged(SequenceSchema.ColItemId) shouldBe "c,d,e"
    merged(SequenceSchema.ColTs) shouldBe "3,4,5"
    merged(SequenceSchema.ColCount) shouldBe "3"
  }

  it should "keep null elements aligned across a merge" in {
    val existing = chunk("a,b", "1,2", 2) + (SequenceSchema.ColRating -> ",4.0")
    val fresh    = chunk("c", "3", 1) + (SequenceSchema.ColRating -> "")
    val merged   = SequenceCodec.merge(existing, fresh, maxRows = 10)
    merged(SequenceSchema.ColRating) shouldBe ",4.0,"
    merged(SequenceSchema.ColCount) shouldBe "3"
  }
}
