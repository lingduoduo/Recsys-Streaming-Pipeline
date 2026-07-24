package com.demo.sequence

/** Pure manipulation of packed column strings. Every alignment rule in the store lives
  * here so it can be tested without Spark or Redis. */
object SequenceCodec {

  def pack(values: Seq[String]): String =
    values.map(v => if (v == null) "" else v).mkString(SequenceSchema.RowSeparator)

  /** Split into exactly `n` elements: pad short columns, truncate long ones.
    * The `-1` limit is required — the default drops trailing empty strings. */
  def unpack(packed: String, n: Int): Seq[String] = {
    if (n <= 0) return Seq.empty
    val parts =
      if (packed == null || packed.isEmpty) Array.empty[String]
      else packed.split(SequenceSchema.RowSeparator, -1)
    if (parts.length == n) parts.toSeq
    else if (parts.length > n) parts.take(n).toSeq
    else parts.toSeq ++ Seq.fill(n - parts.length)("")
  }

  /** Concatenate `fresh` after `existing`, keeping the newest `maxRows` rows.
    * Columns absent from either side are treated as all-null so they stay aligned. */
  def merge(
      existing: Map[String, String],
      fresh: Map[String, String],
      maxRows: Int
  ): Map[String, String] = {
    val existingCount = count(existing)
    val freshCount    = count(fresh)
    val total         = existingCount + freshCount
    if (total <= 0) return Map(SequenceSchema.ColCount -> "0")

    val kept = math.min(total, math.max(0, maxRows))
    val drop = total - kept

    val columns = SequenceSchema.Columns.map { column =>
      val combined =
        unpack(existing.getOrElse(column, ""), existingCount) ++
          unpack(fresh.getOrElse(column, ""), freshCount)
      column -> pack(combined.drop(drop))
    }.toMap

    columns + (SequenceSchema.ColCount -> kept.toString)
  }

  private def count(chunk: Map[String, String]): Int =
    chunk.get(SequenceSchema.ColCount)
      .flatMap(v => scala.util.Try(v.trim.toInt).toOption)
      .filter(_ > 0)
      .getOrElse(0)
}
