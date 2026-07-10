package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QueryAnalysisReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** Mirrors the old integration fixture:
    *   query "Drama" (5 chars → short): 2 impr, 1 click, 0 orders → ctr 0.5, cvr 0.0
    *   query "Sci-Fi Action" (13 chars → long): 1 impr, 1 click, 1 order → ctr 1.0, cvr 1.0 */
  private def fixture = {
    val s = spark; import s.implicits._
    val base = Seq(
      ("u1", "s1", 1, 0, 1.0, Seq("Drama")),
      ("u2", "s2", 0, 0, 0.0, Seq("Drama")),
      ("u1", "s1", 1, 1, 2.0, Seq("Sci-Fi", "Action"))
    ).toDF("user_id", "session_id", "clicked", "ordered", "label", "genres")
    QueryAnalysisReportJob.withQuery(base)
  }

  "withQuery" should "derive query, its length and the length bucket" in {
    val rows = fixture.collect().map(r => r.getAs[String]("query") -> r).toMap
    rows("Drama").getAs[Int]("query_len") shouldBe 5
    rows("Drama").getAs[String]("query_length") shouldBe "short (<=10)"
    rows("Sci-Fi Action").getAs[Int]("query_len") shouldBe 13
    rows("Sci-Fi Action").getAs[String]("query_length") shouldBe "long (>10)"
  }

  "mostCommonQueries" should "aggregate per query with impressions/clicks/ctr/cvr" in {
    val rows = QueryAnalysisReportJob.mostCommonQueries(fixture)
      .collect().map(r => r.getAs[String]("query") -> r).toMap

    rows("Drama").getAs[Long]("impressions") shouldBe 2L
    rows("Drama").getAs[Long]("clicks") shouldBe 1L
    rows("Drama").getAs[Long]("users") shouldBe 2L
    rows("Drama").getAs[Double]("ctr") shouldBe 0.5
    rows("Sci-Fi Action").getAs[Long]("impressions") shouldBe 1L
    rows("Sci-Fi Action").getAs[Double]("cvr") shouldBe 1.0
    // rating_sum is dropped, avg_rating kept
    rows("Sci-Fi Action").getAs[Double]("avg_rating") shouldBe 2.0
  }

  "lengthEngagement" should "roll up CTR/CVR and distinct queries per length bucket" in {
    val rows = QueryAnalysisReportJob.lengthEngagement(fixture)
      .collect().map(r => r.getAs[String]("query_length") -> r).toMap

    rows("short (<=10)").getAs[Double]("ctr") shouldBe 0.5
    rows("short (<=10)").getAs[Double]("cvr") shouldBe 0.0
    rows("short (<=10)").getAs[Long]("distinct_queries") shouldBe 1L
    rows("long (>10)").getAs[Double]("ctr") shouldBe 1.0
    rows("long (>10)").getAs[Double]("cvr") shouldBe 1.0
  }
}
