package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KeywordAnalysisReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** Mirrors the integration-test fixture: two Sci-Fi/Action impressions (1 click) + one Drama. */
  private def fixture = {
    val s = spark; import s.implicits._
    Seq(
      ("u1", "item_1", 1, Seq("Sci-Fi", "Action"), 2015),
      ("u2", "item_1", 0, Seq("Sci-Fi", "Action"), 2015),
      ("u1", "item_2", 1, Seq("Drama"), 1995)
    ).toDF("user_id", "item_id", "clicked", "genres", "release_year")
  }

  "withKeywordsAndCategories" should "derive keyword/subkeyword and l1/l2/l3" in {
    val out = KeywordAnalysisReportJob.withKeywordsAndCategories(fixture)
    val byItem = out.collect().map(r => r.getAs[String]("item_id") -> r).toMap

    val r1 = byItem("item_1")
    r1.getAs[String]("keyword") shouldBe "Sci-Fi"
    r1.getAs[String]("subkeyword") shouldBe "Action"
    r1.getAs[String]("l1") shouldBe "SciFi&Fantasy"
    r1.getAs[String]("l2") shouldBe "Sci-Fi"
    r1.getAs[String]("l3") shouldBe "Sci-Fi·2010s"

    byItem("item_2").getAs[String]("subkeyword") shouldBe "none"
  }

  it should "give unknown decade when release_year is absent" in {
    val s = spark; import s.implicits._
    val noYear = Seq(("item_1", Seq("Sci-Fi", "Action"))).toDF("item_id", "genres")
    val out = KeywordAnalysisReportJob.withKeywordsAndCategories(
      KeywordAnalysisReportJob.ensureMeta(noYear))
    out.collect().head.getAs[String]("l3") shouldBe "Sci-Fi·unknown"
  }

  "keywordDistribution" should "count impressions, distinct movies and query clicks per keyword" in {
    val df = KeywordAnalysisReportJob.withKeywordsAndCategories(fixture)
    val byKw = KeywordAnalysisReportJob.keywordDistribution(df, "keyword")
      .collect().map(r => r.getAs[String]("keyword") -> r).toMap

    byKw("Sci-Fi").getAs[Long]("movie_impressions") shouldBe 2L
    byKw("Sci-Fi").getAs[Long]("distinct_movies") shouldBe 1L
    byKw("Sci-Fi").getAs[Long]("query_clicks") shouldBe 1L
    byKw("Drama").getAs[Long]("movie_impressions") shouldBe 1L
  }

  it should "count subkeywords including none" in {
    val df = KeywordAnalysisReportJob.withKeywordsAndCategories(fixture)
    val bySub = KeywordAnalysisReportJob.keywordDistribution(df, "subkeyword")
      .collect().map(r => r.getAs[String]("subkeyword") -> r).toMap
    bySub("Action").getAs[Long]("movie_impressions") shouldBe 2L
    bySub("none").getAs[Long]("movie_impressions") shouldBe 1L
  }

  "categoryTopKeywords" should "rank exploded genres within each category value" in {
    val df = KeywordAnalysisReportJob.withKeywordsAndCategories(fixture)
    val l1 = KeywordAnalysisReportJob.categoryTopKeywords(df, "l1").collect()
    val scifi = l1.filter(_.getAs[String]("l1") == "SciFi&Fantasy")
      .map(r => r.getAs[String]("keyword") -> r).toMap

    scifi("Sci-Fi").getAs[Long]("movie_impressions") shouldBe 2L
    scifi("Sci-Fi").getAs[Long]("query_clicks") shouldBe 1L
    scifi("Action").getAs[Long]("movie_impressions") shouldBe 2L
    scifi.values.map(_.getAs[Int]("rank")).toSet shouldBe Set(1, 2)
  }
}
