package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RelevanceAnalysisReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** Mirrors the old integration fixture: ordered(2.0), clicked(1.0), impression_only(0.0)×2. */
  private def fixture = {
    val s = spark; import s.implicits._
    val base = Seq(
      ("item_1", 2.0, Seq("Sci-Fi", "Action")),
      ("item_2", 1.0, Seq("Drama")),
      ("item_3", 0.0, Seq("Drama")),
      ("item_4", 0.0, Seq("Sci-Fi", "Action"))
    ).toDF("item_id", "label", "genres")
    RelevanceAnalysisReportJob.withRelevance(base)
  }

  "withRelevance" should "derive relevance_state from label and query from genres" in {
    val rows = fixture.collect().map(r => r.getAs[String]("item_id") -> r).toMap
    rows("item_1").getAs[String]("relevance_state") shouldBe "ordered"
    rows("item_1").getAs[String]("query") shouldBe "Sci-Fi Action"
    rows("item_2").getAs[String]("relevance_state") shouldBe "clicked"
    rows("item_3").getAs[String]("relevance_state") shouldBe "impression_only"
  }

  "stateDistribution" should "give per-state score and proportion" in {
    val rows = RelevanceAnalysisReportJob.stateDistribution(fixture)
      .collect().map(r => r.getAs[String]("relevance_state") -> r).toMap
    rows("impression_only").getAs[Double]("proportion") shouldBe 0.5
    rows("clicked").getAs[Double]("proportion") shouldBe 0.25
    rows("ordered").getAs[Double]("proportion") shouldBe 0.25
    rows("ordered").getAs[Double]("score") shouldBe 2.0
  }

  "byQuery" should "give mean score and per-state shares per genre-combo" in {
    val rows = RelevanceAnalysisReportJob.byQuery(fixture)
      .collect().map(r => r.getAs[String]("query") -> r).toMap
    rows("Sci-Fi Action").getAs[Double]("mean_score") shouldBe 1.0   // mean(2.0, 0.0)
    rows("Sci-Fi Action").getAs[Double]("ordered_share") shouldBe 0.5
    rows("Drama").getAs[Double]("mean_score") shouldBe 0.5           // mean(1.0, 0.0)
  }

  "byGenre" should "explode genres and break down relevance per genre" in {
    val rows = RelevanceAnalysisReportJob.byGenre(fixture)
      .collect().map(r => r.getAs[String]("genre") -> r).toMap
    rows("Drama").getAs[Double]("mean_score") shouldBe 0.5
    rows("Sci-Fi").getAs[Double]("mean_score") shouldBe 1.0
    rows("Action").getAs[Long]("impressions") shouldBe 2L
  }
}
