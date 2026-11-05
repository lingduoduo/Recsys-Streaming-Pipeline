package com.demo.report

import org.scalatest.funsuite.AnyFunSuite

class DashboardAnalysisSpec extends AnyFunSuite {
  private val samples = Seq(
    Map[String,Any]("item_id" -> "1", "genres" -> Seq("Action", "Comedy"), "label" -> 2.0, "release_year" -> 1994),
    Map[String,Any]("item_id" -> "2", "genres" -> Seq("Action", "Drama"), "label" -> 0.0, "release_year" -> 1999),
    Map[String,Any]("item_id" -> "3", "genres" -> Seq.empty[String], "label" -> 1.0, "release_year" -> null))
  test("engagement and query preserve labels, exploded genres and query length buckets") {
    val engagement = DashboardAnalysis.engagement(samples)
    assert(engagement("ctr") == 0.6667)
    assert(engagement("funnel") == Map("impression" -> 3, "click" -> 2, "order" -> 1))
    assert(DashboardAnalysis.query(samples)("average_query_length") == 10.67)
  }
  test("keyword includes all grids, decades and clustered uncertainty") {
    val kw = DashboardAnalysis.keyword(samples)
    val grid = kw("grid").asInstanceOf[Seq[Map[String,Any]]]
    val action = grid.find(_("keyword") == "Action").get
    assert(action("z") == 0.0)
    assert(grid.find(_("keyword") == "Comedy").get.apply("z") == null)
    assert(kw.keySet.contains("topic_grid") && kw.keySet.contains("decade_grid"))
    assert(kw("genre_order").asInstanceOf[Map[String,Any]]("multi_genre_movies") == 2)
  }
  test("hydration preserves existing year column and missing popularity") {
    val catalog = Seq(Map[String,Any]("item_id" -> "3", "genres" -> Seq("Horror"), "release_year" -> 2001))
    val hydrated = DashboardAnalysis.enrichSamples(samples, catalog)
    assert(hydrated.last("release_year") == null)
    assert(hydrated.last("genres") == Seq("Horror"))
    val slate = DashboardAnalysis.enrichSlates(Seq(Map("items" -> Seq(Map("item_id" -> "3")))), catalog, Map.empty)
    assert(!slate.head("items").asInstanceOf[Seq[Map[String,Any]]].head.contains("popularity"))
    intercept[IllegalArgumentException](DashboardAnalysis.enrichSamples(Seq.empty, catalog))
  }
  test("Python generated golden aggregation fixture matches every dashboard field") {
    import ReportSupport._
    val resource = this.getClass.getResource("/report/dashboard-golden.json")
    val fixture = obj(readJson(java.nio.file.Paths.get(resource.toURI).toString))
    val input = seq(fixture("samples")).map(obj)
    def compare(actual: Any, expected: Any): Unit = (actual,expected) match {
      case (a: scala.collection.Map[_,_], b: scala.collection.Map[_,_]) =>
        val aa = obj(a); val bb = obj(b); assert(aa.keySet == bb.keySet)
        aa.keys.foreach(k => withClue(s"field $k: "){compare(aa(k),bb(k))})
      case (a: Seq[_], b: Seq[_]) => assert(a.size == b.size); a.zip(b).foreach {case(x,y) => compare(x,y)}
      case (a: Number,b: Number) => assert(math.abs(a.doubleValue-b.doubleValue) < 1e-9)
      case _ => assert(actual == expected)
    }
    compare(DashboardAnalysis.keyword(input),fixture("keyword"))
    compare(DashboardAnalysis.engagement(input),fixture("engagement"))
    compare(DashboardAnalysis.query(input),fixture("query"))
  }

  test("composition preserves unavailable evaluation and aggregate-only diversity schema") {
    import ReportSupport._
    val result = AnalysisDashboardExportJob.build(samples,Seq.empty,Map.empty,Map.empty,Map.empty,
      Seq.empty,None,None,Map("input" -> "fixture", "now" -> "2026-11-05T00:00:00Z"))
    assert(result("schemaVersion") == "2.0")
    assert(result("input") == "fixture")
    assert(result("rows") == 3)
    assert(result("recall") == null && result("ope") == null)
    assert(obj(result("diversity"))("status") == "unavailable")
    assert(seq(obj(result("ranking"))("rows")).size == 3)
    val out = java.nio.file.Files.createTempFile("dashboard-strict", ".json")
    try {
      writeJson(out.toString,result + ("nonfinite" -> Double.NaN))
      assert(obj(readJson(out.toString))("nonfinite") == null)
    } finally java.nio.file.Files.deleteIfExists(out)
  }
  test("JSON experiences accepts record and column orientations") {
    import ReportSupport._
    val rows = Seq(Map[String,Any]("request_id" -> "r", "items" -> Seq.empty))
    assert(AnalysisDashboardExportJob.jsonSlates(rows) == rows)
    assert(AnalysisDashboardExportJob.jsonSlates(Map("request_id" -> Map("0" -> "r"), "items" -> Map("0" -> Seq.empty))) == rows)
  }

  test("tables truncate while keyword grids retain all observed cells") {
    import ReportSupport._
    val genres = Seq("Action","Adventure","Animation","Children","Comedy","Crime","Documentary","Drama","Fantasy","Film-Noir","Horror","Musical","Mystery","Romance","Sci-Fi","Thriller","War","Western")
    val row = Map[String,Any]("item_id" -> "movie", "genres" -> genres, "label" -> 1, "release_year" -> 2000)
    val kw = DashboardAnalysis.keyword(Seq(row))
    assert(seq(kw("grid")).size == 18)
    assert(seq(kw("topic_grid")).size == 18)
    assert(seq(kw("decade_grid")).size == 18)
    assert(seq(obj(kw("tops"))("l1")).size == 10)
  }

  test("dashboard ranking uses label thresholds and honest sparse popularity coverage") {
    import ReportSupport._
    val input = Seq(
      Map[String,Any]("item_id" -> "a", "user_id" -> "u", "label" -> 2.0, "clicked" -> 0, "position" -> 1),
      Map[String,Any]("item_id" -> "b", "user_id" -> "u", "label" -> 0.0, "clicked" -> 1, "position" -> 2),
      Map[String,Any]("item_id" -> "missing", "user_id" -> "u", "label" -> 1.0, "clicked" -> 0, "position" -> 3))
    val report = AnalysisDashboardExportJob.build(input,Seq.empty,Map("a" -> 10.0,"b" -> 1.0),Map.empty,Map.empty,
      Seq.empty,None,None,Map("now" -> "2026-11-05T00:00:00Z"))
    val rows = seq(obj(report("ranking"))("rows")).map(obj)
    val pop = rows.find(_("signal") == "popularity").get
    assert(pop("n") == 2)
    assert(pop("positives") == 1)
    assert(pop("coverage") == 0.6667)
    assert(pop("positive_rate") == 0.5)
    assert(pop("auc") == 1.0)
    assert(rows.find(_("signal") == "position").get.apply("positives") == 2)
    val noPop = AnalysisDashboardExportJob.build(input,Seq.empty,Map.empty,Map.empty,Map.empty,
      Seq.empty,None,None,Map("now" -> "2026-11-05T00:00:00Z"))
    val unavailable = seq(obj(noPop("ranking"))("rows")).map(obj).find(_("signal") == "popularity").get
    assert(unavailable("n") == 0 && unavailable("coverage") == 0.0)
    assert(unavailable("auc") == null && unavailable("positive_rate") == null)
  }

}
