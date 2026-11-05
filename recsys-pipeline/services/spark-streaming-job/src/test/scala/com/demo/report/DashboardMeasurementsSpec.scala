package com.demo.report
import org.scalatest.funsuite.AnyFunSuite
class DashboardMeasurementsSpec extends AnyFunSuite {
 test("all measurement sections match Python golden fixtures recursively") {
  val path=getClass.getResource("/report/measurement-reference.json").getPath
  def compare(actual:Any,expected:Any,path:String):Unit=(actual,expected) match {
   case (a:Map[_,_],e:Map[_,_]) => assert(a.keySet==e.keySet,path);e.foreach{case(k,v)=>compare(a.asInstanceOf[Map[Any,Any]](k),v,path+"."+k)}
   case (a:Seq[_],e:Seq[_]) => assert(a.size==e.size,path);a.zip(e).zipWithIndex.foreach{case((x,y),i)=>compare(x,y,path+"["+i+"]")}
   case (a:java.lang.Number,e:java.lang.Number)=>assert(math.abs(a.doubleValue-e.doubleValue)<1e-9,s"$path: $a != $e")
   case _=>assert(actual==expected,s"$path: $actual != $expected")
  }
  MeasurementMath.seq(ReportSupport.readJson(path)).zipWithIndex.foreach{case(v,i)=>val c=MeasurementMath.obj(v)
   val samples=MeasurementMath.seq(c("samples")).map(MeasurementMath.obj)
   val slates=Option(c("slates")).map(x=>MeasurementMath.seq(x).map(MeasurementMath.obj))
   val live=Option(c("live")).map(MeasurementMath.obj)
   compare(DashboardMeasurements.build(samples,slates,live,MeasurementMath.obj(c("config"))),c("expected"),"case"+i)
  }
 }
 test("measurement configuration rejects invalid ranges and blank policy") {
  Seq(Map[String,Any]("fairness_min_support"->0),Map[String,Any]("freshness_window_days"-> -1),Map[String,Any]("long_tail_percentile"->1),Map[String,Any]("safety_policy_version"->" ")).foreach { config =>
   intercept[IllegalArgumentException](DashboardMeasurements.build(Seq.empty,None,None,config))
  }
 }
 test("missing sources preserve unavailable semantics") {
  val result = DashboardMeasurements.build(Seq.empty,None,None,Map.empty)
  Seq("relevance","satisfaction","freshness","diversity","fairness","safety","latency").foreach(k => assert(result(k).asInstanceOf[Map[String,Any]]("status") == "unavailable"))
 }
 test("relevance averages user folds and excludes incomplete slate labels") {
  val slates = Seq(Map[String,Any]("user_id"->"u", "items"->Seq(Map("label"->0),Map("label"->2))),Map[String,Any]("user_id"->"u", "items"->Seq(Map("label"->1))),Map[String,Any]("user_id"->"v", "items"->Seq(Map("label"->null))))
  val result = QualityMeasurements.relevance(slates,Seq(1))
  val row = result("rows").asInstanceOf[Seq[Map[String,Any]]].head
  assert(row("recall_at_k")==0.5); assert(row("hit_rate_at_k")==1.0)
  assert(row("label_coverage")==0.75); assert(row("ndcg_at_k")==0.5)
 }
 test("safety distinguishes observed false from absent labels") {
  val row = GovernanceMeasurements.safety(Seq(Map("unsafe_label"->false)),"v")("rows").asInstanceOf[Seq[Map[String,Any]]].head
  assert(row("unsafe_exposure_rate")==0.0); assert(row("filter_decisions")==null)
 }
 test("freshness ages at exposure and emits all bands") {
  val samples=Seq(Map[String,Any]("published_at"->0L,"impression_ts"->86400L,"clicked"->1,"item_id"->"a"))
  val result=DashboardMeasurements.build(samples,None,None,Map("now"->"2026-01-01T00:00:00Z"))("freshness").asInstanceOf[Map[String,Any]]
  assert(result("age_bands").asInstanceOf[Seq[Any]].size==5)
  assert(result("rows").asInstanceOf[Seq[Map[String,Any]]].head("mean_content_age_days")==1.0)
 }
}
