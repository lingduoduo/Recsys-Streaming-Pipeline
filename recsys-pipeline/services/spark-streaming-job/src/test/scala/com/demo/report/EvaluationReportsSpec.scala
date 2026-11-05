package com.demo.report
import org.scalatest.funsuite.AnyFunSuite
class EvaluationReportsSpec extends AnyFunSuite {
  test("ranking averages tied ranks and excludes missing embedding pairs") {
    val rows = EvaluationReports.ranking(Seq(Map("clicked"->1,"item_id"->"a"), Map("clicked"->0,"item_id"->"b")), Map.empty,Map.empty,Map.empty)
    assert(rows.head("auc") == 0.5)
    assert(rows.head("logloss") == 0.6931)
    assert(rows.last("coverage") == 0.0)
    assert(rows.last("auc") == null)
  }
  test("recall excludes other clicks and weights users rather than folds") {
    val catalog = Seq("a","b","c").map(x=>Map[String,Any]("item_id"->x,"title"->"same", "genres"->Seq.empty[String]))
    val samples = Seq("a","b").map(x=>Map[String,Any]("user_id"->"u","item_id"->x,"clicked"->1))
    val rows=EvaluationReports.recall(samples,catalog,Map.empty,Seq(1,2))
    assert(rows.head("recall_at_k")==1.0)
    assert(rows(2)("recall_at_k")==0.0)
    assert(rows.head("instances")==2)
  }
  test("OPE excludes policy outputs, preserves constant reward and seeded intervals") {
    val events = (0 until 8).map(i=>Map[String,Any]("requestId"->s"r$i","action"->"a","reward"->0.5,"actionSpace"->Seq(Map("item"->"a","impressions"->10,"modelPredictions"->Map("tabQ"->i.toDouble)),Map("item"->"b","impressions"->20,"modelPredictions"->Map("tabQ"-> -i.toDouble)))))
    assert(EvaluationReports.featureNames(events)==Seq("coldStart","impressions","clicks"))
    val result=EvaluationReports.ope(events,20,42L)
    assert(result==EvaluationReports.ope(events,20,42L))
    val rows=result("rows").asInstanceOf[Seq[Map[String,Any]]]
    assert(rows.forall(_("value")==0.5))
    assert(rows.forall(_("value_ci_low")==0.5))
    assert(rows.exists(_("policy")=="model:tabQ"))
  }
  test("OPE deterministic point estimates match Python NumPy golden fixture") {
    import ReportSupport._
    val path=this.getClass.getResource("/report/evaluation-python-golden.json").getPath
    val fixture=obj(readJson(path));val expected=obj(fixture("ope"))
    val result=EvaluationReports.ope(seq(fixture("events")).map(obj),0)
    val actualCalibration=obj(result("calibration"));val expectedCalibration=obj(expected("calibration"))
    actualCalibration.keys.foreach(k=>if(actualCalibration(k)==null)assert(expectedCalibration(k)==null)else assert(math.abs(number(actualCalibration(k))-number(expectedCalibration(k)))<1e-9))
    val actualRows=seq(result("rows")).map(obj);val expectedRows=seq(expected("rows")).map(obj)
    actualRows.zip(expectedRows).foreach { case(a,e)=>
      assert(a.keySet==e.keySet)
      a.keys.foreach(k=>if(a(k)==null)assert(e(k)==null)else if(a(k).isInstanceOf[java.lang.Number])assert(math.abs(number(a(k))-number(e(k)))<1e-9,s"$k: ${a(k)} != ${e(k)}")else assert(a(k)==e(k)))
    }
  }
  test("recall and ranking match Python mixed coverage fixtures") {
    import ReportSupport._
    val f=obj(obj(readJson(this.getClass.getResource("/report/evaluation-python-golden.json").getPath))("retrieval"))
    val samples=seq(f("samples")).map(obj)
    def vectors(key:String)=obj(f(key)).map { case(k,v)=>k->seq(v).map(number(_)) }
    val recall=EvaluationReports.recall(samples,seq(f("catalog")).map(obj),vectors("itemVectors"),Seq(1,2,3))
    val ranking=EvaluationReports.ranking(samples,obj(f("popularity")).map {case(k,v)=>k->number(v)},vectors("userVectors"),vectors("itemVectors"))
    assert(recall==seq(f("recall")).map(obj))
    assert(ranking==seq(f("ranking")).map(obj))
  }
  test("MD5 split and random picks match Python and ties keep first candidate") {
    Seq(("",false,1),("r0",false,2),("r1",false,2),("gold-1",true,2),("unicode-é",true,0)).foreach { case(request,test,index)=>
      assert(EvaluationReports.isTest(request)==test)
      val cs=Seq("a","b","c").map(x=>Map[String,Any]("item"->x,"impressions"->1))
      val event=Map[String,Any]("requestId"->request,"actionSpace"->cs)
      assert(EvaluationReports.pick("random",event).get("item")==cs(index)("item"))
      assert(EvaluationReports.pick("popularity",event).get("item")=="a")
    }
  }
  test("single class AUC is unavailable and missing candidates have zero policy coverage") {
    assert(EvaluationReports.ranking(Seq(Map("clicked"->1,"item_id"->"a")),Map.empty,Map.empty,Map.empty).head("auc")==null)
    val rows=EvaluationReports.ope(Seq(Map("requestId"->"r0","reward"->0.0)),0)("rows").asInstanceOf[Seq[Map[String,Any]]]
    assert(rows.find(_("policy")=="logging").get("n_events")==1)
    assert(rows.find(_("policy")=="random").get("n_events")==0)
    assert(rows.forall(_("lift_vs_logging")==null))
  }
}
