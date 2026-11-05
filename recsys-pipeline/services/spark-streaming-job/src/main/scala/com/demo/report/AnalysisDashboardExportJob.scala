package com.demo.report

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object AnalysisDashboardExportJob {
  import ReportSupport._
  def build(samples: Seq[Record], catalog: Seq[Record], popularity: Map[String,Double],
            userVectors: Map[String,Seq[Double]], itemVectors: Map[String,Seq[Double]],
            events: Seq[Record], slates: Option[Seq[Record]], live: Option[Record], config: Record): Record = {
    val enriched = DashboardAnalysis.enrichSamples(samples,catalog)
    val measurementConfig = if(catalog.nonEmpty) config + ("catalog_size" -> catalog.size) else config
    val measurements = DashboardMeasurements.build(enriched,slates.map(DashboardAnalysis.enrichSlates(_,catalog,popularity)),live,measurementConfig)
    val diversity = obj(measurements("diversity"))
    val aggregate = if(diversity.get("status").contains("available")) diversity + ("rows" -> seq(diversity("rows")).take(1)) else diversity
    val recall: Any = if(catalog.isEmpty) null else {
      val rows = EvaluationReports.recall(enriched,catalog,itemVectors,Seq(5,10,20))
      val hy = rows.find(r => r("method") == "hybrid" && number(r("k")) == 10)
      val bm = rows.find(r => r("method") == "bm25" && number(r("k")) == 10)
      Map("rows" -> rows,"headline" -> hy.map(r => f"hybrid recall@10 ${number(r("recall_at_k"))}%.3f vs BM25 ${number(bm.get.apply("recall_at_k"))}%.3f").getOrElse("no evaluable users"))
    }
    // The dashboard uses engagement labels and observed popularity only. The standalone
    // ranking CLI deliberately supports clicked labels and a zero popularity baseline.
    val rankingSamples = enriched.map(r => r + ("clicked" -> (if(number(r("label")) >= 1) 1 else 0)))
    val popularitySamples = rankingSamples.filter(r => popularity.contains(text(r.getOrElse("item_id",null))))
    val popularityRow = EvaluationReports.ranking(popularitySamples,popularity,userVectors,itemVectors)
      .find(_("signal") == "popularity").get + ("coverage" -> round(popularitySamples.size.toDouble / rankingSamples.size))
    val rankingRows = EvaluationReports.ranking(rankingSamples,popularity,userVectors,itemVectors).map { original =>
      val r = if(original("signal") == "popularity") popularityRow else original
      r + ("positive_rate" -> (if(number(r("n")) == 0) null else round(number(r("positives"))/number(r("n")))))
    }
    val best = rankingRows.filter(r => r.getOrElse("auc",null) != null).sortBy(r => -number(r("auc"))).headOption
    val ranking = Map("rows" -> rankingRows,"headline" -> best.map(r => f"best signal '${r("signal")}' AUC ${number(r("auc"))}%.3f").getOrElse("no scorable signal"))
    val rewarded = events.filter(_.getOrElse("reward",null) != null)
    val ope: Any = if(rewarded.isEmpty) null else try {
      val result = EvaluationReports.ope(rewarded,number(config.getOrElse("bootstrap_samples",1000)).toInt)
      if(result.isEmpty) null else result + ("source" -> config.getOrElse("ope_source","redis:replay:recommendations"))
    } catch {case NonFatal(_) => null}
    measurements ++ Map("schemaVersion" -> "2.0", "input" -> config.getOrElse("input","/tmp/spark-recsys/training-samples"),
      "rows" -> enriched.size, "diversity" -> aggregate, "engagement" -> DashboardAnalysis.engagement(enriched),
      "keyword" -> DashboardAnalysis.keyword(enriched), "query" -> DashboardAnalysis.query(enriched),
      "recall" -> recall,"ranking" -> ranking,"ope" -> ope)
  }

  /** pandas read_json accepts record arrays and its default column-oriented object. */
  private[report] def jsonSlates(value: Any): Seq[Record] = value match {
    case _: scala.collection.Map[_,_] =>
      val columns = obj(value)
      val indexes = columns.values.flatMap(v => obj(v).keys).toSeq.distinct.sortBy { k =>
        scala.util.Try(k.toLong).map(n => (0,n,k)).getOrElse((1,0L,k))
      }
      if(indexes.nonEmpty) indexes.map(i => columns.map {case(k,v) => k -> obj(v).getOrElse(i,null)})
      else {
        val count = columns.values.map(seq(_).size).foldLeft(0)(math.max)
        (0 until count).map(i => columns.map {case(k,v) => k -> seq(v).lift(i).orNull})
      }
    case _ => seq(value).map(obj)
  }

  def main(args: Array[String]): Unit = {
    val allowed = Set("input","output","experiences","ope-parquet","live-metrics","fairness-min-support","freshness-window-days","long-tail-percentile","safety-policy-version","redis-host","redis-port","help")
    val opts = options(args,allowed)
    if(opts.contains("help")) { println("AnalysisDashboardExportJob --input PATH --output PATH [--experiences JSON|JSONL|PARQUET] [--ope-parquet PATH] [--live-metrics JSON] [--fairness-min-support 100] [--freshness-window-days 30] [--long-tail-percentile 0.80] [--safety-policy-version catalog-filter-v1]"); return }
    val host = opts.getOrElse("redis-host",sys.env.getOrElse("REDIS_HOST","localhost"))
    val port = opts.getOrElse("redis-port",sys.env.getOrElse("REDIS_PORT","6379")).toInt
    val input = opts.getOrElse("input","/tmp/spark-recsys/training-samples")
    val config: Record = Map("input" -> input,"fairness_min_support" -> opts.getOrElse("fairness-min-support","100").toInt,
      "freshness_window_days" -> opts.getOrElse("freshness-window-days","30").toInt,
      "long_tail_percentile" -> opts.getOrElse("long-tail-percentile","0.80").toDouble,
      "safety_policy_version" -> opts.getOrElse("safety-policy-version","catalog-filter-v1"),
      "ope_source" -> opts.get("ope-parquet").map("parquet:" + _).getOrElse("redis:replay:recommendations"))
    val spark = SparkSession.builder.appName("AnalysisDashboardExportJob").getOrCreate()
    try {
      val samples = records(spark,input)
      require(samples.nonEmpty,s"No training samples found at $input. Wait for the streaming simulation to finish before exporting the dashboard.")
      val slates = opts.get("experiences").map { p =>
        if(p.endsWith(".jsonl")) Files.readAllLines(Paths.get(p),UTF_8).asScala.filter(_.trim.nonEmpty).map(line => obj(parseJson(line))).toSeq
        else if(p.endsWith(".json")) jsonSlates(readJson(p))
        else records(spark,p)
      }
      val live = opts.get("live-metrics").map(p => obj(readJson(p)))
      // Explicit Parquet errors propagate; Redis is consulted only without that override.
      val events = opts.get("ope-parquet").map(records(spark,_)).getOrElse(replay(host,port,"replay:recommendations",-1))
      val result = build(samples,catalog(host,port),popularity(host,port),embeddings(host,port,"uEmb"),embeddings(host,port,"i2vEmb"),events,slates,live,config)
      val output = opts.getOrElse("output","frontend/data/dashboard.json")
      writeJson(output,result)
      println(s"wrote $output (${Files.size(Paths.get(output))} bytes)")
    } finally spark.stop()
  }
}
