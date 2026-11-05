package com.demo.report

/** Pure dashboard aggregations; record keys mirror the Python exporter. */
object DashboardAnalysis {
  import ReportSupport._
  private def n(r: Record, k: String): Double = number(r.getOrElse(k, null))
  private def genres(r: Record): Seq[String] = seq(r.getOrElse("genres", null)).map(text(_))
  private def queryOf(r: Record): String = { val g = genres(r); if(g.isEmpty) "unknown" else g.mkString(" ") }
  private def pct(v: Double): String = f"${math.rint(v * 100)}%.0f%%"
  private def primary(r: Record): String = genres(r).headOption.getOrElse("unknown")
  private val families = Seq("Action&Adventure" -> Seq("Action","Adventure","War","Western"), "SciFi&Fantasy" -> Seq("Sci-Fi","Fantasy","Animation"), "Drama&Romance" -> Seq("Drama","Romance","Musical"), "Comedy" -> Seq("Comedy","Children"), "Crime&Thriller" -> Seq("Crime","Thriller","Mystery","Film-Noir","Horror")).flatMap {case(f,gs) => gs.map(_ -> f)}.toMap
  private def decade(r: Record): String = {
    val v = r.getOrElse("release_year", null)
    try { val year = v match { case n: Number if !n.doubleValue.isNaN && !n.doubleValue.isInfinity => n.intValue; case s: String => s.trim.toInt; case _ => throw new NumberFormatException }; s"${math.floor(year / 10.0).toInt * 10}s" } catch {case _: NumberFormatException => "unknown"}
  }
  def enrichSamples(samples: Seq[Record], catalog: Seq[Record]): Seq[Record] = {
    require(samples.nonEmpty, "No training samples found. Wait for the streaming simulation to finish before exporting the dashboard.")
    val meta = catalog.map(r => text(r("item_id")) -> r).toMap
    val hasLabel = samples.exists(_.contains("label")); val hasClicked = samples.exists(_.contains("clicked")); val hasYear = samples.exists(_.contains("release_year"))
    samples.map { r =>
      val label = if(hasLabel) n(r,"label") else n(r,"clicked")
      val m = meta.getOrElse(text(r.getOrElse("item_id",null)), Map.empty)
      val g = if(genres(r).nonEmpty) genres(r) else genres(m)
      val base = r ++ Map("label" -> label, "clicked" -> (if(hasClicked) n(r,"clicked").toInt else if(label >= 1) 1 else 0), "genres" -> g)
      if(hasYear) base else base + ("release_year" -> m.getOrElse("release_year",null))
    }
  }
  def enrichSlates(slates: Seq[Record], catalog: Seq[Record], popularity: Map[String,Double]): Seq[Record] = {
    val meta = catalog.map(r => text(r("item_id")) -> r).toMap
    slates.map { r => if(!r.contains("items")) r else r + ("items" -> seq(r("items")).map { v =>
      val item = obj(v); val id = text(item.getOrElse("item_id",null))
      val enriched = item + ("genres" -> (if(genres(item).nonEmpty) genres(item) else genres(meta.getOrElse(id,Map.empty))))
      if((!item.contains("popularity") || item("popularity") == null || number(item("popularity"),Double.NaN).isNaN) && popularity.contains(id)) enriched + ("popularity" -> popularity(id)) else enriched
    }) }
  }
  private def rates(rows: Seq[Record]): Record = {
    val clicks = rows.count(n(_,"label") >= 1); val orders = rows.count(n(_,"label") >= 2)
    Map("impressions" -> rows.size, "clicks" -> clicks, "orders" -> orders,
      "ctr" -> round(clicks.toDouble / rows.size), "cvr" -> round(orders.toDouble / rows.size))
  }
  def engagement(samples: Seq[Record]): Record = {
    val clicks = samples.count(n(_,"label") >= 1); val orders = samples.count(n(_,"label") >= 2)
    val ctr = round(clicks.toDouble / samples.size); val cvr = round(orders.toDouble / samples.size)
    def group(pairs: Seq[(String,Record)], key: String): Seq[Record] = pairs.groupBy(_._1).toSeq.sortBy(_._1).map {case(k,rs) =>
      val records = rs.map(_._2); rates(records) ++ Map(key -> k, "mean_score" -> (records.map(n(_,"label")).sum / records.size))
    }.sortBy(r => (-n(r,"mean_score"),-n(r,"impressions"))).take(10)
    Map("headline" -> s"impressions ${samples.size} · CTR ${pct(ctr)} · CVR ${pct(cvr)}", "ctr" -> ctr, "cvr" -> cvr,
      "funnel" -> Map("impression" -> samples.size, "click" -> clicks, "order" -> orders),
      "by_query" -> group(samples.map(r => queryOf(r) -> r),"query"), "by_genre" -> group(samples.flatMap(r => genres(r).map(_ -> r)),"genre"))
  }
  def query(samples: Seq[Record]): Record = {
    val top = samples.groupBy(queryOf).toSeq.sortBy(_._1).map {case(q,rs) => rates(rs) ++ Map("query" -> q,"query_len" -> q.length)}.sortBy(r => -n(r,"impressions"))
    val byLength = samples.groupBy(r => if(queryOf(r).length <= 10) "short (<=10)" else "long (>10)").toSeq.sortBy(_._1).map {case(b,rs) => rates(rs) ++ Map("bucket" -> b,"queries" -> rs.map(queryOf).distinct.size)}
    Map("headline" -> top.headOption.map(r => s"top query '${r("query")}' (${r("impressions")} impr, CTR ${pct(n(r,"ctr"))})").getOrElse("no queries"),
      "top_queries" -> top.take(10), "by_length" -> byLength, "average_query_length" -> (if(samples.isEmpty) null else round(samples.map(queryOf(_).length.toDouble).sum / samples.size,2)))
  }
  def keyword(samples: Seq[Record]): Record = {
    def level(r: Record, name: String): String = name match {case "l1" => families.getOrElse(primary(r),"Other"); case "l2" => primary(r); case "l3" => s"${primary(r)}·${decade(r)}"; case _ => decade(r)}
    def distribution(key: String, f: Record => String): Seq[Record] = {
      val totalClicks = math.max(1,samples.count(n(_,"label") >= 1))
      samples.groupBy(f).toSeq.sortBy(_._1).map {case(k,rs) =>
        val clicks = rs.count(n(_,"label") >= 1); val orders = rs.count(n(_,"label") >= 2)
        val ms = round(rs.size.toDouble / samples.size); val qs = round(clicks.toDouble / totalClicks)
        Map[String,Any](key -> k,"movie_impressions" -> rs.size,"query_clicks" -> clicks,"query_orders" -> orders,"mean_score" -> round(rs.map(n(_,"label")).sum / rs.size),"movie_share" -> ms,"query_share" -> qs,"divergence" -> round(qs-ms),"ctr" -> round(clicks.toDouble/rs.size),"cvr" -> round(orders.toDouble/rs.size))
      }.sortBy(r => -n(r,"movie_impressions"))
    }
    def cells(lvl: String, rowName: String, z: Boolean): Seq[Record] = {
      val exploded = samples.flatMap(r => genres(r).map(g => (level(r,lvl),g,r)))
      val p0 = if(exploded.isEmpty) 0.0 else exploded.count(t => n(t._3,"label") >= 1).toDouble/exploded.size
      exploded.groupBy(t => (t._1,t._2)).toSeq.sortBy(_._1).map {case((l,g), entries) =>
        val clicks = entries.count(t => n(t._3,"label") >= 1)
        val base = Map[String,Any](rowName -> l,"keyword" -> g,"movie_impressions" -> entries.size,"query_clicks" -> clicks,"ctr" -> round(clicks.toDouble/entries.size))
        if(!z) base else {
          val items = entries.filter(_._3.getOrElse("item_id",null) != null).groupBy(t => text(t._3("item_id")))
          val resid2 = items.values.map(rs => math.pow(rs.count(t => n(t._3,"label") >= 1)-p0*rs.size,2)).sum
          val score: Any = if(p0 <= 0 || p0 >= 1 || items.size < 2 || resid2 <= 0) null else round((clicks.toDouble/entries.size-p0)/math.sqrt(resid2/(entries.size.toDouble*entries.size)*items.size/(items.size-1)))
          base + ("z" -> score)
        }
      }
    }
    val byKeyword = distribution("keyword",primary)
    val movies = samples.groupBy(r => text(r.getOrElse("item_id",null))).values.map(_.head).toSeq
    val multi = movies.map(genres).filter(_.size > 1)
    val sortedShare = if(multi.isEmpty) 0.0 else multi.count(g => g == g.sorted).toDouble/multi.size
    val chanceShare = if(multi.isEmpty) 0.0 else multi.map(g => 1.0/(1 to g.size).foldLeft(1.0)(_ * _)).sum/multi.size
    val lead = byKeyword.sortBy(r => -math.abs(n(r,"divergence"))).headOption
    val headline = lead.map(r => s"'${r("keyword")}' diverges most: shown ${pct(n(r,"movie_share"))} vs clicked ${pct(n(r,"query_share"))}").getOrElse("no keywords")
    val tops = Seq("l1","l2","l3").map(l => l -> cells(l,l,false).groupBy(_(l)).toSeq.sortBy(p => text(p._1)).flatMap {case(_,rs) => rs.sortBy(r => (-n(r,"movie_impressions"),text(r("keyword")))).take(10).zipWithIndex.map {case(r,i) => r + ("rank" -> (i+1))}}.take(10)).toMap
    Map("headline" -> (if(multi.size >= 20 && sortedShare >= .95) s"genre lists look alphabetical; primary genre is unreliable -- $headline" else headline), "by_keyword" -> byKeyword.take(50), "by_subkeyword" -> distribution("subkeyword",r => genres(r).lift(1).getOrElse("none")).take(50), "by_decade" -> distribution("decade",decade), "tops" -> tops,
      "grid" -> cells("l1","category",true),"topic_grid" -> cells("l2","topic",true),"decade_grid" -> cells("dec","decade",true),"genre_order" -> Map("multi_genre_movies" -> multi.size,"sorted_share" -> round(sortedShare),"chance_share" -> round(chanceShare)))
  }
}
