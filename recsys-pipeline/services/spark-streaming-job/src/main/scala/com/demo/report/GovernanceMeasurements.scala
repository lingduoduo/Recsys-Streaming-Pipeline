package com.demo.report
/** Bounded outcome accounting; these measurements do not alter recommendation decisions. */
object GovernanceMeasurements {
 import MeasurementMath._
 private def groupString(row:R,key:String):Option[String]=row.getOrElse(key,null) match {
  case value:String=>str(value)
  case value:java.lang.Number=>str(value)
  case value:Boolean=>Some(if(value) "True" else "False")
  case _=>None
 }
 val dimensions=Seq("age_band","gender","occupation","geo","device","country","subscription")
 val reasons=Seq("expired","muted_product_type","muted_genre","muted_keyword","muted_title","unknown")
 def safety(samples:Seq[R],policy:String="catalog-filter-v1"):R={
  if(samples.isEmpty)return unavailable("missing safety samples")
  val observed=samples.flatMap(groupString(_,"filter_reason")).map(x=>if(reasons.init.contains(x))x else "unknown")
  val labels=samples.flatMap(r=>bool(r.getOrElse("unsafe_label",null)))
  if(observed.isEmpty&&labels.isEmpty)return unavailable("missing filter_reason and unsafe_label safety signals")
  val counts=reasons.map(x=>x->(if(observed.isEmpty)null else observed.count(_==x):Any)).toMap
  available("Candidate safety policy accounting",Seq(Map("policy_version"->policy,"evaluated_candidates"->samples.size,"filter_decisions"->(if(observed.isEmpty)null else observed.size),"filter_decision_rate"->(if(observed.isEmpty)null else ratio(observed.size,samples.size,false)),"reason_counts"->counts,"unknown_share"->(if(observed.isEmpty)null else ratio(observed.count(_=="unknown"),samples.size,false)),"unsafe_exposure_rate"->ratio(labels.count(identity),labels.size,false),"unsafe_label_coverage"->ratio(labels.size,samples.size,false))),samples.size,((if(observed.nonEmpty)1d else 0d)+labels.size.toDouble/samples.size)/2)
 }
 private def metrics(rows:Seq[R]):R={
  val has=Seq("request_id","position","label").forall(k=>rows.exists(_.contains(k)))
  val ndcgs=if(!has)Seq.empty else rows.filter(r=>groupString(r,"request_id").nonEmpty).groupBy(r=>groupString(r,"request_id").get).values.toSeq.flatMap{slate=>
   val ranked=slate.map{r=>val label=r.getOrElse("label",null) match {case _:Boolean=>None;case x:java.lang.Number=>num(x).filter(Set(0d,1d,2d));case _=>None};(n(r,"position"),label)}
   if(ranked.exists(x=>x._1.isEmpty||x._2.isEmpty))None else {val labels=ranked.sortBy(_._1.get).map(_._2.get);ndcg(labels,labels.size)}
  }
  val clicks=rows.flatMap(r=>bool(r.getOrElse("clicked",null))).map(x=>if(x)1d else 0d)
  val orders=rows.flatMap(r=>bool(r.getOrElse("ordered",null))).map(x=>if(x)1d else 0d)
  val rewards=numeric(rows,"reward")
  Map("ctr"->mean(clicks,false),"ctr_coverage"->ratio(clicks.size,rows.size,false),"order_rate"->mean(orders,false),"order_coverage"->ratio(orders.size,rows.size,false),"mean_reward"->mean(rewards,false),"reward_coverage"->ratio(rewards.size,rows.size,false),"ndcg"->mean(ndcgs,false),"ndcg_evaluated_slate_count"->ndcgs.size)
 }
 def fairness(samples:Seq[R],minSupport:Int=100,requested:Seq[String]=dimensions):R={
  if(samples.isEmpty)return unavailable("missing fairness samples")
  if(minSupport<=0)return unavailable("min_support must be a positive integer")
  val wanted=requested.filter(dimensions.contains).distinct; val present=wanted.filter(k=>samples.exists(_.contains(k)))
  if(present.isEmpty)return unavailable("missing supported demographic dimensions")
  val overall=metrics(samples);val keys=Seq("ctr","order_rate","mean_reward","ndcg")
  val rows=present.map{dimension=>
   val grouped=samples.groupBy(r=>groupString(r,dimension).getOrElse("unknown"));val eligible=grouped.toSeq.filter(_._2.size>=minSupport).sortBy(_._1)
   val groups=eligible.map{case(group,rs)=>val m=metrics(rs);m++Map("group"->group,"support"->rs.size,"exposure_share"->ratio(rs.size,samples.size,false))++keys.map(k=>(k+"_absolute_gap_from_overall")->value(for(a<-num(m(k));b<-num(overall(k)))yield math.abs(a-b),false))}
   val disparities=keys.flatMap{k=>val vs=groups.flatMap(r=>num(r(k)));Seq((k+"_max_min_gap")->(if(vs.size<2)null else vs.max-vs.min:Any),(k+"_disparity_ratio")->(if(vs.size<2)null else ratio(vs.min,vs.max,false)))}
   Map[String,Any]("dimension"->dimension,"evaluated_candidates"->samples.size,"groups"->groups,"evaluated_group_count"->groups.size,"suppressed_group_count"->(grouped.size-groups.size),"suppressed_candidate_count"->grouped.values.filter(_.size<minSupport).map(_.size).sum)++keys.map(k=>("overall_"+k)->overall(k))++disparities
  }
  available("Outcome parity across supported demographic groups",rows,samples.size,present.size.toDouble/wanted.size,wanted.filterNot(present.contains).map("missing demographic dimension: "+_))
 }
}
