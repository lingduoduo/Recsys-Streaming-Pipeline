package com.demo.report
/** Offline quality measurements. Optional numbers remain null in the JSON contract;
  * absent observations never become zero-valued measurements.
  */
object QualityMeasurements {
 import MeasurementMath._
 def relevance(slates:Seq[R],ks:Seq[Int]=Seq(5,10,20)):R={
  val all=slates.map(r=>items(r));val observed=all.flatten.count(i=>n(i,"label").nonEmpty)
  val complete=slates.zip(all).filter{case(_,is)=>is.nonEmpty&&is.forall(i=>n(i,"label").nonEmpty)}.map{case(r,is)=>(is.map(i=>n(i,"label").get),s(r,"user_id"))}
  if(complete.isEmpty)return unavailable("missing complete labeled slates")
  val rows=ks.filter(_>0).map{k=>val users=complete.filter(_._2.nonEmpty).groupBy(_._2.get).values.toSeq.map(_.map{case(ls,_)=>if(ls.take(k).exists(_>0))1d else 0d});val nd=complete.flatMap(x=>ndcg(x._1,k))
   Map[String,Any]("k"->k,"ndcg_at_k"->mean(nd),"mrr_at_k"->mean(complete.map(x=>x._1.take(k).indexWhere(_>0) match{case -1=>0d;case i=>1d/(i+1)})),"recall_at_k"->mean(users.map(x=>x.sum/x.size)),"hit_rate_at_k"->mean(users.map(x=>if(x.exists(_>0))1d else 0d)),"evaluated_slate_count"->complete.size,"ndcg_evaluated_slate_count"->nd.size,"evaluated_user_count"->users.size,"leave_one_out_fold_count"->users.map(_.size).sum,"positive_label_count"->complete.map(_._1.count(_>0)).sum,"label_coverage"->ratio(observed,all.map(_.size).sum.toDouble))}
  if(rows.isEmpty)unavailable("missing positive relevance cutoffs")else available("Graded relevance across labeled slates",rows,complete.size,observed.toDouble/all.map(_.size).sum)
 }
 def satisfaction(samples:Seq[R]):R={
  if(samples.isEmpty)return unavailable("missing satisfaction samples")
  val pairs=Seq(("clicked","ctr","ctr_coverage"),("ordered","order_rate","order_coverage"),("reward","mean_reward","reward_coverage"),("rating","mean_rating","rating_coverage"),("dwell_millis","mean_dwell_millis","dwell_coverage"),("completion_rate","mean_completion_rate","completion_coverage"))
  val negative=samples.count(r=>r.get("negative_feedback_reason").exists(v=> !missing(v)))
  if(!pairs.exists(x=>numeric(samples,x._1).nonEmpty)&&negative==0)return unavailable("missing observed satisfaction signals")
  val ratings=numeric(samples,"rating")
  val row=pairs.flatMap{case(col,metric,cov)=>val v=numeric(samples,col);Seq(metric->mean(v),cov->ratio(v.size,samples.size))}.toMap++Map("negative_feedback_rate"->(if(negative==0)null else ratio(negative,samples.size)),"negative_feedback_coverage"->ratio(negative,samples.size),"users"->distinct(samples,"user_id"),"items"->distinct(samples,"item_id"),"rated_samples"->ratings.size)
  val timed=samples.flatMap(r=>n(r,"impression_ts").map(t=>(r,t)))
  val b=buckets(timed.map(_._2));val series=b.toSeq.flatMap{case(starts,_,index)=>starts.zipWithIndex.map{case(t,i)=>val part=timed.filter(x=>index(x._2)==i).map(_._1);Map[String,Any]("bucket_start"->rounded(t,1),"impressions"->part.size,"users"->distinctWithColumns(part,samples,"user_id"),"ctr"->mean(numeric(part,"clicked")),"order_rate"->mean(numeric(part,"ordered")),"mean_rating"->mean(numeric(part,"rating")),"ratings"->numeric(part,"rating").size)}}
  val halves=ratings.map(x=>math.min(math.floor(x*2).toInt,9));val distribution=if(halves.isEmpty)Seq.empty else (halves.min to 9).map(i=>Map("rating"->i/2d,"count"->halves.count(_==i)))
  available("Observed user satisfaction",Seq(row),samples.size,numeric(samples,"clicked").size.toDouble/samples.size)++Map("series"->series,"series_bucket_seconds"->b.map[Any](x=>rounded(x._2,3)).orNull,"rating_distribution"->distribution)
 }
 private def distinctWithColumns(part:Seq[R],all:Seq[R],key:String):Any=if(all.exists(_.contains(key)))part.flatMap(_.get(key)).filterNot(missing).distinct.size else null
 private def genres(i:R):Seq[String]=seq(i.getOrElse("genres",null)).flatMap(str)
 private def quantile(v:Seq[Double],q:Double):Option[Double]=if(v.isEmpty)None else {val a=v.sorted;val p=(a.size-1)*q;Some(a(p.toInt)+(a(math.ceil(p).toInt)-a(p.toInt))*(p-p.toInt))}
 def diversity(slates:Seq[R],percentile:Double=.8,catalogSize:Option[Int]=None):R={
  if(!(percentile>0&&percentile<1))return unavailable("long-tail percentile must be between zero and one")
  val entries=slates.zipWithIndex.filter(x=>items(x._1).nonEmpty);if(entries.isEmpty)return unavailable("missing slate items")
  val all=entries.flatMap(x=>items(x._1));val distinctItems=scala.collection.mutable.LinkedHashMap.empty[String,R]
  all.zipWithIndex.foreach{case(i,p)=>val key=s(i,"item_id").getOrElse("#"+p);if(!distinctItems.contains(key))distinctItems(key)=i}
  val pop=scala.collection.mutable.LinkedHashMap.empty[String,Double];all.zipWithIndex.foreach{case(i,p)=>n(i,"popularity").foreach(v=>{val key=s(i,"item_id").getOrElse("#"+p);if(!pop.contains(key))pop(key)=v})};val cutoff=quantile(pop.values.toSeq,percentile)
  val rows=entries.map{case(r,index)=>val is=items(r);val gs=is.map(genres).filter(_.nonEmpty);val flat=gs.flatten;val counts=flat.groupBy(identity).mapValues(_.size);val entropy=if(counts.isEmpty)null else if(counts.size==1)0d else -counts.values.map(c=>c.toDouble/flat.size*math.log(c.toDouble/flat.size)).sum/math.log(counts.size)
   val distances=gs.indices.flatMap(a=>(a+1 until gs.size).map{b=>1d-gs(a).toSet.intersect(gs(b).toSet).size.toDouble/(gs(a).toSet++gs(b).toSet).size});val ps=is.flatMap(n(_,"popularity"))
   Map[String,Any]("scope"->"slate","slate_id"->s(r,"request_id").getOrElse("row-"+index),"unique_genres_at_k"->(if(flat.isEmpty)null else counts.size.toDouble),"normalized_genre_entropy"->entropy,"intra_list_genre_distance"->mean(distances),"long_tail_exposure_share"->cutoff.map[Any](c=>ratio(ps.count(_<c),ps.size)).orNull,"genre_coverage"->ratio(gs.size,is.size),"popularity_coverage"->ratio(ps.size,is.size))}
  val metrics=Seq("unique_genres_at_k","normalized_genre_entropy","intra_list_genre_distance","long_tail_exposure_share")
  val aggregate=Map[String,Any]("scope"->"aggregate","genre_coverage"->ratio(all.count(i=>genres(i).nonEmpty),all.size),"popularity_coverage"->ratio(all.count(i=>n(i,"popularity").nonEmpty),all.size),"long_tail_popularity_cutoff"->value(cutoff))++metrics.map(k=>k->mean(rows.flatMap(r=>num(r(k)))))++spread(entries.map(_._1),catalogSize)
  if(metrics.forall(k=>aggregate(k)==null)&&cutoff.isEmpty)return unavailable("missing genre and popularity diversity signals")
  val exposed=all.flatMap(genres).groupBy(identity).mapValues(_.size);val served=distinctItems.values.toSeq.flatMap(genres).groupBy(identity).mapValues(_.size)
  val genreExposure=exposed.toSeq.sortBy{case(g,c)=>(-c,g)}.map{case(g,c)=>Map("genre"->g,"exposure_share"->ratio(c.toDouble,exposed.values.sum.toDouble),"served_share"->ratio(served.getOrElse(g,0).toDouble,served.values.sum.toDouble))}
  val timed=entries.zip(rows).flatMap{case((r,_),row)=>n(r,"request_ts").map(t=>(row,t))};val b=buckets(timed.map(_._2))
  val series=b.toSeq.flatMap{case(starts,_,index)=>starts.zipWithIndex.map{case(t,i)=>val rs=timed.filter(x=>index(x._2)==i).map(_._1);Map[String,Any]("bucket_start"->rounded(t,1),"slates"->rs.size)++metrics.tail.map(k=>k->mean(rs.flatMap(r=>num(r(k)))))}}
  available("Catalog diversity across slates",aggregate+:rows,entries.size,all.count(i=>genres(i).nonEmpty).toDouble/all.size)++Map("distributions"->Seq("normalized_genre_entropy","intra_list_genre_distance").map(k=>k->histogram(rows.flatMap(r=>num(r(k))))).toMap,"genre_exposure"->genreExposure,"series"->series,"series_bucket_seconds"->b.map[Any](x=>rounded(x._2,3)).orNull)
 }
 // Include unserved catalog items as zeros, and account for repeats under uniform serving.
 private def spread(slates:Seq[R],catalog:Option[Int]):R={
  val ids=slates.flatMap(r=>items(r).flatMap(s(_,"item_id")));val counts=ids.groupBy(identity).values.map(_.size).toSeq.sorted;val n=counts.size;val padded=Seq.fill(math.max(0,catalog.getOrElse(0)-n))(0)++counts;val m=padded.size;val total=counts.sum
  val userIds=slates.filter(r=>s(r,"user_id").nonEmpty).groupBy(r=>s(r,"user_id").get).values.toSeq.map(_.flatMap(r=>items(r).flatMap(s(_,"item_id"))))
  val distinct=userIds.map(_.distinct.size);val seen=userIds.map(_.size).sum
  Map("items_served"->(if(n==0)null else n),"catalog_size"->catalog.map[Any](identity).orNull,"catalog_coverage"->catalog.filter(c=>n>0&&n<=c).map[Any](c=>ratio(n,c)).orNull,"exposure_gini"->(if(n==0)null else rounded(padded.zipWithIndex.map{case(c,i)=>(2d*(i+1)-m-1)*c}.sum/(m.toDouble*total))),"top_decile_exposure_share"->(if(n>0&&m>=10)ratio(padded.takeRight(m/10).sum.toDouble,total)else null),"median_items_per_user"->median(distinct.map(_.toDouble)),"user_repeat_rate"->(if(seen>0)rounded(1-distinct.sum.toDouble/seen)else null),"user_repeat_rate_uniform"->catalog.filter(_>0).filter(_=>seen>0).map[Any](c=>rounded(1-userIds.map(x=>c*(1-math.pow(1-1d/c,x.size))).sum/seen)).orNull)
 }
 private case class Obs(fresh:Boolean,age:Option[Double],exposed:Boolean,click:Option[Double],reward:Option[Double],id:Option[String])
 private def instant(v:Any):Option[Double]=v match {case null=>None;case n:java.lang.Number=>num(n);case x:java.time.Instant=>Some(x.toEpochMilli/1000d);case x:java.util.Date=>Some(x.getTime/1000d);case x=>val t=x.toString.trim.replace(' ','T');scala.util.Try(java.time.Instant.parse(t).toEpochMilli/1000d).toOption.orElse(scala.util.Try(java.time.OffsetDateTime.parse(t).toInstant.toEpochMilli/1000d).toOption).orElse(scala.util.Try(java.time.LocalDateTime.parse(t).toInstant(java.time.ZoneOffset.UTC).toEpochMilli/1000d).toOption).orElse(scala.util.Try(java.time.LocalDate.parse(t).atStartOfDay.toInstant(java.time.ZoneOffset.UTC).toEpochMilli/1000d).toOption)}
 def freshness(samples:Seq[R],now:Any,window:Int=30):R={
  if(samples.isEmpty)return unavailable("missing freshness samples")
  if(window<0)return unavailable("freshness window must be non-negative")
  val nowTs=instant(now).getOrElse(java.time.Instant.now.toEpochMilli/1000d)
  val timestamps=samples.flatMap{r=>instant(r.getOrElse("published_at",null)).map{published=>val exposure=n(r,"impression_ts").filter(t=>t>=0&&t<1e11);val age=math.max(0,(exposure.getOrElse(nowTs)-published)/86400);Obs(age<=window,Some(age),exposure.nonEmpty,n(r,"clicked"),n(r,"reward"),s(r,"item_id"))}}
  val source=if(timestamps.nonEmpty)"published_at"else "boolean_new_release"
  val obs=if(timestamps.nonEmpty)timestamps else samples.flatMap{r=>bool(r.getOrElse("new_release",null)).map(f=>Obs(f,None,false,n(r,"clicked"),n(r,"reward"),s(r,"item_id")))}
  if(obs.isEmpty)return unavailable("missing published_at and new_release freshness signals")
  val fresh=obs.filter(_.fresh);val established=obs.filterNot(_.fresh);val share= fresh.size.toDouble/obs.size
  def cohort(rs:Seq[Obs],prefix:String):R=Map((prefix+"_ctr")->mean(rs.flatMap(_.click)),(prefix+"_ctr_coverage")->ratio(rs.count(_.click.nonEmpty),rs.size),(prefix+"_mean_reward")->mean(rs.flatMap(_.reward)),(prefix+"_reward_coverage")->ratio(rs.count(_.reward.nonEmpty),rs.size))
  val a=cluster(fresh,None);val b=cluster(established,None);val se=for(x<-a;y<-b)yield math.hypot(x._2,y._2);val diff=for(x<-a;y<-b)yield x._1-y._1
  val movieFresh=obs.filter(_.id.nonEmpty).groupBy(_.id.get).values.map(_.exists(_.fresh)).toSeq;val supply=if(movieFresh.isEmpty)None else Some(movieFresh.count(identity).toDouble/movieFresh.size)
  val row=Map[String,Any]("freshness_source"->source,"fresh_share"->ratio(fresh.size,obs.size),"freshness_coverage"->ratio(obs.size,samples.size),"age_at_exposure_coverage"->(if(source=="published_at")ratio(obs.count(_.exposed),obs.size)else null),"mean_content_age_days"->mean(obs.flatMap(_.age)),"median_content_age_days"->median(obs.flatMap(_.age)),"fresh_ctr_diff"->value(diff.filter(_=>se.exists(_>0))),"fresh_ctr_diff_se"->value(se.filter(_>0)),"fresh_ctr_diff_z"->(for(d<-diff;e<-se if e>0)yield rounded(d/e,2)).map[Any](identity).orNull,"fresh_item_share"->value(supply),"fresh_exposure_lift"->value(supply.filter(_>0).map(share/_)))++cohort(fresh,"fresh")++cohort(established,"established")
  val aged=obs.filter(_.age.nonEmpty);val clicks=aged.flatMap(_.click);val p0=if(clicks.isEmpty)None else Some(clicks.sum/clicks.size);val movies=aged.flatMap(_.id).distinct.size
  val bands=if(aged.isEmpty)Seq.empty else Seq(("0-7 d",0d,7d),("8-30 d",7d,30d),("31-90 d",30d,90d),("91-365 d",90d,365d),("> 1 y",365d,Double.PositiveInfinity)).map{case(label,lo,hi)=>val rs=aged.filter(o=>(if(lo==0)o.age.get>=lo else o.age.get>lo)&&o.age.get<=hi);val stats=p0.flatMap(p=>cluster(rs,Some(p)));val itemCount=rs.flatMap(_.id).distinct.size
   Map[String,Any]("band"->label,"exposures"->rs.size,"exposure_share"->ratio(rs.size,aged.size),"items"->itemCount,"item_share"->ratio(itemCount,movies),"ctr"->mean(rs.flatMap(_.click)),"z"->(for(p<-p0;st<-stats if st._2>0)yield rounded((st._1-p)/st._2,2)).map[Any](identity).orNull)}
  available("Fresh-item exposure",Seq(row),samples.size,obs.size.toDouble/samples.size)+("age_bands"->bands)
 }
 // Movie-clustered standard errors preserve repeated-impression dependence.
 private def cluster(obs:Seq[Obs],center:Option[Double]):Option[(Double,Double)]={val rs=obs.filter(o=>o.id.nonEmpty&&o.click.nonEmpty);val groups=rs.groupBy(_.id.get);if(groups.size<2)None else {val mean=rs.flatMap(_.click).sum/rs.size;val pivot=center.getOrElse(mean);val resid=groups.values.map(g=>math.pow(g.flatMap(_.click).sum-pivot*g.size,2)).sum;Some((mean,math.sqrt(resid/(rs.size.toDouble*rs.size)*groups.size/(groups.size-1))))}}
}
