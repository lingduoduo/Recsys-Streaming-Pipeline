package com.demo.report
/** Small pure helpers shared by the measurement calculators only. */
private[report] object MeasurementMath {
 type R=Map[String,Any]
 def num(v:Any):Option[Double]=v match {case null=>None;case b:Boolean=>Some(if(b)1d else 0d);case x=>scala.util.Try(x.toString.toDouble).toOption.filter(java.lang.Double.isFinite)}
 def missing(v:Any):Boolean=v match {case null=>true;case d:Double=>d.isNaN;case f:Float=>f.isNaN;case _=>false}
 def str(v:Any):Option[String]=Option(v).filterNot(missing).map(_.toString.trim).filter(_.nonEmpty)
 def bool(v:Any):Option[Boolean]=v match {case b:Boolean=>Some(b);case s:String if s.trim.equalsIgnoreCase("true")=>Some(true);case s:String if s.trim.equalsIgnoreCase("false")=>Some(false);case n:java.lang.Number=>num(n).filter(x=>x==0||x==1).map(_==1);case _=>None}
 def obj(v:Any):R=ReportSupport.obj(v)
 def seq(v:Any):Seq[Any]=ReportSupport.seq(v)
 def items(r:R):Seq[R]=seq(r.getOrElse("items",null)).collect{case m:Map[_,_]=>obj(m)}
 def n(r:R,k:String):Option[Double]=num(r.getOrElse(k,null))
 def s(r:R,k:String):Option[String]=str(r.getOrElse(k,null))
 def rounded(v:Double,d:Int=4):Double=BigDecimal(v).setScale(d,BigDecimal.RoundingMode.HALF_EVEN).toDouble
 def value(v:Option[Double],round:Boolean=true):Any=v.map[Any](x=>if(round)rounded(x) else x).orNull
 def mean(v:Iterable[Double],round:Boolean=true):Any={val a=v.toSeq;value(if(a.isEmpty)None else Some(a.sum/a.size),round)}
 def median(v:Seq[Double]):Any={val a=v.sorted;value(if(a.isEmpty)None else Some((a((a.size-1)/2)+a(a.size/2))/2))}
 def ratio(a:Double,b:Double,round:Boolean=true):Any=value(if(b<=0)None else Some(a/b),round)
 def available(h:String,rows:Seq[R],size:Int,coverage:Double,warnings:Seq[String]=Seq.empty):R=Map("status"->"available","headline"->h,"sampleSize"->size,"coverage"->rounded(coverage),"window"->null,"warnings"->warnings,"rows"->rows)
 def unavailable(reason:String):R=Map("status"->"unavailable","headline"->"N/A","warnings"->Seq(reason),"rows"->Seq.empty)
 def numeric(rows:Seq[R],key:String):Seq[Double]=rows.flatMap(n(_,key))
 def distinct(rows:Seq[R],key:String):Any=if(!rows.exists(_.contains(key)))null else rows.flatMap(_.get(key)).filterNot(missing).distinct.size
 def dcg(labels:Seq[Double],k:Int):Double=labels.take(k).zipWithIndex.map{case(x,i)=>(math.pow(2,math.max(0,x))-1)/(math.log(i+2)/math.log(2))}.sum
 def ndcg(labels:Seq[Double],k:Int):Option[Double]={val ideal=dcg(labels.sorted.reverse,k);if(ideal==0)None else Some(dcg(labels,k)/ideal)}
 // Whole-second timestamps use whole-second buckets to avoid count sawtooths.
 def buckets(stamps:Seq[Double]):Option[(Seq[Double],Double,Double=>Int)]={if(stamps.isEmpty||stamps.max<=stamps.min)None else {val start=stamps.min;val span=stamps.max-start;val whole=stamps.forall(x=>x%1==0);val width=if(whole)math.ceil((span+1)/24)else span/24;val count=if(whole)math.ceil((span+1)/width).toInt else 24;Some(((0 until count).map(i=>start+i*width),width,(x:Double)=>math.min(math.floor((x-start)/width).toInt,count-1)))}}
 def histogram(v:Seq[Double]):Seq[R]=if(v.isEmpty)Seq.empty else (0 until 10).map(i=>Map("bin_start"->i/10d,"count"->v.count(x=>math.min(rounded(x*10,9).toInt,9)==i)))
}
