package com.demo.report

import java.security.MessageDigest
import scala.collection.mutable
import ReportSupport._

/** Finite driver-side equivalents of the Python evaluation reports.
  * Point estimates preserve the Python formulas. Bootstrap uses java.util.Random,
  * so seeded draws differ from NumPy PCG64; intervals estimate the same distribution.
  */
object EvaluationReports {
  private val methods = Seq("bm25", "embedding", "hybrid")
  private def n(r: Record,k:String):Double=number(r.getOrElse(k,0))
  private def id(r:Record,k:String):String=text(r.getOrElse(k,""))
  private def tokens(s:String):Seq[String]="[a-z0-9]+".r.findAllIn(s.toLowerCase(java.util.Locale.ROOT)).toVector
  private def slice[A](s:Seq[A],k:Int):Seq[A]=s.take(if(k<0) math.max(0,s.size+k) else k)
  private def rank(scores:Iterable[(String,Double)],k:Int):Seq[String]=slice(scores.toSeq.sortBy(x=>(-x._2,x._1)).map(_._1),k)
  def cosine(a:Seq[Double],b:Seq[Double]):Double={val norm=math.sqrt(a.map(x=>x*x).sum*b.map(x=>x*x).sum); if(norm==0)0 else a.zip(b).map{case(x,y)=>x*y}.sum/norm}
  def recall(samples:Seq[Record],catalog:Seq[Record],itemVectors:Map[String,Seq[Double]],ks:Seq[Int]):Seq[Record]={
    require(ks.nonEmpty,"ks must not be empty")
    val corpus=catalog.map(r=>id(r,"item_id")->tokens(id(r,"title")+" "+seq(r.getOrElse("genres",Seq.empty)).map(text(_)).mkString(" "))).toMap
    val tf=corpus.map{case(k,v)=>k->v.groupBy(identity).map{case(t,vs)=>t->vs.size}}
    val df=mutable.Map.empty[String,Int].withDefaultValue(0); corpus.values.foreach(_.distinct.foreach(t=>df(t)+=1))
    val avg=if(corpus.isEmpty || corpus.values.map(_.size).sum==0)1.0 else corpus.values.map(_.size).sum.toDouble/corpus.size
    val clicks=samples.filter(r=>if(r.contains("clicked"))n(r,"clicked")==1 else n(r,"label")>0).groupBy(id(_,"user_id"))
    val sums=mutable.Map.empty[(String,Int),Double].withDefaultValue(0); val hits=mutable.Map.empty[(String,Int),Double].withDefaultValue(0)
    var users=0; var instances=0
    clicks.values.foreach { rows=>
      val clicked=rows.map(id(_,"item_id")).distinct.filter(corpus.contains)
      if(clicked.size>=2){users+=1;val counts=mutable.Map.empty[(String,Int),Int].withDefaultValue(0)
        clicked.foreach { held=>
          val others=clicked.filterNot(_==held);val candidates=corpus.keys.toSeq.filterNot(others.contains);val terms=others.flatMap(corpus).toSet
          val bm=rank(candidates.map { c=>c->terms.toSeq.map { t=>val f=tf(c).getOrElse(t,0); if(f==0)0.0 else math.log(1+(corpus.size-df(t)+0.5)/(df(t)+0.5))*f*2.5/(f+1.5*(0.25+0.75*corpus(c).size/avg)) }.sum },ks.max)
          val vectors=others.flatMap(itemVectors.get);val emb=if(vectors.isEmpty)Seq.empty else {val q=vectors.head.indices.map(i=>vectors.map(_(i)).sum/vectors.size); rank(candidates.filter(itemVectors.contains).map(c=>c->cosine(q,itemVectors(c))),ks.max)}
          val fusion=mutable.Map.empty[String,Double].withDefaultValue(0);Seq(bm,emb).foreach(_.zipWithIndex.foreach{case(c,i)=>fusion(c)+=1.0/(61+i)})
          val rankings=Map("bm25"->bm,"embedding"->emb,"hybrid"->rank(fusion,ks.max));instances+=1
          methods.foreach(m=>ks.foreach(k=>if(slice(rankings(m),k).contains(held))counts((m,k))+=1))
        }
        methods.foreach(m=>ks.foreach{k=>sums((m,k))+=counts((m,k)).toDouble/clicked.size; if(counts((m,k))>0)hits((m,k))+=1})
      }
    }
    methods.flatMap(m=>ks.map(k=>Map[String,Any]("method"->m,"k"->k,"recall_at_k"->(if(users==0)0.0 else round(sums((m,k))/users)),"hitrate_at_k"->(if(users==0)0.0 else round(hits((m,k))/users)),"users_evaluated"->users,"instances"->instances)))
  }
  def auc(scores:Seq[Double],labels:Seq[Int]):Any={
    val positive=labels.count(_==1);val negative=labels.size-positive
    if(positive==0||negative==0)null else {val order=scores.indices.sortBy(scores);var sum=0.0;var i=0
      while(i<order.size){var j=i;while(j+1<order.size && scores(order(j+1))==scores(order(i)))j+=1;sum+=((i+j)/2.0+1)*(i to j).count(t=>labels(order(t))==1);i=j+1}
      (sum-positive*(positive+1)/2.0)/(positive.toDouble*negative)
    }
  }
  def ranking(samples:Seq[Record],popularity:Map[String,Double],userVectors:Map[String,Seq[Double]],itemVectors:Map[String,Seq[Double]]):Seq[Record]={
    Seq("popularity","position","embedding").map { signal=>
      val pairs=samples.flatMap { r=>val label=if(r.contains("clicked"))n(r,"clicked").toInt else if(n(r,"label")>=1)1 else 0
        val score:Option[Double]=signal match{case "popularity"=>Some(popularity.getOrElse(id(r,"item_id"),0.0));case "position"=>Some(-n(r,"position"));case _=>for(u<-userVectors.get(id(r,"user_id")).filter(_.nonEmpty);v<-itemVectors.get(id(r,"item_id")).filter(_.nonEmpty))yield u.zip(v).map{case(a,b)=>a*b}.sum}
        score.map(_->label)
      };val scores=pairs.map(_._1);val labels=pairs.map(_._2);val mean=if(scores.isEmpty)0.0 else scores.sum/scores.size;val std=if(scores.isEmpty)0.0 else math.sqrt(scores.map(x=>math.pow(x-mean,2)).sum/scores.size)
      val loss=if(scores.isEmpty)null else round(scores.zip(labels).map{case(s,y)=>val p=math.max(1e-15,math.min(1-1e-15,if(std==0)0.5 else 1/(1+math.exp(-(s-mean)/std)))); -y*math.log(p)-(1-y)*math.log(1-p)}.sum/scores.size)
      Map[String,Any]("signal"->signal,"n"->pairs.size,"positives"->labels.sum,"coverage"->(if(samples.isEmpty)0.0 else round(pairs.size.toDouble/samples.size)),"auc"->auc(scores,labels),"logloss"->loss)
    }
  }
  private val excluded=Set("tabQ","fqiQ","dpoScore","grpoScore")
  private def candidates(e:Record):Seq[Record]=seq(e.getOrElse("actionSpace",Seq.empty)).map(obj)
  private def predictionKeys(events:Seq[Record]):Seq[String]=events.flatMap(e=>e+:candidates(e)).flatMap(r=>obj(r.getOrElse("modelPredictions",Map.empty)).collect{case(k,v:java.lang.Number)=>k;case(k,v:Boolean)=>k}).distinct.sorted
  def featureNames(events:Seq[Record]):Seq[String]=Seq("coldStart","impressions","clicks")++predictionKeys(events).filterNot(excluded)
  private def truth(v:Any):Boolean=v match{case null=>false;case b:Boolean=>b;case x:java.lang.Number=>x.doubleValue()!=0;case s:String=>s.nonEmpty;case _=>true}
  private def features(r:Record,names:Seq[String]):Seq[Double]=names.map {case "coldStart"=>if(truth(r.getOrElse("coldStart",false)))1.0 else 0.0;case k @ ("impressions"|"clicks")=>n(r,k);case k=>n(obj(r.getOrElse("modelPredictions",Map.empty)),k)}
  private def taken(e:Record):Record=candidates(e).find(_.get("item")==e.get("action")).getOrElse(e)
  private def hash(s:String):BigInt=BigInt(1,MessageDigest.getInstance("MD5").digest(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
  def isTest(request:String):Boolean=hash(request)%5==0
  def pick(policy:String,event:Record):Option[Record]={val cs=candidates(event);if(cs.isEmpty)None else Some(policy match{
    case "random"=>cs((hash(id(event,"requestId")+"random")%cs.size).toInt)
    case "popularity"=>cs.maxBy(n(_,"impressions"))
    case "ctr"=>cs.maxBy(c=>if(n(c,"impressions")>0)n(c,"clicks")/n(c,"impressions") else 0.0)
    case p if p.startsWith("model:")=>cs.maxBy(c=>n(obj(c.getOrElse("modelPredictions",Map.empty)),p.drop(6)))
    case _=>throw new IllegalArgumentException("unknown policy "+policy)
  })}
  private def sigmoid(z:Double):Double=1/(1+math.exp(-math.max(-30,math.min(30,z))))
  def ope(rawEvents:Seq[Record],bootstrapSamples:Int=1000,seed:Long=20260716L):Record={
    require(bootstrapSamples>=0,"bootstrap samples must be nonnegative")
    val events=rawEvents.filter(_.get("reward").exists(_!=null));if(events.isEmpty)return Map.empty
    val names=featureNames(events);val split=events.partition(e=> !isTest(id(e,"requestId")));val train=if(split._1.isEmpty)events else split._1;val test=if(split._1.isEmpty)events else split._2
    val x=train.map(e=>features(taken(e),names));val mean=names.indices.map(j=>x.map(_(j)).sum/x.size);val std=names.indices.map{j=>val s=math.sqrt(x.map(r=>math.pow(r(j)-mean(j),2)).sum/x.size);if(s==0)1.0 else s}
    def design(r:Record):Seq[Double]={val values=features(r,names);1.0+:values.indices.map(j=>(values(j)-mean(j))/std(j))}
    val d=train.map(e=>design(taken(e)));val w=Array.fill(names.size+1)(0.0)
    for(_<-0 until 500){val residual=d.zip(train).map{case(r,e)=>sigmoid(r.indices.map(j=>r(j)*w(j)).sum)-n(e,"reward")};val grad=w.indices.map(j=>d.indices.map(i=>d(i)(j)*residual(i)).sum/train.size+(if(j==0)0.0 else w(j)/train.size));w.indices.foreach(j=>w(j)-=0.5*grad(j))}
    def predict(r:Record):Double={val row=design(r);sigmoid(row.indices.map(j=>row(j)*w(j)).sum)}
    val pred=test.map(e=>predict(taken(e)));val calibration:Record=Map("n_test"->test.size,"mse"->(if(test.isEmpty)null else round(pred.zip(test).map{case(p,e)=>math.pow(p-n(e,"reward"),2)}.sum/test.size)),"auc"->auc(pred,test.map(e=>if(e.contains("clicked"))n(e,"clicked").toInt else if(n(e,"reward")>0)1 else 0)))
    val policies=Seq("logging","popularity","ctr","random")++predictionKeys(events).map("model:"+_)
    val scores=policies.map(p=>events.map(e=>if(p=="logging")Some(n(e,"reward"))else pick(p,e).map(predict)))
    def stats(indexes:Seq[Int]):Seq[(Double,Option[Double],Int)]={val logging=indexes.map(i=>n(events(i),"reward")).sum/indexes.size; scores.zipWithIndex.map{case(col,j)=>val values=indexes.flatMap(i=>col(i));val value=if(j==0)logging else if(values.isEmpty)0.0 else values.sum/values.size;(value,if(logging>0)Some(value/logging-1)else None,values.size)}}
    val point=stats(events.indices);val valueDraws=Array.fill(policies.size)(mutable.ArrayBuffer.empty[Double]);val liftDraws=Array.fill(policies.size)(mutable.ArrayBuffer.empty[Double]);val rng=new java.util.Random(seed)
    for(_<-0 until bootstrapSamples){val draw=stats(events.indices.map(_=>rng.nextInt(events.size)));draw.indices.foreach{j=>valueDraws(j)+=draw(j)._1;draw(j)._2.foreach(liftDraws(j)+=_)}}
    def quantile(values:Seq[Double],p:Double):Any=if(values.isEmpty)null else {val sorted=values.sorted;val index=(sorted.size-1)*p;val lo=index.toInt;round(sorted(lo)+(sorted(math.min(lo+1,sorted.size-1))-sorted(lo))*(index-lo))}
    val rows=policies.indices.sortBy(j=>(-point(j)._1,j)).map{j=>Map[String,Any]("policy"->policies(j),"value"->round(point(j)._1),"lift_vs_logging"->point(j)._2.map(round(_)).getOrElse(null),"n_events"->point(j)._3,"estimator_auc"->calibration("auc"),"estimator_mse"->calibration("mse"),"value_ci_low"->quantile(valueDraws(j),0.025),"value_ci_high"->quantile(valueDraws(j),0.975),"lift_ci_low"->quantile(liftDraws(j),0.025),"lift_ci_high"->quantile(liftDraws(j),0.975))}
    val best=rows.head;val logging=rows.find(_("policy")=="logging").get;val aucText=if(calibration("auc")==null)"N/A"else f"${number(calibration("auc"))}%.3f"
    Map("rows"->rows,"calibration"->calibration,"headline"->(s"best '${best("policy")}' value "+f"${number(best("value"))}%.3f vs logging ${number(logging("value"))}%.3f"+s" · est AUC $aucText"))
  }
}
