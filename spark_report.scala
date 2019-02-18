import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object Chap2 extends App {

  val ages = Array(20, 50, 35, 41)
  ages.foreach(println)
  println("\n")


  def isOddAge(age:Int) : Boolean = {
    (age % 2) == 1
  }
  ages.filter(age => isOddAge(age)).foreach(println)
  println("\n")

}

object Chap4 extends App{

  val conf = new SparkConf().setAppName("For Chapter 4").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val rdd = sc.parallelize(1 to 10).map(x => (x, Random.nextInt(100)* x))
  rdd.foreach(println)

  val peopleRDD = sc.parallelize(Array(
    Row(1L, "John Doe",  30L),
    Row(2L, "Mary Jane", 25L)))

  val schema = StructType(Array(
    StructField("id", LongType, true),
    StructField("name", StringType, true),
    StructField("age", LongType, true)
  ))

  val session = SparkSession.builder().appName("SparkSQL").master("local[*]").getOrCreate()
  val data = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("results.csv")

  data.printSchema()

  // Working with Columns
  data.select("userid", "source", "y_hat", "y_prob").show(5)
  data.selectExpr("y_prob", "y_prob>0.5 as ind").show(5)

  data.filter(data.col("y_prob")>=0.9).show(5)
  data.where(data.col("y_prob")>=0.9).show(5)
  data.select("userid", "y_prob").sort(data.col("y_prob")).show(5)
  data.select("userid", "y_prob").orderBy(data.col("y_prob").desc).show(5)

  //Working SQL
  val infoDF = session.sql("select current_date() as today , 1 + 100 as value")
  infoDF.show

  data.createOrReplaceTempView("data")
  session.catalog.listTables.show
  session.sql("select * from data where source='web' ").show(5)

  val tmp = session.sql("select email_domain, count(*) as count from data group by email_domain")
    tmp.where(tmp.col("count") > 1000)
    .orderBy(tmp.col("count").desc)
    .show(5)

}

object Chap5 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("For Chapter5").master("local[*]").getOrCreate()

  val df = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("results.csv")

  println("number of users:" + "\n")
  df.count()

  println("data schema:" + "\n")
  df.printSchema()

  println("count by suspended" + "\n")
  val results1 = df.groupBy("suspended").count().as("count")
  results1
    .orderBy(results1.col("count").desc)
    .coalesce(1)
    .write
    .format("csv")
    .mode("append")
    .save("result")

  println("count by suspended and geo" + "\n")
  val results2 = df.groupBy("suspended", "geo").count().as("count")
  val results3 = results2.groupBy("geo").pivot("suspended").agg(
    "count" -> "sum"
  )
  results3.orderBy(results3.col("true").desc).coalesce(1).write.format("csv").mode("append").save("result")

  println("distribution by email domain" + "\n")
  val results4 = df.groupBy("suspended", "email_domain").count().as("count")
  val results5 = results4.groupBy("email_domain").pivot("suspended").agg(
    "count" -> "sum"
  )
  results5.coalesce(1).write.format("csv").mode("append").save("result")

   println("number of users:")
   data.groupBy("source")
     .agg(
       "userid" -> "count"
     )
     .show()
  
   println("top email domain:")
   val result = data.groupBy("email_domain").count().as("count")
   result.orderBy(result.col("count").desc).show(10)
  
   println("top email domain by source:")
   val result2 = data.groupBy("source", "email_domain").count().as("count")
   result2.where(result2.col("source") === "android")
     .orderBy(result2.col("count").desc)
     .show(10)
   result2.where(result2.col("source") === "web")
     .orderBy(result2.col("count").desc)
     .show(10)
  
   println("score distribution:")
   val result3 = data.groupBy("email_domain")
     .agg(
       "userid" -> "count",
       "y_prob" -> "min",
       "y_prob" -> "max",
       "y_prob" -> "avg"
     )
   result3.orderBy(result3.col("count(userid)").desc).show(10)
   result3.orderBy(result3.col("avg(y_prob)").desc).show(10)
  
   println("top email domain distribution:")
   val result4 = data.groupBy("email_domain").pivot("suspended").count()
     result4.orderBy(result4.col("false").desc).show(10)
     result4.orderBy(result4.col("true").desc).show(10)
  
   data.groupBy("email_domain").pivot("suspended")
     .agg(
       "userid" -> "count",
       "y_prob" -> "min",
       "y_prob" -> "max",
       "y_prob" -> "avg"
       )
     .show()
  
   data.rollup("source", "email_domain")
     .agg("userid" -> "count").show()
  
   data.cube("source", "email_domain")
     .agg("userid" -> "count").show()


 val forRankingWindow = Window.partitionBy("y_prob").orderBy("y_prob")
 //  val txDataWithRankDF = data.withColumn("y_prob", rank().over(forRankingWindow))

 df.createOrReplaceTempView("data")
 //  session.sql("select source, email_domain, y_prob, RANK() OVER (PARTITION BY source, email_domain ORDER BY y_prob DESC) as rank from data")
 //    .show()
//  val results = session.sql("select source, email_domain, y_prob, " +
//    "RANK() OVER (PARTITION BY source, email_domain ORDER BY y_prob DESC) as rank from data")

}

object Chap6 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession
    .builder()
    .appName("For Chapter6")
    .master("local[*]")
    .getOrCreate()

  val rawData = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("active.csv")
  rawData.createOrReplaceTempView("data")

  // Fetch data with right schema
  val df = session.sql("select * from data")
  val df2 = df.select(
    df("userid").cast(IntegerType).as("userid"),
    df("issuspended").cast(StringType).as("issuspended"),
    df("devicetype").cast(StringType).as("devicetype"),
    df("geo").cast(StringType).as("geo"),
    df("account_age").cast(IntegerType).as("account_age"),
    df("email_domain").cast(StringType).as("email_domain"),
    df("l7").cast(IntegerType).as("l7"),
    df("tot_follow").cast(IntegerType).as("tot_follow"),
    df("tot_unfollow").cast(IntegerType).as("tot_unfollow"),
    df("tot_received_follow").cast(IntegerType).as("tot_received_follow"),
    df("tot_received_unfollow").cast(IntegerType).as("tot_received_unfollow"),
    df("tot_blog_subscription").cast(IntegerType).as("tot_blog_subscription"),
    df("tot_received_blog_subscription").cast(IntegerType).as("tot_received_blog_subscription"),
    df("followers").cast(IntegerType).as("followers"),
    df("received_followers").cast(IntegerType).as("received_followers"),
    df("days_follow").cast(IntegerType).as("days_follow"),
    df("days_unfollow").cast(IntegerType).as("days_unfollow"),
    df("days_received_follow").cast(IntegerType).as("days_received_follow"),
    df("days_received_unfollow").cast(IntegerType).as("days_received_unfollow"),
    df("days_blog_subscription").cast(IntegerType).as("days_blog_subscription"),
    df("days_received_blog_subscription").cast(IntegerType).as("days_received_blog_subscription"),
    df("tot_new_message").cast(IntegerType).as("tot_new_message"),
    df("tot_message").cast(IntegerType).as("tot_message"),
    df("tot_replies").cast(IntegerType).as("tot_replies"),
    df("num_max_daily_new_message").cast(IntegerType).as("num_max_daily_new_message"),
    df("mean").cast(LongType).as("mean"),
    df("std").cast(LongType).as("std"),
    df("days_new_message").cast(IntegerType).as("days_new_message"),
    df("days_message").cast(IntegerType).as("days_message"),
    df("days_replies").cast(IntegerType).as("days_replies"),
    df("tot_original_post").cast(IntegerType).as("tot_original_post"),
    df("tot_reblog_post").cast(IntegerType).as("tot_reblog_post"),
    df("tot_like_post").cast(IntegerType).as("tot_like_post"),
    df("days_original_post").cast(IntegerType).as("days_original_post"),
    df("days_reblog_post").cast(IntegerType).as("days_reblog_post"),
    df("days_reply_post").cast(IntegerType).as("days_reply_post"),
    df("tot_search").cast(IntegerType).as("tot_search"),
    df("tot_blacklistterm").cast(IntegerType).as("tot_blacklistterm"),
    df("querylist").cast(StringType).as("querylist"),
    df("days_search").cast(IntegerType).as("days_search")
  )
  df2.show(10)
 //  df2.columns.filterNot(_.contains("userid"))


  // Device reports
  def deviceReport(results: Dataset[_], deviceType: String) = {

    val df = results.where(results.col("devicetype") === deviceType)

    println("Number of users:" + "\n")
    println(df.count().toString)

    println("Count by suspended" + "\n")
    val results1 = df.groupBy("issuspended").count().as("count")
    results1.orderBy(results1.col("count").desc).show()

    println("Count by suspended and geo" + "\n")
    val results2 = df.groupBy("issuspended", "geo").count().as("count")
    val results3 = results2.groupBy("geo").pivot("issuspended").agg(
      "count" -> "sum"
    )
    results3.orderBy(results3.col("true").desc).show()

    println("Distribution by email domain" + "\n")
    val results4 = df.groupBy("issuspended", "email_domain").count().as("count")
    val results5 = results4.groupBy("email_domain").pivot("issuspended").agg(
      "count" -> "sum"
    )
    results5.orderBy(results5.col("email_domain").desc).show()
  }

  val devices = Set("android", "ios", "web", "other")
//  val devices = Set("android")
//  for (device <- devices) {
//    deviceReport(df2, "android")
//  }

  // Geo Transformer
  // estimator:  to convert categorical strings to numbers
  val geos = Set("US", "GB", "DE", "RU", "MX", "AU", "CA", "XO", "KR")
  val geo_filter = udf((geo: String) => {
        if (geos.contains(geo)) geo
        else "other"
  })
  val df3 = df2.withColumn("geo", geo_filter(df2.col("geo")))
  //  df3.groupBy(df3.col("geo")).count().show()

  // Email Domain Transformer
  val email_domains = Set("gmail.com", "yahoo.com", "hotmail.com", "mail.ru", "outlook.com", "icloud.com")
  val email_domains_filter = udf((email_domain: String) => {
    if (email_domains.contains(email_domain)) email_domain
    else "other"
  })
  val df4 = df3.withColumn("email_domain", email_domains_filter(df3.col("email_domain")))
  //  df4.groupBy(df4.col("email_domain")).count().show()

  val geoIndexer = new StringIndexer()
    .setInputCol("geo")
    .setOutputCol("geoIdx")
    .setHandleInvalid("skip")
  val df5 = geoIndexer
    .fit(df4)
    .transform(df4)
  //  df5.groupBy("geoIdx", "geo").count().show

  val geoEncoder = new OneHotEncoder()
    .setInputCol("geoIdx")
    .setOutputCol("geoEncoder")
  val df6 = geoEncoder
    .transform(df5)
//  df6.groupBy("geoEncoder", "geo").count().show

  val email_domainsIndexer = new StringIndexer()
    .setInputCol("email_domain")
    .setOutputCol("email_domainIdx")
    .setHandleInvalid("skip")
  val df7 = email_domainsIndexer
    .fit(df6)
    .transform(df6)

  val email_domainsEncoder = new OneHotEncoder()
    .setInputCol("email_domainIdx")
    .setOutputCol("email_domainEncoder")
  val df8 = email_domainsEncoder
    .transform(df7)

  // Label generation
  val labelIndexer = new StringIndexer()
    .setInputCol("issuspended")
    .setOutputCol("label")

  val df9 = labelIndexer
    .fit(df8)
    .transform(df8)

//  val cols = df9.columns.diff(Array("label","userid", "issuspended", "devicetype", "geo", "email_domain", "geoIdx", "email_domainIdx", " querylist", "tot_blacklistterm"))
  val cols = Array(
  "label",
  "geoEncoder",
  "email_domainEncoder",
  "account_age",
  "l7",
  "tot_follow",
  "tot_unfollow",
  "tot_received_follow",
  "tot_received_unfollow",
  "tot_blog_subscription",
  "tot_received_blog_subscription",
  "followers",
  "received_followers",
  "days_follow",
  "days_unfollow",
  "days_received_follow",
  "days_received_unfollow",
  "days_blog_subscription",
  "days_received_blog_subscription",
  "tot_new_message",
  "tot_message",
  "tot_replies",
  "num_max_daily_new_message",
  "mean",
  "std",
  "days_new_message",
  "days_message",
  "days_replies",
  "tot_original_post",
  "tot_reblog_post",
  "tot_like_post",
  "days_original_post",
  "days_reblog_post",
  "days_reply_post",
  "tot_search"
//  "tot_blacklistterm",
//  "days_search"
)
  val assembler = new VectorAssembler()
    .setInputCols(cols)
    .setOutputCol("features")

  val newdf = assembler
    .transform(df9)
    .select("label", "features")

  newdf.show()

  // Split the dataset
//  val Splits = newdf.randomSplit(Array(0.8, 0.2))
  val Splits = df4.randomSplit(Array(0.7, 0.3))
  val (train_set, test_set) = (Splits(0), Splits(1))

  val logisticRegression = new LogisticRegression().setFamily("binomial")
  val pipeline = new Pipeline().setStages(Array(geoIndexer, geoEncoder, email_domainsIndexer, email_domainsEncoder, labelIndexer, assembler, logisticRegression))

//
//  val lrmodel = logisticRegression.fit(train_set)

//  // train the algorithm with the training data
  val model = pipeline.fit(train_set)


  //  // perform the predictions
    val predictions = model.transform(test_set)
  //  predictions
  //    .select("features", "label", "prediction")
  //    .show()

    // 0.736349476069126
    val evaluator = new BinaryClassificationEvaluator()
    println(evaluator.evaluate(predictions))



////
////  model.write.overwrite().save("/tmp/model/")
////  println("Pipeline saved")
//////  model.load()
//////  cvModel = crossval.fit(trainingData)
//////  myBestModel = cvModel.bestModel
////  val model2 = model.load("/tmp/model/")
////  println("Pipeline loaded")
//
//

//
//  val predictionAndLabels = predictions
//    .select("label", "prediction")
//    .rdd
//    .map(row => (row.getDouble(0), row.getDouble(1)))
//
//  // Instantiate metrics object
//  val metrics = new BinaryClassificationMetrics(predictionAndLabels)
//
//  // Precision by threshold
//  val precision = metrics.precisionByThreshold
//  precision.foreach { case (t, p) =>
//    println(s"Threshold: $t, Precision: $p")
//  }
//
//  // Recall by threshold
//  val recall = metrics.recallByThreshold
//  recall.foreach { case (t, r) =>
//    println(s"Threshold: $t, Recall: $r")
//  }
//
//  // F-measure
//  val f1Score = metrics.fMeasureByThreshold
//  f1Score.foreach { case (t, f) =>
//    println(s"Threshold: $t, F-score: $f, Beta = 1")
//  }
//
//  val beta = 0.5
//  val fScore = metrics.fMeasureByThreshold(beta)
//  f1Score.foreach { case (t, f) =>
//    println(s"Threshold: $t, F-score: $f, Beta = 0.5")
//  }
//
//  // ROC Curve
//  val roc = metrics.roc
//
//  // AUROC
//  val auROC = metrics.areaUnderROC
//  println(s"Area under ROC = $auROC")
//
//

}


val session = SparkSession
  .builder()
  .appName("Active User Analysis")
  .config("hive.metastore.uris", "thrift://hive-server.bf2.tumblr.net:9083")
  .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
  .enableHiveSupport()
  .getOrCreate()

val own_post_appeal = session.sql("select dt, count(distinct post_id) from own_post_appeal where dt between '2019-01-15' and '2019-01-22' and action = 'request_review' group by dt")
own_post_appeal.show()


val new_smp_post_classifications = session.sql("select dt, count(distinct post_id) from  new_smp_post_classifications where dt between '2019-01-15' and '2019-01-22' and classification != 'NONE' group by dt")
new_smp_post_classifications.show()

val own_post_appeal_new_smp_post_classification = session.sql("select dt, count(distinct post_id) from  new_smp_post_classifications where dt between '2019-01-15' and '2019-01-22' and classification != 'NONE' group by dt")
own_post_appeal_new_smp_post_classification.show()

println("\n" + "Tensorflow requests in a week" + "\n")
val tensorflow_callback = session.sql("select dt, count(distinct post_id) from smp_classification_result " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and (msg not like '%deepnet%' and (msg like '%tumblr-en-US% ' or msg like '%tumblr-video-en-US% ' )) group by dt order by dt desc ")
tensorflow_callback.show()

println("\n" + "Deepnet requests in a week" + "\n")
val deepnet_callback = session.sql("select dt, count(distinct post_id) from smp_classification_result " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and (msg like '%deepnet%' and (msg like '%tumblr-en-US% ' or msg like '%tumblr-video-en-US% ' )) group by dt order by dt desc ")
deepnet_callback.show()



// println("\n" + "Check confidence breakdown" + "\n")
// val new_smp_post_classifications_even_breakdown = session.sql("select post_id, blog_id, classification, confidence from new_smp_post_classifications where dt between '2019-01-15' and '2019-01-21' and classification != 'NONE' group by  post_id, blog_id, classification, confidence")
// new_smp_post_classifications_even_breakdown .show()
// val confidenceBucket = new_smp_post_classifications_even_breakdown.withColumn("confidencebins", new_smp_post_classifications_even_breakdown.col("confidence").divide(0.1).cast("integer"))

// val new_smp_post_classifications_breakdown = session.sql(
//     "select post_id, blog_id, classification, confidence " +
//     "from new_smp_post_classifications " +
//     "where dt between '2019-01-15' and '2019-01-21' " +
//     "and classification != 'NONE' " +
//     "group by  post_id, blog_id, classification, confidence")
// new_smp_post_classifications_breakdown.show()



println("\n" + "Check confidence breakdown" + "\n")
val new_smp_post_classifications_breakdown = session.sql("select classification," +
  "if(confidence > 0.9, 0.9, " +
  "if(confidence > 0.5, 0.5, " +
  "if (confidence > 0.25, 0.25, " +
  "if(confidence > 0.1, 0.1, 0)))) conf_bins, " +
  "count(distinct post_id) " +
  "from new_smp_post_classifications " +
  "where classification <> 'NONE' " +
  "and dt between '2019-01-15' and '2019-01-21' " +
  "group by classification, " +
  "if(confidence > 0.9, 0.9, " +
  "if(confidence > 0.5, 0.5, " +
  "if (confidence > 0.25, 0.25, " +
  "if(confidence > 0.1, 0.1, 0))))")
new_smp_post_classifications_breakdown.show()

println("\n" + "Check confidence breakdown for tensorflow" + "\n")
val new_smp_post_tensorflow = session.sql("select a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if(a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) conf_bins, count(distinct a.post_id) " +
  "from " +
  "(select classification, post_id, confidence " +
  "from new_smp_post_classifications " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and classification <> 'NONE') a " +
  "inner join" +
  "(select post_id " +
  "from smp_classification_result " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and (msg not like '%deepnet%' and msg not like '%agentid%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
  "group by post_id) b " +
  "on a.post_id = b.post_id " +
  "group by a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if (a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) "
)
new_smp_post_tensorflow.show()

println("\n" + "Check confidence breakdown for deepnet" + "\n")
val new_smp_post_deepnet = session.sql("select a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if(a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) conf_bins, count(distinct a.post_id) " +
  "from " +
  "(select classification, post_id, confidence " +
  "from new_smp_post_classifications " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and classification <> 'NONE') a " +
  "inner join" +
  "(select post_id " +
  "from smp_classification_result " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and (msg like '%deepnet%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
  "group by post_id) b " +
  "on a.post_id = b.post_id " +
  "group by a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if (a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) "
)
new_smp_post_deepnet.show()


println("\n" + "Check confidence breakdown for tensorflow" + "\n")
val new_smp_post_appeal = session.sql("select a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if(a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) conf_bins, count(distinct a.post_id) " +
  "from " +
  "(select classification, post_id, confidence " +
  "from new_smp_post_classifications " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and classification <> 'NONE') a " +
  "inner join (select post_id from own_post_appeal " +
  "where dt between '2019-01-15' and '2019-01-21' and action = 'request_review' group by post_id) b on a.post_id = b.post_id  " +
  "group by a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if (a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) "
)
new_smp_post_appeal.show()


println("\n" + "Check confidence breakdown for tensorflow" + "\n")
val new_smp_post_tensorflow_appeal = session.sql("select a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if(a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) conf_bins, count(distinct a.post_id) " +
  "from " +
  "(select classification, post_id, confidence " +
  "from new_smp_post_classifications " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and classification <> 'NONE') a " +
  "inner join (select post_id from own_post_appeal " +
  "where dt between '2019-01-15' and '2019-01-21' and action = 'request_review' group by post_id) b on a.post_id = b.post_id  " +
  "inner join" +
  "(select post_id " +
  "from smp_classification_result " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and (msg not like '%deepnet%' and msg not like '%agentid%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
  "group by post_id) c " +
  "on a.post_id = c.post_id " +
  "group by a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if (a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) "
)
new_smp_post_tensorflow_appeal.show()

println("\n" + "Check confidence breakdown for deepnet" + "\n")
val new_smp_post_deepnet_appeal = session.sql("select a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if(a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) conf_bins, count(distinct a.post_id) " +
  "from " +
  "(select classification, post_id, confidence " +
  "from new_smp_post_classifications " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and classification <> 'NONE') a " +
  "inner join (select post_id from own_post_appeal " +
  "where dt between '2019-01-15' and '2019-01-21' and action = 'request_review' group by post_id) b on a.post_id = b.post_id  " +
  "inner join" +
  "(select post_id " +
  "from smp_classification_result " +
  "where dt between '2019-01-15' and '2019-01-21' " +
  "and (msg like '%deepnet%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
  "group by post_id) c " +
  "on a.post_id = c.post_id " +
  "group by a.classification, " +
  "if(a.confidence > 0.9, 0.9, " +
  "if(a.confidence > 0.5, 0.5, " +
  "if (a.confidence > 0.25, 0.25, " +
  "if(a.confidence > 0.1, 0.1, 0)))) "
)
new_smp_post_deepnet_appeal.show()


//  new BinaryClassificationEvaluator().
//    evaluate(cv.bestModel.transform(adultvalid))

//  val lr = new LogisticRegression
//  lr.setRegParam(0.01).setMaxIter(500).setFitIntercept(true)
//  val lrmodel = lr.fit(adulttrain)

//  val encodedFeatures = df2.flatMap{
//    name =>
//      val indexer = new StringIndexer()
//        .setInputCol(name)
//        .setOutputCol(name + "_Index")
//      Array(indexer)
//  }.toArray


//  df2.map(line => line.map(point => Math.round(point / 20000) * 20000).orElse(None))

//  val responseWithSalaryBucket = responses.withColumn(SALARY_MIDPOINT_BUCKET,
//    responses.col(SALARY_MIDPOINT).divide(20000).cast("integer").multiply(20000))

//  def emailLookup(email_domain: String): String = {
//    if (email_domains.contains(email_domain)) email_domain else "other"
//  }

//def geoLookup(geo: String): String = {
//  if (geos.contains(geo)) geo else "other"
//}

    //  val df3 = df2.select(df("geo"))
    //    .transform(geoLookup)
    //    .as("geo")


    //  raw_df.map{
    //
    //  }
    //
    //  val tumblrlogs: TypedPipe[(Long, Boolean, Boolean, Boolean, Boolean, Boolean)] =
    //    TumblelogsSource()
    //      .all
    //      .map { case r =>
    //        (r.userId, isUntitled(r.title), isNoAvatar(r.avatarId), isDefaultTheme(r.themeId), isNodescription(r.description), isNoCustomCss(r.customCss))
    //      }


    //  val df2 = df.map{ case (issuspended, feature) => LabeledPoint(issuspended, feature)}
    //  val df = rawdata.select(
    //    rawdata.col("issuspended").cast("String").alias("suspended")
    ////


    //  def prepareData(deviceType: String) = {
    //
    //    val df = results.where(results.col("devicetype") === deviceType)
    //

    //
    //    import org.apache.spark.ml.feature.StringIndexer

//
//    Statistics.colStats()

//    import org.apache.spark.ml.feature.StringIndexer
//    val indexer = new StringIndexer()
//      .setInputCol("issuspended")
//      .setOutputCol("label")

//    val indexed = indexer.fit(df).transform(df)
//    indexed.show()

//  results.rdd.saveAsTextFile("output.csv")
//  results.write.option("delimiter", "\t").csv("output.tsv")

//  val export = results.printSchema



//  val rdd = results.foreach(println)

//  for (rdd <- results.rdd.collect()) println(rdd)

//  results.toJavaRDD.rdd.saveAsTextFile("output.csv")

//  results.toJavaRDD.rdd.write.option("delimiter", "\t").csv("output.tsv")


//  val results = session.sql("select * from active_user_social_message_post_search limit 10")

//  val response = results.rdd.collect()
//  println(">>> response converted to rdd successfully!....")
//  val rdd = response.foreach(println)

//  val resultsRdd = results.rdd.collect()

//  for (result <- results) println()

//  for (response <- results.rdd.collect()) println(response)

//  val resultsRdd = for (line <- results.rdd.collect()) println(line)

//  for (resultsRdd <- results.rdd.collect() println(results))


//
//  val rdd = results.collect.foreach(println)

//  rdd.saveAsTsv(FsPath(output))

//  results.write.option("delimiter", "\t").csv("output.tsv")


////  results.rdd.saveAsTextFile("output.csv")
//    val result2 = results.map(x => x.mkString("|"))
//
//
////  results.rdd.saveAsTextFile("output.csv")