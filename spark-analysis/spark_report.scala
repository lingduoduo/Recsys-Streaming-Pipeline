import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Random

object ScalaBasics extends App {

  val ages = Array(20, 50, 35, 41)
  ages.foreach(println)

  def isOddAge(age: Int): Boolean = (age % 2) == 1
  ages.filter(isOddAge).foreach(println)
}

object SparkDataFrameBasics extends App {

  // Derive SparkContext from the session to avoid creating two SparkContexts in one JVM
  val session = SparkSession.builder().appName("SparkDataFrameBasics").master("local[*]").getOrCreate()
  val sc = session.sparkContext

  val rdd = sc.parallelize(1 to 10).map(x => (x, Random.nextInt(100) * x))
  rdd.foreach(println)

  val data = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("results.csv")

  data.printSchema()
  data.select("userid", "source", "y_hat", "y_prob").show(5)
  data.selectExpr("y_prob", "y_prob>0.5 as ind").show(5)
  data.filter(data.col("y_prob") >= 0.9).show(5)
  data.select("userid", "y_prob").orderBy(data.col("y_prob").desc).show(5)

  session.sql("select current_date() as today, 1 + 100 as value").show()

  data.createOrReplaceTempView("data")
  session.catalog.listTables().show()
  session.sql("select * from data where source='web'").show(5)

  val tmp = session.sql("select email_domain, count(*) as count from data group by email_domain")
  tmp.where(tmp.col("count") > 1000).orderBy(tmp.col("count").desc).show(5)
}

object UserSuspensionReport extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("UserSuspensionReport").master("local[*]").getOrCreate()

  val df = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("results.csv")
  df.cache()

  println("number of users: " + df.count())
  df.printSchema()

  println("count by suspended")
  val bySuspended = df.groupBy("suspended").count().as("count")
  bySuspended.orderBy(bySuspended.col("count").desc)
    .coalesce(1).write.format("csv").mode("append").save("result")

  println("count by suspended and geo")
  val bySuspendedGeo = df.groupBy("suspended", "geo").count().as("count")
  bySuspendedGeo.groupBy("geo").pivot("suspended").agg("count" -> "sum")
    .orderBy(col("true").desc)
    .coalesce(1).write.format("csv").mode("append").save("result")

  println("distribution by email domain")
  df.groupBy("suspended", "email_domain").count().as("count")
    .groupBy("email_domain").pivot("suspended").agg("count" -> "sum")
    .coalesce(1).write.format("csv").mode("append").save("result")

  println("number of users by source:")
  df.groupBy("source").agg("userid" -> "count").show()

  println("top email domains:")
  val byDomain = df.groupBy("email_domain").count().as("count")
  byDomain.orderBy(byDomain.col("count").desc).show(10)

  println("top email domains by source:")
  val bySourceDomain = df.groupBy("source", "email_domain").count().as("count")
  bySourceDomain.where(bySourceDomain.col("source") === "android").orderBy(bySourceDomain.col("count").desc).show(10)
  bySourceDomain.where(bySourceDomain.col("source") === "web").orderBy(bySourceDomain.col("count").desc).show(10)

  println("score distribution by email domain:")
  val scoreByDomain = df.groupBy("email_domain").agg(
    "userid" -> "count",
    "y_prob" -> "min",
    "y_prob" -> "max",
    "y_prob" -> "avg"
  )
  scoreByDomain.orderBy(scoreByDomain.col("count(userid)").desc).show(10)
  scoreByDomain.orderBy(scoreByDomain.col("avg(y_prob)").desc).show(10)

  println("suspension distribution by email domain:")
  val suspensionByDomain = df.groupBy("email_domain").pivot("suspended").count()
  suspensionByDomain.orderBy(suspensionByDomain.col("false").desc).show(10)
  suspensionByDomain.orderBy(suspensionByDomain.col("true").desc).show(10)

  df.groupBy("email_domain").pivot("suspended")
    .agg("userid" -> "count", "y_prob" -> "min", "y_prob" -> "max", "y_prob" -> "avg")
    .show()

  df.rollup("source", "email_domain").agg("userid" -> "count").show()
  df.cube("source", "email_domain").agg("userid" -> "count").show()

  df.unpersist()
}

object ActiveUserSuspensionModel extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val session = SparkSession.builder().appName("ActiveUserSuspensionModel").master("local[*]").getOrCreate()

  val df = session.read
    .option("header", "true")
    .option("inferSchema", value = true)
    .csv("active.csv")

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

  def deviceReport(results: Dataset[_], deviceType: String): Unit = {
    val df = results.where(results.col("devicetype") === deviceType)
    println(s"Device: $deviceType — users: ${df.count()}")

    df.groupBy("issuspended").count().as("count")
      .orderBy(col("count").desc).show()

    df.groupBy("issuspended", "geo").count().as("count")
      .groupBy("geo").pivot("issuspended").agg("count" -> "sum")
      .orderBy(col("true").desc).show()

    df.groupBy("issuspended", "email_domain").count().as("count")
      .groupBy("email_domain").pivot("issuspended").agg("count" -> "sum")
      .orderBy(col("email_domain").desc).show()
  }

  val devices = Set("android", "ios", "web", "other")
  for (device <- devices) deviceReport(df2, device)

  val df3 = df2.withColumn("geo",
    when(col("geo").isin("US", "GB", "DE", "RU", "MX", "AU", "CA", "XO", "KR"), col("geo"))
      .otherwise(lit("other"))
  )
  val df4 = df3.withColumn("email_domain",
    when(col("email_domain").isin("gmail.com", "yahoo.com", "hotmail.com", "mail.ru", "outlook.com", "icloud.com"), col("email_domain"))
      .otherwise(lit("other"))
  )

  val geoIndexer           = new StringIndexer().setInputCol("geo").setOutputCol("geoIdx").setHandleInvalid("skip")
  val geoEncoder           = new OneHotEncoder().setInputCol("geoIdx").setOutputCol("geoEncoder")
  val emailDomainIndexer   = new StringIndexer().setInputCol("email_domain").setOutputCol("email_domainIdx").setHandleInvalid("skip")
  val emailDomainEncoder   = new OneHotEncoder().setInputCol("email_domainIdx").setOutputCol("email_domainEncoder")
  val labelIndexer         = new StringIndexer().setInputCol("issuspended").setOutputCol("label")

  val cols = Array(
    "label", "geoEncoder", "email_domainEncoder",
    "account_age", "l7",
    "tot_follow", "tot_unfollow", "tot_received_follow", "tot_received_unfollow",
    "tot_blog_subscription", "tot_received_blog_subscription",
    "followers", "received_followers",
    "days_follow", "days_unfollow", "days_received_follow", "days_received_unfollow",
    "days_blog_subscription", "days_received_blog_subscription",
    "tot_new_message", "tot_message", "tot_replies", "num_max_daily_new_message",
    "mean", "std",
    "days_new_message", "days_message", "days_replies",
    "tot_original_post", "tot_reblog_post", "tot_like_post",
    "days_original_post", "days_reblog_post", "days_reply_post",
    "tot_search"
  )
  val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

  // Step-by-step transforms for inspection
  val df5  = geoIndexer.fit(df4).transform(df4)
  val df6  = geoEncoder.transform(df5)
  val df7  = emailDomainIndexer.fit(df6).transform(df6)
  val df8  = emailDomainEncoder.transform(df7)
  val df9  = labelIndexer.fit(df8).transform(df8)
  assembler.transform(df9).select("label", "features").show()

  val Array(trainSet, testSet) = df4.randomSplit(Array(0.7, 0.3))

  val pipeline = new Pipeline().setStages(
    Array(geoIndexer, geoEncoder, emailDomainIndexer, emailDomainEncoder, labelIndexer, assembler,
      new LogisticRegression().setFamily("binomial"))
  )
  println("Accuracy: " + new BinaryClassificationEvaluator().evaluate(pipeline.fit(trainSet).transform(testSet)))
}


object ContentClassificationReport extends App {

  val session = SparkSession
    .builder()
    .appName("ContentClassificationReport")
    .config("hive.metastore.uris", sys.env.getOrElse("HIVE_METASTORE_URI", "thrift://localhost:9083"))
    .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()

  val own_post_appeal = session.sql(
    "select dt, count(distinct post_id) from own_post_appeal " +
    "where dt between '2019-01-15' and '2019-01-22' and action = 'request_review' group by dt")
  own_post_appeal.show()

  val new_smp_post_classifications = session.sql(
    "select dt, count(distinct post_id) from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-22' and classification != 'NONE' group by dt")
  new_smp_post_classifications.show()

  println("\nTensorflow requests in a week\n")
  session.sql(
    "select dt, count(distinct post_id) from smp_classification_result " +
    "where dt between '2019-01-15' and '2019-01-21' " +
    "and (msg not like '%deepnet%' and (msg like '%tumblr-en-US% ' or msg like '%tumblr-video-en-US% ' )) " +
    "group by dt order by dt desc").show()

  println("\nDeepnet requests in a week\n")
  session.sql(
    "select dt, count(distinct post_id) from smp_classification_result " +
    "where dt between '2019-01-15' and '2019-01-21' " +
    "and (msg like '%deepnet%' and (msg like '%tumblr-en-US% ' or msg like '%tumblr-video-en-US% ' )) " +
    "group by dt order by dt desc").show()

  println("\nConfidence breakdown\n")
  session.sql(
    "select post_id, blog_id, classification, confidence " +
    "from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-21' and classification != 'NONE' " +
    "group by post_id, blog_id, classification, confidence").show()

  println("\nConfidence breakdown by bin\n")
  session.sql(
    "select classification, " +
    "if(confidence > 0.9, 0.9, if(confidence > 0.5, 0.5, if(confidence > 0.25, 0.25, if(confidence > 0.1, 0.1, 0)))) conf_bins, " +
    "count(distinct post_id) " +
    "from new_smp_post_classifications " +
    "where classification <> 'NONE' and dt between '2019-01-15' and '2019-01-21' " +
    "group by classification, if(confidence > 0.9, 0.9, if(confidence > 0.5, 0.5, if(confidence > 0.25, 0.25, if(confidence > 0.1, 0.1, 0))))").show()

  println("\nConfidence breakdown for tensorflow\n")
  session.sql(
    "select a.classification, " +
    "if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0)))) conf_bins, " +
    "count(distinct a.post_id) " +
    "from (select classification, post_id, confidence from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-21' and classification <> 'NONE') a " +
    "inner join (select post_id from smp_classification_result " +
    "where dt between '2019-01-15' and '2019-01-21' " +
    "and (msg not like '%deepnet%' and msg not like '%agentid%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
    "group by post_id) b on a.post_id = b.post_id " +
    "group by a.classification, if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0))))").show()

  println("\nConfidence breakdown for deepnet\n")
  session.sql(
    "select a.classification, " +
    "if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0)))) conf_bins, " +
    "count(distinct a.post_id) " +
    "from (select classification, post_id, confidence from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-21' and classification <> 'NONE') a " +
    "inner join (select post_id from smp_classification_result " +
    "where dt between '2019-01-15' and '2019-01-21' " +
    "and (msg like '%deepnet%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
    "group by post_id) b on a.post_id = b.post_id " +
    "group by a.classification, if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0))))").show()

  println("\nConfidence breakdown for appeals\n")
  session.sql(
    "select a.classification, " +
    "if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0)))) conf_bins, " +
    "count(distinct a.post_id) " +
    "from (select classification, post_id, confidence from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-21' and classification <> 'NONE') a " +
    "inner join (select post_id from own_post_appeal " +
    "where dt between '2019-01-15' and '2019-01-21' and action = 'request_review' group by post_id) b on a.post_id = b.post_id " +
    "group by a.classification, if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0))))").show()

  println("\nConfidence breakdown for tensorflow + appeals\n")
  session.sql(
    "select a.classification, " +
    "if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0)))) conf_bins, " +
    "count(distinct a.post_id) " +
    "from (select classification, post_id, confidence from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-21' and classification <> 'NONE') a " +
    "inner join (select post_id from own_post_appeal " +
    "where dt between '2019-01-15' and '2019-01-21' and action = 'request_review' group by post_id) b on a.post_id = b.post_id " +
    "inner join (select post_id from smp_classification_result " +
    "where dt between '2019-01-15' and '2019-01-21' " +
    "and (msg not like '%deepnet%' and msg not like '%agentid%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
    "group by post_id) c on a.post_id = c.post_id " +
    "group by a.classification, if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0))))").show()

  println("\nConfidence breakdown for deepnet + appeals\n")
  session.sql(
    "select a.classification, " +
    "if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0)))) conf_bins, " +
    "count(distinct a.post_id) " +
    "from (select classification, post_id, confidence from new_smp_post_classifications " +
    "where dt between '2019-01-15' and '2019-01-21' and classification <> 'NONE') a " +
    "inner join (select post_id from own_post_appeal " +
    "where dt between '2019-01-15' and '2019-01-21' and action = 'request_review' group by post_id) b on a.post_id = b.post_id " +
    "inner join (select post_id from smp_classification_result " +
    "where dt between '2019-01-15' and '2019-01-21' " +
    "and (msg like '%deepnet%' and (msg like '%tumblr-en-US%' or msg like '%tumblr-video-en-US%' and category = 'smp_classification_result_info')) " +
    "group by post_id) c on a.post_id = c.post_id " +
    "group by a.classification, if(a.confidence > 0.9, 0.9, if(a.confidence > 0.5, 0.5, if(a.confidence > 0.25, 0.25, if(a.confidence > 0.1, 0.1, 0))))").show()
}
