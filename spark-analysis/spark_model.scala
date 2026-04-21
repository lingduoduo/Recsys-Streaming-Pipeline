package com.demo.analysis

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}

object ActiveUsersJob {

    def main(args: Array[String]): Unit = {
        val hiveMetastoreUri = sys.env.getOrElse("HIVE_METASTORE_URI", "thrift://localhost:9083")

        val session = SparkSession
          .builder()
          .appName("Active User Analysis")
          .config("hive.metastore.uris", hiveMetastoreUri)
          .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
          .enableHiveSupport()
          .getOrCreate()

        try {
            val results = session.sql("select * from active_user_social_message_post_search")
            results.cache()

            val devices = Set("android", "ios", "web", "other")
            for (device <- devices) {
                println("Device Type:\n" + device)
                val df = results.where(results.col("devicetype") === device)
                deviceReport(df)
                dataPreparation(df)
            }

            results.unpersist()
        } finally {
            session.stop()
        }
    }

    def deviceReport(df: Dataset[_]): Unit = {
        println("Number of users:\n" + df.count())

        println("Count by suspended\n")
        val results1 = df.groupBy("issuspended").count().as("count")
        results1.orderBy(results1.col("count").desc).show()

        println("Count by suspended and geo\n")
        val results2 = df.groupBy("issuspended", "geo").count().as("count")
        val results3 = results2.groupBy("geo").pivot("issuspended").agg("count" -> "sum")
        results3.orderBy(results3.col("true").desc).show()

        println("Distribution by email domain\n")
        val results4 = df.groupBy("issuspended", "email_domain").count().as("count")
        val results5 = results4.groupBy("email_domain").pivot("issuspended").agg("count" -> "sum")
        results5.orderBy(results5.col("true").desc).show()
    }

    def dataPreparation(df: Dataset[_]): Unit = {
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

      // Replace UDFs with native isin/when — optimizer-transparent, no serialization overhead
      val df3 = df2.withColumn("geo",
        when(col("geo").isin("US", "GB", "DE", "RU", "MX", "AU", "CA", "XO", "KR"), col("geo"))
          .otherwise(lit("other"))
      )

      val df4 = df3.withColumn("email_domain",
        when(col("email_domain").isin("gmail.com", "yahoo.com", "hotmail.com", "mail.ru", "outlook.com", "icloud.com", "bk.ru", "inbox.ru"), col("email_domain"))
          .otherwise(lit("other"))
      )
      df4.groupBy("email_domain").count().show()

      val geoIndexer = new StringIndexer()
        .setInputCol("geo").setOutputCol("geoIdx").setHandleInvalid("skip")
      val geoEncoder = new OneHotEncoder()
        .setInputCol("geoIdx").setOutputCol("geoEncoder")
      val email_domainsIndexer = new StringIndexer()
        .setInputCol("email_domain").setOutputCol("email_domainIdx").setHandleInvalid("skip")
      val email_domainsEncoder = new OneHotEncoder()
        .setInputCol("email_domainIdx").setOutputCol("email_domainEncoder")
      val labelIndexer = new StringIndexer()
        .setInputCol("issuspended").setOutputCol("label")

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
        "days_original_post", "days_reblog_post", "days_reply_post"
      )
      val assembler = new VectorAssembler().setInputCols(cols).setOutputCol("features")

      val Array(train_set, test_set) = df4.randomSplit(Array(0.7, 0.3))

      val pipeline = new Pipeline().setStages(
        Array(geoIndexer, geoEncoder, email_domainsIndexer, email_domainsEncoder, labelIndexer, assembler,
          new LogisticRegression().setFamily("binomial"))
      )
      val predictions = pipeline.fit(train_set).transform(test_set)
      println("Accuracy\n" + new BinaryClassificationEvaluator().evaluate(predictions))
    }
}
