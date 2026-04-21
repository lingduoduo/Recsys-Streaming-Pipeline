package com.demo.analysis

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

// scalastyle:off line.size.limit
object AdjustUserRetentionDataJob {

  case class UserRetention(
    platform: String,
    user_id: String,
    d1: Int,
    d2: Int,
    l7: Int,
    wau: Int
  )

  case class UserRetentionRaw(
    platform: Option[String],
    user_id: String,
    date: Option[java.sql.Date],
    dau: Option[Int]
  )

  // scalastyle:off
  def query(partition: String): String =
    s"""
       |with users as (
       |SELECT 'display' AS platform,
       |  user_id
       |FROM `acmacquisition.bqr.display_adjust_$partition` WHERE user_id != ' '
       |UNION ALL
       |SELECT 'facebook' AS platform,
       |  user_id
       |FROM `acmacquisition.bqr.facebook_adjust_$partition` WHERE user_id != ' '
       |)
       |
       |SELECT a.platform,
       |  a.user_id,
       |  b.date,
       |  if(activity.dau.today, 1, 0) AS dau
       |FROM users a
       |JOIN `subs-analytics.scd.user_*` b
       |USING (user_id)
       |WHERE _table_suffix
       |BETWEEN FORMAT_DATE("%Y%m%d", DATE_ADD(PARSE_DATE("%Y%m%d","$partition"), INTERVAL 1 day))
       |AND FORMAT_DATE("%Y%m%d", DATE_ADD(PARSE_DATE("%Y%m%d","$partition"), INTERVAL 9 day))
       |GROUP BY 1, 2, 3, 4
       |""".stripMargin
  // scalastyle:on

  def main(args: Array[String]): Unit = {
    val argMap = args.grouped(2).collect { case Array(k, v) => k.stripPrefix("--") -> v }.toMap
    val partition = argMap.getOrElse("date",     throw new IllegalArgumentException("--date required"))
    val outputBq  = argMap.getOrElse("outputBq", throw new IllegalArgumentException("--outputBq required"))
    val project   = argMap.getOrElse("project",  sys.env.getOrElse("GCP_PROJECT", ""))

    val spark = SparkSession.builder()
      .appName("AdjustUserRetentionDataJob")
      .getOrCreate()

    try {
      val rawData = spark.read
        .format("bigquery")
        .option("project", project)
        .option("query", query(partition))
        .load()
        .as[UserRetentionRaw](spark.implicits.newProductEncoder)

      val result = pipeline(spark, rawData, partition)

      result.write
        .format("bigquery")
        .option("table", outputBq)
        .option("writeDisposition", "WRITE_EMPTY")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .save()

      println(s"Total records written: ${result.count()}")
    } finally {
      spark.stop()
    }
  }

  def pipeline(
    spark: SparkSession,
    data: Dataset[UserRetentionRaw],
    partitionDate: String
  ): Dataset[UserRetention] = {
    import spark.implicits._

    // Cache: data is scanned four times (users, day1, day2, week)
    data.cache()

    val partitionDt = LocalDate.parse(partitionDate, DateTimeFormatter.BASIC_ISO_DATE)
    val day1Str = partitionDt.plusDays(1).toString
    val day2Str = partitionDt.plusDays(2).toString

    val users = data
      .select($"user_id", coalesce($"platform", lit(" ")).as("platform"))
      .distinct()

    val firstDay = data
      .filter($"date" === lit(day1Str))
      .groupBy("user_id").agg(sum(coalesce($"dau", lit(0))).cast("int").as("d1"))

    val secondDay = data
      .filter($"date" === lit(day2Str))
      .groupBy("user_id").agg(sum(coalesce($"dau", lit(0))).cast("int").as("d2"))

    val week = data
      .filter($"date" =!= lit(day1Str) && $"date" =!= lit(day2Str))
      .groupBy("user_id").agg(sum(coalesce($"dau", lit(0))).cast("int").as("l7"))

    val result = users
      .join(firstDay,  Seq("user_id"), "left")
      .join(secondDay, Seq("user_id"), "left")
      .join(week,      Seq("user_id"), "left")
      .select(
        $"platform",
        $"user_id",
        coalesce($"d1", lit(0)).as("d1"),
        coalesce($"d2", lit(0)).as("d2"),
        coalesce($"l7", lit(0)).as("l7"),
        when(coalesce($"l7", lit(0)) > 0, 1).otherwise(0).as("wau")
      )
      .as[UserRetention]

    data.unpersist()
    result
  }
}
