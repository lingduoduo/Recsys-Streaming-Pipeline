package com.spotify.data.zissou.adjust

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.joda.time.{LocalDate => JodaLocalDate}

import com.spotify.data.counters._
import com.spotify.scio.{ContextAndArgs, ScioMetrics}
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.metrics.Counter

/*
$ sbt
> project zissou
> runMain com.spotify.data.zissou.adjust.AdjustUserRetentionDataTestJob
--project=acmacquisition
--runner=DataflowRunner
--region=europe-west1
--outputBq=acmacquisition:adjust.test10
--date=20200512
--tempLocation=gs://zissou-secrets/dataflow/tmp
--dataEndpoint=AdjustUserRetentionDataJob
--dataPartition=2020-05-12
*/


// scalastyle:off line.size.limit
object AdjustUserRetentionDataJob {

val totalRecords: Counter = ScioMetrics.counter("adjust-user-activity-total-counts")

@BigQueryType.toTable
@description("ACM Adjust User Retention Data")
case class UserRetention(
   @description("Platform") platform: String,
   @description("{ policy: { semanticType: userId }, description: 'User ID' }") user_id: String,
   @description("Day 1 Retention") d1: Int,
   @description("Day 2 Retention") d2: Int,
   @description("Number of Retention Days from Day 3 to Day 9") l7: Int,
   @description("Weekly Active Status from Day 3 to Day 9") wau: Int
 )

 @BigQueryType.toTable
 case class UserRetentionRaw(
   platform: Option[String],
   user_id: String,
   date: Option[JodaLocalDate],
   dau: Option[Int]
 )

 //scalastyle:off
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
 //scalastyle:on
 
 def main(cmdlineArgs: Array[String]): Unit = {
   val (sc, args) = ContextAndArgs(cmdlineArgs)
   val partition = args("date")
   val outputBq = args("outputBq")

   sc.initCounter(totalRecords.getName.getName)

   val userRetentionRawData = sc
     .typedBigQuery[UserRetentionRaw](Query(query(partition)))
     .withName("ReadUserRetentionData")

   pipeline(userRetentionRawData, partition)
     .map(row => {
       totalRecords.inc()
       row
     })
     .saveAsTypedBigQueryTable(
       Table.Spec(outputBq),
       writeDisposition = WRITE_EMPTY,
       createDisposition = CREATE_IF_NEEDED
     )

   sc.runAndPublishMetrics()

 }


 def pipeline(
   data: SCollection[UserRetentionRaw],
   partitionDate: String
 ): SCollection[UserRetention] = {

   val partitionDt = LocalDate.parse(partitionDate, DateTimeFormatter.BASIC_ISO_DATE)
   val DAY1 = Some(
     JodaLocalDate.parse(partitionDt.plusDays(1).format(DateTimeFormatter.ISO_LOCAL_DATE))
   )

   val DAY2 = Some(
     JodaLocalDate.parse(partitionDt.plusDays(2).format(DateTimeFormatter.ISO_LOCAL_DATE))
   )

   val users = data.map(row => (row.user_id, row.platform.getOrElse(" "))).distinct
   val firstday = mapUserDates(data.filter(_.date == DAY1))
   val seconday = mapUserDates(data.filter(_.date == DAY2))
   val week = mapUserDates(data.filter(row => row.date != DAY1 && row.date != DAY2)).sumByKey

   MultiJoin
     .left(users, firstday, seconday, week)
     .map {
       case (user_id, (platform, d1, d2, l7)) =>
         UserRetention(
           platform = platform,
           user_id = user_id,
           d1 = d1.getOrElse(0),
           d2 = d2.getOrElse(0),
           l7 = l7.getOrElse(0),
           wau = if (l7.getOrElse(0) > 0) 1 else 0
         )
     }
 }

 def mapUserDates(userData: SCollection[UserRetentionRaw]): SCollection[(String, Int)] = {
   userData.map {
     case UserRetentionRaw(_, user_id, _, dau) => (user_id, dau.getOrElse(0))
   }
 }
}
