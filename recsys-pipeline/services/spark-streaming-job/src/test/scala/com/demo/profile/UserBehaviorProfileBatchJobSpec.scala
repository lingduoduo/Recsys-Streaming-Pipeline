package com.demo.profile

import com.demo.SparkTestSupport
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sha2}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files

private[profile] case class ProfileSample(
    sample_id: String,
    request_id: String,
    user_id: String,
    item_id: String,
    impression_ts: java.lang.Long,
    clicked: java.lang.Boolean,
    ordered: java.lang.Boolean,
    rating: java.lang.Double,
    genres: Seq[String],
    tags: Seq[String],
    new_release: java.lang.Boolean
)

class UserBehaviorProfileBatchJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val config = ProfileConfig(referenceEpochSeconds = 1000L, halfLifeSeconds = 100L)

  private def samples(values: ProfileSample*): DataFrame = {
    val session = spark
    import session.implicits._
    values.toDF()
  }

  "validateAndDeduplicate" should "reject invalid rows, prefer explicit ids, and hash fallback identities" in {
    val session = spark
    import session.implicits._
    val input = samples(
      ProfileSample("explicit", "r1", "u1", "i1", 900L, true, false, null, Seq(" Sci-Fi ", "sci-fi"), Seq(" Space "), true),
      ProfileSample("explicit", "r1", "u1", "i1", 900L, true, false, null, Seq("sci-fi"), Seq("space"), true),
      ProfileSample(null, "r2", "u2", "i2", 901L, false, false, null, Seq("Drama"), null, false),
      ProfileSample(null, "r2", "u2", "i2", 901L, false, false, null, Seq("Drama"), null, false),
      ProfileSample("missing-user", "r3", null, "i3", 902L, false, false, null, Seq("Comedy"), null, false),
      ProfileSample("bad-time", "r4", "u4", "i4", null, false, false, null, Seq("Comedy"), null, false)
    )

    val prepared = UserBehaviorProfileBatchJob.validateAndDeduplicate(input, config)

    prepared.valid.count() shouldBe 2L
    prepared.rejected.groupBy("rejection_reason").count().as[(String, Long)].collect().toMap shouldBe
      Map("missing_user" -> 1L, "invalid_timestamp" -> 1L)
    prepared.valid.filter(col("sample_id") === "explicit").select("dedupe_id").as[String].head() shouldBe "explicit"

    val fallback = prepared.valid.filter(col("sample_id").isNull).select("dedupe_id").as[String].head()
    val expected = input.filter(col("sample_id").isNull && col("user_id") === "u2")
      .select(sha2(org.apache.spark.sql.functions.concat_ws("\u001f", col("user_id"), col("request_id"), col("item_id"),
        col("impression_ts"), col("clicked"), col("ordered"), col("rating")), 256).as("id"))
      .as[String].head()
    fallback shouldBe expected
    prepared.valid.filter(col("sample_id") === "explicit").select("genres", "tags").collect().head.toSeq shouldBe
      Seq(Seq("sci-fi"), Seq("space"))
    prepared.metrics.deduplicatedCount shouldBe 2L
  }

  "buildProfiles" should "aggregate decayed preference evidence and explicit null metrics per user" in {
    val valid = UserBehaviorProfileBatchJob.validateAndDeduplicate(samples(
      ProfileSample("s1", "r1", "u1", "i1", 1000L, true, false, null, Seq(" Sci-Fi "), Seq(" Space "), true),
      ProfileSample("s2", "r2", "u1", "i2", 1000L, true, true, null, Seq("sci-fi"), Seq("space"), true),
      ProfileSample("s3", "r3", "u1", "i3", 900L, true, false, null, Seq("Drama"), Seq("Character"), false),
      ProfileSample("s4", "r4", "u1", "i4", 1000L, false, false, null, Seq("Comedy"), Seq("Silly"), false),
      ProfileSample("s5", "r5", "u1", "i5", 1000L, true, false, null, Seq("SCI-FI"), Seq("SPACE"), true)
    ), config).valid

    val row = UserBehaviorProfileBatchJob.buildProfiles(valid, config).head()
    row.getAs[Long]("evidence_count") shouldBe 5L
    row.getAs[Long]("impressions") shouldBe 5L
    row.getAs[Long]("clicks") shouldBe 4L
    row.getAs[Long]("orders") shouldBe 1L
    row.getAs[Double]("engagement_rate") shouldBe 0.8 +- 1e-9
    row.getAs[Double]("conversion_rate") shouldBe 0.2 +- 1e-9
    row.isNullAt(row.fieldIndex("average_rating")) shouldBe true
    row.getAs[Double]("recent_release_affinity") shouldBe (5.0 / 5.5) +- 1e-9

    val genres = row.getAs[Seq[org.apache.spark.sql.Row]]("genre_preferences")
    genres.map(_.getAs[String]("value")) shouldBe Seq("sci-fi", "drama", "comedy")
    genres.head.getAs[Double]("score") shouldBe 0.625 +- 1e-9
    genres(1).getAs[Double]("score") shouldBe (0.5 / 6.0) +- 1e-9
    genres(2).getAs[Double]("score") shouldBe (-0.1 / 6.0) +- 1e-9

    val personas = row.getAs[Seq[org.apache.spark.sql.Row]]("personas")
    personas.map(_.getAs[String]("type")) should contain allOf (
      "genre_enthusiast", "focused_viewer", "recent_release_seeker", "high_intent_engager")
    personas.find(_.getAs[String]("type") == "genre_enthusiast").get
      .getAs[scala.collection.Map[String, Double]]("evidence")("preference_score") shouldBe 0.625 +- 1e-9
  }

  "run" should "write deterministic parquet profiles and the version-one JSON contract" in {
    val inputDir = Files.createTempDirectory("profile-input")
    val outputOne = Files.createTempDirectory("profile-output-one").resolve("profiles")
    val outputTwo = Files.createTempDirectory("profile-output-two").resolve("profiles")
    val source = samples(
      ProfileSample("fixture-1", "request-1", "fixture-user", "item-1", 1000L, true, false, null,
        Seq("Sci-Fi"), Seq("Space"), true)
    )
    source.write.mode("overwrite").parquet(inputDir.toString)

    val first = UserBehaviorProfileBatchJob.run(spark, inputDir.toString, outputOne.toString, config,
      "fixture-run", "1970-01-01T00:16:40Z")
    val second = UserBehaviorProfileBatchJob.run(spark, inputDir.toString, outputTwo.toString, config,
      "second-run", "1970-01-01T00:16:40Z")
    first.metrics.profileCount shouldBe 1L
    second.metrics.profileCount shouldBe 1L

    val firstRow = spark.read.parquet(outputOne.toString).head()
    val secondRow = spark.read.parquet(outputTwo.toString).head()
    firstRow.getAs[String]("profile_json") should include ("\"average_rating\":null")
    val parsed = new ObjectMapper().readTree(firstRow.getAs[String]("profile_json"))
    parsed.path("behavioral_features").path("average_rating").isNull shouldBe true
    firstRow.getAs[String]("profile_json").replace("fixture-run", "RUN").replace("1970-01-01T00:16:40Z", "TIME") shouldBe
      secondRow.getAs[String]("profile_json").replace("second-run", "RUN").replace("1970-01-01T00:16:40Z", "TIME")

    val fixture = new String(Files.readAllBytes(java.nio.file.Paths.get(
      "../../integration-tests/fixtures/user_profile_v1.json")), StandardCharsets.UTF_8).trim
    firstRow.getAs[String]("profile_json") shouldBe fixture
  }
}
