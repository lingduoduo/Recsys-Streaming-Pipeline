package com.demo.profile

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.Instant
import java.util.UUID

final case class ProfileMetrics(
    inputCount: Long,
    validCount: Long,
    rejectedCount: Long,
    deduplicatedCount: Long,
    profileCount: Long = 0L
)

final case class ProfileInput(valid: DataFrame, rejected: DataFrame, metrics: ProfileMetrics)

final case class ProfileRunResult(runId: String, profiles: DataFrame, outputPath: String, metrics: ProfileMetrics)

/** Builds deterministic, explainable version-one behavioral-profile snapshots. */
object UserBehaviorProfileBatchJob {
  private val ProfileVersion = 1
  private val LookbackSeconds = 30L * 24L * 60L * 60L

  private val preferenceSchema = StructType(Seq(
    StructField("value", StringType, nullable = false),
    StructField("score", DoubleType, nullable = false),
    StructField("evidence_count", LongType, nullable = false)
  ))
  private val personaSchema = StructType(Seq(
    StructField("type", StringType, nullable = false),
    StructField("label", StringType, nullable = false),
    StructField("confidence", DoubleType, nullable = false),
    StructField("evidence", MapType(StringType, DoubleType, valueContainsNull = false), nullable = false)
  ))

  /** Validates source rows in the configured lookback, then removes equivalent valid events. */
  def validateAndDeduplicate(input: DataFrame, config: ProfileConfig): ProfileInput = {
    val normalized = prepareSource(input, config)
    val withReason = normalized.withColumn("rejection_reason",
      when(col("user_id").isNull || length(trim(col("user_id"))) === 0, lit("missing_user"))
        .when(col("impression_ts").isNull, lit("invalid_timestamp"))
        .when(col("rating").isNotNull &&
          (col("rating") < lit(config.ratingMin) || col("rating") > lit(config.ratingMax)), lit("invalid_behavior"))
        .when(col("impression_ts") < lit(sourceWindowStart(config)) ||
          col("impression_ts") >= lit(sourceWindowEndExclusive(config)), lit("outside_source_window"))
    )

    val rejected = withReason.filter(col("rejection_reason").isNotNull)
    val validBeforeDedup = withReason.filter(col("rejection_reason").isNull)
      .drop("rejection_reason")
    val valid = validBeforeDedup.dropDuplicates("dedupe_id")
    val inputCount = input.count()
    val beforeDedupCount = validBeforeDedup.count()
    val validCount = valid.count()
    val rejectedCount = rejected.count()
    ProfileInput(valid, rejected, ProfileMetrics(
      inputCount = inputCount,
      validCount = validCount,
      rejectedCount = rejectedCount,
      deduplicatedCount = beforeDedupCount - validCount
    ))
  }

  /** Aggregates one row per user. The default metadata is deterministic for direct use and tests. */
  def buildProfiles(valid: DataFrame, config: ProfileConfig): DataFrame =
    withMetadata(buildProfileBody(valid, config), config, "profile-run", iso(config.referenceEpochSeconds))

  def run(spark: SparkSession, inputPath: String, outputPath: String, config: ProfileConfig): ProfileRunResult =
    run(spark, inputPath, outputPath, config, UUID.randomUUID().toString, Instant.now().toString)

  /** Testable metadata overload; production callers use the unique-id overload above. */
  def run(
      spark: SparkSession,
      inputPath: String,
      outputPath: String,
      config: ProfileConfig,
      runId: String,
      generatedAt: String
  ): ProfileRunResult = {
    val prepared = validateAndDeduplicate(spark.read.parquet(inputPath), config)
    val profiles = withMetadata(buildProfileBody(prepared.valid, config), config, runId, generatedAt)
    val profileCount = profiles.count()
    profiles.write.mode("errorifexists").parquet(outputPath)
    ProfileRunResult(runId, profiles, outputPath, prepared.metrics.copy(profileCount = profileCount))
  }

  private def prepareSource(input: DataFrame, config: ProfileConfig): DataFrame = {
    val withRequired = Seq(
      "sample_id" -> StringType,
      "request_id" -> StringType,
      "user_id" -> StringType,
      "item_id" -> StringType,
      "impression_ts" -> LongType,
      "clicked" -> BooleanType,
      "ordered" -> BooleanType
    ).foldLeft(input) { case (frame, (name, dataType)) => ensureColumn(frame, name, dataType) }
    val optional = ensureColumn(ensureColumn(ensureColumn(withRequired, "tags", ArrayType(StringType)),
      "rating", DoubleType), "new_release", BooleanType)
    val withGenres = ensureColumn(optional, "genres", ArrayType(StringType))

    withGenres
      .withColumn("user_id", trim(col("user_id").cast(StringType)))
      .withColumn("request_id", trim(col("request_id").cast(StringType)))
      .withColumn("item_id", trim(col("item_id").cast(StringType)))
      .withColumn("sample_id", trim(col("sample_id").cast(StringType)))
      .withColumn("impression_ts", col("impression_ts").cast(LongType))
      .withColumn("clicked", coalesce(col("clicked").cast(BooleanType), lit(false)))
      .withColumn("ordered", coalesce(col("ordered").cast(BooleanType), lit(false)))
      .withColumn("rating", col("rating").cast(DoubleType))
      .withColumn("new_release", col("new_release").cast(BooleanType))
      .withColumn("genres", normalizedTerms("genres"))
      .withColumn("tags", normalizedTerms("tags"))
      .withColumn("dedupe_id", when(col("sample_id").isNotNull && length(col("sample_id")) > 0, col("sample_id"))
        .otherwise(sha2(concat_ws("\u001f", col("user_id"), col("request_id"), col("item_id"),
          col("impression_ts"), col("clicked"), col("ordered"), col("rating")), 256)))
  }

  private def ensureColumn(frame: DataFrame, name: String, dataType: DataType): DataFrame =
    if (frame.columns.contains(name)) frame else frame.withColumn(name, lit(null).cast(dataType))

  private def normalizedTerms(name: String): Column = {
    val normalized = transform(coalesce(col(name), expr("array()").cast(ArrayType(StringType))),
      value => lower(trim(value)))
    array_sort(array_distinct(filter(normalized, value => value.isNotNull && length(value) > 0)))
  }

  private def buildProfileBody(valid: DataFrame, config: ProfileConfig): DataFrame = {
    val weighted = valid.withColumn("event_weight", eventWeightColumn(config))
    val activity = weighted.groupBy("user_id").agg(
      count(lit(1)).as("evidence_count"),
      count(lit(1)).as("impressions"),
      sum(when(col("clicked"), lit(1L)).otherwise(lit(0L))).cast(LongType).as("clicks"),
      sum(when(col("ordered"), lit(1L)).otherwise(lit(0L))).cast(LongType).as("orders"),
      avg(col("rating")).as("average_rating")
    ).withColumn("engagement_rate", when(col("impressions") > 0,
      col("clicks").cast(DoubleType) / col("impressions")))
      .withColumn("conversion_rate", when(col("impressions") > 0,
        col("orders").cast(DoubleType) / col("impressions")))
      .withColumn("activity_level", when(col("evidence_count") < lit(config.minimumEvidence), lit("low"))
        .when(col("evidence_count") < lit(config.minimumEvidence * 4L), lit("medium"))
        .otherwise(lit("high")))

    val genrePreferences = preferenceTable(weighted, "genres", "genre_preferences", config.maxGenres, config)
    val tagPreferences = preferenceTable(weighted, "tags", "tag_preferences", config.maxTags, config)
    val genreShape = genreShapeTable(weighted)
    val recentRelease = weighted.filter(col("event_weight") > 0.0).groupBy("user_id").agg(
      (sum(when(coalesce(col("new_release"), lit(false)), col("event_weight")).otherwise(lit(0.0))) /
        sum(col("event_weight"))).as("recent_release_affinity")
    )
    val emptyPreferences = from_json(lit("[]"), ArrayType(preferenceSchema))
    val withFeatures = activity.join(genrePreferences, Seq("user_id"), "left")
      .join(tagPreferences, Seq("user_id"), "left")
      .join(genreShape, Seq("user_id"), "left")
      .join(recentRelease, Seq("user_id"), "left")
      .withColumn("genre_preferences", coalesce(col("genre_preferences"), emptyPreferences))
      .withColumn("tag_preferences", coalesce(col("tag_preferences"), emptyPreferences))

    val personas = personaColumn(config)
    withFeatures.withColumn("personas", personas(struct(
      col("evidence_count"), col("impressions"), col("clicks"), col("orders"),
      col("genre_preferences"), col("tag_preferences"), col("engagement_rate"), col("conversion_rate"),
      col("average_rating"), col("genre_diversity"), col("preference_concentration"), col("recent_release_affinity")
    )))
  }

  private def eventWeightColumn(config: ProfileConfig): Column = {
    val ratingWeight = when(col("rating") >= lit(config.ratingMidpoint),
      (col("rating") - lit(config.ratingMidpoint)) / lit(config.ratingMax - config.ratingMidpoint))
      .otherwise((col("rating") - lit(config.ratingMidpoint)) / lit(config.ratingMidpoint - config.ratingMin))
    val base = when(col("rating").isNotNull, ratingWeight)
      .when(col("ordered"), lit(config.orderWeight))
      .when(col("clicked"), lit(config.clickWeight))
      .otherwise(lit(config.impressionWeight))
    val age = greatest(lit(0L), lit(config.referenceEpochSeconds) - col("impression_ts"))
    base * pow(lit(0.5), age.cast(DoubleType) / lit(config.halfLifeSeconds.toDouble))
  }

  private def preferenceTable(
      weighted: DataFrame,
      source: String,
      output: String,
      maximum: Int,
      config: ProfileConfig
  ): DataFrame = {
    val aggregated = weighted.select(col("user_id"), col("event_weight"), explode_outer(col(source)).as("value"))
      .filter(col("value").isNotNull && length(col("value")) > 0)
      .groupBy("user_id", "value")
      .agg(sum(col("event_weight")).as("raw_score"), count(lit(1)).as("evidence_count"))
      .withColumn("score", greatest(lit(-1.0), least(lit(1.0),
        col("raw_score") / (col("evidence_count").cast(DoubleType) + lit(config.shrinkage)))))
      .select(col("user_id"), struct((-col("score")).as("sort_score"), col("value"),
        col("score"), col("evidence_count")).as("preference"))
      .groupBy("user_id").agg(array_sort(collect_list(col("preference"))).as("ordered"))
      .withColumn("ordered", slice(col("ordered"), 1, maximum))

    aggregated.select(col("user_id"), transform(col("ordered"), preference => struct(
      preference.getField("value").as("value"), preference.getField("score").as("score"),
      preference.getField("evidence_count").as("evidence_count"))).as(output))
  }

  private def genreShapeTable(weighted: DataFrame): DataFrame = {
    val positive = weighted.filter(col("event_weight") > 0.0)
      .select(col("user_id"), col("event_weight"), explode_outer(col("genres")).as("genre"))
      .filter(col("genre").isNotNull && length(col("genre")) > 0)
      .groupBy("user_id", "genre").agg(sum(col("event_weight")).as("positive_weight"))
    val shares = positive.join(positive.groupBy("user_id").agg(sum(col("positive_weight")).as("total_weight")),
      Seq("user_id"))
      .withColumn("share", col("positive_weight") / col("total_weight"))
    shares.groupBy("user_id").agg(
      (lit(1.0) - sum(pow(col("share"), 2.0))).as("genre_diversity"),
      max(col("share")).as("preference_concentration")
    )
  }

  private def personaColumn(config: ProfileConfig): UserDefinedFunction = {
    val rule = udf(new UDF1[Row, Seq[Row]] {
      override def call(features: Row): Seq[Row] = {
      def optionDouble(name: String): Option[Double] =
        if (features.isNullAt(features.fieldIndex(name))) None else Some(features.getAs[Double](name))
      def preferences(name: String): Seq[Preference] =
        if (features.isNullAt(features.fieldIndex(name))) Seq.empty else features.getAs[Seq[Row]](name).map { row =>
          Preference(row.getAs[String]("value"), row.getAs[Double]("score"), row.getAs[Long]("evidence_count"))
        }
      val derived = UserProfileDerivations.classifyPersonas(BehavioralFeatures(
        evidenceCount = features.getAs[Long]("evidence_count"),
        impressions = features.getAs[Long]("impressions"),
        clicks = features.getAs[Long]("clicks"),
        orders = features.getAs[Long]("orders"),
        genrePreferences = preferences("genre_preferences"),
        tagPreferences = preferences("tag_preferences"),
        engagementRate = optionDouble("engagement_rate"),
        conversionRate = optionDouble("conversion_rate"),
        averageRating = optionDouble("average_rating"),
        genreDiversity = optionDouble("genre_diversity"),
        preferenceConcentration = optionDouble("preference_concentration"),
        recentReleaseAffinity = optionDouble("recent_release_affinity"),
        activityLevel = "unknown"
      ), config)
        derived.map(persona => Row(persona.personaType, persona.label, persona.confidence, persona.evidence))
      }
    }, ArrayType(personaSchema, containsNull = false))
    rule
  }

  private def withMetadata(body: DataFrame, config: ProfileConfig, runId: String, generatedAt: String): DataFrame = {
    val profile = body
      .withColumn("profile_version", lit(ProfileVersion))
      .withColumn("run_id", lit(runId))
      .withColumn("generated_at", lit(generatedAt))
      .withColumn("source_window", struct(lit(iso(sourceWindowStart(config))).as("start"),
        lit(iso(sourceWindowEndExclusive(config))).as("end")))
      .withColumn("preferences", struct(col("genre_preferences").as("genres"), col("tag_preferences").as("tags")))
      .withColumn("behavioral_features", struct(
        col("engagement_rate"), col("conversion_rate"), col("genre_diversity"), col("preference_concentration"),
        col("recent_release_affinity"), col("average_rating"), col("activity_level")))
      .select(col("user_id"), col("profile_version"), col("run_id"), col("generated_at"), col("source_window"),
        col("evidence_count"), col("preferences"), col("behavioral_features"), col("personas"),
        col("impressions"), col("clicks"), col("orders"), col("engagement_rate"), col("conversion_rate"),
        col("average_rating"), col("genre_diversity"), col("preference_concentration"), col("recent_release_affinity"),
        col("activity_level"), col("genre_preferences"), col("tag_preferences"))
    profile.withColumn("profile_json", to_json(struct(
      col("user_id"), col("profile_version"), col("run_id"), col("generated_at"), col("source_window"),
      col("evidence_count"), col("preferences"), col("behavioral_features"), col("personas")),
      Map("ignoreNullFields" -> "false")))
  }

  private def sourceWindowStart(config: ProfileConfig): Long = config.referenceEpochSeconds - LookbackSeconds
  private def sourceWindowEndExclusive(config: ProfileConfig): Long = config.referenceEpochSeconds + 1L
  private def iso(epochSeconds: Long): String = Instant.ofEpochSecond(epochSeconds).toString
}
