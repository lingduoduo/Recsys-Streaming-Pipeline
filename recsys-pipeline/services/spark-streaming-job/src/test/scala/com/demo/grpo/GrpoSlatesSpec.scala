package com.demo.grpo

import com.demo.SparkTestSupport
import com.demo.event.{EventParsing, EventSchemas}
import com.demo.process.OnlineJoinerStreamingJob
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Top-level (not nested in the spec class): Spark cannot derive a case-class encoder for a class
// defined inside another class, since that would require capturing the outer instance.
private case class TestItem(item_id: String, label: Double, item_features: Map[String, String])
private case class TestSlate(
    slate_id: String, request_id: String, user_id: String, request_ts: Long,
    slate_reward: Double, slate_size: Int, items: Seq[TestItem])

class GrpoSlatesSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  private val cfg = GrpoJobConfig.from(Map.empty)
  private def vec(v: Double): String = "v1:" + Array.fill(10)(v).mkString(",")

  "parseFeatureVector" should "accept a correctly versioned vector of the right width" in {
    GrpoSlates.parseFeatureVector(vec(0.5), "v1", 10).map(_.length) shouldBe Some(10)
  }

  it should "reject an unknown version rather than misalign weights against features" in {
    GrpoSlates.parseFeatureVector("v2:" + Array.fill(10)(0.5).mkString(","), "v1", 10) shouldBe None
  }

  it should "reject a vector of the wrong width" in {
    GrpoSlates.parseFeatureVector("v1:0.5,0.5", "v1", 10) shouldBe None
  }

  it should "reject an unparseable vector" in {
    GrpoSlates.parseFeatureVector("v1:a,b,c", "v1", 10) shouldBe None
  }

  "toGroups" should "keep a slate with reward variance and count it" in {
    val (groups, counts) = GrpoSlates.toGroups(slateFrame(Seq(("m1", 1.0), ("m2", 0.0))), cfg)
    groups should have size 1
    groups.head.rewards shouldBe Array(1.0, 0.0)
    counts.kept shouldBe 1L
  }

  it should "drop a slate where every reward is identical" in {
    val (groups, counts) = GrpoSlates.toGroups(slateFrame(Seq(("m1", 0.0), ("m2", 0.0))), cfg)
    groups shouldBe empty
    counts.zeroVariance shouldBe 1L
  }

  it should "drop a single-item slate" in {
    val (groups, counts) = GrpoSlates.toGroups(slateFrame(Seq(("m1", 1.0))), cfg)
    groups shouldBe empty
    counts.tooSmall shouldBe 1L
  }

  it should "drop a slate whose feature version it does not recognise" in {
    val (groups, counts) = GrpoSlates.toGroups(
      slateFrame(Seq(("m1", 1.0), ("m2", 0.0)), featureVersion = "v9"), cfg)
    groups shouldBe empty
    counts.badFeatureVersion shouldBe 1L
  }

  it should "drop a slate still carrying v1 feature vectors after the v2 cutover" in {
    // The exact cutover hazard: v1 rows left behind in training_samples after the v2 rollout must
    // be refused, not silently reinterpreted against the v2 (9-wide) feature layout.
    val (groups, counts) = GrpoSlates.toGroups(
      slateFrame(Seq(("m1", 1.0), ("m2", 0.0)), featureVersion = "v1"), cfg)
    groups shouldBe empty
    counts.badFeatureVersion shouldBe 1L
  }

  /** One slate carrying the given (item, label) pairs, shaped like ExperienceCollector publishes.
    *
    * Uses the top-level TestItem/TestSlate case classes rather than plain tuples: a tuple nested
    * inside an array column loses its field names (Spark calls them _1.._N), but GrpoSlates.toGroups
    * reads the nested fields by name ("item_features", "label").
    */
  private def slateFrame(items: Seq[(String, Double)], featureVersion: String = "v2") = {
    val s = spark
    import s.implicits._
    val rows = items.map { case (item, label) =>
      TestItem(item, label,
        Map("grpo_x" -> (featureVersion + ":" + Array.fill(9)(0.5).mkString(",")),
            "prediction_score" -> "0.4"))
    }
    Seq(TestSlate("req-1:u1", "req-1", "u1", 1000L, items.map(_._2).sum, items.size, rows)).toDF()
  }

  // The brief's Step 5 references a `OnlineJoinerStreamingJob.EventSchema` value that does not
  // exist -- the joiner has no public schema; it decodes canonical Avro fields via
  // `EventParsing.canonicalEvents`. `OnlineJoinerStreamingJobSpec`/`EventParsingSpec` already
  // establish the pattern for feeding it JSON in tests: parse a Kafka-shaped `value` column
  // through `EventSchemas.joiner` (the same field names/types `canonicalEvents` selects on),
  // then hand the result to `parseEvents`. That is what this test does, and it is the real gate
  // the serving path's event lands in, not a stand-in for it.
  "the Java emitter's event shape" should "parse under the joiner's gate with the GRPO fields set" in {
    val s = spark; import s.implicits._
    // Verbatim shape of one GrpoImpressionEvents.build(...) element (see
    // java-retrieval-service/.../grpo/GrpoImpressionEvents.java).
    val emitted =
      """{"event_id":"e1","request_id":"req-1","session_id":"sess_abcd1234","user_id":"u1",
        |"item_id":"m1","event_type":"impression","timestamp_ms":1000,"position":0,
        |"user_features":{"algorithm":"hybrid"},
        |"item_features":{"prediction_score":"0.73","grpo_x":"v2:1.0,0.7,0.4,0.3,0.05,0.0,2.4,1.1,0.18"},
        |"context_features":{}}""".stripMargin.replaceAll("\n", "")

    val kafkaShaped = Seq(emitted).toDF("value")
    val decoded = EventParsing.fromJson(kafkaShaped, EventSchemas.joiner)
    val gated = OnlineJoinerStreamingJob.parseEvents(decoded)
    val row = gated.kept.collect().head

    // OnlineJoinerStreamingJob.parseEvents drops rows with any of these null.
    row.getAs[String]("request_id") shouldBe "req-1"
    row.getAs[String]("user_id") shouldBe "u1"
    row.getAs[String]("item_id") shouldBe "m1"
    // And the two keys GRPO depends on survived the round trip.
    val itemFeatures = row.getAs[Map[String, String]]("item_features")
    itemFeatures("prediction_score") shouldBe "0.73"
    GrpoSlates.parseFeatureVector(itemFeatures("grpo_x"), "v2", 9).map(_.length) shouldBe Some(9)
  }
}
