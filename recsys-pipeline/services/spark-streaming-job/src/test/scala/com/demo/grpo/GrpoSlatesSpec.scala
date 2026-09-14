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
  private def vec(v: Double): String = "v2:" + Array.fill(9)(v).mkString(",")

  "parseFeatureVector" should "accept a correctly versioned vector of the right width" in {
    GrpoSlates.parseFeatureVector(vec(0.5), "v2", 9).map(_.length) shouldBe Some(9)
  }

  it should "reject an unknown version rather than misalign weights against features" in {
    GrpoSlates.parseFeatureVector("v1:" + Array.fill(9)(0.5).mkString(","), "v2", 9) shouldBe None
  }

  it should "reject a vector of the wrong width" in {
    GrpoSlates.parseFeatureVector("v2:0.5,0.5", "v2", 9) shouldBe None
  }

  it should "reject an unparseable vector" in {
    GrpoSlates.parseFeatureVector("v2:a,b,c", "v2", 9) shouldBe None
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

  /** A frozen copy of the collect-then-gate implementation, as the equivalence oracle.
    *
    * Deliberately duplicated rather than delegating: comparing the new path against a copy of
    * itself would assert nothing. If toGroups is rewritten again, this stays as it is.
    */
  private def legacyToGroups(slates: org.apache.spark.sql.DataFrame,
                             cfg: GrpoJobConfig): (Seq[GrpoGroup], GateCounts) = {
    def classifyRow(row: org.apache.spark.sql.Row): Either[String, GrpoGroup] = {
      val items = row.getSeq[org.apache.spark.sql.Row](1)
      if (items.size < 2) return Left(GrpoSlates.TooSmall)
      val features = items.map(_.getAs[Map[String, String]]("item_features"))
      val x = features.map(f =>
        GrpoSlates.parseFeatureVector(f.getOrElse("grpo_x", null), cfg.featureVersion, cfg.dim))
      if (x.exists(_.isEmpty)) return Left(GrpoSlates.BadFeatureVersion)
      val rewards = items.map { item =>
        if (item.isNullAt(item.fieldIndex("label"))) 0.0 else item.getAs[Double]("label")
      }.toArray
      if (GrpoMath.advantages(rewards).isEmpty) return Left(GrpoSlates.ZeroVariance)
      val logged = features.map { f =>
        try f.getOrElse("prediction_score", "0.0").toDouble
        catch { case _: NumberFormatException => 0.0 }
      }.toArray
      Right(GrpoGroup(row.getString(0), x.map(_.get).toArray, logged, rewards))
    }
    val classified = slates.select("slate_id", "items").collect().map(classifyRow)
    val kept = classified.collect { case Right(g) => g }.toSeq
    val dropped = classified.collect { case Left(r) => r }
      .groupBy(identity).map { case (r, hits) => r -> hits.length.toLong }
    (kept, GateCounts(
      kept = kept.size.toLong,
      tooSmall = dropped.getOrElse(GrpoSlates.TooSmall, 0L),
      zeroVariance = dropped.getOrElse(GrpoSlates.ZeroVariance, 0L),
      badFeatureVersion = dropped.getOrElse(GrpoSlates.BadFeatureVersion, 0L)))
  }

  private def sameGroups(a: Seq[GrpoGroup], b: Seq[GrpoGroup]): Boolean =
    a.length == b.length && a.zip(b).forall { case (l, r) =>
      l.slateId == r.slateId && l.rewards.sameElements(r.rewards) &&
        l.logged.sameElements(r.logged) &&
        l.x.length == r.x.length && l.x.zip(r.x).forall { case (p, q) => p.sameElements(q) }
    }

  private def item(id: String, label: Double, v: Double, prediction: String = "0.5"): TestItem =
    TestItem(id, label, Map("grpo_x" -> vec(v), "prediction_score" -> prediction))

  it should "return the same groups and counts as the collect-then-gate implementation" in {
    val s = spark
    import s.implicits._
    // One slate per gate outcome plus two kept, so every branch is compared in a single frame.
    val frame = Seq(
      TestSlate("keep1", "r1", "u1", 1L, 1.0, 2,
        Seq(item("a", 1.0, 0.5, "0.7"), item("b", 0.0, 0.2, "0.1"))),
      TestSlate("small", "r2", "u1", 2L, 0.0, 1, Seq(item("a", 1.0, 0.5))),
      TestSlate("badver", "r3", "u1", 3L, 0.0, 2, Seq(
        TestItem("a", 1.0, Map("grpo_x" -> "v1:0.5", "prediction_score" -> "0.7")),
        item("b", 0.0, 0.2))),
      TestSlate("flat", "r4", "u1", 4L, 0.0, 2,
        Seq(item("a", 0.0, 0.5), item("b", 0.0, 0.2))),
      // "bad" prediction_score exercises the NumberFormatException fallback on a KEPT slate.
      TestSlate("keep2", "r5", "u1", 5L, 1.0, 3, Seq(
        item("a", 1.0, 0.9, "0.9"), item("b", 0.0, 0.1, "bad"), item("c", 0.0, 0.3, "0.3")))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    val (wantGroups, wantCounts) = legacyToGroups(frame, cfg)

    sameGroups(groups, wantGroups) shouldBe true
    counts shouldBe wantCounts
    counts.kept shouldBe 2L
    counts.tooSmall shouldBe 1L
    counts.badFeatureVersion shouldBe 1L
    counts.zeroVariance shouldBe 1L
    groups.map(_.slateId) shouldBe Seq("keep1", "keep2")   // input order preserved
  }

  it should "count a slate under the first gate it fails, not a later one" in {
    val s = spark
    import s.implicits._
    // Counts now come from the Spark side while groups come from the driver side, so precedence
    // is the property most at risk. A one-item slate with a bad vector fails size AND version;
    // a flat two-item slate with a bad vector fails version AND variance.
    val frame = Seq(
      TestSlate("small-and-bad", "r1", "u1", 1L, 0.0, 1, Seq(
        TestItem("a", 1.0, Map("grpo_x" -> "v1:nope", "prediction_score" -> "0.7")))),
      TestSlate("bad-and-flat", "r2", "u1", 2L, 0.0, 2, Seq(
        TestItem("a", 0.0, Map("grpo_x" -> "v2:1,2", "prediction_score" -> "0.7")),
        item("b", 0.0, 0.2)))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    val (_, wantCounts) = legacyToGroups(frame, cfg)

    groups shouldBe empty
    counts shouldBe wantCounts
    counts.tooSmall shouldBe 1L            // not badFeatureVersion
    counts.badFeatureVersion shouldBe 1L   // not zeroVariance
    counts.zeroVariance shouldBe 0L
  }

  it should "leave the dropped slates behind rather than collecting them first" in {
    val s = spark
    import s.implicits._
    // The whole point of the change: at low click-through the variance gate rejects most slates
    // and they must not reach the driver on the way to being discarded. Every observable value is
    // identical either way, so the claim is only testable by asserting on the gated frame -- which
    // is exactly the frame toGroups collects from, exposed for this purpose and nothing else.
    val keptCount = 5
    val droppedCount = 95
    val frame = ((0 until keptCount).map { i =>
      TestSlate(s"k$i", s"r$i", "u1", i.toLong, 1.0, 2,
        Seq(item("a", 1.0, 0.5), item("b", 0.0, 0.2)))
    } ++ (0 until droppedCount).map { i =>
      TestSlate(s"d$i", s"rd$i", "u1", i.toLong, 0.0, 2,
        Seq(item("a", 0.0, 0.5), item("b", 0.0, 0.2)))
    }).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    groups should have size keptCount
    counts.kept shouldBe keptCount.toLong
    counts.zeroVariance shouldBe droppedCount.toLong

    val gated = GrpoSlates.tagged(frame, cfg)
    gated.count() shouldBe (keptCount + droppedCount).toLong
    gated.filter(gated(GrpoSlates.DropReasonColumn).isNull).count() shouldBe keptCount.toLong
  }

  it should "treat a slate with no items as too small rather than failing" in {
    val s = spark
    import s.implicits._
    val frame = Seq(
      TestSlate("empty", "r1", "u1", 1L, 0.0, 0, Seq.empty[TestItem]),
      TestSlate("keep", "r2", "u1", 2L, 1.0, 2,
        Seq(item("a", 1.0, 0.5), item("b", 0.0, 0.2)))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)
    val (wantGroups, wantCounts) = legacyToGroups(frame, cfg)
    sameGroups(groups, wantGroups) shouldBe true
    counts shouldBe wantCounts
    counts.tooSmall shouldBe 1L
  }
}
