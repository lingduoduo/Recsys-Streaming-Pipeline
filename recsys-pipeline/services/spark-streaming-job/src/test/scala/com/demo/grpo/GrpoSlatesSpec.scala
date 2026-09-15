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

  it should "filter the dropped slates inside the collect query, not on the driver" in {
    val s = spark
    import s.implicits._
    // The one property this change exists to deliver, and the only one invisible in every output:
    // GateCounts and the groups are identical whether the gate runs in Spark or on the driver
    // after collecting everything. An earlier version of this test asserted on a SEPARATELY built
    // `tagged(frame, cfg)` frame and claimed it was "the frame toGroups collects from" -- it was
    // not, nothing tied them, and a mutant that collected all 100 rows and filtered on the driver
    // passed the whole suite. Only the executed plan of the query toGroups actually runs
    // distinguishes them.
    val keptCount = 5
    val droppedCount = 95
    val frame = ((0 until keptCount).map { i =>
      TestSlate(s"k$i", s"r$i", "u1", i.toLong, 1.0, 2,
        Seq(item("a", 1.0, 0.5), item("b", 0.0, 0.2)))
    } ++ (0 until droppedCount).map { i =>
      TestSlate(s"d$i", s"rd$i", "u1", i.toLong, 0.0, 2,
        Seq(item("a", 0.0, 0.5), item("b", 0.0, 0.2)))
    }).toDF()

    val plans = scala.collection.mutable.ArrayBuffer[String]()
    val listener = new org.apache.spark.sql.util.QueryExecutionListener {
      override def onSuccess(name: String, qe: org.apache.spark.sql.execution.QueryExecution,
                             durationNs: Long): Unit =
        plans.synchronized { plans += qe.executedPlan.toString }
      override def onFailure(name: String, qe: org.apache.spark.sql.execution.QueryExecution,
                             exception: Exception): Unit = ()
    }
    def sawGate: Boolean =
      plans.synchronized(plans.exists(_.toLowerCase.contains("isnull(grpo_drop_reason")))
    s.listenerManager.register(listener)
    val (groups, counts) = try {
      val result = GrpoSlates.toGroups(frame, cfg)
      // QueryExecutionListener fires asynchronously and SparkContext#listenerBus is private, so
      // poll for the callback rather than reach into it.
      val deadline = System.currentTimeMillis() + 10000
      while (System.currentTimeMillis() < deadline && !sawGate) Thread.sleep(50)
      result
    } finally s.listenerManager.unregister(listener)

    groups should have size keptCount
    counts.kept shouldBe keptCount.toLong
    counts.zeroVariance shouldBe droppedCount.toLong

    val collectPlans = plans.synchronized(plans.toList)
    // The gate predicate must appear in a plan Spark executed -- that is what proves the dropped
    // slates never crossed to the driver.
    withClue(s"executed plans:\n${collectPlans.mkString("\n---\n")}\n") {
      collectPlans.exists(_.toLowerCase.contains("isnull(grpo_drop_reason")) shouldBe true
      // And the persist must be doing its job: without it the gate and the parse run twice, once
      // per action. Removing the persist is otherwise undetectable.
      collectPlans.exists(_.contains("InMemoryTableScan")) shouldBe true
    }
  }

  it should "keep the input order of surviving slates across several partitions" in {
    val s = spark
    // The order assertion elsewhere runs on a single partition, where nothing can reorder. This
    // one spreads the slates so the claim that no shuffle is introduced has something to bite on.
    val rows = (0 until 40).map { i =>
      org.apache.spark.sql.Row(s"s$i", s"r$i", "u1", i.toLong, 1.0, 2,
        Seq(org.apache.spark.sql.Row("a", 1.0, Map("grpo_x" -> vec(0.5),
              "prediction_score" -> "0.7")),
            org.apache.spark.sql.Row("b", 0.0, Map("grpo_x" -> vec(0.2),
              "prediction_score" -> "0.1"))))
    }
    val itemType = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("label", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField("item_features",
        org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType,
                                           org.apache.spark.sql.types.StringType))))
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("slate_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("request_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("user_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("request_ts", org.apache.spark.sql.types.LongType),
      org.apache.spark.sql.types.StructField("slate_reward",
        org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField("slate_size",
        org.apache.spark.sql.types.IntegerType),
      org.apache.spark.sql.types.StructField("items",
        org.apache.spark.sql.types.ArrayType(itemType))))
    val frame = s.createDataFrame(s.sparkContext.parallelize(rows, 4), schema)

    val (groups, _) = GrpoSlates.toGroups(frame, cfg)

    groups.map(_.slateId) shouldBe (0 until 40).map(i => s"s$i")
  }

  it should "drop a slate whose item_features map is absent rather than crashing on it" in {
    val s = spark
    // A second behavior change on this branch, and it needs saying: master's classify called
    // getOrElse straight on the null map and threw NPE. featuresOf now returns an empty map, so
    // the slate fails the feature-version gate like any other unusable vector. Only reachable
    // because "no grpo_x" and "no features map" coincide today -- if the version gate ever
    // defaulted a missing vector, buildGroup would be the one reading the map.
    val itemType = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("label", org.apache.spark.sql.types.DoubleType),
      org.apache.spark.sql.types.StructField("item_features",
        org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType,
                                           org.apache.spark.sql.types.StringType),
        nullable = true)))
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("slate_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("items",
        org.apache.spark.sql.types.ArrayType(itemType))))
    val frame = s.createDataFrame(s.sparkContext.parallelize(Seq(
      org.apache.spark.sql.Row("nullmap", Seq(
        org.apache.spark.sql.Row("a", 1.0, null),
        org.apache.spark.sql.Row("b", 0.0, null)))), 1), schema)

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)

    groups shouldBe empty
    counts.badFeatureVersion shouldBe 1L
  }

  it should "read a null label as a zero reward" in {
    val s = spark
    // TestItem.label is a Double and cannot express null, so this needs a Row-built fixture. It
    // also exercises fieldIndex("label") resolving inside the UDF, which only works because
    // Spark hands an ArrayType(StructType) back as a schema-carrying Row.
    val itemType = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("item_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("label", org.apache.spark.sql.types.DoubleType,
        nullable = true),
      org.apache.spark.sql.types.StructField("item_features",
        org.apache.spark.sql.types.MapType(org.apache.spark.sql.types.StringType,
                                           org.apache.spark.sql.types.StringType))))
    val schema = org.apache.spark.sql.types.StructType(Seq(
      org.apache.spark.sql.types.StructField("slate_id", org.apache.spark.sql.types.StringType),
      org.apache.spark.sql.types.StructField("items",
        org.apache.spark.sql.types.ArrayType(itemType))))
    val features = Map("grpo_x" -> vec(0.5), "prediction_score" -> "0.7")
    val frame = s.createDataFrame(s.sparkContext.parallelize(Seq(
      org.apache.spark.sql.Row("nulllabel", Seq(
        org.apache.spark.sql.Row("a", 1.0, features),
        org.apache.spark.sql.Row("b", null, features)))), 1), schema)

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)

    counts.kept shouldBe 1L
    groups.head.rewards shouldBe Array(1.0, 0.0)
  }

  it should "treat a slate whose items array is absent as too small" in {
    val s = spark
    import s.implicits._
    // `items` is nullable in SlateSchema and from_json yields null for a missing field, so a
    // malformed slate off Kafka can reach here with no array at all. dropReason guards for it.
    val frame = Seq(
      TestSlate("nullitems", "r1", "u1", 1L, 0.0, 0, null),
      TestSlate("keep", "r2", "u1", 2L, 1.0, 2,
        Seq(item("a", 1.0, 0.5), item("b", 0.0, 0.2)))
    ).toDF()

    val (groups, counts) = GrpoSlates.toGroups(frame, cfg)

    groups.map(_.slateId) shouldBe Seq("keep")
    counts.kept shouldBe 1L
    counts.tooSmall shouldBe 1L
  }

  it should "gate an absent items array where the collect-then-gate path crashed on it" in {
    val s = spark
    import s.implicits._
    // Characterizes a behavior change rather than an equivalence: the old path did
    // `row.getSeq[Row](1).size`, and getSeq returns null for a null field, so it threw NPE on a
    // slate the new path simply drops. Asserted so the difference is on the record and so that
    // anyone restoring the old shape sees what they are restoring.
    val frame = Seq(TestSlate("nullitems", "r1", "u1", 1L, 0.0, 0, null)).toDF()

    a[NullPointerException] should be thrownBy legacyToGroups(frame, cfg)
    GrpoSlates.toGroups(frame, cfg)._2.tooSmall shouldBe 1L
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
