package com.demo.report

import com.demo.SparkTestSupport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SessionReportJobSpec extends AnyFlatSpec with Matchers with SparkTestSupport {

  /** Mirrors the old integration fixture:
    *   session sA (u1) spans 2 slates: req_1 (2 impr, 1 click) + req_2 (2 impr, 1 click) → ctr 0.5
    *   session sB (u2): 1 slate req_3 (2 impr, 0 click) → ctr 0.0
    * Plus a session-less row that must be dropped. */
  private def fixture = {
    val s = spark; import s.implicits._
    Seq(
      ("sA", "u1", "req_1", 1, 0), ("sA", "u1", "req_1", 0, 0),
      ("sA", "u1", "req_2", 1, 0), ("sA", "u1", "req_2", 0, 0),
      ("sB", "u2", "req_3", 0, 0), ("sB", "u2", "req_3", 0, 0),
      ("", "u3", "req_4", 1, 0) // no session → dropped
    ).toDF("session_id", "user_id", "request_id", "clicked", "ordered")
  }

  "perSession" should "aggregate per (session, user) and drop session-less rows" in {
    val rows = SessionReportJob.perSession(fixture)
      .collect().map(r => r.getAs[String]("session_id") -> r).toMap

    rows.keySet shouldBe Set("sA", "sB")
    val sA = rows("sA")
    sA.getAs[Long]("slates") shouldBe 2L
    sA.getAs[Long]("impressions") shouldBe 4L
    sA.getAs[Long]("clicks") shouldBe 2L
    sA.getAs[Double]("ctr") shouldBe 0.5
    rows("sB").getAs[Long]("slates") shouldBe 1L
    rows("sB").getAs[Double]("ctr") shouldBe 0.0
  }

  "summary" should "roll up sessions/users and the per-session ratios" in {
    val s = SessionReportJob.summary(SessionReportJob.perSession(fixture))
    s.sessions shouldBe 2L
    s.users shouldBe 2L
    s.sessionsPerUser shouldBe 1.0
    s.slatesPerSession shouldBe 1.5          // (2 + 1) / 2
    s.impressionsPerSession shouldBe 3.0     // (4 + 2) / 2
    s.clicksPerSession shouldBe 1.0          // (2 + 0) / 2
    s.meanSessionCtr shouldBe 0.25           // mean(0.5, 0.0)
  }

  it should "return zeros for an empty session frame" in {
    val s = spark; import s.implicits._
    val empty = Seq.empty[(String, String, String, Int, Int)]
      .toDF("session_id", "user_id", "request_id", "clicked", "ordered")
    val out = SessionReportJob.summary(SessionReportJob.perSession(empty))
    out.sessions shouldBe 0L
    out.sessionsPerUser shouldBe 0.0
    out.meanSessionCtr shouldBe 0.0
  }
}
