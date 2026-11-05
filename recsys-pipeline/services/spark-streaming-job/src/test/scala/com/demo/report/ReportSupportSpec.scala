package com.demo.report

import com.demo.SparkTestSupport
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import org.scalatest.funsuite.AnyFunSuite

class ReportSupportSpec extends AnyFunSuite with SparkTestSupport {
  test("unreachable Redis preserves unavailable optional inputs") {
    val socket = new java.net.ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    assert(ReportSupport.catalog("127.0.0.1", port).isEmpty)
    assert(ReportSupport.popularity("127.0.0.1", port).isEmpty)
    assert(ReportSupport.embeddings("127.0.0.1", port, "i2vEmb").isEmpty)
    assert(ReportSupport.replay("127.0.0.1", port).isEmpty)
  }

  test("nested JSON round trips with strict nulls for non-finite values") {
    val file = Files.createTempDirectory("report-json").resolve("nested/data.json")
    ReportSupport.writeJson(file.toString, Map("items" -> Seq(Map("x" -> 2.5)),
      "missing" -> Double.NaN, "infinity" -> Double.PositiveInfinity))
    val result = ReportSupport.obj(ReportSupport.readJson(file.toString))
    assert(result("missing") == null)
    assert(result("infinity") == null)
    assert(ReportSupport.number(ReportSupport.obj(ReportSupport.seq(result("items")).head)("x")) == 2.5)
    assert(ReportSupport.parseJson("{\"ok\":true}") == Map("ok" -> true))
  }

  test("Parquet nested rows maps arrays and timestamps become JSON-compatible values") {
    val session = spark
    import session.implicits._
    val file = Files.createTempDirectory("report-parquet").resolve("samples")
    Seq(("u1", Seq("Drama", "Comedy"), Map("score" -> 0.7),
      java.sql.Timestamp.valueOf("2026-01-02 03:04:05")))
      .toDF("user_id", "genres", "modelPredictions", "event_time").write.parquet(file.toString)
    val row = ReportSupport.records(spark, file.toString).head
    assert(row("genres") == Seq("Drama", "Comedy"))
    assert(ReportSupport.obj(row("modelPredictions"))("score") == 0.7)
    assert(row("event_time").isInstanceOf[String])
    assert(ReportSupport.parseJson(ReportSupport.json(row)) == row)
  }

  test("CSV writes the requested columns, quotes special characters and leaves nulls empty") {
    val file = Files.createTempFile("report-csv", ".csv")
    ReportSupport.writeCsv(file.toString, Seq("name", "metric", "absent"),
      Seq(Map("name" -> "a,\"b\"\nnext", "metric" -> 0.5, "absent" -> null)))
    assert(new String(Files.readAllBytes(file), UTF_8) ==
      "name,metric,absent\n\"a,\"\"b\"\"\nnext\",0.5,\n")
  }

  test("command options reject missing values duplicates and unknown arguments") {
    assert(ReportSupport.options(Array("--input", "x", "--ks", "5,10")) ==
      Map("input" -> "x", "ks" -> "5,10"))
    assert(ReportSupport.options(Array("--help")) == Map("help" -> "true"))
    intercept[IllegalArgumentException](ReportSupport.options(Array("--input")))
    intercept[IllegalArgumentException](ReportSupport.options(Array("--input", "x", "--input", "y")))
    intercept[IllegalArgumentException](ReportSupport.options(Array("--oops", "x"), Set("input")))
    intercept[IllegalArgumentException](ReportSupport.options(Array("positional")))
    assert(ReportSupport.options(Array("--limit", "-1"))("limit") == "-1")
  }
}
