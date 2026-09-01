package com.demo.event

import java.nio.file.{Files, Paths}

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * The java-retrieval-service module and this one do not share a classpath, so a contract test
 * driving EventParsing.fromJson (as two earlier ones did) proves nothing about whether serving's
 * encoder and this job's decoder actually agree — that is not the production decode path. The only
 * way to prove it is to hand real bytes between the two modules.
 *
 * This fixture was produced by RecsysEventAvroCodec (java-retrieval-service) encoding a
 * representative serving impression event; see RecsysEventFixtureTest.representativeImpressionEvent
 * there for the exact field values. Decoding it here through EventAvroCodec.decode — the real
 * decoder ExecutionEngine.run wires up, not a JSON parser — is what a JSON-vs-Avro mismatch like the
 * one this fixture guards against would fail: it would come back Left(InvalidMarker), the same
 * failure that dead-lettered every event serving published before this fix.
 */
class ServingImpressionFixtureSpec extends AnyFlatSpec with Matchers with OptionValues {

  private val fixturePath = Seq(
    Paths.get("recsys-pipeline/schemas/fixtures/serving-impression-v3.avro"),
    Paths.get("../../schemas/fixtures/serving-impression-v3.avro")
  ).find(Files.exists(_)).value

  "EventAvroCodec" should "decode the java-retrieval-service serving-impression fixture" in {
    val bytes = Files.readAllBytes(fixturePath)

    val decoded = EventAvroCodec.decode(bytes).toOption.value

    decoded.get("request_id").toString shouldBe "req-fixture-1"
    decoded.get("user_id").toString shouldBe "u-fixture-1"
    decoded.get("item_id").toString shouldBe "m-fixture-1"
    decoded.get("event_type").toString shouldBe "impression"
  }
}
