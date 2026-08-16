package com.demo.event

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventSchemasSpec extends AnyFlatSpec with Matchers {

  /** `from_json` does not enforce `nullable = false` — it emits null and every consumer then
    * filters for it. A schema that claims otherwise describes a guarantee nothing provides. */
  "the joiner schema" should "declare every gated field nullable" in {
    val gated = Set("user_id", "item_id", "event_type", "request_id")
    EventSchemas.joiner.fields
      .filter(field => gated.contains(field.name) && !field.nullable)
      .map(_.name) shouldBe empty
  }

  "the base fields" should "all be nullable" in {
    EventSchemas.baseFields.filterNot(_.nullable).map(_.name) shouldBe empty
  }

  "request_id" should "match the Avro contract, which unions it with null" in {
    EventSchemas.joiner.fields.find(_.name == "request_id").map(_.nullable) shouldBe Some(true)
  }
}
