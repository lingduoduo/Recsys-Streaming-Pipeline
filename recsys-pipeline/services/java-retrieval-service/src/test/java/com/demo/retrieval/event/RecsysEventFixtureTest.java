package com.demo.retrieval.event;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * The Java retrieval-service module and spark-streaming-job do not share a classpath, so the only
 * way to prove serving's encoder and the joiner's decoder agree is to hand real bytes between
 * them. This test encodes a representative serving impression event and checks it against a
 * fixture committed at recsys-pipeline/schemas/fixtures/serving-impression-v3.avro; a companion
 * Scala test in spark-streaming-job decodes that same fixture through the real production decode
 * path, EventAvroCodec.decode. Drift in this encoder — the same class of bug that shipped raw JSON
 * to an Avro decoder — fails this comparison instead of silently dead-lettering in production.
 */
class RecsysEventFixtureTest {

    private static final Path FIXTURE = Path.of("../../schemas/fixtures/serving-impression-v3.avro");

    /** Field values here must match the companion Scala test's assertions exactly. */
    static Map<String, Object> representativeImpressionEvent() {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", "e-fixture-1");
        event.put("request_id", "req-fixture-1");
        event.put("session_id", "sess-fixture-1");
        event.put("user_id", "u-fixture-1");
        event.put("item_id", "m-fixture-1");
        event.put("event_type", "impression");
        event.put("timestamp_ms", 1735689600000L);
        event.put("position", 0);
        event.put("user_features", Map.of("algorithm", "hybrid"));
        Map<String, String> itemFeatures = new LinkedHashMap<>();
        itemFeatures.put("prediction_score", "0.83");
        // Frozen codec fixture, not a live GrpoFeatures sample: grpo_x is an opaque
        // map<string,string> value to this byte-identity check, and changing this v1/10-wide
        // string would require regenerating the committed .avro fixture below for no encoding
        // benefit -- GrpoFeatures has since moved to v2/9-wide.
        itemFeatures.put("grpo_x", "v1:1.0,0.4,0.3,0.05,0.7,0.0,0.0,0.0,0.0,0.3");
        event.put("item_features", itemFeatures);
        event.put("context_features", Map.of());
        return event;
    }

    @Test
    void encodingIsByteIdenticalToTheCommittedFixture() throws IOException {
        byte[] encoded = new RecsysEventAvroCodec().encode(representativeImpressionEvent());
        byte[] expected = Files.readAllBytes(FIXTURE);

        assertArrayEquals(expected, encoded);
    }
}
