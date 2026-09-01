package com.demo.retrieval.event;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The v3 event schema now lives in three places: the canonical file, the Spark job's bundled
 * copy, and this module's bundled copy. A drifted copy fails exactly the way the JSON-vs-Avro
 * wire-format bug this suite guards against did — as dead letters nobody looks at, since nothing
 * else compares them. Fingerprint equality is the right thing to compare: it is exactly what
 * EventAvroCodec.decode keys its writer-schema lookup on, so two schemas that fingerprint the same
 * are interchangeable for wire purposes even if formatted differently.
 */
class RecsysEventSchemaDriftTest {

    private static final Path CANONICAL_SCHEMA = Path.of("../../schemas/recsys-event-v3.avsc");

    @Test
    void bundledSchemaFingerprintMatchesTheCanonicalSchema() throws IOException {
        Schema canonical;
        try (InputStream input = Files.newInputStream(CANONICAL_SCHEMA)) {
            canonical = new Schema.Parser().parse(input);
        }

        RecsysEventAvroCodec codec = new RecsysEventAvroCodec();

        assertEquals(SchemaNormalization.parsingFingerprint64(canonical), codec.fingerprint());
    }
}
