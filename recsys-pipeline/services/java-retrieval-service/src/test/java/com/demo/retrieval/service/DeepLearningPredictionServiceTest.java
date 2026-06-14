package com.demo.retrieval.service;

import com.demo.retrieval.model.ModelPrediction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepLearningPredictionServiceTest {

    @Test
    void loadsClasspathArtifactsAndPredictsWithStringLookups() throws Exception {
        DeepLearningPredictionService service = new DeepLearningPredictionService(new ObjectMapper());
        try {
            Optional<ModelPrediction> prediction = service.predict("user_employee_01", "action_benefits");

            assertTrue(prediction.isPresent());
            assertEquals("mlp_embedding", prediction.get().model());
            assertEquals(0L, prediction.get().userId());
            assertEquals(0L, prediction.get().itemId());
            assertTrue(prediction.get().score() >= 0.0);
            assertTrue(prediction.get().score() <= 1.0);
            assertEquals(32, service.metadata().get("users"));
            assertEquals(12, service.metadata().get("items"));
        } finally {
            service.close();
        }
    }

    @Test
    void returnsEmptyForUnknownLookupIds() throws Exception {
        DeepLearningPredictionService service = new DeepLearningPredictionService(new ObjectMapper());
        try {
            assertFalse(service.predict("unknown_user", "action_benefits").isPresent());
            assertFalse(service.predict("user_employee_01", "unknown_item").isPresent());
        } finally {
            service.close();
        }
    }
}
