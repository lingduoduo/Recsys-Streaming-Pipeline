package com.demo.retrieval.service;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OnnxValue;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.LongBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class DeepLearningPredictionService {
    private static final String MODEL_NAME = "mlp_embedding";
    private static final String MODEL_RESOURCE = "mlp_embedding_model.onnx";
    private static final String LOOKUP_RESOURCE = "mlp_embedding_lookups.json";
    private static final String USER_INPUT = "user_ids";
    private static final String ITEM_INPUT = "item_ids";

    private final OrtEnvironment environment;
    private final OrtSession session;
    private final Map<String, Long> userLookup;
    private final Map<String, Long> itemLookup;

    public DeepLearningPredictionService(ObjectMapper objectMapper) {
        try {
            this.environment = OrtEnvironment.getEnvironment();
            this.session = environment.createSession(new ClassPathResource(MODEL_RESOURCE).getContentAsByteArray());
            LookupTables lookups = readLookups(objectMapper);
            this.userLookup = lookups.userLookup();
            this.itemLookup = lookups.itemLookup();
        } catch (IOException | OrtException e) {
            throw new IllegalStateException("Failed to load deep learning prediction artifacts", e);
        }
    }

    public Optional<ModelPrediction> predict(String user, String item) {
        Long userId = userLookup.get(user);
        Long itemId = itemLookup.get(item);
        if (userId == null || itemId == null) {
            return Optional.empty();
        }
        return Optional.of(predict(user, item, userId, itemId));
    }

    public ModelPrediction predict(long userId, long itemId) {
        return predict(null, null, userId, itemId);
    }

    public Map<String, Object> metadata() {
        return Map.of(
            "model", MODEL_NAME,
            "users", userLookup.size(),
            "items", itemLookup.size(),
            "inputs", session.getInputNames(),
            "outputs", session.getOutputNames()
        );
    }

    @PreDestroy
    void close() throws OrtException {
        session.close();
    }

    private ModelPrediction predict(String user, String item, long userId, long itemId) {
        try (
            OnnxTensor userTensor = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{userId}), new long[]{1});
            OnnxTensor itemTensor = OnnxTensor.createTensor(environment, LongBuffer.wrap(new long[]{itemId}), new long[]{1});
            OrtSession.Result result = session.run(Map.of(USER_INPUT, userTensor, ITEM_INPUT, itemTensor))
        ) {
            double score = readScore(result.get(0));
            return new ModelPrediction(user, item, userId, itemId, score, MODEL_NAME);
        } catch (OrtException e) {
            throw new IllegalStateException("Failed to run deep learning prediction", e);
        }
    }

    private double readScore(OnnxValue value) throws OrtException {
        Object raw = value.getValue();
        if (raw instanceof float[][] scores && scores.length > 0 && scores[0].length > 0) {
            return scores[0][0];
        }
        if (raw instanceof float[] scores && scores.length > 0) {
            return scores[0];
        }
        if (raw instanceof double[][] scores && scores.length > 0 && scores[0].length > 0) {
            return scores[0][0];
        }
        if (raw instanceof double[] scores && scores.length > 0) {
            return scores[0];
        }
        throw new IllegalStateException("Unsupported prediction output shape: " + raw.getClass().getName());
    }

    private LookupTables readLookups(ObjectMapper objectMapper) throws IOException {
        Map<String, Map<String, Long>> raw = objectMapper.readValue(
            new ClassPathResource(LOOKUP_RESOURCE).getInputStream(),
            new TypeReference<>() {
            }
        );
        return new LookupTables(
            Map.copyOf(raw.getOrDefault("user_lookup", new LinkedHashMap<>())),
            Map.copyOf(raw.getOrDefault("item_lookup", new LinkedHashMap<>()))
        );
    }

    private record LookupTables(Map<String, Long> userLookup, Map<String, Long> itemLookup) {
    }
}
