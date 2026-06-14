package com.demo.retrieval.service;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.FloatBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class TwoTowerPredictionService {
    private static final Logger log = LoggerFactory.getLogger(TwoTowerPredictionService.class);

    private static final String USER_TOWER_ENV = "ONNX_USER_TOWER_PATH";
    private static final String ITEM_TOWER_ENV = "ONNX_ITEM_TOWER_PATH";
    private static final String RANKING_ENV    = "ONNX_RANKING_PATH";

    private static final float W_CLICK    = 0.35f;
    private static final float W_RATING   = 0.25f;
    private static final float W_FAVORITE = 0.20f;
    private static final float W_REWATCH  = 0.12f;
    private static final float W_DWELL    = 0.08f;

    private final OrtEnvironment env;
    private final OrtSession userTower;
    private final OrtSession itemTower;
    private final OrtSession ranking;
    private final Map<String, Long> userLookup;
    private final Map<String, Long> itemLookup;
    private final boolean enabled;

    private volatile float[][] itemEmbeddings;

    public TwoTowerPredictionService() {
        this(new ObjectMapper());
    }

    public TwoTowerPredictionService(ObjectMapper objectMapper) {
        String userTowerPath = System.getenv(USER_TOWER_ENV);
        String itemTowerPath = System.getenv(ITEM_TOWER_ENV);
        String rankingPath   = System.getenv(RANKING_ENV);

        if (userTowerPath == null || itemTowerPath == null || rankingPath == null) {
            log.info("TwoTowerPredictionService disabled: set {}, {}, {} to enable",
                USER_TOWER_ENV, ITEM_TOWER_ENV, RANKING_ENV);
            this.env = null;
            this.userTower = null;
            this.itemTower = null;
            this.ranking = null;
            this.userLookup = Map.of();
            this.itemLookup = Map.of();
            this.enabled = false;
            return;
        }

        try {
            this.env = OrtEnvironment.getEnvironment();
            OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            this.userTower = env.createSession(Files.readAllBytes(Path.of(userTowerPath)), opts);
            this.itemTower = env.createSession(Files.readAllBytes(Path.of(itemTowerPath)), opts);
            this.ranking   = env.createSession(Files.readAllBytes(Path.of(rankingPath)),   opts);

            Path lookupPath = Path.of(userTowerPath).resolveSibling("movielens_lookups.json");
            Map<String, Map<String, Long>> raw =
                objectMapper.readValue(lookupPath.toFile(), new TypeReference<>() {});
            this.userLookup = Map.copyOf(raw.getOrDefault("user_lookup", new LinkedHashMap<>()));
            this.itemLookup = Map.copyOf(raw.getOrDefault("item_lookup", new LinkedHashMap<>()));
            this.enabled = true;
            log.info("TwoTowerPredictionService loaded: {} users, {} items",
                userLookup.size(), itemLookup.size());
        } catch (IOException | OrtException e) {
            throw new IllegalStateException("Failed to load two-tower ONNX models", e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    public Map<String, Double> predictBatch(String user, List<String> items) {
        if (!enabled || items.isEmpty()) return Map.of();
        Long userIdx = userLookup.get(user);
        if (userIdx == null) return Map.of();

        List<String> known = items.stream().filter(itemLookup::containsKey).toList();
        if (known.isEmpty()) return Map.of();

        try {
            float[] userEmb = computeUserEmbedding(userIdx);
            float[][] allItemEmbs = getItemEmbeddings();
            int n = known.size();
            int embDim = userEmb.length;

            float[] candidateFlat = new float[n * embDim];
            for (int i = 0; i < n; i++) {
                long itemIdx = itemLookup.get(known.get(i));
                System.arraycopy(allItemEmbs[(int) itemIdx], 0, candidateFlat, i * embDim, embDim);
            }

            float[] mask = buildIsolationMask(n);
            int S = n + 1;

            try (
                OnnxTensor userTensor  = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(userEmb), new long[]{1, embDim});
                OnnxTensor candTensor  = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(candidateFlat), new long[]{n, embDim});
                OnnxTensor maskTensor  = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(mask), new long[]{S, S});
                OrtSession.Result result = ranking.run(Map.of(
                    "user_emb", userTensor,
                    "candidate_embs", candTensor,
                    "attn_mask", maskTensor
                ))
            ) {
                float[] click    = readFloats(result.get(0).getValue());
                float[] rating   = readFloats(result.get(1).getValue());
                float[] favorite = readFloats(result.get(2).getValue());
                float[] rewatch  = readFloats(result.get(3).getValue());
                float[] dwell    = readFloats(result.get(4).getValue());

                Map<String, Double> scores = new HashMap<>(n * 4 / 3 + 1);
                for (int i = 0; i < n; i++) {
                    double score = W_CLICK * click[i] + W_RATING * rating[i]
                        + W_FAVORITE * favorite[i] + W_REWATCH * rewatch[i]
                        + W_DWELL * dwell[i];
                    scores.put(known.get(i), score);
                }
                return Map.copyOf(scores);
            }
        } catch (OrtException e) {
            log.warn("Two-tower prediction failed for user {}: {}", user, e.getMessage());
            return Map.of();
        }
    }

    @PreDestroy
    void close() {
        if (!enabled) return;
        try { userTower.close(); } catch (OrtException ignored) {}
        try { itemTower.close(); } catch (OrtException ignored) {}
        try { ranking.close(); }   catch (OrtException ignored) {}
    }

    private float[] computeUserEmbedding(long userIdx) throws OrtException {
        try (
            OnnxTensor t = OnnxTensor.createTensor(env,
                LongBuffer.wrap(new long[]{userIdx}), new long[]{1});
            OrtSession.Result r = userTower.run(Map.of("user_id", t))
        ) {
            float[][] raw = (float[][]) r.get(0).getValue();
            return raw[0];
        }
    }

    private float[][] getItemEmbeddings() throws OrtException {
        if (itemEmbeddings != null) return itemEmbeddings;
        synchronized (this) {
            if (itemEmbeddings != null) return itemEmbeddings;
            int n = itemLookup.size();
            long[] iids = new long[n];
            for (long idx = 0; idx < n; idx++) iids[(int) idx] = idx;
            float[] genreFeats = new float[n * 15];

            try (
                OnnxTensor idTensor  = OnnxTensor.createTensor(env,
                    LongBuffer.wrap(iids), new long[]{n});
                OnnxTensor genreTensor = OnnxTensor.createTensor(env,
                    FloatBuffer.wrap(genreFeats), new long[]{n, 15});
                OrtSession.Result r = itemTower.run(Map.of("movie_id", idTensor, "genre_feat", genreTensor))
            ) {
                itemEmbeddings = (float[][]) r.get(0).getValue();
            }
            return itemEmbeddings;
        }
    }

    private float[] buildIsolationMask(int k) {
        int S = k + 1;
        float[] mask = new float[S * S];
        Arrays.fill(mask, Float.NEGATIVE_INFINITY);
        for (int j = 0; j < S; j++) mask[j] = 0.0f;
        for (int i = 0; i < S; i++) mask[i * S] = 0.0f;
        for (int i = 0; i < S; i++) mask[i * S + i] = 0.0f;
        return mask;
    }

    private float[] readFloats(Object value) {
        if (value instanceof float[][] v) {
            float[] out = new float[v.length];
            for (int i = 0; i < v.length; i++) out[i] = v[i][0];
            return out;
        }
        if (value instanceof float[] v) return v;
        throw new IllegalStateException("Unexpected ONNX output type: " + value.getClass());
    }
}
