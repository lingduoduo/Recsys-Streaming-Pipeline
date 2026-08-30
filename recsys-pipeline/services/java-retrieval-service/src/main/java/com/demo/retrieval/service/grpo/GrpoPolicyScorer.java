package com.demo.retrieval.service.grpo;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Serves the GRPO policy score, off by default.
 *
 *   off    — not computed, no Redis read
 *   shadow — computed and logged at blend weight 0.0
 *   on     — claims part of the blend weight MovieLensOutcomeScorer leaves unclaimed
 *
 * Mirrors recsys.sequence.behavior-mode so operators meet a rollout shape they already know.
 * Every failure path scores 0.0: a missing, stale or wrongly versioned weight vector must leave
 * recommendations exactly as they were, never fail a request.
 */
@Service
public class GrpoPolicyScorer {

    private static final Logger log = LoggerFactory.getLogger(GrpoPolicyScorer.class);

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";
    public static final String WEIGHTS_KEY = "grpo:policy:weights";

    /**
     * MovieLensOutcomeScorer's exploitation weights sum to 0.85, not 1.0, because a fourth term
     * was always 0.0 at runtime, and it documents that the remainder is deliberately NOT
     * renormalized. Taking 0.10 of the unclaimed 0.15 means no existing weight changes and no
     * existing score moves when GRPO is switched on.
     */
    public static final double ON_BLEND_WEIGHT = 0.10;

    private final StringRedisTemplate redis;
    private final String mode;

    public GrpoPolicyScorer(StringRedisTemplate redis, RecommendationProperties properties) {
        this.redis = redis;
        String configured = properties.getGrpo().getMode();
        String normalized = configured == null ? MODE_OFF : configured.trim().toLowerCase(Locale.ROOT);
        if (!MODE_OFF.equals(normalized) && !MODE_SHADOW.equals(normalized) && !MODE_ON.equals(normalized)) {
            log.warn("Unrecognized recsys.grpo.mode '{}', treating as '{}'", configured, MODE_OFF);
            normalized = MODE_OFF;
        }
        this.mode = normalized;
    }

    public boolean enabled() {
        return !MODE_OFF.equals(mode);
    }

    public double blendWeight() {
        return MODE_ON.equals(mode) ? ON_BLEND_WEIGHT : 0.0;
    }

    public double score(ServedMovie movie, int position, int slateSize) {
        if (!enabled()) {
            return 0.0;
        }
        Optional<double[]> weights = readWeights();
        if (weights.isEmpty()) {
            return 0.0;
        }
        double[] w = weights.get();
        double[] x = GrpoFeatures.of(movie, position, slateSize);
        double sum = 0.0;
        for (int i = 0; i < GrpoFeatures.DIM; i++) {
            sum += w[i] * x[i];
        }
        return sum;
    }

    private Optional<double[]> readWeights() {
        Map<Object, Object> raw = redis.opsForHash().entries(WEIGHTS_KEY);
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        Object version = raw.get("feature_version");
        if (version == null || !GrpoFeatures.VERSION.equals(version.toString())) {
            log.warn("GRPO weights carry feature_version '{}', expected '{}' — ignoring",
                version, GrpoFeatures.VERSION);
            return Optional.empty();
        }
        Object packed = raw.get("weights");
        if (packed == null) {
            return Optional.empty();
        }
        String[] parts = packed.toString().split(",");
        if (parts.length != GrpoFeatures.DIM) {
            return Optional.empty();
        }
        double[] w = new double[GrpoFeatures.DIM];
        try {
            for (int i = 0; i < parts.length; i++) {
                w[i] = Double.parseDouble(parts[i]);
            }
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return Optional.of(w);
    }
}
