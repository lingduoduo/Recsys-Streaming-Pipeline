package com.demo.retrieval.service.grpo;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
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
 *
 * Constructed directly by MovieLensServingSideEffects' owner rather than injected, matching
 * GrpoEventPublisher: both are serving-path side effects, not collaborators anything else
 * resolves from the context.
 */
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
        return dot(weights.get(), movie, position, slateSize);
    }

    private static double dot(double[] w, ServedMovie movie, int position, int slateSize) {
        double[] x = GrpoFeatures.of(movie, position, slateSize);
        double sum = 0.0;
        for (int i = 0; i < GrpoFeatures.DIM; i++) {
            sum += w[i] * x[i];
        }
        return sum;
    }

    /**
     * Logs one line per slate describing how the GRPO policy would have ordered it.
     *
     * Called from the serving side effects because that is the only place a slate position exists:
     * feature 8 is position/slateSize, and modelPredictions — the obvious alternative home for the
     * score — is built before selection assigns positions. Putting grpoScore there would train on a
     * position the serving side never had, so the score is logged and deliberately not carried.
     *
     * Rank agreement is reported as pairwise concordance: over every ordered pair (i &lt; j) of
     * served positions, the fraction the GRPO score ranks the same way serving did. Top-N overlap
     * was the alternative; concordance wins because it uses the whole slate and moves continuously,
     * so the spec's flip criterion reads a trend rather than a step function whose value depends on
     * a chosen N and collapses on slates shorter than it.
     *
     * Shadow only. In `on` mode the score already steers the order it would be compared against, so
     * the measure would grade the policy on its own output; in `off` mode nothing runs at all — not
     * even the Redis read.
     */
    public void recordShadowSlate(ServingSideEffectRequest request) {
        if (!MODE_SHADOW.equals(mode)) {
            return;
        }
        try {
            List<ServedMovie> served = request.selected();
            int slateSize = served.size();
            // A single-item slate has no pair to be concordant on; concordance would always read
            // 0.0 and look like total disagreement.
            if (slateSize < 2) {
                return;
            }
            // One Redis read for the whole slate, not one per item.
            Optional<double[]> weights = readWeights();
            if (weights.isEmpty()) {
                return;
            }
            double[] w = weights.get();
            double[] scores = new double[slateSize];
            for (int i = 0; i < slateSize; i++) {
                scores[i] = dot(w, served.get(i), i, slateSize);
            }
            log.info("GRPO shadow slate requestId={} slateSize={} pairwiseConcordance={}",
                request.requestId(), slateSize, pairwiseConcordance(scores));
        } catch (Exception e) {
            // Same contract as GrpoEventPublisher.publish: shadow observability must never
            // turn into a failed recommendation.
            log.warn("Failed to log GRPO shadow slate for request {}", request.requestId(), e);
        }
    }

    /**
     * Fraction of served position pairs the scores order the same way, in [0, 1].
     *
     * A tie counts as disagreement: the served order committed to one of them, and a policy that
     * cannot separate two items did not reproduce that decision.
     */
    static double pairwiseConcordance(double[] scores) {
        long pairs = 0L;
        long concordant = 0L;
        for (int i = 0; i < scores.length; i++) {
            for (int j = i + 1; j < scores.length; j++) {
                pairs++;
                if (scores[i] > scores[j]) {
                    concordant++;
                }
            }
        }
        return pairs == 0L ? 0.0 : (double) concordant / pairs;
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
                // Double.parseDouble accepts "NaN" and "Infinity" — a diverged training run would
                // otherwise be read back as a valid vector and every score would come out NaN,
                // which sorts unpredictably instead of degrading to the no-weights case.
                if (!Double.isFinite(w[i])) {
                    log.warn("GRPO weight {} is not finite ('{}') — ignoring the weight vector",
                        i, parts[i]);
                    return Optional.empty();
                }
            }
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return Optional.of(w);
    }
}
