package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;

import java.util.StringJoiner;

/**
 * The GRPO policy's feature vector — one definition, read by the serving-side emitter and by the
 * shadow scorer, so a weight vector fit in Scala is never applied to a different layout here.
 *
 * Every value is already computed during scoring; building this vector adds no model call to the
 * request path. The version prefix travels on the wire because weights and features must agree:
 * a consumer that reads an unknown version drops the row rather than misaligning them.
 */
public final class GrpoFeatures {

    public static final int DIM = 10;
    public static final String VERSION = "v1";

    private GrpoFeatures() {}

    public static double[] of(ServedMovie movie, int position, int slateSize) {
        return new double[] {
            1.0,                                              // 0 bias
            movie.banditScore(),                              // 1
            movie.estimatedReward(),                          // 2
            movie.onlineScore(),                              // 3
            movie.explorationBonus(),                         // 4
            movie.coldStart() ? 1.0 : 0.0,                    // 5
            Math.log1p(movie.impressions()),                  // 6
            Math.log1p(movie.clicks()),                       // 7
            slateSize <= 0 ? 0.0 : (double) position / slateSize, // 8
            movie.clicks() / (double) (movie.impressions() + 1) // 9 smoothed CTR
        };
    }

    public static String pack(double[] x) {
        StringJoiner joiner = new StringJoiner(",");
        for (double value : x) {
            joiner.add(Double.toString(value));
        }
        return VERSION + ":" + joiner;
    }

    /** The behavior policy's logit. Falls back to banditScore, which predictionScore derives from. */
    public static double predictionScore(ServedMovie movie) {
        Object raw = movie.modelPredictions().get("predictionScore");
        return raw instanceof Number number ? number.doubleValue() : movie.banditScore();
    }
}
