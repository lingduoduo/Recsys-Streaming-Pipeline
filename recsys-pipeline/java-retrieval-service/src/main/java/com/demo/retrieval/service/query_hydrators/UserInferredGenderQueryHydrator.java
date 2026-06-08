package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;

import org.springframework.stereotype.Component;
import java.util.Optional;

/**
 * Hydrates inferredGender and inferredGenderScore.
 *
 * Mirrors the Rust UserInferredGenderQueryHydrator: try the store first (mh_client);
 * if absent and the user is new (ratingCount == 0), fall back to the gRPC prediction
 * client. In MovieLens there is no gRPC model, so the fallback uses the self-reported
 * demographics gender (written by MovieLensContextCollectorStreamingJob) with score 1.0.
 */
@Component
public class UserInferredGenderQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public UserInferredGenderQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        Optional<MovieLensUserFeatures> features = featureClient.getUserFeatures(userId);

        String inferredGender = features.map(MovieLensUserFeatures::inferredGender).orElse(null);
        Double inferredGenderScore = features.map(MovieLensUserFeatures::inferredGenderScore).orElse(null);

        if (inferredGender == null) {
            boolean isNewUser = features.map(f -> f.ratingCount() == 0).orElse(true);
            if (isNewUser) {
                String demoGender = features.map(f -> f.demographics().gender()).orElse("");
                if (!demoGender.isBlank()) {
                    inferredGender = demoGender;
                    inferredGenderScore = 1.0;
                }
            }
        }

        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withInferredGender(inferredGender, inferredGenderScore),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withInferredGender(
                hydrated.userFeatures().inferredGender(),
                hydrated.userFeatures().inferredGenderScore()
            ),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
