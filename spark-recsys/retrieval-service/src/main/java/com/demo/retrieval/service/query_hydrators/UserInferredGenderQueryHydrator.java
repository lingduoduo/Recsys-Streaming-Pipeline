package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

import org.springframework.stereotype.Component;

@Component
public class UserInferredGenderQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public UserInferredGenderQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        MovieLensUserFeatures fetched = featureClient.getUserFeatures(userId)
            .orElseGet(() -> MovieLensUserFeatures.forUser(userId));
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withInferredGender(fetched.inferredGender(), fetched.inferredGenderScore()),
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
