package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

@Component
public class UserMovieFeaturesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public UserMovieFeaturesQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        MovieLensUserFeatures userFeatures = featureClient.getUserFeatures(userId)
            .orElseGet(() -> MovieLensUserFeatures.forUser(userId));
        return new ScoredMoviesQuery(
            userId,
            userFeatures,
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            hydrated.userFeatures(),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
