package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class UserActionSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public UserActionSequenceQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> actionSequence = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::actionSequenceMovieIds)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withActionSequenceMovieIds(actionSequence),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withActionSequenceMovieIds(hydrated.userFeatures().actionSequenceMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
