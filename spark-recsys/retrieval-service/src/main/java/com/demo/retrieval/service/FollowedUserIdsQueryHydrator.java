package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FollowedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public FollowedUserIdsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> followedUserIds = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::followedUserIds)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(userId, query.userFeatures().withFollowedUserIds(followedUserIds),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedUserIds(hydrated.userFeatures().followedUserIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
