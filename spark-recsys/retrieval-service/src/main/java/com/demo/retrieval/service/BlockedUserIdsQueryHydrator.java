package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BlockedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public BlockedUserIdsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> blockedUserIds = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::blockedUserIds)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(userId, query.userFeatures().withBlockedUserIds(blockedUserIds),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withBlockedUserIds(hydrated.userFeatures().blockedUserIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
