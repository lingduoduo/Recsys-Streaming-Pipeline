package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MutedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public MutedUserIdsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> mutedUserIds = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::mutedUserIds)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withMutedUserIds(mutedUserIds),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withMutedUserIds(hydrated.userFeatures().mutedUserIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
