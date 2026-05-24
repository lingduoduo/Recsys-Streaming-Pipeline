package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FollowedStarterPacksQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public FollowedStarterPacksQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Integer> packs = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::followedStarterPacks)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(userId, query.userFeatures().withFollowedStarterPacks(packs),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedStarterPacks(hydrated.userFeatures().followedStarterPacks()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
