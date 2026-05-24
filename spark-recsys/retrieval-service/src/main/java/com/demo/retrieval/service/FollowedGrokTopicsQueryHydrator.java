package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FollowedGrokTopicsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public FollowedGrokTopicsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Integer> topics = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::followedGrokTopics)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(userId, query.userFeatures().withFollowedGrokTopics(topics),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedGrokTopics(hydrated.userFeatures().followedGrokTopics()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
