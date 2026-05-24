package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RetrievalSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public RetrievalSequenceQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> sequence = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::retrievalSequenceMovieIds)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withRetrievalSequenceMovieIds(sequence),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withRetrievalSequenceMovieIds(hydrated.userFeatures().retrievalSequenceMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
