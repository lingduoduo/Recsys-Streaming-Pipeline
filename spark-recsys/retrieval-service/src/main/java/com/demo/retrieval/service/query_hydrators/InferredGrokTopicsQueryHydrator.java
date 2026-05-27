package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InferredGrokTopicsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public InferredGrokTopicsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Integer> topics = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::inferredGrokTopics)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withInferredGrokTopics(topics),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withInferredGrokTopics(hydrated.userFeatures().inferredGrokTopics()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
