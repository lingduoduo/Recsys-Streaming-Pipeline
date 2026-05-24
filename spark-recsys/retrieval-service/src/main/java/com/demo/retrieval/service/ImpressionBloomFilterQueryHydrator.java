package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ImpressionBloomFilterQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public ImpressionBloomFilterQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Long> bloomFilter = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::impressionBloomFilter)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withImpressionBloomFilter(bloomFilter),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withImpressionBloomFilter(hydrated.userFeatures().impressionBloomFilter()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
