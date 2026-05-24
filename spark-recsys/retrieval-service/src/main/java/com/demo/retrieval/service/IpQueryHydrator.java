package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

@Component
public class IpQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public IpQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        String ipLocation = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::ipLocation)
            .orElse("");
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withIpLocation(ipLocation),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withIpLocation(hydrated.userFeatures().ipLocation()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
