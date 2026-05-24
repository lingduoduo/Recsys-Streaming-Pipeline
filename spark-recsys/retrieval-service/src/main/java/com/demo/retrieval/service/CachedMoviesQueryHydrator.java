package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CachedMoviesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public CachedMoviesQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        MovieLensUserFeatures fetched = featureClient.getUserFeatures(userId)
            .orElseGet(() -> MovieLensUserFeatures.forUser(userId));
        return new ScoredMoviesQuery(userId,
            query.userFeatures().withCachedMovieIds(fetched.cachedMovieIds(), fetched.hasCachedMovies()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withCachedMovieIds(
                hydrated.userFeatures().cachedMovieIds(),
                hydrated.userFeatures().hasCachedMovies()
            ),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
