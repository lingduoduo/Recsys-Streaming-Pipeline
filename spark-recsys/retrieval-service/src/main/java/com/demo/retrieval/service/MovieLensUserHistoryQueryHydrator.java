package com.demo.retrieval.service;

import org.springframework.stereotype.Component;

@Component
public class MovieLensUserHistoryQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final UserMovieHistoryClient client;

    public MovieLensUserHistoryQueryHydrator(UserMovieHistoryClient client) {
        this.client = client;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        return new ScoredMoviesQuery(
            userId,
            client.getWatchedMovies(userId),
            client.getRatedMovies(userId)
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            hydrated.watchedMovieIds(),
            hydrated.ratedMovieIds()
        );
    }
}
