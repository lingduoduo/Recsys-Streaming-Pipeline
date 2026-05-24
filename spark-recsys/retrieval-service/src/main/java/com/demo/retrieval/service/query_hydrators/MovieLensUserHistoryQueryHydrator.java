package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

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
            query.userFeatures(),
            client.getWatchedMovies(userId),
            client.getRatedMovies(userId),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures(),
            hydrated.watchedMovieIds(),
            hydrated.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
