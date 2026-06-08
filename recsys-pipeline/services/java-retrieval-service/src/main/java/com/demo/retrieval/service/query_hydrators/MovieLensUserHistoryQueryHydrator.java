package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.UserMovieHistoryClient;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;

import org.springframework.stereotype.Component;

@Component
public class MovieLensUserHistoryQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final UserMovieHistoryClient client;

    public MovieLensUserHistoryQueryHydrator(UserMovieHistoryClient client) {
        this.client = client;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        UserMovieHistory history = client.getMovieHistory(query.userId());
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures(),
            history.watched(),
            history.rated(),
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
