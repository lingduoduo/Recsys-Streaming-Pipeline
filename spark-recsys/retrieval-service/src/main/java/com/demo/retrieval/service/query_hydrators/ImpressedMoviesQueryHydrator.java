package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.ImpressedMoviesClient;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates impressedMovieIds from the impression tracking store.
 *
 * Mirrors the Rust ImpressedPostsQueryHydrator which calls a dedicated
 * ImpressedPostsClient rather than the general feature store, because
 * impression data has its own write path separate from user ratings.
 */
@Component
public class ImpressedMoviesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final ImpressedMoviesClient impressedMoviesClient;

    public ImpressedMoviesQueryHydrator(ImpressedMoviesClient impressedMoviesClient) {
        this.impressedMoviesClient = impressedMoviesClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withImpressedMovieIds(
                impressedMoviesClient.getImpressedMovieIds(query.userId())),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withImpressedMovieIds(
                hydrated.userFeatures().impressedMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
