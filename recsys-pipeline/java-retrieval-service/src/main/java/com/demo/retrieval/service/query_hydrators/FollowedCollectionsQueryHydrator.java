package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.FollowedCollectionsClient;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates followedCollections from the starter pack follow store.
 *
 * Mirrors the Rust FollowedCollectionsQueryHydrator: fetch followed pack IDs
 * from a dedicated store client, then call ids_to_bool_array(&ids, &PACK_IDS).
 * MovieLens has no starter pack concept, so the client returns an empty list
 * and followedCollections remains empty for all users.
 */
@Component
public class FollowedCollectionsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final FollowedCollectionsClient starterPacksClient;

    public FollowedCollectionsQueryHydrator(FollowedCollectionsClient starterPacksClient) {
        this.starterPacksClient = starterPacksClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedStarterPacks(
                starterPacksClient.getFollowedPackIds(query.userId())),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedStarterPacks(
                hydrated.userFeatures().followedCollections()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
