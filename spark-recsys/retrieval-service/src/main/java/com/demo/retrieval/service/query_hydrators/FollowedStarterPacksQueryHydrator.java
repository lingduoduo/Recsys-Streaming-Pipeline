package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.FollowedStarterPacksClient;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates followedStarterPacks from the starter pack follow store.
 *
 * Mirrors the Rust FollowedStarterPacksQueryHydrator: fetch followed pack IDs
 * from a dedicated store client, then call ids_to_bool_array(&ids, &PACK_IDS).
 * MovieLens has no starter pack concept, so the client returns an empty list
 * and followedStarterPacks remains empty for all users.
 */
@Component
public class FollowedStarterPacksQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final FollowedStarterPacksClient starterPacksClient;

    public FollowedStarterPacksQueryHydrator(FollowedStarterPacksClient starterPacksClient) {
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
                hydrated.userFeatures().followedStarterPacks()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
