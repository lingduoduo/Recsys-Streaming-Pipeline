package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.SocialGraphClient;
import org.springframework.stereotype.Component;

/**
 * Hydrates mutedUserIds from the social graph service.
 *
 * Mirrors the Rust MutedUserIdsQueryHydrator which calls
 * socialgraph_client.get_muted_user_ids().
 */
@Component
public class MutedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final SocialGraphClient socialGraphClient;

    public MutedUserIdsQueryHydrator(SocialGraphClient socialGraphClient) {
        this.socialGraphClient = socialGraphClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withMutedUserIds(
                socialGraphClient.getMutedUserIds(query.userId())),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withMutedUserIds(
                hydrated.userFeatures().mutedUserIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
