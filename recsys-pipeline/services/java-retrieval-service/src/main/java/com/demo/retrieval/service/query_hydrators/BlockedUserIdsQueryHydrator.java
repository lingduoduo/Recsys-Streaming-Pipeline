package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.SocialGraphClient;
import org.springframework.stereotype.Component;

/**
 * Hydrates blockedUserIds from the social graph service.
 *
 * Mirrors the Rust BlockedUserIdsQueryHydrator which calls
 * socialgraph_client.get_blocked_user_ids(). Uses a dedicated SocialGraphClient
 * rather than the feature store because block relationships have their own write
 * path and freshness requirements.
 */
@Component
public class BlockedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final SocialGraphClient socialGraphClient;

    public BlockedUserIdsQueryHydrator(SocialGraphClient socialGraphClient) {
        this.socialGraphClient = socialGraphClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withBlockedUserIds(
                socialGraphClient.getBlockedUserIds(query.userId())),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withBlockedUserIds(
                hydrated.userFeatures().blockedUserIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
