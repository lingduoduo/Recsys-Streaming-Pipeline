package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.SocialGraphClient;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates subscribedUserIds from the social graph store.
 *
 * Mirrors the Rust SubscribedUserIdsQueryHydrator which uses SocialGraphClientOps
 * .get_subscribed_user_ids(). Subscriptions (curated user lists, mutual-interest
 * follows) live in the same social graph store as blocked/muted/followed so that
 * all relationship data shares a single write path.
 */
@Component
public class SubscribedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final SocialGraphClient socialGraphClient;

    public SubscribedUserIdsQueryHydrator(SocialGraphClient socialGraphClient) {
        this.socialGraphClient = socialGraphClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> subscribedUserIds = socialGraphClient.getSubscribedUserIds(userId);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withSubscribedUserIds(subscribedUserIds),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withSubscribedUserIds(hydrated.userFeatures().subscribedUserIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
