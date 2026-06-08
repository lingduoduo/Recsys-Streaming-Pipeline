package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.SocialGraphClient;
import org.springframework.stereotype.Component;

/**
 * Hydrates followedUserIds from the social graph service.
 *
 * Mirrors the Rust FollowedUserIdsQueryHydrator which calls
 * socialgraph_client.get_followed_user_ids().
 */
@Component
public class FollowedUserIdsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final SocialGraphClient socialGraphClient;

    public FollowedUserIdsQueryHydrator(SocialGraphClient socialGraphClient) {
        this.socialGraphClient = socialGraphClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedUserIds(
                socialGraphClient.getFollowedUserIds(query.userId())),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedUserIds(
                hydrated.userFeatures().followedUserIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
