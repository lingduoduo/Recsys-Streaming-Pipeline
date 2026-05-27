package com.demo.retrieval.service.clients;

import java.util.List;

/**
 * Fetches per-user social graph relationships.
 *
 * Mirrors the Rust SocialGraphClientOps trait used by BlockedUserIdsQueryHydrator,
 * MutedUserIdsQueryHydrator, and FollowedUserIdsQueryHydrator. Kept as a separate
 * client from MovieLensFeatureClient because social graph state (block/mute/follow)
 * has different write paths and freshness requirements from rating-based user features.
 */
public interface SocialGraphClient {
    List<String> getBlockedUserIds(String userId);
    List<String> getMutedUserIds(String userId);
    List<String> getFollowedUserIds(String userId);
    List<String> getSubscribedUserIds(String userId);
}
