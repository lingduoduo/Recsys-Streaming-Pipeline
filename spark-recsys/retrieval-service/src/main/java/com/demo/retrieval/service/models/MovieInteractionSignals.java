package com.demo.retrieval.service.models;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

public record MovieInteractionSignals(
    long positiveFeedbackCount,
    long discussionCount,
    long shareCount,
    long reviewCount,
    int audienceSize
) {
    public static MovieInteractionSignals from(MovieCandidate candidate, MovieProfile profile) {
        return new MovieInteractionSignals(
            count(candidate == null ? null : candidate.favoriteCount(), profile == null ? null : profile.getFavoriteCount()),
            count(candidate == null ? null : candidate.replyCount(), profile == null ? null : profile.getReplyCount()),
            count(candidate == null ? null : candidate.repostCount(), profile == null ? null : profile.getRepostCount()),
            count(candidate == null ? null : candidate.quoteCount(), profile == null ? null : profile.getQuoteCount()),
            Math.max(0, firstNonNull(candidate == null ? null : candidate.authorFollowersCount(), profile == null ? null : profile.getAuthorFollowersCount(), 0))
        );
    }

    public long totalInteractionCount() {
        return positiveFeedbackCount + discussionCount + shareCount + reviewCount;
    }

    private static long count(Long first, Long second) {
        Long value = first == null ? second : first;
        return value == null ? 0L : Math.max(0L, value);
    }

    private static int firstNonNull(Integer first, Integer second, int fallback) {
        Integer value = first == null ? second : first;
        return value == null ? fallback : value;
    }
}
