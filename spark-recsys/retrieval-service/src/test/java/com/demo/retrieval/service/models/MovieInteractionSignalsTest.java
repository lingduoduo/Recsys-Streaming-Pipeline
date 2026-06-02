package com.demo.retrieval.service.models;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MovieInteractionSignalsTest {
    @Test
    void mergesHydratedInteractionSignalsOverCatalogSignals() {
        MovieProfile profile = new MovieProfile();
        profile.setFavoriteCount(1L);
        profile.setReplyCount(2L);
        profile.setRepostCount(3L);
        profile.setQuoteCount(4L);
        profile.setAuthorFollowersCount(10);

        MovieCandidate candidate = new MovieCandidate("movie-1", 1.0, 0.0, false)
            .withEngagementCounts(5L, null, 7L, null)
            .withGizmoduck(20, "author", null);

        MovieInteractionSignals signals = MovieInteractionSignals.from(candidate, profile);

        assertEquals(5L, signals.positiveFeedbackCount());
        assertEquals(2L, signals.discussionCount());
        assertEquals(7L, signals.shareCount());
        assertEquals(4L, signals.reviewCount());
        assertEquals(18L, signals.totalInteractionCount());
        assertEquals(20, signals.audienceSize());
    }
}
