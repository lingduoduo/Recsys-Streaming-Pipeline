package com.demo.retrieval.service.models;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MovieCandidateContextTest {
    @Test
    void resolvesCanonicalMovieAndRelatedMovies() {
        MovieProfile profile = new MovieProfile();
        profile.setOwnerId("collection-1");
        profile.setSourceMovieId("source-movie");
        profile.setQuotedMovieId("quote-movie");
        profile.setInReplyToMovieId("related-movie");
        profile.setHasMedia(true);
        profile.setInNetwork(true);

        MovieCandidateContext context = MovieCandidateContext.from(new MovieCandidate("movie-1", 1.0, 0.0, true), profile);

        assertEquals("source-movie", context.canonicalMovieId());
        assertEquals("collection-1", context.diversityGroupId());
        assertEquals(Set.of("source-movie", "quote-movie", "related-movie"), context.relatedMovieIds());
        assertTrue(context.flags().hasMedia());
        assertTrue(context.flags().sourceCandidate());
        assertTrue(context.flags().quotedCandidate());
        assertTrue(context.flags().relatedCandidate());
        assertTrue(context.flags().inNetwork());
        assertTrue(context.flags().coldStartSource());
    }

    @Test
    void candidateHydrationOverridesCatalogContext() {
        MovieProfile profile = new MovieProfile();
        profile.setOwnerId("profile-group");
        profile.setSourceMovieId("profile-source");

        MovieCandidate candidate = new MovieCandidate("movie-1", 1.0, 0.0, false)
            .withCoreData("candidate-group", "unused-user", "candidate-source", null, "metadata");

        MovieCandidateContext context = MovieCandidateContext.from(candidate, profile);

        assertEquals("candidate-group", context.diversityGroupId());
        assertEquals("candidate-source", context.canonicalMovieId());
        assertEquals(Set.of("candidate-source"), context.relatedMovieIds());
    }
}
