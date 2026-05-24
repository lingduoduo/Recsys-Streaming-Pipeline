package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.candidate_hydrators.CoreDataCandidateHydrator;
import com.demo.retrieval.service.candidate_hydrators.LanguageCodeCandidateHydrator;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.candidate_hydrators.VisibilityFilteringCandidateHydrator;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CandidateHydratorsTest {
    @Test
    void coreDataHydratorCopiesMovieMetadataOntoCandidates() {
        MovieProfile profile = new MovieProfile();
        profile.setOwnerId("owner-1");
        profile.setSourceUserId("source-user-1");
        profile.setSourceMovieId("source-movie-1");
        profile.setInReplyToMovieId("reply-movie-1");
        profile.setCoreDataText("core text");

        MovieCandidate hydrated = new CoreDataCandidateHydrator(Map.of("movie-1", profile))
            .hydrate(ScoredMoviesQuery.forUser("u1"), List.of(candidate("movie-1")))
            .get(0);

        assertEquals("owner-1", hydrated.ownerId());
        assertEquals("source-user-1", hydrated.sourceUserId());
        assertEquals("source-movie-1", hydrated.sourceMovieId());
        assertEquals("reply-movie-1", hydrated.inReplyToMovieId());
        assertEquals("core text", hydrated.coreDataText());
    }

    @Test
    void languageHydratorCopiesLanguageCodeOntoCandidates() {
        MovieProfile profile = new MovieProfile();
        profile.setLanguageCode("en");

        MovieCandidate hydrated = new LanguageCodeCandidateHydrator(Map.of("movie-1", profile))
            .hydrate(ScoredMoviesQuery.forUser("u1"), List.of(candidate("movie-1")))
            .get(0);

        assertEquals("en", hydrated.languageCode());
    }

    @Test
    void visibilityHydratorCopiesPrimaryReasonAndDropsAncillaryReferences() {
        MovieProfile primary = new MovieProfile();
        primary.setVisibilityReason("primary_drop");
        primary.setAncestorMovieIds(List.of("ancestor-1"));

        MovieProfile ancestor = new MovieProfile();
        ancestor.setVisibilityReason("ancestor_drop");

        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("movie-1", primary);
        catalog.put("ancestor-1", ancestor);

        MovieCandidate hydrated = new VisibilityFilteringCandidateHydrator(catalog)
            .hydrate(ScoredMoviesQuery.forUser("u1"), List.of(candidate("movie-1")))
            .get(0);

        assertEquals("primary_drop", hydrated.visibilityReason());
        assertEquals(List.of("ancestor-1"), hydrated.ancestorMovieIds());
        assertTrue(hydrated.dropAncillaryMovies());
    }

    private MovieCandidate candidate(String id) {
        return new MovieCandidate(id, 1.0, 0.0, false);
    }
}
