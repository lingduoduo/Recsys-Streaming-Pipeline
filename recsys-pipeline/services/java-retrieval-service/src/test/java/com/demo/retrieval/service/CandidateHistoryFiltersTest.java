package com.demo.retrieval.service;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesBackupFilter;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesFilter;
import com.demo.retrieval.service.filters.PreviouslyServedMoviesFilter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CandidateHistoryFiltersTest {
    @Test
    void previouslySeenFilterRemovesPrimaryAndRelatedMovieIds() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1"),
            List.of("watched", "quote_seen"),
            List.of("rated"),
            List.of()
        );
        List<MovieCandidate> candidates = List.of(
            candidate("watched"),
            candidate("fresh").withQuote("quote_seen", null, null, null),
            candidate("kept")
        );

        CandidateFilterResult result = new PreviouslySeenMoviesFilter().filter(query, candidates);

        assertEquals(List.of("kept"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("watched", "fresh"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void previouslySeenBackupFilterRemovesImpressedRelatedMovieIds() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withImpressedMovieIds(List.of("ancestor_seen")),
            List.of(),
            List.of(),
            List.of()
        );
        MovieCandidate related = candidate("fresh").withVisibility(null, false, List.of("ancestor_seen"), null);

        CandidateFilterResult result = new PreviouslySeenMoviesBackupFilter()
            .filter(query, List.of(related, candidate("kept")));

        assertEquals(List.of("kept"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("fresh"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    @Test
    void previouslyServedFilterRemovesServedRelatedMovieIds() {
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withServedMovieIds(List.of("served_reply")),
            List.of(),
            List.of(),
            List.of()
        );
        MovieCandidate related = candidate("fresh").withCoreData(null, null, null, "served_reply", null);

        CandidateFilterResult result = new PreviouslyServedMoviesFilter()
            .filter(query, List.of(related, candidate("kept")));

        assertEquals(List.of("kept"), result.kept().stream().map(MovieCandidate::movieId).toList());
        assertEquals(List.of("fresh"), result.removed().stream().map(MovieCandidate::movieId).toList());
    }

    private MovieCandidate candidate(String id) {
        return new MovieCandidate(id, 1.0, 0.0, false);
    }
}
