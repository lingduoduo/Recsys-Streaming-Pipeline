package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MovieLensUserFeaturesTest {
    @Test
    void storesOnlyMovieLensBehaviorAndPreferenceSignals() {
        MovieLensUserFeatures features = MovieLensUserFeatures.forUser("u1")
            .withActionSequenceMovieIds(List.of("a"))
            .withRetrievalSequenceMovieIds(List.of("b"))
            .withScoringSequenceMovieIds(List.of("c"))
            .withServedMovieIds(List.of("d"))
            .withImpressedMovieIds(List.of("e"))
            .withCachedMovieIds(List.of("f"), true)
            .withInferredGenres(List.of(1, 2));

        assertEquals(List.of("a"), features.actionSequenceMovieIds());
        assertEquals(List.of("b"), features.retrievalSequenceMovieIds());
        assertEquals(List.of("c"), features.scoringSequenceMovieIds());
        assertEquals(List.of("d"), features.servedMovieIds());
        assertEquals(List.of("e"), features.impressedMovieIds());
        assertEquals(List.of("f"), features.cachedMovieIds());
        assertEquals(List.of(1, 2), features.inferredGenres());
        assertTrue(features.hasCachedMovies());
    }
}
