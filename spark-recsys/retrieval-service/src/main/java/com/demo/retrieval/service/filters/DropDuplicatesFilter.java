package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Removes candidates with duplicate movie IDs, preserving the first occurrence.
 * Should run early in the pipeline to prevent redundant hydration and scoring work.
 */
public class DropDuplicatesFilter implements CandidateFilter {

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<String> seenIds = new HashSet<>();
        List<MovieCandidate> kept = new ArrayList<>();
        List<MovieCandidate> removed = new ArrayList<>();

        for (MovieCandidate candidate : candidates) {
            if (seenIds.add(candidate.movieId())) {
                kept.add(candidate);
            } else {
                removed.add(candidate);
            }
        }

        return new CandidateFilterResult(kept, removed);
    }
}
