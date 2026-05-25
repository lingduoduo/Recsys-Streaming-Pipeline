package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RetweetDeduplicationFilter implements CandidateFilter {
    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<String> seenMovieIds = new HashSet<>();
        List<MovieCandidate> kept = new ArrayList<>();
        List<MovieCandidate> removed = new ArrayList<>();
        for (MovieCandidate candidate : candidates) {
            String dedupId = candidate.sourceMovieId() == null || candidate.sourceMovieId().isBlank()
                ? candidate.movieId()
                : candidate.sourceMovieId();
            if (seenMovieIds.add(dedupId)) {
                kept.add(candidate);
            } else {
                removed.add(candidate);
            }
        }
        return new CandidateFilterResult(List.copyOf(kept), List.copyOf(removed));
    }
}
