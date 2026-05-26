package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    public static class DropDuplicatesFilter implements CandidateFilter {
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

    public static class DedupConversationFilter implements CandidateFilter {
        @Override
        public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
            Map<String, MovieCandidate> bestPerThread = new HashMap<>();
            for (MovieCandidate candidate : candidates) {
                List<String> ancestors = candidate.ancestorMovieIds();
                if (ancestors.isEmpty()) continue;
                String threadKey = ancestors.stream().min(Comparator.naturalOrder()).orElse(candidate.movieId());
                MovieCandidate current = bestPerThread.get(threadKey);
                if (current == null || candidate.popularityScore() > current.popularityScore()) {
                    bestPerThread.put(threadKey, candidate);
                }
            }
            List<MovieCandidate> kept = new ArrayList<>();
            List<MovieCandidate> removed = new ArrayList<>();
            for (MovieCandidate candidate : candidates) {
                if (candidate.ancestorMovieIds().isEmpty()) {
                    kept.add(candidate);
                } else {
                    String threadKey = candidate.ancestorMovieIds().stream()
                        .min(Comparator.naturalOrder()).orElse(candidate.movieId());
                    if (candidate == bestPerThread.get(threadKey)) {
                        kept.add(candidate);
                    } else {
                        removed.add(candidate);
                    }
                }
            }
            return new CandidateFilterResult(kept, removed);
        }
    }
}
