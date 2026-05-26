package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplicates candidates that belong to the same reply/ancestor conversation.
 * Candidates with no ancestors (root movies) are always kept.
 * For candidates with ancestors, only the one with the highest popularity score
 * per thread (identified by the lexicographically smallest ancestor ID) is kept.
 */
public class DedupConversationFilter implements CandidateFilter {

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
