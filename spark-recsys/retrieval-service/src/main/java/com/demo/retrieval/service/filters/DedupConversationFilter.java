package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Deduplicates candidates that belong to the same reply/ancestor conversation.
 * For each conversation (identified by the lexicographically smallest ancestor
 * movie ID, or the movie's own ID when it has no ancestors), only the candidate
 * with the highest popularity score is kept. All others are moved to removed.
 *
 * This prevents the same discussion thread from occupying multiple slots in the
 * recommendation slate, analogous to conversation deduplication in a tweet feed.
 */
public class DedupConversationFilter implements CandidateFilter {

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Map<String, MovieCandidate> bestPerConversation = new LinkedHashMap<>();
        List<MovieCandidate> removed = new ArrayList<>();

        for (MovieCandidate candidate : candidates) {
            String conversationId = conversationId(candidate);
            MovieCandidate current = bestPerConversation.get(conversationId);
            if (current == null) {
                bestPerConversation.put(conversationId, candidate);
            } else if (candidate.popularityScore() > current.popularityScore()) {
                removed.add(current);
                bestPerConversation.put(conversationId, candidate);
            } else {
                removed.add(candidate);
            }
        }

        return new CandidateFilterResult(List.copyOf(bestPerConversation.values()), removed);
    }

    private static String conversationId(MovieCandidate candidate) {
        List<String> ancestors = candidate.ancestorMovieIds();
        if (ancestors.isEmpty()) {
            return candidate.movieId();
        }
        return ancestors.stream().min(Comparator.naturalOrder()).orElse(candidate.movieId());
    }
}
