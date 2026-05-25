package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.LinkedHashSet;
import java.util.Set;

final class CandidateRelatedMovieIds {
    private CandidateRelatedMovieIds() {
    }

    static Set<String> get(MovieCandidate candidate) {
        LinkedHashSet<String> ids = new LinkedHashSet<>();
        add(ids, candidate.movieId());
        add(ids, candidate.sourceMovieId());
        add(ids, candidate.inReplyToMovieId());
        add(ids, candidate.quotedMovieId());
        candidate.ancestorMovieIds().forEach(id -> add(ids, id));
        return ids;
    }

    private static void add(Set<String> ids, String id) {
        if (id != null && !id.isBlank()) {
            ids.add(id);
        }
    }
}
