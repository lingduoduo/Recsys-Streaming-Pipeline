package com.demo.retrieval.service.models;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.LinkedHashSet;
import java.util.Set;

public record MovieCandidateContext(
    String movieId,
    String canonicalMovieId,
    String diversityGroupId,
    String sourceMovieId,
    String quotedItemId,
    String relatedMovieId,
    boolean inNetwork,
    boolean hasMedia,
    boolean sourceCandidate,
    boolean quotedCandidate,
    boolean relatedCandidate,
    boolean coldStartSource
) {
    public static MovieCandidateContext from(MovieCandidate candidate, MovieProfile profile) {
        String movieId = candidate == null ? "" : candidate.movieId();
        String sourceMovieId = firstNonBlank(candidate == null ? null : candidate.sourceMovieId(), profile == null ? null : profile.getSourceMovieId());
        String quotedItemId = firstNonBlank(candidate == null ? null : candidate.quotedMovieId(), profile == null ? null : profile.getQuotedMovieId());
        String relatedMovieId = firstNonBlank(candidate == null ? null : candidate.inReplyToMovieId(), profile == null ? null : profile.getInReplyToMovieId());
        Boolean inNetwork = firstNonNull(candidate == null ? null : candidate.inNetwork(), profile == null ? null : profile.getInNetwork());
        Boolean hasMedia = firstNonNull(candidate == null ? null : candidate.hasMedia(), profile == null ? null : profile.getHasMedia());
        String diversityGroupId = firstNonBlank(candidate == null ? null : candidate.ownerId(), profile == null ? null : profile.getOwnerId());

        return new MovieCandidateContext(
            movieId,
            firstNonBlank(sourceMovieId, movieId),
            diversityGroupId,
            sourceMovieId,
            quotedItemId,
            relatedMovieId,
            Boolean.TRUE.equals(inNetwork),
            Boolean.TRUE.equals(hasMedia),
            !isBlank(sourceMovieId),
            !isBlank(quotedItemId),
            !isBlank(relatedMovieId),
            candidate != null && candidate.coldStartSource()
        );
    }

    public Set<String> relatedMovieIds() {
        Set<String> related = new LinkedHashSet<>();
        if (!isBlank(sourceMovieId)) {
            related.add(sourceMovieId);
        }
        if (!isBlank(quotedItemId)) {
            related.add(quotedItemId);
        }
        if (!isBlank(relatedMovieId)) {
            related.add(relatedMovieId);
        }
        return Set.copyOf(related);
    }

    public MovieCandidateFlags flags() {
        return new MovieCandidateFlags(hasMedia, sourceCandidate, quotedCandidate, relatedCandidate, inNetwork, coldStartSource);
    }

    private static String firstNonBlank(String first, String second) {
        return isBlank(first) ? normalize(second) : normalize(first);
    }

    private static <T> T firstNonNull(T first, T second) {
        return first == null ? second : first;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public record MovieCandidateFlags(
        boolean hasMedia,
        boolean sourceCandidate,
        boolean quotedCandidate,
        boolean relatedCandidate,
        boolean inNetwork,
        boolean coldStartSource
    ) {
    }
}
