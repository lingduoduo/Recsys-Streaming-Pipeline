package com.demo.retrieval.service.candidate_hydrators;

import java.util.List;

public record MovieCandidate(
    String movieId,
    double popularityScore,
    double contentScore,
    boolean coldStartSource,
    String ownerId,
    String sourceUserId,
    String sourceMovieId,
    String inReplyToMovieId,
    String coreDataText,
    String languageCode,
    String visibilityReason,
    boolean dropAncillaryMovies,
    List<String> ancestorMovieIds,
    String quotedMovieId
) {
    public MovieCandidate {
        ancestorMovieIds = ancestorMovieIds == null ? List.of() : List.copyOf(ancestorMovieIds);
    }

    public MovieCandidate(
        String movieId,
        double popularityScore,
        double contentScore,
        boolean coldStartSource
    ) {
        this(movieId, popularityScore, contentScore, coldStartSource, null, null, null, null, null, null, null, false, List.of(), null);
    }

    public MovieCandidate withCoreData(
        String ownerId,
        String sourceUserId,
        String sourceMovieId,
        String inReplyToMovieId,
        String coreDataText
    ) {
        return new MovieCandidate(
            movieId,
            popularityScore,
            contentScore,
            coldStartSource,
            ownerId,
            sourceUserId,
            sourceMovieId,
            inReplyToMovieId,
            coreDataText,
            languageCode,
            visibilityReason,
            dropAncillaryMovies,
            ancestorMovieIds,
            quotedMovieId
        );
    }

    public MovieCandidate withLanguageCode(String languageCode) {
        return new MovieCandidate(
            movieId,
            popularityScore,
            contentScore,
            coldStartSource,
            ownerId,
            sourceUserId,
            sourceMovieId,
            inReplyToMovieId,
            coreDataText,
            languageCode,
            visibilityReason,
            dropAncillaryMovies,
            ancestorMovieIds,
            quotedMovieId
        );
    }

    public MovieCandidate withVisibility(
        String visibilityReason,
        boolean dropAncillaryMovies,
        List<String> ancestorMovieIds,
        String quotedMovieId
    ) {
        return new MovieCandidate(
            movieId,
            popularityScore,
            contentScore,
            coldStartSource,
            ownerId,
            sourceUserId,
            sourceMovieId,
            inReplyToMovieId,
            coreDataText,
            languageCode,
            visibilityReason,
            dropAncillaryMovies,
            ancestorMovieIds,
            quotedMovieId
        );
    }
}
