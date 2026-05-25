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
    String quotedMovieId,
    String quotedOwnerId,
    Boolean quotedAuthorBlocksViewer,
    Integer quotedVideoDurationMillis,
    String subscriptionAuthorId,
    Boolean hasMedia
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
        this(movieId, popularityScore, contentScore, coldStartSource, null, null, null, null, null, null, null, false, List.of(), null, null, null, null, null, null);
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
            quotedMovieId,
            quotedOwnerId,
            quotedAuthorBlocksViewer,
            quotedVideoDurationMillis,
            subscriptionAuthorId,
            hasMedia
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
            quotedMovieId,
            quotedOwnerId,
            quotedAuthorBlocksViewer,
            quotedVideoDurationMillis,
            subscriptionAuthorId,
            hasMedia
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
            quotedMovieId,
            quotedOwnerId,
            quotedAuthorBlocksViewer,
            quotedVideoDurationMillis,
            subscriptionAuthorId,
            hasMedia
        );
    }

    public MovieCandidate withQuote(
        String quotedMovieId,
        String quotedOwnerId,
        Boolean quotedAuthorBlocksViewer,
        Integer quotedVideoDurationMillis
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
            quotedMovieId,
            quotedOwnerId,
            quotedAuthorBlocksViewer,
            quotedVideoDurationMillis,
            subscriptionAuthorId,
            hasMedia
        );
    }

    public MovieCandidate withSubscriptionAuthorId(String subscriptionAuthorId) {
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
            quotedMovieId,
            quotedOwnerId,
            quotedAuthorBlocksViewer,
            quotedVideoDurationMillis,
            subscriptionAuthorId,
            hasMedia
        );
    }

    public MovieCandidate withHasMedia(Boolean hasMedia) {
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
            quotedMovieId,
            quotedOwnerId,
            quotedAuthorBlocksViewer,
            quotedVideoDurationMillis,
            subscriptionAuthorId,
            hasMedia
        );
    }
}
