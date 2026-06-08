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
    Boolean hasMedia,
    Boolean authorBlocksViewer,
    List<String> followingRepliedUserIds,
    Double mutualFollowJaccard,
    Long favoriteCount,
    Long replyCount,
    Long repostCount,
    Long quoteCount,
    List<Integer> matchedGenreIds,
    List<Integer> unmatchedGenreIds,
    Boolean inNetwork,
    Integer authorFollowersCount,
    String authorScreenName,
    String retweetedScreenName
) {
    public MovieCandidate {
        ancestorMovieIds = ancestorMovieIds == null ? List.of() : List.copyOf(ancestorMovieIds);
        followingRepliedUserIds = followingRepliedUserIds == null ? List.of() : List.copyOf(followingRepliedUserIds);
        matchedGenreIds = matchedGenreIds == null ? List.of() : List.copyOf(matchedGenreIds);
        unmatchedGenreIds = unmatchedGenreIds == null ? List.of() : List.copyOf(unmatchedGenreIds);
    }

    public MovieCandidate(
        String movieId,
        double popularityScore,
        double contentScore,
        boolean coldStartSource
    ) {
        this(movieId, popularityScore, contentScore, coldStartSource, null, null, null, null, null, null, null,
            false, List.of(), null, null, null, null, null, null, null, List.of(), null,
            null, null, null, null, List.of(), List.of(), null, null, null, null);
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
        );
    }

    public MovieCandidate withAuthorBlocksViewer(Boolean authorBlocksViewer) {
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
        );
    }

    public MovieCandidate withFollowingRepliedUserIds(List<String> followingRepliedUserIds) {
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
        );
    }

    public MovieCandidate withMutualFollowJaccard(Double mutualFollowJaccard) {
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
            hasMedia,
            authorBlocksViewer,
            followingRepliedUserIds,
            mutualFollowJaccard,
            favoriteCount,
            replyCount,
            repostCount,
            quoteCount,
            matchedGenreIds,
            unmatchedGenreIds,
            inNetwork,
            authorFollowersCount,
            authorScreenName,
            retweetedScreenName
        );
    }

    public MovieCandidate withEngagementCounts(Long favoriteCount, Long replyCount, Long repostCount, Long quoteCount) {
        return new MovieCandidate(
            movieId, popularityScore, contentScore, coldStartSource, ownerId, sourceUserId, sourceMovieId,
            inReplyToMovieId, coreDataText, languageCode, visibilityReason, dropAncillaryMovies, ancestorMovieIds,
            quotedMovieId, quotedOwnerId, quotedAuthorBlocksViewer, quotedVideoDurationMillis, subscriptionAuthorId,
            hasMedia, authorBlocksViewer, followingRepliedUserIds, mutualFollowJaccard,
            favoriteCount, replyCount, repostCount, quoteCount, matchedGenreIds, unmatchedGenreIds, inNetwork,
            authorFollowersCount, authorScreenName, retweetedScreenName
        );
    }

    public MovieCandidate withMatchedGenres(List<Integer> matchedGenreIds, List<Integer> unmatchedGenreIds) {
        return new MovieCandidate(
            movieId, popularityScore, contentScore, coldStartSource, ownerId, sourceUserId, sourceMovieId,
            inReplyToMovieId, coreDataText, languageCode, visibilityReason, dropAncillaryMovies, ancestorMovieIds,
            quotedMovieId, quotedOwnerId, quotedAuthorBlocksViewer, quotedVideoDurationMillis, subscriptionAuthorId,
            hasMedia, authorBlocksViewer, followingRepliedUserIds, mutualFollowJaccard,
            favoriteCount, replyCount, repostCount, quoteCount, matchedGenreIds, unmatchedGenreIds, inNetwork,
            authorFollowersCount, authorScreenName, retweetedScreenName
        );
    }

    public MovieCandidate withInNetwork(Boolean inNetwork) {
        return new MovieCandidate(
            movieId, popularityScore, contentScore, coldStartSource, ownerId, sourceUserId, sourceMovieId,
            inReplyToMovieId, coreDataText, languageCode, visibilityReason, dropAncillaryMovies, ancestorMovieIds,
            quotedMovieId, quotedOwnerId, quotedAuthorBlocksViewer, quotedVideoDurationMillis, subscriptionAuthorId,
            hasMedia, authorBlocksViewer, followingRepliedUserIds, mutualFollowJaccard,
            favoriteCount, replyCount, repostCount, quoteCount, matchedGenreIds, unmatchedGenreIds, inNetwork,
            authorFollowersCount, authorScreenName, retweetedScreenName
        );
    }

    public MovieCandidate withGizmoduck(Integer authorFollowersCount, String authorScreenName, String retweetedScreenName) {
        return new MovieCandidate(
            movieId, popularityScore, contentScore, coldStartSource, ownerId, sourceUserId, sourceMovieId,
            inReplyToMovieId, coreDataText, languageCode, visibilityReason, dropAncillaryMovies, ancestorMovieIds,
            quotedMovieId, quotedOwnerId, quotedAuthorBlocksViewer, quotedVideoDurationMillis, subscriptionAuthorId,
            hasMedia, authorBlocksViewer, followingRepliedUserIds, mutualFollowJaccard,
            favoriteCount, replyCount, repostCount, quoteCount, matchedGenreIds, unmatchedGenreIds, inNetwork,
            authorFollowersCount, authorScreenName, retweetedScreenName
        );
    }
}
