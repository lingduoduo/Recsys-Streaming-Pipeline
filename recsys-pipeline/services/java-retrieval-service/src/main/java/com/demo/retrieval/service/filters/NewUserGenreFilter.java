package com.demo.retrieval.service.filters;

import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.candidate_hydrators.MovieCandidate;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NewUserGenreFilter implements CandidateFilter {
    @Override
    public boolean enable(ScoredMoviesQuery query) {
        return isNewUser(query) && !genreIds(query).isEmpty();
    }

    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<Integer> expandedTopicIds = GenreExpansion.expand(genreIds(query));
        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate ->
                Boolean.TRUE.equals(candidate.inNetwork())
                    || candidate.matchedGenreIds().stream().anyMatch(expandedTopicIds::contains)));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.TRUE, List.of()),
            partitioned.getOrDefault(Boolean.FALSE, List.of())
        );
    }

    private boolean isNewUser(ScoredMoviesQuery query) {
        MovieLensUserFeatures features = query.userFeatures();
        return query.watchedMovieIds().isEmpty()
            && query.ratedMovieIds().isEmpty()
            && features.recentlyRatedMovieIds().isEmpty()
            && features.actionSequenceMovieIds().isEmpty()
            && features.retrievalSequenceMovieIds().isEmpty()
            && features.scoringSequenceMovieIds().isEmpty();
    }

    private Set<Integer> genreIds(ScoredMoviesQuery query) {
        MovieLensUserFeatures features = query.userFeatures();
        return Stream.of(features.inferredGenres(), features.followedGenres(), features.followedCollections())
            .flatMap(List::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
