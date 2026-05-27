package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

@Component
public class RetrievalSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_RETRIEVAL_SEQ_LENGTH = 100;

    private final MovieLensFeatureClient featureClient;

    public RetrievalSequenceQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> raw = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);
        List<String> retrievalSeq = truncate(deduplicate(raw), MAX_RETRIEVAL_SEQ_LENGTH);
        return new ScoredMoviesQuery(userId,
            query.userFeatures().withRetrievalSequenceMovieIds(retrievalSeq),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withRetrievalSequenceMovieIds(
                hydrated.userFeatures().retrievalSequenceMovieIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> deduplicate(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
