package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.MovieLensFeatureClient;
import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Hydrates scoringSequenceMovieIds from the user's behavior event list.
 *
 * Mirrors the Rust ScoringSequenceQueryHydrator + DenseAggregatedActionFilter pattern:
 * read the engagement stream, deduplicate (keeping only the first/most-recent occurrence
 * of each movie), then truncate to a short, high-signal window for the ranking model.
 * The head of the recency-ordered list represents the densest engagement signal.
 */
@Component
public class ScoringSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_SCORING_SEQ_LENGTH = 20;

    private final MovieLensFeatureClient featureClient;

    public ScoringSequenceQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> raw = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);

        List<String> scoringSeq = truncate(deduplicate(raw), MAX_SCORING_SEQ_LENGTH);

        return new ScoredMoviesQuery(userId,
            query.userFeatures().withScoringSequenceMovieIds(scoringSeq),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withScoringSequenceMovieIds(
                hydrated.userFeatures().scoringSequenceMovieIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> deduplicate(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
