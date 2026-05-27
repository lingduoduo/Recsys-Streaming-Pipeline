package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.MovieLensFeatureClient;
import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Derives the scoring sequence from the user's behavior event list.
 *
 * Mirrors the Rust ScoringSequenceQueryHydrator / DenseAggregatedActionFilter pattern:
 * take the most recent (densest-signal) interactions and truncate to a shorter window
 * suitable for the ranking model. Because recentlyRatedMovieIds is already ordered
 * newest-first, the head of the list is the highest-signal subset.
 */
@Component
public class BehaviorEventListQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_SCORING_SEQ_LENGTH = 20;

    private final MovieLensFeatureClient featureClient;

    public BehaviorEventListQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> raw = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);

        List<String> dense = deduplicate(raw);
        List<String> scoringSeq = dense.size() > MAX_SCORING_SEQ_LENGTH
            ? dense.subList(0, MAX_SCORING_SEQ_LENGTH) : dense;

        return new ScoredMoviesQuery(userId,
            query.userFeatures().withScoringSequenceMovieIds(scoringSeq),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withScoringSequenceMovieIds(hydrated.userFeatures().scoringSequenceMovieIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> deduplicate(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }
}
