package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;

/**
 * Hydrates actionSequenceMovieIds from the user's recent engagement history.
 *
 * Mirrors the Rust UserActionSeqQueryHydrator: read the raw user action stream
 * (recentlyRatedMovieIds, written newest-first by MovieLensContextCollectorStreamingJob),
 * deduplicate preserving order (analogous to DefaultAggregator + KeepOriginalUserActionFilter),
 * and truncate to the action sequence window used by sequential models.
 */
@Component
public class UserActionSequenceQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_ACTION_SEQ_LENGTH = 50;

    private final MovieLensFeatureClient featureClient;

    public UserActionSequenceQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> raw = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of);

        List<String> actionSeq = truncate(deduplicate(raw), MAX_ACTION_SEQ_LENGTH);

        return new ScoredMoviesQuery(userId,
            query.userFeatures().withActionSequenceMovieIds(actionSeq),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withActionSequenceMovieIds(
                hydrated.userFeatures().actionSequenceMovieIds()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    private static List<String> deduplicate(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? items.subList(0, maxLen) : items;
    }
}
