package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.MovieLensFeatureClient;
import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates followedGrokTopics as a list of genre/content-category IDs.
 *
 * The Rust version converts topic IDs to a bool vector via ids_to_bool_array,
 * which is specific to Grok's parent-topic taxonomy. For MovieLens there is no
 * equivalent taxonomy, so we read followedGrokTopics directly from the feature
 * store as a plain list of related content-category identifiers.
 */
@Component
public class FollowedGrokTopicsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final MovieLensFeatureClient featureClient;

    public FollowedGrokTopicsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<Integer> topics = featureClient.getUserFeatures(query.userId())
            .map(MovieLensUserFeatures::followedGrokTopics)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedGrokTopics(topics),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedGrokTopics(
                hydrated.userFeatures().followedGrokTopics()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
