package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.MovieLensFeatureClient;
import com.demo.retrieval.service.MovieLensUserFeatures;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates related content-category IDs for the user.
 *
 * Reads the content-category identifiers stored under followedGrokTopics in
 * the feature store. These are written by the content-category tagging pipeline
 * and represent genre/topic clusters the user engages with.
 */
@Component
public class RelatedContentsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final MovieLensFeatureClient featureClient;

    public RelatedContentsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<Integer> relatedContents = featureClient.getUserFeatures(query.userId())
            .map(MovieLensUserFeatures::followedGrokTopics)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedGrokTopics(relatedContents),
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
