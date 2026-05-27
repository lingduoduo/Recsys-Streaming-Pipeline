package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.*;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MutualFollowQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public MutualFollowQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Long> minhash = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::mutualFollowMinhash)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withMutualFollowMinhash(minhash),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withMutualFollowMinhash(hydrated.userFeatures().mutualFollowMinhash()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
