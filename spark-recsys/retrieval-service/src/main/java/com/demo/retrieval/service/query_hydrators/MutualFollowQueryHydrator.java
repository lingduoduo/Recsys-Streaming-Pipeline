package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.ScoredMoviesQuery;
import com.demo.retrieval.service.SimilarityMinHashClient;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates mutualFollowMinhash with the user's MinHash signature for Jaccard
 * similarity computation.
 *
 * Mirrors the Rust MutualFollowQueryHydrator which calls StratoClient
 * .get_minhash_with_count() to fetch a MinHash signature for estimating
 * follower-graph overlap. In MovieLens, the MinHash is computed over co-rated
 * movie sets to approximate taste similarity between users.
 */
@Component
public class MutualFollowQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final SimilarityMinHashClient minHashClient;

    public MutualFollowQueryHydrator(SimilarityMinHashClient minHashClient) {
        this.minHashClient = minHashClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Long> minhash = minHashClient.getMinHash(userId);
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
