package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.FollowedGenreTopicsClient;
import com.demo.retrieval.service.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hydrates followedGrokTopics as a genre presence bool array.
 *
 * Mirrors the Rust FollowedGrokTopicsQueryHydrator: fetch followed topic IDs
 * from a dedicated store client, then call ids_to_bool_array(ids, &PARENT_TOPIC_IDS)
 * to produce a fixed-length boolean vector. In MovieLens the topics are the 18
 * canonical genres; favoriteGenres (written by the streaming job) plays the role
 * of followed topic IDs.
 */
@Component
public class FollowedGrokTopicsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final List<String> MOVIELENS_GENRES = List.of(
        "Action", "Adventure", "Animation", "Children's", "Comedy",
        "Crime", "Documentary", "Drama", "Fantasy", "Film-Noir",
        "Horror", "Musical", "Mystery", "Romance", "Sci-Fi",
        "Thriller", "War", "Western"
    );

    private final FollowedGenreTopicsClient genreTopicsClient;

    public FollowedGrokTopicsQueryHydrator(FollowedGenreTopicsClient genreTopicsClient) {
        this.genreTopicsClient = genreTopicsClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> followed = genreTopicsClient.getFollowedGenres(query.userId());
        List<Integer> boolArray = genresToBoolArray(followed);
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedGrokTopics(boolArray),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withFollowedGrokTopics(
                hydrated.userFeatures().followedGrokTopics()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    // Mirrors Rust ids_to_bool_array(ids, &PARENT_TOPIC_IDS).
    static List<Integer> genresToBoolArray(List<String> followedGenres) {
        Set<String> followed = new HashSet<>(followedGenres);
        return MOVIELENS_GENRES.stream()
            .map(g -> followed.contains(g) ? 1 : 0)
            .toList();
    }
}
