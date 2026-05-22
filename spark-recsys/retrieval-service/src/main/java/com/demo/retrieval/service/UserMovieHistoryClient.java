package com.demo.retrieval.service;

import java.util.List;

public interface UserMovieHistoryClient {
    List<String> getWatchedMovies(String userId);

    List<String> getRatedMovies(String userId);
}
