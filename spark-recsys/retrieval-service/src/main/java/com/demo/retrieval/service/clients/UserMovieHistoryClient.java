package com.demo.retrieval.service.clients;

import java.util.List;

public interface UserMovieHistoryClient {
    List<String> getWatchedMovies(String userId);

    List<String> getRatedMovies(String userId);
}
