package com.demo.retrieval.service;

import java.util.List;

public interface CachedMoviesClient {
    List<String> getCachedMovieIds(String userId);
}
