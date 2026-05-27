package com.demo.retrieval.service;

import java.util.List;

public interface ImpressedMoviesClient {
    List<String> getImpressedMovieIds(String userId);
}
