package com.demo.retrieval.service.clients;

import com.demo.retrieval.service.MovieLensUserFeatures;

import java.util.Optional;

public interface MovieLensFeatureClient {
    Optional<MovieLensUserFeatures> getUserFeatures(String userId);
}
