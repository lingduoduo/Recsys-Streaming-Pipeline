package com.demo.retrieval.service;

import java.util.Optional;

public interface MovieLensFeatureClient {
    Optional<MovieLensUserFeatures> getUserFeatures(String userId);
}
