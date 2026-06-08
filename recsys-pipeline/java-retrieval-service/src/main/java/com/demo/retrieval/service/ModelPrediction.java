package com.demo.retrieval.service;

public record ModelPrediction(
    String user,
    String item,
    long userId,
    long itemId,
    double score,
    String model
) {
}
