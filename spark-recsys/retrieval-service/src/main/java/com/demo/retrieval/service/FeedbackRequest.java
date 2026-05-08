package com.demo.retrieval.service;

public record FeedbackRequest(
    String user,
    String item,
    boolean clicked,
    double reward
) {
}
