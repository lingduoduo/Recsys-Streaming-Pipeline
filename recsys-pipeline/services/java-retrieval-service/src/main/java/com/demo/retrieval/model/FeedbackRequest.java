package com.demo.retrieval.model;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;

public record FeedbackRequest(
    @NotBlank String user,
    @NotBlank String item,
    boolean clicked,
    @DecimalMin("0.0") @DecimalMax("1.0") double reward
) {
}
