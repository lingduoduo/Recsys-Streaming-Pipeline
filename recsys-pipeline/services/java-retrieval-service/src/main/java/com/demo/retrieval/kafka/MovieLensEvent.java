package com.demo.retrieval.kafka;

public sealed interface MovieLensEvent
        permits UserEvent, MovieEvent, RatingEvent, MovieInteractionEvent {}
