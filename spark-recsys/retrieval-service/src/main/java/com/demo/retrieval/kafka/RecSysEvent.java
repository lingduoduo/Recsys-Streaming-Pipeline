package com.demo.retrieval.kafka;

import java.util.List;

public sealed interface RecSysEvent
        permits RecSysEvent.UserUpdated, RecSysEvent.MovieUpdated,
                RecSysEvent.RatingCreated, RecSysEvent.InteractionCreated {

    byte[] toByteArray();

    static RecSysEvent userUpdated(
            String userId, int age, String gender,
            String occupation, String zipCode, long timestamp) {
        return new UserUpdated(userId, age, gender, occupation, zipCode, timestamp);
    }

    static RecSysEvent movieUpdated(
            String movieId, String title, List<String> genres,
            int releaseYear, long timestamp) {
        return new MovieUpdated(movieId, title, genres, releaseYear, timestamp);
    }

    static RecSysEvent ratingCreated(
            String userId, String movieId, double rating, long timestamp) {
        return new RatingCreated(userId, movieId, rating, timestamp);
    }

    static RecSysEvent interactionCreated(
            String userId, String movieId, String eventType, long timestamp) {
        return new InteractionCreated(userId, movieId, eventType, timestamp);
    }

    record UserUpdated(String userId, int age, String gender,
                       String occupation, String zipCode, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() { return new byte[0]; }
    }

    record MovieUpdated(String movieId, String title, List<String> genres,
                        int releaseYear, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() { return new byte[0]; }
    }

    record RatingCreated(String userId, String movieId, double rating, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() { return new byte[0]; }
    }

    record InteractionCreated(String userId, String movieId, String eventType, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() { return new byte[0]; }
    }
}
