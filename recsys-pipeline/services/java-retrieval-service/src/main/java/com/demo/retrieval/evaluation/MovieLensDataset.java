package com.demo.retrieval.evaluation;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Immutable ratings retained from a MovieLens CSV after bipartite fixed-point filtering. */
public final class MovieLensDataset {
    private static final double PRIOR_STRENGTH = 20.0;

    private final Map<Integer, Map<Integer, Double>> ratingsByUser;
    private final List<Integer> userIds;
    private final List<Integer> movieIds;
    private final Map<Integer, Integer> movieCounts;
    private final Map<Integer, Double> movieSums;
    private final double globalMean;

    private MovieLensDataset(Map<Integer, Map<Integer, Double>> ratings) {
        Map<Integer, Map<Integer, Double>> immutableRatings = new LinkedHashMap<>();
        ratings.forEach((userId, userRatings) ->
                immutableRatings.put(userId,
                        Collections.unmodifiableMap(new LinkedHashMap<>(userRatings))));
        this.ratingsByUser = Collections.unmodifiableMap(immutableRatings);
        this.userIds = List.copyOf(ratingsByUser.keySet());

        TreeMap<Integer, Integer> counts = new TreeMap<>();
        TreeMap<Integer, Double> sums = new TreeMap<>();
        ratingsByUser.values().forEach(userRatings -> userRatings.forEach((movieId, rating) -> {
            counts.merge(movieId, 1, Integer::sum);
            sums.merge(movieId, rating, Double::sum);
        }));
        this.movieIds = List.copyOf(counts.keySet());
        this.movieCounts = Collections.unmodifiableMap(new LinkedHashMap<>(counts));
        this.movieSums = Collections.unmodifiableMap(new LinkedHashMap<>(sums));
        double total = sums.values().stream().mapToDouble(Double::doubleValue).sum();
        int count = counts.values().stream().mapToInt(Integer::intValue).sum();
        this.globalMean = total / count;
    }

    public static MovieLensDataset load(Path file, int minUserRatings, int minMovieRatings)
            throws IOException {
        if (minUserRatings < 0 || minMovieRatings < 0) {
            throw new IllegalArgumentException("Rating thresholds must be nonnegative");
        }

        TreeMap<Integer, Map<Integer, Double>> ratings = new TreeMap<>();
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            String header = reader.readLine();
            if (header == null || !hasExpectedHeader(header)) {
                throw new IllegalArgumentException("Expected header prefix userId,movieId,rating in " + file);
            }

            String row;
            int lineNumber = 1;
            while ((row = reader.readLine()) != null) {
                lineNumber++;
                String[] fields = row.split(",", -1);
                if (fields.length < 3) {
                    throw malformed(file, lineNumber, "expected at least three fields", null);
                }
                try {
                    int userId = Integer.parseInt(fields[0]);
                    int movieId = Integer.parseInt(fields[1]);
                    double rating = Double.parseDouble(fields[2]);
                    if (!Double.isFinite(rating)) {
                        throw malformed(file, lineNumber, "rating must be finite", null);
                    }
                    Map<Integer, Double> userRatings =
                            ratings.computeIfAbsent(userId, ignored -> new TreeMap<>());
                    if (userRatings.putIfAbsent(movieId, rating) != null) {
                        throw malformed(file, lineNumber, "duplicate user/movie pair", null);
                    }
                } catch (NumberFormatException error) {
                    throw malformed(file, lineNumber, "invalid numeric field", error);
                }
            }
        }

        filterUntilStable(ratings, minUserRatings, minMovieRatings);
        if (ratings.values().stream().allMatch(Map::isEmpty)) {
            throw new IllegalArgumentException("No ratings remain after filtering " + file);
        }
        return new MovieLensDataset(ratings);
    }

    private static boolean hasExpectedHeader(String header) {
        String[] fields = header.split(",", -1);
        return fields.length >= 3
                && fields[0].equals("userId")
                && fields[1].equals("movieId")
                && fields[2].equals("rating");
    }

    private static void filterUntilStable(Map<Integer, Map<Integer, Double>> ratings,
                                          int minUserRatings,
                                          int minMovieRatings) {
        boolean changed;
        do {
            Map<Integer, Long> counts = ratings.values().stream()
                    .flatMap(userRatings -> userRatings.keySet().stream())
                    .collect(Collectors.groupingBy(movieId -> movieId, Collectors.counting()));
            Set<Integer> removedMovies = counts.entrySet().stream()
                    .filter(entry -> entry.getValue() < minMovieRatings)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            int beforeUsers = ratings.size();
            int beforeRatings = ratings.values().stream().mapToInt(Map::size).sum();
            ratings.values().forEach(userRatings ->
                    removedMovies.forEach(userRatings::remove));
            ratings.entrySet().removeIf(entry -> entry.getValue().size() < minUserRatings);
            int afterRatings = ratings.values().stream().mapToInt(Map::size).sum();
            changed = beforeUsers != ratings.size() || beforeRatings != afterRatings;
        } while (changed);
    }

    private static IllegalArgumentException malformed(Path file, int lineNumber,
                                                       String detail, Throwable cause) {
        String message = "Malformed ratings row in " + file + " at line " + lineNumber + ": " + detail;
        return cause == null ? new IllegalArgumentException(message)
                : new IllegalArgumentException(message, cause);
    }

    public List<Integer> userIds() {
        return userIds;
    }

    public List<Integer> movieIds() {
        return movieIds;
    }

    public Map<Integer, Double> ratingsFor(int userId) {
        return ratingsByUser.getOrDefault(userId, Map.of());
    }

    public double rating(int userId, int movieId) {
        return ratingsFor(userId).get(movieId);
    }

    public Map<Integer, Integer> movieCounts() {
        return movieCounts;
    }

    public double globalMean() {
        return globalMean;
    }

    public double scoreExcludingUser(int userId, int movieId) {
        double sum = movieSums.get(movieId);
        int count = movieCounts.get(movieId);
        Double heldOut = ratingsFor(userId).get(movieId);
        if (heldOut != null) {
            sum -= heldOut;
            count--;
        }
        return (sum + PRIOR_STRENGTH * globalMean) / (count + PRIOR_STRENGTH);
    }
}
