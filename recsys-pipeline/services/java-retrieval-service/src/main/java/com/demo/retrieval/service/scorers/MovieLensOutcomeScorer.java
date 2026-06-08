package com.demo.retrieval.service.scorers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MovieLensOutcomeScorer {
    private static final double DIVERSITY_DECAY = 0.72;
    private static final double DIVERSITY_FLOOR = 0.55;

    public ScoringResult score(ScoringInput input) {
        MovieLensOutcomeProbabilities probabilities = movieLensOutcomeProbabilities(input);
        double weightedOutcome = weightedOutcome(probabilities);
        double exploitation = 0.55 * input.banditRankingScore()
            + 0.25 * weightedOutcome
            + 0.15 * clamp(input.dlScore())
            + 0.05 * clamp(input.qValue());
        double predictionScore = clamp(exploitation + input.explorationBonus());
        double estimatedReward = clamp(0.65 * input.posteriorMean() + 0.35 * weightedOutcome);
        return new ScoringResult(
            estimatedReward,
            weightedOutcome,
            probabilities.overall(),
            predictionScore,
            1.0,
            predictionScore
        );
    }

    public <T extends DiversityCandidate> List<T> applyDiversity(List<T> candidates) {
        if (candidates.size() < 2) {
            return candidates;
        }

        List<T> ordered = new ArrayList<>(candidates);
        ordered.sort(Comparator.comparingDouble(DiversityCandidate::preDiversityScore).reversed());

        Map<String, Integer> diversityCounts = new HashMap<>();
        List<T> adjusted = new ArrayList<>(ordered.size());
        for (T candidate : ordered) {
            String key = diversityKey(candidate);
            int position = diversityCounts.getOrDefault(key, 0);
            diversityCounts.put(key, position + 1);

            double multiplier = diversityMultiplier(position);
            @SuppressWarnings("unchecked")
            T updated = (T) candidate.withDiversity(multiplier, candidate.preDiversityScore() * multiplier);
            adjusted.add(updated);
        }

        adjusted.sort(Comparator.comparingDouble(DiversityCandidate::finalScore).reversed());
        return List.copyOf(adjusted);
    }

    private MovieLensOutcomeProbabilities movieLensOutcomeProbabilities(ScoringInput input) {
        double ctr = input.impressions() <= 0 ? 0.0 : (double) input.clicks() / input.impressions();
        double smoothedCtr = (input.clicks() + 1.0) / (input.impressions() + 4.0);
        double novelty = input.noveltyScore();

        double base = -1.15
            + 1.20 * clamp(input.relevance())
            + 0.85 * clamp(input.content())
            + 0.65 * clamp(input.popularity())
            + 0.75 * clamp(input.dlScore())
            + 0.55 * smoothedCtr
            + 0.20 * novelty;

        double click = sigmoid(base + 0.35 * ctr);
        double positiveRating = sigmoid(base - 0.05 + 0.20 * input.relevance());
        double preference = sigmoid(base - 0.15 + 0.15 * input.content());
        double watch = sigmoid(base + 0.10 + 0.25 * input.content());
        double novelDiscovery = sigmoid(base - 0.70 + 0.20 * novelty);
        double negativeFeedback = sigmoid(-base - 0.40 + 0.25 * Math.max(0.0, 0.08 - smoothedCtr));

        return new MovieLensOutcomeProbabilities(
            positiveRating,
            preference,
            click,
            watch,
            novelDiscovery,
            negativeFeedback
        );
    }

    private double weightedOutcome(MovieLensOutcomeProbabilities p) {
        double positive = 0.30 * p.positiveRating()
            + 0.22 * p.preference()
            + 0.18 * p.click()
            + 0.22 * p.watch()
            + 0.08 * p.novelDiscovery();
        return clamp(positive - 0.35 * p.negativeFeedback());
    }

    private String diversityKey(DiversityCandidate candidate) {
        String diversityGroup = candidate.diversityGroupId();
        if (diversityGroup != null && !diversityGroup.isBlank()) {
            return "group:" + diversityGroup;
        }
        String genre = candidate.primaryGenre();
        if (genre != null && !genre.isBlank()) {
            return "genre:" + genre;
        }
        return "item:" + candidate.itemId();
    }

    private double diversityMultiplier(int position) {
        return (1.0 - DIVERSITY_FLOOR) * Math.pow(DIVERSITY_DECAY, position) + DIVERSITY_FLOOR;
    }

    private static double sigmoid(double value) {
        return 1.0 / (1.0 + Math.exp(-value));
    }

    private static double clamp(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, value));
    }

    public record ScoringInput(
        String itemId,
        double relevance,
        double content,
        double popularity,
        double posteriorMean,
        double banditRankingScore,
        double explorationBonus,
        double noveltyScore,
        double dlScore,
        double qValue,
        long impressions,
        long clicks
    ) {
    }

    public record MovieLensOutcomeProbabilities(
        double positiveRating,
        double preference,
        double click,
        double watch,
        double novelDiscovery,
        double negativeFeedback
    ) {
        public double overall() {
            return clamp((positiveRating + preference + click + watch + novelDiscovery) / 5.0
                - 0.25 * negativeFeedback);
        }
    }

    public record ScoringResult(
        double estimatedReward,
        double weightedOutcomeScore,
        double outcomeProbability,
        double predictionScore,
        double diversityScore,
        double finalScore
    ) {
    }

    public interface DiversityCandidate {
        String itemId();

        String diversityGroupId();

        String primaryGenre();

        double preDiversityScore();

        double finalScore();

        DiversityCandidate withDiversity(double diversityScore, double finalScore);
    }
}
