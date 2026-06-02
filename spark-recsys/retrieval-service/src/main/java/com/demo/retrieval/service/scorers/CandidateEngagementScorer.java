package com.demo.retrieval.service.scorers;

import com.demo.retrieval.config.RecommendationProperties.MovieProfile;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CandidateEngagementScorer {
    private static final double AUTHOR_DIVERSITY_DECAY = 0.72;
    private static final double AUTHOR_DIVERSITY_FLOOR = 0.55;

    public ScoringResult score(ScoringInput input) {
        EngagementProbabilities probabilities = engagementProbabilities(input);
        double weightedEngagement = weightedEngagement(probabilities);
        double exploitation = 0.55 * input.banditRankingScore()
            + 0.25 * weightedEngagement
            + 0.15 * clamp(input.dlScore())
            + 0.05 * clamp(input.qValue());
        double predictionScore = clamp(exploitation + input.explorationBonus());
        double estimatedReward = clamp(0.65 * input.posteriorMean() + 0.35 * weightedEngagement);
        return new ScoringResult(
            estimatedReward,
            weightedEngagement,
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

        Map<String, Integer> authorCounts = new HashMap<>();
        List<T> adjusted = new ArrayList<>(ordered.size());
        for (T candidate : ordered) {
            String key = diversityKey(candidate);
            int position = authorCounts.getOrDefault(key, 0);
            authorCounts.put(key, position + 1);

            double multiplier = diversityMultiplier(position);
            @SuppressWarnings("unchecked")
            T updated = (T) candidate.withDiversity(multiplier, candidate.preDiversityScore() * multiplier);
            adjusted.add(updated);
        }

        adjusted.sort(Comparator.comparingDouble(DiversityCandidate::finalScore).reversed());
        return List.copyOf(adjusted);
    }

    private EngagementProbabilities engagementProbabilities(ScoringInput input) {
        MovieProfile profile = input.profile();
        double ctr = input.impressions() <= 0 ? 0.0 : (double) input.clicks() / input.impressions();
        double smoothedCtr = (input.clicks() + 1.0) / (input.impressions() + 4.0);
        double social = profile == null ? 0.0 : clamp(Math.log1p(
            count(profile.getFavoriteCount())
                + count(profile.getReplyCount())
                + count(profile.getRepostCount())
                + count(profile.getQuoteCount())) / 100.0);
        double creator = profile == null || profile.getAuthorFollowersCount() == null
            ? 0.0
            : clamp(Math.log1p(Math.max(0, profile.getAuthorFollowersCount())) / 200.0);
        double network = profile != null && Boolean.TRUE.equals(profile.getInNetwork()) ? 1.0 : 0.0;
        double novelty = input.noveltyScore();

        double base = -1.15
            + 1.20 * clamp(input.relevance())
            + 0.85 * clamp(input.content())
            + 0.65 * clamp(input.popularity())
            + 0.75 * clamp(input.dlScore())
            + 0.55 * smoothedCtr
            + 0.45 * social
            + 0.20 * novelty;

        double click = sigmoid(base + 0.35 * ctr);
        double favorite = sigmoid(base - 0.15 + 0.55 * social + 0.15 * network);
        double reply = sigmoid(base - 0.55 + 0.35 * social + 0.20 * network);
        double repost = sigmoid(base - 0.45 + 0.40 * social + 0.10 * network);
        double quote = sigmoid(base - 0.70 + 0.30 * social);
        double dwell = sigmoid(base + 0.10 + 0.25 * input.content());
        double followAuthor = sigmoid(base - 0.90 + 0.30 * creator + 0.20 * network);
        double negative = sigmoid(-base - 0.40 + 0.25 * Math.max(0.0, 0.08 - smoothedCtr));

        return new EngagementProbabilities(favorite, reply, repost, quote, click, dwell, followAuthor, negative);
    }

    private double weightedEngagement(EngagementProbabilities p) {
        double positive = 0.22 * p.favorite()
            + 0.16 * p.reply()
            + 0.14 * p.repost()
            + 0.12 * p.quote()
            + 0.18 * p.click()
            + 0.18 * p.dwell()
            + 0.08 * p.followAuthor();
        return clamp(positive - 0.35 * p.negative());
    }

    private String diversityKey(DiversityCandidate candidate) {
        String author = candidate.authorId();
        if (author != null && !author.isBlank()) {
            return "author:" + author;
        }
        String genre = candidate.primaryGenre();
        if (genre != null && !genre.isBlank()) {
            return "genre:" + genre;
        }
        return "item:" + candidate.itemId();
    }

    private double diversityMultiplier(int position) {
        return (1.0 - AUTHOR_DIVERSITY_FLOOR) * Math.pow(AUTHOR_DIVERSITY_DECAY, position) + AUTHOR_DIVERSITY_FLOOR;
    }

    private static long count(Long value) {
        return value == null ? 0L : Math.max(0L, value);
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
        MovieProfile profile,
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

    public record EngagementProbabilities(
        double favorite,
        double reply,
        double repost,
        double quote,
        double click,
        double dwell,
        double followAuthor,
        double negative
    ) {
        public double overall() {
            return clamp((favorite + reply + repost + quote + click + dwell + followAuthor) / 7.0 - 0.25 * negative);
        }
    }

    public record ScoringResult(
        double estimatedReward,
        double weightedEngagementScore,
        double engagementProbability,
        double predictionScore,
        double diversityScore,
        double finalScore
    ) {
    }

    public interface DiversityCandidate {
        String itemId();

        String authorId();

        String primaryGenre();

        double preDiversityScore();

        double finalScore();

        DiversityCandidate withDiversity(double diversityScore, double finalScore);
    }
}
