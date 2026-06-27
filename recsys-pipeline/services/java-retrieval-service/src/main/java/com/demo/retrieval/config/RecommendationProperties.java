package com.demo.retrieval.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "recsys")
public class RecommendationProperties {
    private Cache cache = new Cache();
    private Embeddings embeddings = new Embeddings();
    private CandidateGeneration candidateGeneration = new CandidateGeneration();
    private Filtering filtering = new Filtering();
    private Bandit bandit = new Bandit();
    private ReplayBuffer replayBuffer = new ReplayBuffer();
    private RewardModel rewardModel = new RewardModel();
    private Map<String, MovieProfile> catalog = new LinkedHashMap<>();
    // Optional path to a catalog JSON file ({itemId: MovieProfile}). When set, its entries are
    // merged on top of the inline `catalog` at startup so the catalog can outgrow application.yml
    // and stay in sync with the Python pipeline's catalog. Empty = use inline catalog only.
    private String catalogPath = "";

    public Embeddings getEmbeddings() {
        return embeddings;
    }

    public void setEmbeddings(Embeddings embeddings) {
        this.embeddings = embeddings;
    }

    public CandidateGeneration getCandidateGeneration() {
        return candidateGeneration;
    }

    public void setCandidateGeneration(CandidateGeneration candidateGeneration) {
        this.candidateGeneration = candidateGeneration;
    }

    public Filtering getFiltering() {
        return filtering;
    }

    public void setFiltering(Filtering filtering) {
        this.filtering = filtering;
    }

    public Bandit getBandit() {
        return bandit;
    }

    public void setBandit(Bandit bandit) {
        this.bandit = bandit;
    }

    public ReplayBuffer getReplayBuffer() {
        return replayBuffer;
    }

    public void setReplayBuffer(ReplayBuffer replayBuffer) {
        this.replayBuffer = replayBuffer;
    }

    public RewardModel getRewardModel() {
        return rewardModel;
    }

    public void setRewardModel(RewardModel rewardModel) {
        this.rewardModel = rewardModel;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }

    public Map<String, MovieProfile> getCatalog() {
        return catalog;
    }

    public void setCatalog(Map<String, MovieProfile> catalog) {
        this.catalog = catalog;
    }

    public String getCatalogPath() {
        return catalogPath;
    }

    public void setCatalogPath(String catalogPath) {
        this.catalogPath = catalogPath;
    }

    public static class Cache {
        private int itemVectorMaxSize = 10_000;
        private long itemVectorTtlSeconds = 300;
        private int rewardMaxSize = 50_000;
        private long rewardTtlSeconds = 30;

        public int getItemVectorMaxSize() { return itemVectorMaxSize; }
        public void setItemVectorMaxSize(int itemVectorMaxSize) { this.itemVectorMaxSize = itemVectorMaxSize; }
        public long getItemVectorTtlSeconds() { return itemVectorTtlSeconds; }
        public void setItemVectorTtlSeconds(long itemVectorTtlSeconds) { this.itemVectorTtlSeconds = itemVectorTtlSeconds; }
        public int getRewardMaxSize() { return rewardMaxSize; }
        public void setRewardMaxSize(int rewardMaxSize) { this.rewardMaxSize = rewardMaxSize; }
        public long getRewardTtlSeconds() { return rewardTtlSeconds; }
        public void setRewardTtlSeconds(long rewardTtlSeconds) { this.rewardTtlSeconds = rewardTtlSeconds; }
    }

    public static class Embeddings {
        private String itemPrefix = "i2vEmb";
        private String userPrefix = "uEmb";

        public String getItemPrefix() {
            return itemPrefix;
        }

        public void setItemPrefix(String itemPrefix) {
            this.itemPrefix = itemPrefix;
        }

        public String getUserPrefix() {
            return userPrefix;
        }

        public void setUserPrefix(String userPrefix) {
            this.userPrefix = userPrefix;
        }
    }

    public static class CandidateGeneration {
        private int popularityFetchMultiplier = 5;
        private int coldStartPoolSize = 25;
        private int topNRandomizationPool = 5;
        private int candidatePoolSize = 0; // 0 = auto: limit × popularityFetchMultiplier

        public int getPopularityFetchMultiplier() {
            return popularityFetchMultiplier;
        }

        public void setPopularityFetchMultiplier(int popularityFetchMultiplier) {
            this.popularityFetchMultiplier = popularityFetchMultiplier;
        }

        public int getColdStartPoolSize() {
            return coldStartPoolSize;
        }

        public void setColdStartPoolSize(int coldStartPoolSize) {
            this.coldStartPoolSize = coldStartPoolSize;
        }

        public int getTopNRandomizationPool() {
            return topNRandomizationPool;
        }

        public void setTopNRandomizationPool(int topNRandomizationPool) {
            this.topNRandomizationPool = topNRandomizationPool;
        }

        public int getCandidatePoolSize() {
            return candidatePoolSize;
        }

        public void setCandidatePoolSize(int candidatePoolSize) {
            this.candidatePoolSize = candidatePoolSize;
        }
    }

    public static class Filtering {
        private boolean enabled = true;
        private List<String> mutedProductTypes = new ArrayList<>();
        private List<String> mutedGenres = new ArrayList<>();
        private List<String> mutedKeywords = new ArrayList<>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public List<String> getMutedProductTypes() {
            return mutedProductTypes;
        }

        public void setMutedProductTypes(List<String> mutedProductTypes) {
            this.mutedProductTypes = mutedProductTypes;
        }

        public List<String> getMutedGenres() {
            return mutedGenres;
        }

        public void setMutedGenres(List<String> mutedGenres) {
            this.mutedGenres = mutedGenres;
        }

        public List<String> getMutedKeywords() {
            return mutedKeywords;
        }

        public void setMutedKeywords(List<String> mutedKeywords) {
            this.mutedKeywords = mutedKeywords;
        }

    }

    public static class Bandit {
        private String algorithm = "ucb";
        private double explorationAlpha = 0.75;
        private double maxExplorationBonus = 0.25;
        private int coldStartExposureThreshold = 5;
        private double coldStartBoost = 1.35;
        private double relevanceWeight = 0.6;
        private double contentWeight = 0.25;
        private double popularityWeight = 0.15;
        private double deepLearningWeight = 0.0;
        private double qLearningAlpha = 0.1;
        private double qLearningGamma = 0.9;
        private double qLearningEpsilon = 0.1;

        public String getAlgorithm() {
            return algorithm;
        }

        public void setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
        }

        public double getExplorationAlpha() {
            return explorationAlpha;
        }

        public void setExplorationAlpha(double explorationAlpha) {
            this.explorationAlpha = explorationAlpha;
        }

        public double getMaxExplorationBonus() {
            return maxExplorationBonus;
        }

        public void setMaxExplorationBonus(double maxExplorationBonus) {
            this.maxExplorationBonus = maxExplorationBonus;
        }

        public int getColdStartExposureThreshold() {
            return coldStartExposureThreshold;
        }

        public void setColdStartExposureThreshold(int coldStartExposureThreshold) {
            this.coldStartExposureThreshold = coldStartExposureThreshold;
        }

        public double getColdStartBoost() {
            return coldStartBoost;
        }

        public void setColdStartBoost(double coldStartBoost) {
            this.coldStartBoost = coldStartBoost;
        }

        public double getRelevanceWeight() {
            return relevanceWeight;
        }

        public void setRelevanceWeight(double relevanceWeight) {
            this.relevanceWeight = relevanceWeight;
        }

        public double getContentWeight() {
            return contentWeight;
        }

        public void setContentWeight(double contentWeight) {
            this.contentWeight = contentWeight;
        }

        public double getPopularityWeight() {
            return popularityWeight;
        }

        public void setPopularityWeight(double popularityWeight) {
            this.popularityWeight = popularityWeight;
        }

        public double getDeepLearningWeight() {
            return deepLearningWeight;
        }

        public void setDeepLearningWeight(double deepLearningWeight) {
            this.deepLearningWeight = deepLearningWeight;
        }

        public double getQLearningAlpha() {
            return qLearningAlpha;
        }

        public void setQLearningAlpha(double qLearningAlpha) {
            this.qLearningAlpha = qLearningAlpha;
        }

        public double getQLearningGamma() {
            return qLearningGamma;
        }

        public void setQLearningGamma(double qLearningGamma) {
            this.qLearningGamma = qLearningGamma;
        }

        public double getQLearningEpsilon() {
            return qLearningEpsilon;
        }

        public void setQLearningEpsilon(double qLearningEpsilon) {
            this.qLearningEpsilon = qLearningEpsilon;
        }
    }

    public static class ReplayBuffer {
        private int maxSize = 10000;
        private int candidateSnapshotSize = 20;

        public int getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        public int getCandidateSnapshotSize() {
            return candidateSnapshotSize;
        }

        public void setCandidateSnapshotSize(int candidateSnapshotSize) {
            this.candidateSnapshotSize = candidateSnapshotSize;
        }
    }

    public static class RewardModel {
        private double weight = 0.25;
        private double globalWeight = 0.15;
        private double itemWeight = 0.45;
        private double genreWeight = 0.25;
        private double tagWeight = 0.15;
        private int minFeatureCount = 3;

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }

        public double getGlobalWeight() {
            return globalWeight;
        }

        public void setGlobalWeight(double globalWeight) {
            this.globalWeight = globalWeight;
        }

        public double getItemWeight() {
            return itemWeight;
        }

        public void setItemWeight(double itemWeight) {
            this.itemWeight = itemWeight;
        }

        public double getGenreWeight() {
            return genreWeight;
        }

        public void setGenreWeight(double genreWeight) {
            this.genreWeight = genreWeight;
        }

        public double getTagWeight() {
            return tagWeight;
        }

        public void setTagWeight(double tagWeight) {
            this.tagWeight = tagWeight;
        }

        public int getMinFeatureCount() {
            return minFeatureCount;
        }

        public void setMinFeatureCount(int minFeatureCount) {
            this.minFeatureCount = minFeatureCount;
        }
    }

    public static class MovieProfile {
        private String title;
        private String productType;
        private List<String> genres = new ArrayList<>();
        private List<String> tags = new ArrayList<>();
        private List<String> keywords = new ArrayList<>();
        private boolean newRelease;
        private long expiresAtEpochMillis;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getProductType() {
            return productType;
        }

        public void setProductType(String productType) {
            this.productType = productType;
        }

        public List<String> getGenres() {
            return genres;
        }

        public void setGenres(List<String> genres) {
            this.genres = genres;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public List<String> getKeywords() {
            return keywords;
        }

        public void setKeywords(List<String> keywords) {
            this.keywords = keywords;
        }

        public boolean isNewRelease() {
            return newRelease;
        }

        public void setNewRelease(boolean newRelease) {
            this.newRelease = newRelease;
        }

        public long getExpiresAtEpochMillis() {
            return expiresAtEpochMillis;
        }

        public void setExpiresAtEpochMillis(long expiresAtEpochMillis) {
            this.expiresAtEpochMillis = expiresAtEpochMillis;
        }
    }
}
