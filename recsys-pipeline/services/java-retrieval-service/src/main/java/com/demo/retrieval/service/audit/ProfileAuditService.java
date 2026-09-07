package com.demo.retrieval.service.audit;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.demo.retrieval.service.audit.ProfileAuditReport.Summary;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.audit.ProfileAuditStore.ScanResult;
import com.demo.retrieval.service.content.CatalogContentScoring;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Walks user → has_profile → preferences → catalog content, breadth-first by level, and returns
 * a findings-only report. One audit at a time; level-1 chunks fan out over a small dedicated
 * executor where each task owns exactly one pipelined round trip.
 */
@Component
public class ProfileAuditService {

    public static final class AuditBusyException extends RuntimeException {
        public AuditBusyException() {
            super("a profile audit is already running");
        }
    }

    public static final class ProfileAuditFailedException extends RuntimeException {
        public ProfileAuditFailedException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    private final ProfileAuditStore store;
    private final RecommendationProperties.ProfileAudit config;
    private final CatalogContentScoring catalogScoring;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ProfileAuditService(ProfileAuditStore store, RecommendationProperties properties, ObjectMapper objectMapper) {
        this.store = store;
        this.config = properties.getProfileAudit();
        this.catalogScoring = new CatalogContentScoring(properties);
        this.objectMapper = objectMapper;
        this.executor = Executors.newFixedThreadPool(config.getParallelism());
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }

    /** {@code limit} null means {@code max-users}; otherwise it must lie in 1..max-users. */
    public ProfileAuditReport audit(Integer limit) {
        int maxUsers = config.getMaxUsers();
        int effectiveLimit = limit == null ? maxUsers : limit;
        if (effectiveLimit < 1 || effectiveLimit > maxUsers) {
            throw new IllegalArgumentException("limit must be between 1 and " + maxUsers);
        }
        if (!running.compareAndSet(false, true)) {
            throw new AuditBusyException();
        }
        long started = System.nanoTime();
        try {
            CatalogPreferenceIndex index = CatalogPreferenceIndex.build(catalogScoring.normalizedCatalog());
            ScanResult scan = store.scanUserIds(config.getUserKeyPattern(), effectiveLimit);
            String activeRun = store.activeRun().orElse(null);
            Aggregation aggregation = new Aggregation();

            if (activeRun == null) {
                aggregation.noProfileByReason.put(ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN, scan.userIds().size());
            } else {
                for (RawProfile raw : readAllProfiles(activeRun, scan.userIds())) {
                    aggregation.add(UserAuditClassifier.classify(raw, activeRun, index, objectMapper, config.getSampleItems()));
                }
            }

            return new ProfileAuditReport(
                activeRun == null ? ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN : ProfileAuditReport.STATUS_OK,
                activeRun,
                Instant.now().toString(),
                (System.nanoTime() - started) / 1_000_000L,
                scan.truncated(),
                aggregation.summary(scan.userIds().size(), index.catalogSize()),
                aggregation.rows
            );
        } catch (RuntimeException e) {
            if (e instanceof ProfileAuditFailedException) {
                throw e;
            }
            throw new ProfileAuditFailedException(e);
        } finally {
            running.set(false);
        }
    }

    /** Fans chunks over the executor, waits for all of them, then rethrows the first failure in chunk order. */
    private List<RawProfile> readAllProfiles(String activeRun, List<String> userIds) {
        List<CompletableFuture<List<RawProfile>>> futures = new ArrayList<>();
        for (int from = 0; from < userIds.size(); from += config.getChunkSize()) {
            List<String> chunk = userIds.subList(from, Math.min(from + config.getChunkSize(), userIds.size()));
            futures.add(CompletableFuture.supplyAsync(() -> store.readProfiles(activeRun, chunk), executor));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).exceptionally(t -> null).join();
        List<RawProfile> all = new ArrayList<>(userIds.size());
        for (CompletableFuture<List<RawProfile>> future : futures) {
            try {
                all.addAll(future.join());
            } catch (CompletionException e) {
                throw new ProfileAuditFailedException(e.getCause() == null ? e : e.getCause());
            }
        }
        return all;
    }

    private static final class Aggregation {
        final List<UserRow> rows = new ArrayList<>();
        final Map<String, Integer> noProfileByReason = new TreeMap<>();
        final Map<String, Integer> findingsByType = new TreeMap<>();
        final Map<String, Map<String, Integer>> unmatchedPreferences = new TreeMap<>();
        int usersWithProfile;
        int usersHealthy;
        Long minTtl;

        void add(UserRow row) {
            if (row.hasProfile()) {
                usersWithProfile++;
                if (row.ttlSeconds() != null && (minTtl == null || row.ttlSeconds() < minTtl)) {
                    minTtl = row.ttlSeconds();
                }
            }
            if (row.findings().isEmpty()) {
                usersHealthy++;
                return;
            }
            rows.add(row);
            for (Finding finding : row.findings()) {
                if (ProfileAuditReport.FINDING_NO_PROFILE.equals(finding.type())) {
                    noProfileByReason.merge(finding.reason(), 1, Integer::sum);
                    continue;
                }
                findingsByType.merge(finding.type(), 1, Integer::sum);
                if (finding.preferences() != null) {
                    for (PreferenceRef ref : new LinkedHashSet<>(finding.preferences())) {
                        unmatchedPreferences.computeIfAbsent(ref.kind(), k -> new TreeMap<>()).merge(ref.value(), 1, Integer::sum);
                    }
                }
            }
        }

        Summary summary(int usersScanned, int catalogSize) {
            return new Summary(usersScanned, usersWithProfile, usersHealthy,
                noProfileByReason, findingsByType, unmatchedPreferences, minTtl, catalogSize);
        }
    }
}
