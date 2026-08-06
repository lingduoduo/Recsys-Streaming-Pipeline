package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/** Reads the profile belonging to the run selected by the Redis active-run pointer. */
@Component
public class RedisUserProfileClient implements UserProfileClient {
    private static final Logger log = LoggerFactory.getLogger(RedisUserProfileClient.class);
    private static final int PROFILE_VERSION = 1;

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final String keyPrefix;

    public RedisUserProfileClient(
        StringRedisTemplate redis,
        ObjectMapper objectMapper,
        MeterRegistry meterRegistry,
        @Value("${recsys.user-profile.key-prefix:user-profile:v1}") String keyPrefix
    ) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public Optional<UserBehaviorProfile> getProfile(String userId) {
        Timer.Sample timer = Timer.start(meterRegistry);
        try {
            String runId = redis.opsForValue().get(keyPrefix + ":active-run");
            if (runId == null || runId.isBlank()) {
                return fallback("missing_active_run");
            }
            String rawProfile = redis.opsForValue().get(keyPrefix + ":" + runId + ":" + userId);
            if (rawProfile == null) {
                return fallback("missing_profile");
            }
            UserBehaviorProfile profile = objectMapper.readValue(rawProfile, UserBehaviorProfile.class);
            if (profile.profileVersion() != PROFILE_VERSION) {
                return fallback("unsupported_version");
            }
            if (!userId.equals(profile.userId())) {
                return fallback("user_mismatch");
            }
            return Optional.of(normalizePreferenceNames(profile));
        } catch (JsonProcessingException e) {
            log.warn("Unable to parse user profile for user {}", userId);
            return fallback("invalid_json");
        } catch (RuntimeException e) {
            log.warn("User profile lookup failed for user {}", userId);
            return fallback("redis_error");
        } finally {
            timer.stop(Timer.builder("profile.lookup").register(meterRegistry));
        }
    }

    private Optional<UserBehaviorProfile> fallback(String reason) {
        Counter.builder("profile.lookup.fallback").tag("reason", reason).register(meterRegistry).increment();
        return Optional.empty();
    }

    private static UserBehaviorProfile normalizePreferenceNames(UserBehaviorProfile profile) {
        UserBehaviorProfile.Preferences preferences = profile.preferences();
        if (preferences == null) {
            return profile;
        }
        return new UserBehaviorProfile(
            profile.userId(),
            profile.profileVersion(),
            profile.runId(),
            profile.generatedAt(),
            profile.sourceWindow(),
            profile.evidenceCount(),
            new UserBehaviorProfile.Preferences(normalize(preferences.genres()), normalize(preferences.tags())),
            profile.behavioralFeatures(),
            profile.personas()
        );
    }

    private static List<UserBehaviorProfile.Preference> normalize(List<UserBehaviorProfile.Preference> preferences) {
        return preferences.stream().map(preference -> new UserBehaviorProfile.Preference(
            preference.value() == null ? null : preference.value().trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT),
            preference.score(),
            preference.evidenceCount()
        )).toList();
    }
}
