package com.demo.retrieval.config;

import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import com.demo.retrieval.service.query_hydrators.RatingSequencesQueryHydrator;
import com.demo.retrieval.service.sequence.RedisSequenceClient;
import com.demo.retrieval.service.sequence.SequenceClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Clock;

@Configuration
public class SequenceConfig {

    @Bean
    public SequenceClient sequenceClient(StringRedisTemplate redis, RecommendationProperties properties) {
        return new RedisSequenceClient(redis, properties.getSequence().getBucketFetchChunk(), Clock.systemUTC());
    }

    @Bean
    public RatingSequencesQueryHydrator ratingSequencesQueryHydrator(
        MovieLensFeatureClient featureClient,
        SequenceClient sequenceClient,
        RecommendationProperties properties
    ) {
        return new RatingSequencesQueryHydrator(
            featureClient,
            sequenceClient,
            properties.getSequence().getMode(),
            properties.getSequence().getLookbackDays()
        );
    }
}
