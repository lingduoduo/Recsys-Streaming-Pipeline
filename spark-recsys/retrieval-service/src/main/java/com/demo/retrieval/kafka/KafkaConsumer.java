package com.demo.retrieval.kafka;

import java.util.List;

public class KafkaConsumer implements MessageSource {

    @SuppressWarnings("unused")
    private final KafkaConsumerConfig config;

    public KafkaConsumer(KafkaConsumerConfig config) {
        this.config = config;
    }

    public List<KafkaMessage> poll(int timeoutMs) {
        return List.of();
    }

    public void commitOffsets() {}

    public List<PartitionLag> getPartitionLags() {
        return List.of();
    }
}
