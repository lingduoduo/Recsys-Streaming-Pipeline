package com.demo.retrieval.kafka;

public class KafkaProducer {

    @SuppressWarnings("unused")
    private final KafkaProducerConfig config;

    public KafkaProducer(KafkaProducerConfig config) {
        this.config = config;
    }

    public void start() {}

    public void send(byte[] payload) {}
}
