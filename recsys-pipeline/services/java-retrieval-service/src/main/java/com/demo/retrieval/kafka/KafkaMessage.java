package com.demo.retrieval.kafka;

public record KafkaMessage(byte[] payload, int partition, long offset) {}
