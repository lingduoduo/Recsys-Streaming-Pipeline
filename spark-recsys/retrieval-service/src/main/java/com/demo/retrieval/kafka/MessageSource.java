package com.demo.retrieval.kafka;

import java.util.List;

public interface MessageSource {
    List<KafkaMessage> poll(int timeoutMs);
    void commitOffsets();
    List<PartitionLag> getPartitionLags();

    // Returns true for finite sources (e.g. local files) so callers can exit the poll loop.
    default boolean isFinite() { return false; }
}
