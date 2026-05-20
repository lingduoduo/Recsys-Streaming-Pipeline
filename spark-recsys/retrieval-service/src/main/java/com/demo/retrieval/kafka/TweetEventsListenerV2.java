package com.demo.retrieval.kafka;

import java.util.concurrent.BlockingQueue;

public class TweetEventsListenerV2 {
    public static void startTweetEventProcessingV2(
            KafkaConsumerConfig config,
            PostStore postStore,
            Args args,
            BlockingQueue<Long> tx) {
        throw new UnsupportedOperationException("not implemented");
    }
}
