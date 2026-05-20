package com.demo.retrieval.kafka;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public class KafkaStarter {

    private static final String TWEET_EVENT_TOPIC        = "";
    private static final String TWEET_EVENT_DEST         = "";
    private static final String IN_NETWORK_EVENTS_DEST   = "";
    private static final String IN_NETWORK_EVENTS_TOPIC  = "";

    public static CompletableFuture<Void> startKafka(
            Args args,
            PostStore postStore,
            String user,
            BlockingQueue<Long> tx) {
        return CompletableFuture.runAsync(() -> {
            String saslPassword = Optional
                    .ofNullable(System.getenv(""))
                    .orElse(args.getSaslPassword());
            String producerSaslPassword = Optional
                    .ofNullable(System.getenv(""))
                    .orElse(args.getProducerSaslPassword());

            if (args.isServing()) {
                KafkaConsumerConfig v2ConsumerConfig = KafkaUtils.consumerConfig(
                    KafkaUtils.kafkaConfig(
                        args.getInNetworkEventsConsumerDest(),
                        IN_NETWORK_EVENTS_TOPIC,
                        KafkaUtils.producerSsl(args, producerSaslPassword)),
                    args.getKafkaGroupId() + "-" + UUID.randomUUID(),
                    args, true, 1024 * 1024 * 100);
                TweetEventsListenerV2.startTweetEventProcessingV2(v2ConsumerConfig, postStore, args, tx);
            } else {
                KafkaConsumerConfig tweetEventsConsumerConfig = KafkaUtils.consumerConfig(
                    KafkaUtils.kafkaConfig(
                        TWEET_EVENT_DEST,
                        TWEET_EVENT_TOPIC,
                        KafkaUtils.consumerSsl(args, saslPassword)),
                    args.getKafkaGroupId() + "-" + user,
                    args, false, 1024 * 1024 * 10);
                KafkaProducerConfig producerConfig = KafkaUtils.producerConfig(
                    KafkaUtils.kafkaConfig(
                        IN_NETWORK_EVENTS_DEST,
                        IN_NETWORK_EVENTS_TOPIC,
                        KafkaUtils.producerSsl(args, producerSaslPassword)));
                TweetEventsListener.startTweetEventProcessing(tweetEventsConsumerConfig, producerConfig, args);
            }
        });
    }
}
