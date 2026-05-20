package com.demo.retrieval.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MovieLensEventProcessor {

    private static final AtomicInteger BATCH_LOG_COUNTER = new AtomicInteger(0);

    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler;

    public MovieLensEventProcessor(int numThreads) {
        this.executorService = Executors.newFixedThreadPool(numThreads);
        this.scheduler = Executors.newScheduledThreadPool(numThreads);
    }

    public void startProcessing(
            KafkaConsumerConfig baseConfig,
            KafkaProducerConfig producerConfig,
            Args args) {
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < args.getNumPartitions(); i++) {
            partitions.add(i);
        }

        KafkaProducer producer = new KafkaProducer(producerConfig);
        producer.start();

        spawnProcessingThreads(baseConfig, partitions, producer, args);
    }

    private void spawnProcessingThreads(
            KafkaConsumerConfig baseConfig,
            List<Integer> partitions,
            KafkaProducer producer,
            Args args) {
        int partitionsPerThread =
                (int) Math.ceil((double) partitions.size() / args.getKafkaNumThreads());

        for (int threadId = 0; threadId < args.getKafkaNumThreads(); threadId++) {
            int start = threadId * partitionsPerThread;
            int end = Math.min(start + partitionsPerThread, partitions.size());

            if (start >= partitions.size()) {
                break;
            }

            final int tid = threadId;
            KafkaConsumerConfig threadConfig =
                    baseConfig.withPartitions(new ArrayList<>(partitions.subList(start, end)));

            executorService.submit(() -> {
                try {
                    KafkaConsumer consumer = KafkaUtils.createKafkaConsumer(threadConfig);
                    startPartitionLagMonitor(
                            consumer,
                            threadConfig.baseConfig().topic(),
                            args.getLagMonitorIntervalSecs());
                    processMovieLensEvents(consumer, producer, args.getKafkaBatchSize());
                } catch (Exception e) {
                    throw new RuntimeException(
                            "MovieLens event processing thread failed: " + tid, e);
                }
            });
        }
    }

    private void processMovieLensEvents(
            KafkaConsumer consumer,
            KafkaProducer producer,
            int batchSize) throws Exception {
        List<KafkaMessage> buffer = new ArrayList<>();
        int batchNum = 0;

        while (true) {
            try {
                buffer.addAll(consumer.poll(100));

                if (buffer.size() >= batchSize) {
                    List<KafkaMessage> batch = new ArrayList<>(buffer);
                    buffer.clear();
                    processBatch(batch, ++batchNum, producer);
                    consumer.commitOffsets();
                }
            } catch (Exception e) {
                Metrics.KAFKA_POLL_ERRORS.inc();
                Thread.sleep(100);
            }
        }
    }

    private void processBatch(
            List<KafkaMessage> messages,
            int batchNum,
            KafkaProducer producer) throws Exception {
        List<MovieLensEvent> events =
                KafkaUtils.deserializeKafkaMessages(messages, MovieLensDeserializer::deserialize);

        List<RecSysEvent> outputEvents = new ArrayList<>();
        for (MovieLensEvent event : events) {
            if (event instanceof UserEvent e)             outputEvents.add(handleUserEvent(e));
            else if (event instanceof MovieEvent e)       outputEvents.add(handleMovieEvent(e));
            else if (event instanceof RatingEvent e)      outputEvents.add(handleRatingEvent(e));
            else if (event instanceof MovieInteractionEvent e) outputEvents.add(handleInteractionEvent(e));
        }

        for (RecSysEvent outEvent : outputEvents) {
            producer.send(outEvent.toByteArray());
        }

        int batchCount = BATCH_LOG_COUNTER.incrementAndGet();
        if (batchCount % 1000 == 0) {
            System.out.printf(
                    "Processed %d MovieLens batches, latest batch=%d, events=%d%n",
                    batchCount, batchNum, events.size());
        }
    }

    private RecSysEvent handleUserEvent(UserEvent event) {
        return RecSysEvent.userUpdated(
                event.getUserId(), event.getAge(), event.getGender(),
                event.getOccupation(), event.getZipCode(), event.getTimestamp());
    }

    private RecSysEvent handleMovieEvent(MovieEvent event) {
        return RecSysEvent.movieUpdated(
                event.getMovieId(), event.getTitle(), event.getGenres(),
                event.getReleaseYear(), event.getTimestamp());
    }

    private RecSysEvent handleRatingEvent(RatingEvent event) {
        return RecSysEvent.ratingCreated(
                event.getUserId(), event.getMovieId(),
                event.getRating(), event.getTimestamp());
    }

    private RecSysEvent handleInteractionEvent(MovieInteractionEvent event) {
        return RecSysEvent.interactionCreated(
                event.getUserId(), event.getMovieId(),
                event.getEventType(), event.getTimestamp());
    }

    private void startPartitionLagMonitor(
            KafkaConsumer consumer,
            String topic,
            long intervalSecs) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                for (PartitionLag lag : consumer.getPartitionLags()) {
                    Metrics.KAFKA_PARTITION_LAG
                            .labels(topic, String.valueOf(lag.getPartitionId()))
                            .set(lag.getLag());
                }
            } catch (Exception e) {
                System.err.println("Failed to monitor partition lag: " + e.getMessage());
            }
        }, 0, intervalSecs, TimeUnit.SECONDS);
    }
}
