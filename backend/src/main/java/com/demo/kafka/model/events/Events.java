package com.demo.kafka.model.events;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Regroupe les differents payloads diffuses via WebSocket.
 * Chacun est enveloppe dans un DemoEvent avec son "type" associe.
 */
public final class Events {

    private Events() {
    }

    public static final String PRODUCER_MESSAGE = "PRODUCER_MESSAGE";
    public static final String PRODUCER_ERROR = "PRODUCER_ERROR";
    public static final String PRODUCER_CONFIG = "PRODUCER_CONFIG";
    public static final String CONSUMER_MESSAGE = "CONSUMER_MESSAGE";
    public static final String CONSUMER_ASSIGNED = "CONSUMER_ASSIGNED";
    public static final String CONSUMER_ADDED = "CONSUMER_ADDED";
    public static final String CONSUMER_REMOVED = "CONSUMER_REMOVED";
    public static final String DLT_MESSAGE = "DLT_MESSAGE";
    public static final String TOPIC_CONFIG = "TOPIC_CONFIG";
    public static final String STATE_SNAPSHOT = "STATE_SNAPSHOT";
    public static final String CONSUMER_SEEK = "CONSUMER_SEEK";

    public record ProducerMessage(
            long id,
            String key,
            int partition,
            long offset,
            Instant timestamp,
            boolean error
    ) {
    }

    public record ProducerError(long id, String key, String reason, Instant timestamp) {
    }

    public record ProducerConfig(boolean running, long intervalMs, int errorRatePercent) {
    }

    public record ConsumerMessage(
            String consumerId,
            int partition,
            long offset,
            String key,
            Instant timestamp,
            long lag
    ) {
    }

    public record ConsumerAssigned(String consumerId, List<Integer> partitions) {
    }

    public record ConsumerAdded(String consumerId) {
    }

    public record ConsumerRemoved(String consumerId) {
    }

    public record DltMessage(long originalId, int partition, long offset, String key, String reason,
                              Instant timestamp) {
    }

    public record TopicConfig(String topic, int partitions) {
    }

    /**
     * Diffuse quand un offset est repositionne manuellement (debut / fin / valeur precise) :
     * permet au front de montrer visuellement le "retour en arriere" sur la pile du consumer.
     */
    public record ConsumerSeek(String consumerId, int partition, long newOffset, String mode) {
    }

    public record ConsumerSnapshot(String consumerId, List<Integer> partitions, boolean paused) {
    }

    public record StateSnapshot(
            ProducerConfig producer,
            TopicConfig topic,
            List<ConsumerSnapshot> consumers,
            Map<Integer, Long> endOffsets
    ) {
    }
}
