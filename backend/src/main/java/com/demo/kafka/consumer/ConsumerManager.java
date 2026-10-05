package com.demo.kafka.consumer;

import com.demo.kafka.admin.KafkaAdminService;
import com.demo.kafka.model.events.Events;
import com.demo.kafka.ws.EventBroadcaster;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Gere dynamiquement le pool de consumers de la demo. Augmenter/diminuer le nombre
 * de consumers demarre/arrete des ConsumerWorker reels, tous membres du meme groupe,
 * ce qui declenche un rebalance Kafka naturel (visible dans l'UI).
 */
@Service
public class ConsumerManager {

    private final Map<String, ConsumerWorker> workers = new ConcurrentHashMap<>();
    private final AtomicInteger idSeq = new AtomicInteger(0);

    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final String dltTopic;
    private final EventBroadcaster broadcaster;
    private final KafkaTemplate<String, String> dltTemplate;

    public ConsumerManager(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                            @Value("${demo.consumer.group-id}") String groupId,
                            @Value("${demo.consumer.initial-count}") int initialCount,
                            KafkaAdminService adminService,
                            EventBroadcaster broadcaster,
                            KafkaTemplate<String, String> dltTemplate) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = adminService.mainTopic();
        this.dltTopic = adminService.dltTopic();
        this.broadcaster = broadcaster;
        this.dltTemplate = dltTemplate;
        setConsumerCount(initialCount);
    }

    public synchronized int setConsumerCount(int target) {
        target = Math.max(0, target);
        while (workers.size() < target) {
            addConsumer();
        }
        while (workers.size() > target) {
            removeOneConsumer();
        }
        return workers.size();
    }

    private void addConsumer() {
        String id = "c-" + idSeq.incrementAndGet();
        ConsumerWorker worker = new ConsumerWorker(id, bootstrapServers, groupId, topic, dltTopic,
                broadcaster, dltTemplate);
        workers.put(id, worker);
        worker.start();
        broadcaster.broadcast(Events.CONSUMER_ADDED, new Events.ConsumerAdded(id));
    }

    private void removeOneConsumer() {
        // On retire le dernier consumer ajoute : comportement previsible et lisible pour la demo.
        String lastId = workers.keySet().stream()
                .max((a, b) -> Integer.compare(extractSeq(a), extractSeq(b)))
                .orElse(null);
        if (lastId == null) {
            return;
        }
        removeConsumer(lastId);
    }

    public synchronized void removeConsumer(String id) {
        ConsumerWorker worker = workers.remove(id);
        if (worker != null) {
            worker.shutdown();
            broadcaster.broadcast(Events.CONSUMER_REMOVED, new Events.ConsumerRemoved(id));
        }
    }

    private int extractSeq(String id) {
        try {
            return Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    public List<Events.ConsumerSnapshot> snapshot() {
        return workers.values().stream()
                .map(w -> new Events.ConsumerSnapshot(w.id(), w.assignedPartitions(), false))
                .toList();
    }

    public int count() {
        return workers.size();
    }

    private ConsumerWorker require(String id) {
        ConsumerWorker worker = workers.get(id);
        if (worker == null) {
            throw new IllegalArgumentException("Consumer inconnu : " + id);
        }
        return worker;
    }

    public void pausePartition(String consumerId, int partition) {
        require(consumerId).pausePartition(partition);
    }

    public void resumePartition(String consumerId, int partition) {
        require(consumerId).resumePartition(partition);
    }

    public void seekToBeginning(String consumerId, int partition) {
        require(consumerId).seekToBeginning(partition);
    }

    public void seekToEnd(String consumerId, int partition) {
        require(consumerId).seekToEnd(partition);
    }

    public void seekToOffset(String consumerId, int partition, long offset) {
        require(consumerId).seekToOffset(partition, offset);
    }
}
