package com.demo.kafka.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Centralise les operations d'administration Kafka utilisees par la demo :
 * creation des topics, (re)dimensionnement du nombre de partitions.
 * Note Kafka : il est possible d'augmenter le nombre de partitions d'un topic a chaud,
 * mais jamais de le reduire (limitation native de Kafka).
 */
@Service
public class KafkaAdminService {

    private final KafkaAdmin kafkaAdmin;
    private final String mainTopic;
    private final String dltTopic;
    private final AtomicInteger currentPartitions;

    public KafkaAdminService(KafkaAdmin kafkaAdmin,
                              @Value("${demo.topic.main}") String mainTopic,
                              @Value("${demo.topic.dlt}") String dltTopic,
                              @Value("${demo.topic.initial-partitions}") int initialPartitions) {
        this.kafkaAdmin = kafkaAdmin;
        this.mainTopic = mainTopic;
        this.dltTopic = dltTopic;
        this.currentPartitions = new AtomicInteger(initialPartitions);
        ensureTopics(initialPartitions);
    }

    public String mainTopic() {
        return mainTopic;
    }

    public String dltTopic() {
        return dltTopic;
    }

    public int currentPartitionCount() {
        return currentPartitions.get();
    }

    private void ensureTopics(int partitions) {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            NewTopic main = new NewTopic(mainTopic, partitions, (short) 1);
            NewTopic dlt = new NewTopic(dltTopic, partitions, (short) 1);
            admin.createTopics(List.of(main, dlt)).all().get();
        } catch (ExecutionException e) {
            // Topic deja existant : on l'ignore silencieusement, cas frequent au redemarrage.
            if (!(e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) {
                throw new IllegalStateException("Impossible de creer les topics de demo", e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Creation des topics interrompue", e);
        }
    }

    /**
     * Augmente le nombre de partitions du topic principal (et du DLT en miroir).
     * Operation irreversible : impossible de redescendre en dessous du compte actuel.
     * Non utilisee dans cette demo : le nombre de partitions est fixe pour eviter
     * toute confusion visuelle (Kafka ne permet pas de revenir en arriere).
     */
    public synchronized int increasePartitions(int newCount) {
        int existing = currentPartitions.get();
        if (newCount <= existing) {
            return existing;
        }
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            Map<String, NewPartitions> request = Map.of(
                    mainTopic, NewPartitions.increaseTo(newCount),
                    dltTopic, NewPartitions.increaseTo(newCount)
            );
            admin.createPartitions(request).all().get();
            currentPartitions.set(newCount);
            return newCount;
        } catch (ExecutionException | InterruptedException e) {
            throw new IllegalStateException("Impossible d'augmenter le nombre de partitions", e);
        }
    }

    public Map<Integer, Long> describeEndOffsets() {
        // Delegue au consumer manager pour les offsets ; place-holder pour extension future.
        return Collections.emptyMap();
    }

    TopicDescription describe(AdminClient admin, String topic) throws ExecutionException, InterruptedException {
        KafkaFuture<TopicDescription> future = admin.describeTopics(List.of(topic)).topicNameValues().get(topic);
        return future.get();
    }
}
