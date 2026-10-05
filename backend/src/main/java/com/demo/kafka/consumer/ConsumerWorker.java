package com.demo.kafka.consumer;

import com.demo.kafka.model.events.Events;
import com.demo.kafka.ws.EventBroadcaster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Un "consumer" visuel de la demo = un vrai KafkaConsumer tournant dans son propre thread,
 * membre du meme consumer group que les autres. Demarrer/arreter un worker declenche donc
 * un veritable rebalance Kafka, visible dans Kafka-UI et dans l'animation du front.
 *
 * Toutes les operations qui touchent le KafkaConsumer (pause/resume/seek) ne sont pas
 * thread-safe : elles sont deposees dans une file de commandes executee en debut de boucle
 * de poll, depuis le thread proprietaire du consumer.
 */
public class ConsumerWorker implements Runnable {

    private static final Pattern ERROR_PATTERN = Pattern.compile("\"error\":true");
    private static final Pattern ID_PATTERN = Pattern.compile("\"id\":(\\d+)");

    private final String id;
    private final String bootstrapServers;
    private final String groupId;
    private final String topic;
    private final String dltTopic;
    private final EventBroadcaster broadcaster;
    private final KafkaTemplate<String, String> dltTemplate;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CopyOnWriteArraySet<Integer> pausedPartitions = new CopyOnWriteArraySet<>();
    private final ConcurrentLinkedQueue<Runnable> commands = new ConcurrentLinkedQueue<>();
    private volatile Thread thread;
    private volatile KafkaConsumer<String, String> consumer;
    private volatile List<Integer> assignedPartitions = List.of();

    public ConsumerWorker(String id, String bootstrapServers, String groupId, String topic, String dltTopic,
                           EventBroadcaster broadcaster, KafkaTemplate<String, String> dltTemplate) {
        this.id = id;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.topic = topic;
        this.dltTopic = dltTopic;
        this.broadcaster = broadcaster;
        this.dltTemplate = dltTemplate;
    }

    public String id() {
        return id;
    }

    public List<Integer> assignedPartitions() {
        return assignedPartitions;
    }

    public boolean isPartitionPaused(int partition) {
        return pausedPartitions.contains(partition);
    }

    public void start() {
        thread = new Thread(this, "demo-consumer-" + id);
        thread.setDaemon(true);
        thread.start();
    }

    public void shutdown() {
        running.set(false);
        if (consumer != null) {
            consumer.wakeup();
        }
        try {
            if (thread != null) {
                thread.join(Duration.ofSeconds(5).toMillis());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void pausePartition(int partition) {
        commands.add(() -> {
            TopicPartition tp = new TopicPartition(topic, partition);
            if (consumer.assignment().contains(tp)) {
                consumer.pause(List.of(tp));
                pausedPartitions.add(partition);
            }
        });
    }

    public void resumePartition(int partition) {
        commands.add(() -> {
            TopicPartition tp = new TopicPartition(topic, partition);
            if (consumer.assignment().contains(tp)) {
                consumer.resume(List.of(tp));
                pausedPartitions.remove(partition);
            }
        });
    }

    public void seekToBeginning(int partition) {
        commands.add(() -> {
            TopicPartition tp = new TopicPartition(topic, partition);
            if (consumer.assignment().contains(tp)) {
                consumer.seekToBeginning(List.of(tp));
                long newOffset = consumer.position(tp);
                broadcaster.broadcast(Events.CONSUMER_SEEK,
                        new Events.ConsumerSeek(id, partition, newOffset, "beginning"));
            }
        });
    }

    public void seekToEnd(int partition) {
        commands.add(() -> {
            TopicPartition tp = new TopicPartition(topic, partition);
            if (consumer.assignment().contains(tp)) {
                consumer.seekToEnd(List.of(tp));
                long newOffset = consumer.position(tp);
                broadcaster.broadcast(Events.CONSUMER_SEEK,
                        new Events.ConsumerSeek(id, partition, newOffset, "end"));
            }
        });
    }

    public void seekToOffset(int partition, long offset) {
        commands.add(() -> {
            TopicPartition tp = new TopicPartition(topic, partition);
            if (consumer.assignment().contains(tp)) {
                consumer.seek(tp, offset);
                broadcaster.broadcast(Events.CONSUMER_SEEK,
                        new Events.ConsumerSeek(id, partition, offset, "manual"));
            }
        });
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "demo-consumer-" + id);

        try (KafkaConsumer<String, String> kc = new KafkaConsumer<>(props)) {
            this.consumer = kc;
            kc.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Rien a committer explicitement ici : commit fait apres chaque batch traite.
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    List<Integer> parts = partitions.stream().map(TopicPartition::partition).sorted().toList();
                    assignedPartitions = parts;
                    broadcaster.broadcast(Events.CONSUMER_ASSIGNED, new Events.ConsumerAssigned(id, parts));
                }
            });

            while (running.get()) {
                Runnable cmd;
                while ((cmd = commands.poll()) != null) {
                    cmd.run();
                }

                ConsumerRecords<String, String> records = kc.poll(Duration.ofMillis(300));
                if (records.isEmpty()) {
                    continue;
                }

                Map<TopicPartition, Long> endOffsets = kc.endOffsets(kc.assignment());
                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();

                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record, endOffsets);
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    toCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                }
                if (!toCommit.isEmpty()) {
                    kc.commitSync(toCommit);
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                throw e;
            }
        } finally {
            this.consumer = null;
        }
    }

    private void handleRecord(ConsumerRecord<String, String> record, Map<TopicPartition, Long> endOffsets) {
        String value = record.value();
        boolean isError = value != null && ERROR_PATTERN.matcher(value).find();
        long originalId = extractId(value);

        if (isError) {
            dltTemplate.send(dltTopic, record.key(), value);
            broadcaster.broadcast(Events.DLT_MESSAGE,
                    new Events.DltMessage(originalId, record.partition(), record.offset(), record.key(),
                            "simulated-error", Instant.now()));
            return;
        }

        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        long end = endOffsets.getOrDefault(tp, record.offset() + 1);
        long lag = Math.max(0, end - record.offset() - 1);

        broadcaster.broadcast(Events.CONSUMER_MESSAGE,
                new Events.ConsumerMessage(id, record.partition(), record.offset(), record.key(),
                        Instant.now(), lag));
    }

    private long extractId(String value) {
        if (value == null) {
            return -1;
        }
        Matcher matcher = ID_PATTERN.matcher(value);
        return matcher.find() ? Long.parseLong(matcher.group(1)) : -1;
    }
}
