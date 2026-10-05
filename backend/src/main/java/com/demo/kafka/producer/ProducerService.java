package com.demo.kafka.producer;

import com.demo.kafka.admin.KafkaAdminService;
import com.demo.kafka.model.events.Events;
import com.demo.kafka.ws.EventBroadcaster;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Produit des messages de demo a intervalle regulier et configurable a chaud.
 * Le rythme, le taux d'erreur et le declenchement manuel d'erreurs peuvent etre
 * modifies sans jamais arreter le thread de planification : la prochaine iteration
 * relit simplement les valeurs courantes.
 */
@Service
public class ProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventBroadcaster broadcaster;
    private final String topic;
    private final int keySpace;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "demo-producer");
        t.setDaemon(true);
        return t;
    });

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong intervalMs;
    private final AtomicInteger errorRatePercent = new AtomicInteger(0);
    private final AtomicInteger pendingManualErrors = new AtomicInteger(0);
    private final AtomicLong messageSeq = new AtomicLong(0);
    private volatile ScheduledFuture<?> currentTask;

    public ProducerService(KafkaTemplate<String, String> kafkaTemplate,
                            EventBroadcaster broadcaster,
                            KafkaAdminService adminService,
                            @Value("${demo.producer.initial-interval-ms}") long initialIntervalMs,
                            @Value("${demo.producer.initial-error-rate-percent}") int initialErrorRate,
                            @Value("${demo.consumer.initial-count:2}") int keySpace) {
        this.kafkaTemplate = kafkaTemplate;
        this.broadcaster = broadcaster;
        this.topic = adminService.mainTopic();
        this.intervalMs = new AtomicLong(initialIntervalMs);
        this.errorRatePercent.set(initialErrorRate);
        this.keySpace = Math.max(4, keySpace * 2);
    }

    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            scheduleNext();
            broadcastConfig();
        }
    }

    public synchronized void stop() {
        running.set(false);
        if (currentTask != null) {
            currentTask.cancel(false);
        }
        broadcastConfig();
    }

    public void setIntervalMs(long ms) {
        intervalMs.set(Math.max(50, ms));
        broadcastConfig();
    }

    public void setErrorRatePercent(int percent) {
        errorRatePercent.set(Math.max(0, Math.min(100, percent)));
        broadcastConfig();
    }

    public void triggerManualErrors(int count) {
        pendingManualErrors.addAndGet(Math.max(0, count));
    }

    public Events.ProducerConfig currentConfig() {
        return new Events.ProducerConfig(running.get(), intervalMs.get(), errorRatePercent.get());
    }

    private void scheduleNext() {
        if (!running.get()) {
            return;
        }
        currentTask = scheduler.schedule(() -> {
            try {
                sendOneMessage();
            } finally {
                scheduleNext();
            }
        }, intervalMs.get(), TimeUnit.MILLISECONDS);
    }

    private void sendOneMessage() {
        long id = messageSeq.incrementAndGet();
        String key = "key-" + ThreadLocalRandom.current().nextInt(keySpace);
        boolean forceError = decideError();
        String value = buildPayload(id, forceError);

        kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
            if (ex != null) {
                broadcaster.broadcast(Events.PRODUCER_ERROR,
                        new Events.ProducerError(id, key, ex.getMessage(), Instant.now()));
                return;
            }
            RecordMetadata metadata = result.getRecordMetadata();
            broadcaster.broadcast(Events.PRODUCER_MESSAGE,
                    new Events.ProducerMessage(id, key, metadata.partition(), metadata.offset(),
                            Instant.now(), forceError));
        });
    }

    private boolean decideError() {
        if (pendingManualErrors.get() > 0) {
            pendingManualErrors.decrementAndGet();
            return true;
        }
        int rate = errorRatePercent.get();
        return rate > 0 && ThreadLocalRandom.current().nextInt(100) < rate;
    }

    private String buildPayload(long id, boolean forceError) {
        // Payload volontairement simple (pseudo-JSON) : le marqueur ERROR_FLAG declenche
        // le traitement en erreur cote consumer, qui route alors vers le DLT.
        return "{\"id\":" + id + ",\"ts\":\"" + Instant.now() + "\",\"error\":" + forceError + "}";
    }

    private void broadcastConfig() {
        broadcaster.broadcast(Events.PRODUCER_CONFIG, currentConfig());
    }
}
