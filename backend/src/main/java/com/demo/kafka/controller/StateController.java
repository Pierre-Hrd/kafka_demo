package com.demo.kafka.controller;

import com.demo.kafka.admin.KafkaAdminService;
import com.demo.kafka.consumer.ConsumerManager;
import com.demo.kafka.model.events.Events;
import com.demo.kafka.producer.ProducerService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Fournit un instantane de l'etat courant, utilise par le front au chargement
 * de la page (avant de recevoir les mises a jour live via WebSocket).
 */
@RestController
public class StateController {

    private final ProducerService producerService;
    private final ConsumerManager consumerManager;
    private final KafkaAdminService adminService;

    public StateController(ProducerService producerService, ConsumerManager consumerManager,
                            KafkaAdminService adminService) {
        this.producerService = producerService;
        this.consumerManager = consumerManager;
        this.adminService = adminService;
    }

    @GetMapping("/api/state")
    public Events.StateSnapshot state() {
        return new Events.StateSnapshot(
                producerService.currentConfig(),
                new Events.TopicConfig(adminService.mainTopic(), adminService.currentPartitionCount()),
                consumerManager.snapshot(),
                Map.of()
        );
    }
}
