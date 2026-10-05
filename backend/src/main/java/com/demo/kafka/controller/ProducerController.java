package com.demo.kafka.controller;

import com.demo.kafka.model.events.Events;
import com.demo.kafka.producer.ProducerService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/producer")
public class ProducerController {

    private final ProducerService producerService;

    public ProducerController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping("/start")
    public Events.ProducerConfig start() {
        producerService.start();
        return producerService.currentConfig();
    }

    @PostMapping("/stop")
    public Events.ProducerConfig stop() {
        producerService.stop();
        return producerService.currentConfig();
    }

    @PutMapping("/interval")
    public Events.ProducerConfig setInterval(@RequestBody Map<String, Long> body) {
        producerService.setIntervalMs(body.getOrDefault("ms", 1000L));
        return producerService.currentConfig();
    }

    @PutMapping("/error-rate")
    public Events.ProducerConfig setErrorRate(@RequestBody Map<String, Integer> body) {
        producerService.setErrorRatePercent(body.getOrDefault("percent", 0));
        return producerService.currentConfig();
    }

    @PostMapping("/trigger-error")
    public Events.ProducerConfig triggerError(@RequestBody Map<String, Integer> body) {
        producerService.triggerManualErrors(body.getOrDefault("count", 1));
        return producerService.currentConfig();
    }
}
