package com.demo.kafka.controller;

import com.demo.kafka.consumer.ConsumerManager;
import com.demo.kafka.model.events.Events;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/consumers")
public class ConsumerController {

    private final ConsumerManager consumerManager;

    public ConsumerController(ConsumerManager consumerManager) {
        this.consumerManager = consumerManager;
    }

    @PutMapping("/count")
    public Map<String, Integer> setCount(@RequestBody Map<String, Integer> body) {
        int count = consumerManager.setConsumerCount(body.getOrDefault("count", 1));
        return Map.of("count", count);
    }

    @org.springframework.web.bind.annotation.GetMapping
    public List<Events.ConsumerSnapshot> list() {
        return consumerManager.snapshot();
    }

    @PostMapping("/{id}/pause/{partition}")
    public void pause(@PathVariable String id, @PathVariable int partition) {
        consumerManager.pausePartition(id, partition);
    }

    @PostMapping("/{id}/resume/{partition}")
    public void resume(@PathVariable String id, @PathVariable int partition) {
        consumerManager.resumePartition(id, partition);
    }

    @PostMapping("/{id}/seek-beginning/{partition}")
    public void seekToBeginning(@PathVariable String id, @PathVariable int partition) {
        consumerManager.seekToBeginning(id, partition);
    }

    @PostMapping("/{id}/seek-end/{partition}")
    public void seekToEnd(@PathVariable String id, @PathVariable int partition) {
        consumerManager.seekToEnd(id, partition);
    }

    @PostMapping("/{id}/seek/{partition}")
    public void seekToOffset(@PathVariable String id, @PathVariable int partition,
                              @RequestBody Map<String, Long> body) {
        consumerManager.seekToOffset(id, partition, body.getOrDefault("offset", 0L));
    }
}
