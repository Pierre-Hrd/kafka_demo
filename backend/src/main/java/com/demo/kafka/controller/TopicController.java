package com.demo.kafka.controller;

import com.demo.kafka.admin.KafkaAdminService;
import com.demo.kafka.model.events.Events;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/topic")
public class TopicController {

    private final KafkaAdminService adminService;

    public TopicController(KafkaAdminService adminService) {
        this.adminService = adminService;
    }

    @GetMapping
    public Events.TopicConfig get() {
        return new Events.TopicConfig(adminService.mainTopic(), adminService.currentPartitionCount());
    }
}
