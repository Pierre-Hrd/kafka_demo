package com.demo.kafka.ws;

import com.demo.kafka.model.events.DemoEvent;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

/**
 * Point d'entree unique pour diffuser des evenements au front via STOMP.
 * Tous les evenements transitent sur la destination /topic/events.
 */
@Component
public class EventBroadcaster {

    private static final String DESTINATION = "/topic/events";

    private final SimpMessagingTemplate messagingTemplate;

    public EventBroadcaster(SimpMessagingTemplate messagingTemplate) {
        this.messagingTemplate = messagingTemplate;
    }

    public void broadcast(String type, Object payload) {
        messagingTemplate.convertAndSend(DESTINATION, DemoEvent.of(type, payload));
    }
}
