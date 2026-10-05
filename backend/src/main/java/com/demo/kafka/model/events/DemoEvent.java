package com.demo.kafka.model.events;

/**
 * Enveloppe generique envoyee sur le canal WebSocket /topic/events.
 * Le champ "type" permet au front de savoir comment interpreter "payload".
 */
public record DemoEvent(String type, Object payload) {

    public static DemoEvent of(String type, Object payload) {
        return new DemoEvent(type, payload);
    }
}
