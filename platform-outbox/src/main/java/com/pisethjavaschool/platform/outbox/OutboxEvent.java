package com.pisethjavaschool.platform.outbox;

import java.util.UUID;

/** Event envelope persisted before Kafka delivery. */
public record OutboxEvent(UUID eventId, String topic, String partitionKey, String payload) {

    public OutboxEvent {
        if (eventId == null || isBlank(topic) || isBlank(partitionKey) || isBlank(payload)) {
            throw new IllegalArgumentException("Outbox event fields must be populated");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}