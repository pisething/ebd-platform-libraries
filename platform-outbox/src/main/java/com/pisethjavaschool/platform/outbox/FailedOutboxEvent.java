package com.pisethjavaschool.platform.outbox;

import java.time.Instant;
import java.util.UUID;

/** Metadata for investigating a failed delivery; deliberately excludes the payload. */
public record FailedOutboxEvent(
        UUID eventId,
        String topic,
        String partitionKey,
        int attempts,
        Instant createdAt
) {
}