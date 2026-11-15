package com.pisethjavaschool.platform.outbox;

import java.util.UUID;

public record ClaimedOutboxEvent(
        UUID eventId,
        String topic,
        String partitionKey,
        String payload,
        UUID claimToken
) {
}