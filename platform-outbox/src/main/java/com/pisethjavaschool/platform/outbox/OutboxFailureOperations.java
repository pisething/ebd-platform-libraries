package com.pisethjavaschool.platform.outbox;

import java.time.OffsetDateTime;
import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Operational access to terminally failed outbox messages.
 * No automatic requeue or HTTP endpoint is registered by this component.
 */
public final class OutboxFailureOperations {

    private final DatabaseClient databaseClient;

    public OutboxFailureOperations(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Flux<FailedOutboxEvent> findFailed(int limit) {
        if (limit < 1 || limit > 500) {
            return Flux.error(new IllegalArgumentException("Limit must be between 1 and 500"));
        }

        return databaseClient.sql("""
                SELECT event_id, topic, partition_key, attempts, created_at
                  FROM outbox_event
                 WHERE status = 'FAILED'
                 ORDER BY created_at, event_id
                 LIMIT :limit
                """)
                .bind("limit", limit)
                .map((row, metadata) -> new FailedOutboxEvent(
                        row.get("event_id", UUID.class),
                        row.get("topic", String.class),
                        row.get("partition_key", String.class),
                        row.get("attempts", Integer.class),
                        row.get("created_at", OffsetDateTime.class).toInstant()))
                .all();
    }

    /**
     * Requeues only an existing FAILED event. A successful result means the
     * state transition happened; it does not mean Kafka delivery succeeded.
     * Invoke only after investigating the cause of failure.
     */
    public Mono<Boolean> requeue(UUID eventId) {
        if (eventId == null) {
            return Mono.error(new IllegalArgumentException("Event ID is required"));
        }

        return databaseClient.sql("""
                UPDATE outbox_event
                   SET status = 'PENDING', attempts = 0,
                       claim_token = NULL, leased_until = NULL,
                       next_attempt_at = CURRENT_TIMESTAMP
                 WHERE event_id = :eventId AND status = 'FAILED'
                """)
                .bind("eventId", eventId)
                .fetch()
                .rowsUpdated()
                .map(updated -> updated == 1);
    }
}
