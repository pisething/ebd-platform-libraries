package com.pisethjavaschool.platform.outbox;

import java.util.UUID;
import java.util.function.Function;

import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

/**
 * Inserts using the caller's R2DBC connection and transaction context.
 * Callers MUST compose this Mono with their business write inside one
 * reactive transaction. This class does not open a transaction.
 * Each service must install the matching outbox_event Liquibase migration.
 */
public final class R2dbcOutboxWriter {

    private final DatabaseClient databaseClient;

    public R2dbcOutboxWriter(DatabaseClient databaseClient) {
        this.databaseClient = databaseClient;
    }

    public Mono<Void> append(OutboxEvent event) {
        return databaseClient.sql("""
                        INSERT INTO outbox_event (event_id, topic, partition_key, payload, status, created_at)
                        VALUES (:eventId, :topic, :partitionKey, CAST(:payload AS jsonb), 'PENDING', CURRENT_TIMESTAMP)
                        """)
                .bind("eventId", event.eventId())
                .bind("topic", event.topic())
                .bind("partitionKey", event.partitionKey())
                .bind("payload", event.payload())
                .fetch()
                .rowsUpdated()
                .then();
    }
    /**
     * Creates a database-generated event ID and writes a payload containing that same ID.
     * Both statements participate in the caller's reactive transaction.
     */
    public Mono<UUID> appendGenerated(String topic, String partitionKey, Function<UUID, String> payloadFactory) {
        return Mono.defer(() -> databaseClient.sql("""
                        INSERT INTO outbox_event (topic, partition_key, payload, status, created_at)
                        VALUES (:topic, :partitionKey, '{}'::jsonb, 'PENDING', CURRENT_TIMESTAMP)
                        RETURNING event_id
                        """)
                .bind("topic", topic)
                .bind("partitionKey", partitionKey)
                .map((row, metadata) -> row.get("event_id", UUID.class))
                .one()
                .flatMap(eventId -> databaseClient.sql("""
                                UPDATE outbox_event
                                SET payload = CAST(:payload AS jsonb)
                                WHERE event_id = :eventId
                                """)
                        .bind("payload", payloadFactory.apply(eventId))
                        .bind("eventId", eventId)
                        .fetch()
                        .rowsUpdated()
                        .flatMap(updated -> updated == 1
                                ? Mono.just(eventId)
                                : Mono.error(new IllegalStateException("Outbox event payload update failed")))));
    }

}