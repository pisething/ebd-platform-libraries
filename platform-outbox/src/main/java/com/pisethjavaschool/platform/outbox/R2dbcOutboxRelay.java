package com.pisethjavaschool.platform.outbox;

import java.time.Duration;
import java.util.UUID;

import org.springframework.r2dbc.core.DatabaseClient;

import reactor.core.publisher.Mono;

/**
 * One-at-a-time PostgreSQL outbox relay. A short SQL transaction claims a row;
 * the Kafka send happens outside that transaction. A fencing token prevents
 * a stale worker from acknowledging a claim taken over by another worker.
 * Delivery is at least once; consumers must deduplicate by event ID.
 */
public final class R2dbcOutboxRelay {

    private final DatabaseClient databaseClient;
    private final OutboxDelivery delivery;
    private final Duration lease;
    private final Duration retryDelay;
    private final Duration deliveryTimeout;
    private final int maxAttempts;

    public R2dbcOutboxRelay(
            DatabaseClient databaseClient,
            OutboxDelivery delivery,
            Duration lease,
            Duration retryDelay
    ) {
        this(databaseClient, delivery, lease, retryDelay, defaultDeliveryTimeout(lease), 10);
    }

    public R2dbcOutboxRelay(
            DatabaseClient databaseClient,
            OutboxDelivery delivery,
            Duration lease,
            Duration retryDelay,
            Duration deliveryTimeout
    ) {
        this(databaseClient, delivery, lease, retryDelay, deliveryTimeout, 10);
    }

    public R2dbcOutboxRelay(
            DatabaseClient databaseClient,
            OutboxDelivery delivery,
            Duration lease,
            Duration retryDelay,
            Duration deliveryTimeout,
            int maxAttempts
    ) {
        if (lease == null || lease.isZero() || lease.isNegative()
                || retryDelay == null || retryDelay.isNegative()
                || deliveryTimeout == null || deliveryTimeout.isZero()
                || deliveryTimeout.isNegative() || deliveryTimeout.compareTo(lease) >= 0
                || maxAttempts < 1) {
            throw new IllegalArgumentException("Invalid outbox relay configuration");
        }
        this.databaseClient = databaseClient;
        this.delivery = delivery;
        this.lease = lease;
        this.retryDelay = retryDelay;
        this.deliveryTimeout = deliveryTimeout;
        this.maxAttempts = maxAttempts;
    }

    private static Duration defaultDeliveryTimeout(Duration lease) {
        if (lease == null || lease.toMillis() < 2) {
            throw new IllegalArgumentException("Outbox lease must be at least two milliseconds");
        }
        return Duration.ofMillis(Math.min(30_000, lease.toMillis() / 2));
    }

    /** Returns true if a row was claimed and a delivery was attempted. */
    public Mono<Boolean> relayOne() {
        return Mono.defer(() -> {
            UUID token = UUID.randomUUID();
            return markExhaustedClaims().then(claim(token)).flatMap(event ->
                    delivery.publish(event.topic(), event.partitionKey(), event.payload())
                            .timeout(deliveryTimeout)
                            .onErrorResume(error -> releaseForRetry(event)
                                    .then(Mono.error(error)))
                            .then(acknowledge(event)))
                    .defaultIfEmpty(false);
        });
    }

    private Mono<Void> markExhaustedClaims() {
        return databaseClient.sql("""
                UPDATE outbox_event
                   SET status = 'FAILED', claim_token = NULL, leased_until = NULL
                 WHERE attempts >= :maxAttempts
                   AND ((status = 'PENDING')
                       OR (status = 'IN_FLIGHT' AND leased_until < CURRENT_TIMESTAMP))
                """)
                .bind("maxAttempts", maxAttempts)
                .fetch().rowsUpdated().then();
    }

    private Mono<ClaimedOutboxEvent> claim(UUID token) {
        return databaseClient.sql("""
                WITH candidate AS (
                    SELECT event_id FROM outbox_event
                    WHERE ((status = 'PENDING' AND next_attempt_at <= CURRENT_TIMESTAMP)
                       OR (status = 'IN_FLIGHT' AND leased_until < CURRENT_TIMESTAMP))
                      AND attempts < :maxAttempts
                    ORDER BY created_at, event_id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE outbox_event AS o
                   SET status = 'IN_FLIGHT', claim_token = :token,
                       leased_until = CURRENT_TIMESTAMP + (:leaseMillis * INTERVAL '1 millisecond'),
                       attempts = attempts + 1
                  FROM candidate
                 WHERE o.event_id = candidate.event_id
                RETURNING o.event_id, o.topic, o.partition_key, o.payload::text AS payload
                """)
                .bind("token", token)
                .bind("leaseMillis", lease.toMillis())
                .bind("maxAttempts", maxAttempts)
                .map((row, metadata) -> new ClaimedOutboxEvent(
                        row.get("event_id", UUID.class),
                        row.get("topic", String.class),
                        row.get("partition_key", String.class),
                        row.get("payload", String.class), token))
                .one();
    }

    private Mono<Boolean> acknowledge(ClaimedOutboxEvent event) {
        return databaseClient.sql("""
                UPDATE outbox_event SET status = 'PUBLISHED', published_at = CURRENT_TIMESTAMP,
                    claim_token = NULL, leased_until = NULL
                WHERE event_id = :eventId AND claim_token = :token AND status = 'IN_FLIGHT'
                """)
                .bind("eventId", event.eventId())
                .bind("token", event.claimToken())
                .fetch().rowsUpdated().map(updated -> updated == 1);
    }

    private Mono<Void> releaseForRetry(ClaimedOutboxEvent event) {
        return databaseClient.sql("""
                UPDATE outbox_event
                   SET status = CASE WHEN attempts >= :maxAttempts THEN 'FAILED' ELSE 'PENDING' END,
                       claim_token = NULL,
                       leased_until = NULL,
                       next_attempt_at = CURRENT_TIMESTAMP + (:delayMillis * INTERVAL '1 millisecond')
                WHERE event_id = :eventId AND claim_token = :token AND status = 'IN_FLIGHT'
                """)
                .bind("eventId", event.eventId())
                .bind("token", event.claimToken())
                .bind("delayMillis", retryDelay.toMillis())
                .bind("maxAttempts", maxAttempts)
                .fetch().rowsUpdated().then();
    }
}
