package com.pisethjavaschool.platform.outbox;

import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** Polls a bounded number of events without overlapping executions. */
public final class OutboxRelayScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxRelayScheduler.class);

    private final R2dbcOutboxRelay relay;
    private final OutboxRelayProperties properties;
    private final AtomicBoolean running = new AtomicBoolean();

    public OutboxRelayScheduler(R2dbcOutboxRelay relay, OutboxRelayProperties properties) {
        this.relay = relay;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${platform.outbox.relay.poll-interval-ms:1000}")
    public void poll() {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        relayBatch()
                .doOnError(error -> LOGGER.error("Outbox relay polling failed", error))
                .doFinally(signal -> running.set(false))
                .subscribe(ignored -> { }, error -> { });
    }

    /** Exposed for deterministic tests; production scheduling uses {@link #poll()}. */
    Mono<Void> relayBatch() {
        return Flux.range(0, properties.getBatchSize())
                .concatMap(index -> relay.relayOne())
                .takeUntil(attempted -> !attempted)
                .then();
    }
}
