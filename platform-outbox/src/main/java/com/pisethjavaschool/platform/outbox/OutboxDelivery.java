package com.pisethjavaschool.platform.outbox;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface OutboxDelivery {

    Mono<Void> publish(String topic, String key, String payload);
}
