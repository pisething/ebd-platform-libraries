package com.pisethjavaschool.platform.outbox;

import com.pisethjavaschool.platform.kafka.KafkaEventPublisher;
import reactor.core.publisher.Mono;

public final class OutboxKafkaDelivery implements OutboxDelivery {

    private final KafkaEventPublisher publisher;

    public OutboxKafkaDelivery(KafkaEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public Mono<Void> publish(String topic, String key, String payload) {
        return publisher.publish(topic, key, payload);
    }
}