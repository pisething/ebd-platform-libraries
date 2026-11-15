package com.pisethjavaschool.platform.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Mono;

/** Transport adapter. Event contracts remain independent of Kafka. */
public final class KafkaEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<Void> publish(String topic, String key, String payload) {
        return Mono.defer(() -> Mono.fromFuture(kafkaTemplate.send(topic, key, payload)))
                .then();
    }
}