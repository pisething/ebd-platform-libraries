package com.pisethjavaschool.platform.outbox.autoconfigure;

import com.pisethjavaschool.platform.kafka.KafkaEventPublisher;
import com.pisethjavaschool.platform.outbox.OutboxDelivery;
import com.pisethjavaschool.platform.outbox.OutboxKafkaDelivery;
import com.pisethjavaschool.platform.outbox.OutboxRelayProperties;
import com.pisethjavaschool.platform.outbox.OutboxRelayScheduler;
import com.pisethjavaschool.platform.outbox.R2dbcOutboxRelay;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import com.pisethjavaschool.platform.kafka.autoconfigure.PlatformKafkaAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@AutoConfiguration(after = {OutboxAutoConfiguration.class, PlatformKafkaAutoConfiguration.class})
@ConditionalOnClass({DatabaseClient.class, KafkaEventPublisher.class})
@ConditionalOnProperty(prefix = "platform.outbox.relay", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(OutboxRelayProperties.class)
@EnableScheduling
public class OutboxRelayAutoConfiguration {

    @Bean
    @ConditionalOnBean(KafkaEventPublisher.class)
    @ConditionalOnMissingBean(OutboxDelivery.class)
    public OutboxDelivery outboxDelivery(KafkaEventPublisher publisher) {
        return new OutboxKafkaDelivery(publisher);
    }

    @Bean
    @ConditionalOnBean({DatabaseClient.class, OutboxDelivery.class})
    @ConditionalOnMissingBean(R2dbcOutboxRelay.class)
    public R2dbcOutboxRelay r2dbcOutboxRelay(
            DatabaseClient databaseClient,
            OutboxDelivery delivery,
            OutboxRelayProperties properties
    ) {
        return new R2dbcOutboxRelay(
                databaseClient,
                delivery,
                properties.getLease(),
                properties.getRetryDelay(),
                properties.getDeliveryTimeout(),
                properties.getMaxAttempts());
    }

    @Bean
    @ConditionalOnBean(R2dbcOutboxRelay.class)
    @ConditionalOnMissingBean(OutboxRelayScheduler.class)
    public OutboxRelayScheduler outboxRelayScheduler(
            R2dbcOutboxRelay relay,
            OutboxRelayProperties properties
    ) {
        return new OutboxRelayScheduler(relay, properties);
    }
}