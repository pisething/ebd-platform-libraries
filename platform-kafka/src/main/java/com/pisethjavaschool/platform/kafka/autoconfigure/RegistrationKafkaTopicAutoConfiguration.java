package com.pisethjavaschool.platform.kafka.autoconfigure;

import com.pisethjavaschool.platform.events.registration.RegistrationEventTopics;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/** Provisions registration topics only in the designated administration service. */
@AutoConfiguration(after = PlatformKafkaAutoConfiguration.class)
@ConditionalOnClass({KafkaAdmin.class, TopicBuilder.class})
@ConditionalOnProperty(prefix = "platform.kafka.registration-topics", name = "enabled", havingValue = "true")
public class RegistrationKafkaTopicAutoConfiguration {

    @Bean
    public KafkaAdmin.NewTopics registrationKafkaTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(RegistrationEventTopics.REGISTRATION_REQUESTED)
                        .partitions(3).replicas(3).build(),
                TopicBuilder.name(RegistrationEventTopics.OWNER_PROVISIONED)
                        .partitions(3).replicas(3).build(),
                TopicBuilder.name(RegistrationEventTopics.ROLE_ASSIGNED)
                        .partitions(3).replicas(3).build());
    }
}