package com.pisethjavaschool.platform.outbox.autoconfigure;

import com.pisethjavaschool.platform.outbox.OutboxFailureOperations;
import com.pisethjavaschool.platform.outbox.R2dbcOutboxWriter;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.r2dbc.core.DatabaseClient;

@AutoConfiguration(afterName = {
        "org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration",
        "org.springframework.boot.data.r2dbc.autoconfigure.DataR2dbcAutoConfiguration"
})
@ConditionalOnClass({DatabaseClient.class, R2dbcOutboxWriter.class})
@ConditionalOnBean(DatabaseClient.class)
public class OutboxAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(OutboxFailureOperations.class)
    public OutboxFailureOperations outboxFailureOperations(DatabaseClient databaseClient) {
        return new OutboxFailureOperations(databaseClient);
    }

    @Bean
    @ConditionalOnMissingBean(R2dbcOutboxWriter.class)
    public R2dbcOutboxWriter r2dbcOutboxWriter(DatabaseClient databaseClient) {
        return new R2dbcOutboxWriter(databaseClient);
    }
}