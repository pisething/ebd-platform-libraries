package com.pisethjavaschool.platform.outbox;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "platform.outbox.relay")
public class OutboxRelayProperties {

    private int batchSize = 20;
    private int maxAttempts = 10;
    private Duration lease = Duration.ofMinutes(2);
    private Duration retryDelay = Duration.ofSeconds(10);
    private Duration deliveryTimeout = Duration.ofSeconds(30);

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        this.maxAttempts = maxAttempts;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        if (batchSize < 1 || batchSize > 1000) {
            throw new IllegalArgumentException("batchSize must be between 1 and 1000");
        }
        this.batchSize = batchSize;
    }

    public Duration getLease() {
        return lease;
    }

    public void setLease(Duration lease) {
        if (lease == null || lease.isZero() || lease.isNegative()) {
            throw new IllegalArgumentException("lease must be positive");
        }
        this.lease = lease;
    }

    public Duration getDeliveryTimeout() {
        return deliveryTimeout;
    }

    public void setDeliveryTimeout(Duration deliveryTimeout) {
        if (deliveryTimeout == null || deliveryTimeout.isZero() || deliveryTimeout.isNegative()) {
            throw new IllegalArgumentException("deliveryTimeout must be positive");
        }
        this.deliveryTimeout = deliveryTimeout;
    }

    public Duration getRetryDelay() {
        return retryDelay;
    }

    public void setRetryDelay(Duration retryDelay) {
        if (retryDelay == null || retryDelay.isNegative()) {
            throw new IllegalArgumentException("retryDelay cannot be negative");
        }
        this.retryDelay = retryDelay;
    }
}