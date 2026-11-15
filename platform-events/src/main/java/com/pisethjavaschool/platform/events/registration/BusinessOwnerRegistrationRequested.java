package com.pisethjavaschool.platform.events.registration;

import java.time.Instant;
import java.util.UUID;

/** Event emitted after the registration intent is durably persisted. Never include credentials. */
public record BusinessOwnerRegistrationRequested(
        UUID eventId,
        UUID registrationId,
        UUID platformUserId,
        Instant occurredAt,
        String ownerType,
        String businessType,
        String displayName,
        String businessName,
        String businessRegistrationNumber,
        String taxNumber,
        String phoneNumber,
        String email,
        String telegram) {
}