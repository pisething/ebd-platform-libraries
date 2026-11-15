package com.pisethjavaschool.platform.events.registration;

import java.time.Instant;
import java.util.UUID;

public record PropertyOwnerProvisioned(
        UUID eventId,
        UUID registrationId,
        UUID platformUserId,
        UUID propertyOwnerId,
        Instant occurredAt,
        String businessType) {
}
