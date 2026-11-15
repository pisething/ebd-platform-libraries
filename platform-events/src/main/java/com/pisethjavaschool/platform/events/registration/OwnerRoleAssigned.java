package com.pisethjavaschool.platform.events.registration;

import java.time.Instant;
import java.util.UUID;

public record OwnerRoleAssigned(
        UUID eventId,
        UUID registrationId,
        UUID platformUserId,
        Instant occurredAt) {
}