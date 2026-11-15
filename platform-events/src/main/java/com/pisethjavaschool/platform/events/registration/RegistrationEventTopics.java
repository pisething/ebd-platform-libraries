package com.pisethjavaschool.platform.events.registration;

public final class RegistrationEventTopics {

    public static final String REGISTRATION_REQUESTED = "registration.business-owner.requested.v1";
    public static final String OWNER_PROVISIONED = "registration.property-owner.provisioned.v1";
    public static final String ROLE_ASSIGNED = "registration.owner-role.assigned.v1";

    private RegistrationEventTopics() {
    }
}