package com.hkg.dataplatform.contracts;

import java.time.Instant;
import java.util.Objects;

public record ContractTransition(
        String requestId,
        ContractStatus fromStatus,
        ContractStatus toStatus,
        String actor,
        Instant decidedAt,
        String reason) {

    public ContractTransition {
        requestId = requireText(requestId, "requestId");
        fromStatus = Objects.requireNonNull(fromStatus, "fromStatus");
        toStatus = Objects.requireNonNull(toStatus, "toStatus");
        actor = requireText(actor, "actor");
        decidedAt = Objects.requireNonNull(decidedAt, "decidedAt");
        reason = requireText(reason, "reason");
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
