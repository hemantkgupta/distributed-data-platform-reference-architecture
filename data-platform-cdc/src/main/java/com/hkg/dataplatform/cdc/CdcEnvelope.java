package com.hkg.dataplatform.cdc;

import com.hkg.dataplatform.contracts.SourceContract;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record CdcEnvelope(
        String eventId,
        String contractId,
        int contractVersion,
        String aggregateKey,
        Operation operation,
        Instant occurredAt,
        Instant capturedAt,
        CdcPosition position,
        Map<String, String> before,
        Map<String, String> after) {

    public CdcEnvelope {
        eventId = requireText(eventId, "eventId");
        contractId = requireText(contractId, "contractId");
        if (contractVersion <= 0) {
            throw new IllegalArgumentException("contractVersion must be positive");
        }
        aggregateKey = requireText(aggregateKey, "aggregateKey");
        operation = Objects.requireNonNull(operation, "operation");
        occurredAt = Objects.requireNonNull(occurredAt, "occurredAt");
        capturedAt = Objects.requireNonNull(capturedAt, "capturedAt");
        position = Objects.requireNonNull(position, "position");
        before = before == null ? Map.of() : Map.copyOf(before);
        after = after == null ? Map.of() : Map.copyOf(after);
        if (operation == Operation.DELETE && before.isEmpty()) {
            throw new IllegalArgumentException("delete events require a before image");
        }
        if (operation != Operation.DELETE && after.isEmpty()) {
            throw new IllegalArgumentException(operation + " events require an after image");
        }
    }

    public String idempotencyKey() {
        return contractId + ":" + contractVersion + ":" + position.sourcePartition() + ":" + position.offset() + ":" + eventId;
    }

    public List<String> validateAgainst(SourceContract contract) {
        Objects.requireNonNull(contract, "contract");
        List<String> violations = new ArrayList<>();
        if (!contract.contractId().equals(contractId)) {
            violations.add("Envelope contractId does not match contract");
        }
        if (contract.version() != contractVersion) {
            violations.add("Envelope contractVersion does not match contract");
        }
        for (String keyColumn : contract.primaryKeyColumns()) {
            if (!before.containsKey(keyColumn) && !after.containsKey(keyColumn)) {
                violations.add("Missing primary key column in envelope: " + keyColumn);
            }
        }
        return List.copyOf(violations);
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
