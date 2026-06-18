package com.hkg.dataplatform.contracts;

import java.util.Objects;

public record ColumnDefinition(
        String name,
        String type,
        boolean nullable,
        DataSensitivity sensitivity,
        String description) {

    public ColumnDefinition {
        name = requireText(name, "name");
        type = requireText(type, "type");
        sensitivity = Objects.requireNonNull(sensitivity, "sensitivity");
        description = description == null ? "" : description;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
