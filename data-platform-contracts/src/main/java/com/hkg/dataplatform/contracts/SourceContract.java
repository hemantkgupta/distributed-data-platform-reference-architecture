package com.hkg.dataplatform.contracts;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public record SourceContract(
        String contractId,
        int version,
        String sourceSystem,
        String datasetName,
        String owner,
        String grain,
        List<String> primaryKeyColumns,
        List<ColumnDefinition> columns,
        FreshnessSlo freshnessSlo,
        ContractStatus status,
        Instant effectiveFrom) {

    public SourceContract {
        contractId = requireText(contractId, "contractId");
        if (version <= 0) {
            throw new IllegalArgumentException("version must be positive");
        }
        sourceSystem = requireText(sourceSystem, "sourceSystem");
        datasetName = requireText(datasetName, "datasetName");
        owner = requireText(owner, "owner");
        grain = requireText(grain, "grain");
        primaryKeyColumns = List.copyOf(primaryKeyColumns);
        columns = List.copyOf(columns);
        freshnessSlo = Objects.requireNonNull(freshnessSlo, "freshnessSlo");
        status = Objects.requireNonNull(status, "status");
        effectiveFrom = Objects.requireNonNull(effectiveFrom, "effectiveFrom");
        if (primaryKeyColumns.isEmpty()) {
            throw new IllegalArgumentException("primaryKeyColumns is required");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns is required");
        }
    }

    public Optional<ColumnDefinition> column(String name) {
        return columns.stream().filter(column -> column.name().equals(name)).findFirst();
    }

    public CompatibilityResult checkBackwardCompatibility(SourceContract previous) {
        Objects.requireNonNull(previous, "previous");
        List<String> violations = new ArrayList<>();
        Map<String, ColumnDefinition> currentColumns = byName(columns);

        for (ColumnDefinition oldColumn : previous.columns()) {
            ColumnDefinition newColumn = currentColumns.get(oldColumn.name());
            if (newColumn == null) {
                violations.add("Removed column: " + oldColumn.name());
                continue;
            }
            if (!oldColumn.type().equals(newColumn.type())) {
                violations.add("Changed type for " + oldColumn.name() + ": " + oldColumn.type() + " -> " + newColumn.type());
            }
            if (oldColumn.nullable() && !newColumn.nullable()) {
                violations.add("Strengthened nullability for " + oldColumn.name());
            }
        }

        for (String keyColumn : previous.primaryKeyColumns()) {
            if (!primaryKeyColumns.contains(keyColumn)) {
                violations.add("Removed primary key column: " + keyColumn);
            }
        }

        return violations.isEmpty() ? CompatibilityResult.success() : CompatibilityResult.incompatible(violations);
    }

    private static Map<String, ColumnDefinition> byName(List<ColumnDefinition> columns) {
        Map<String, ColumnDefinition> byName = new LinkedHashMap<>();
        for (ColumnDefinition column : columns) {
            ColumnDefinition previous = byName.putIfAbsent(column.name(), column);
            if (previous != null) {
                throw new IllegalArgumentException("Duplicate column: " + column.name());
            }
        }
        return byName;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
