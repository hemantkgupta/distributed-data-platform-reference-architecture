package com.hkg.dataplatform.cdc;

public record CdcPosition(String sourcePartition, long offset) {
    public CdcPosition {
        if (sourcePartition == null || sourcePartition.isBlank()) {
            throw new IllegalArgumentException("sourcePartition is required");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be non-negative");
        }
    }
}
