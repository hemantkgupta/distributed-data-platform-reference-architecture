package com.hkg.dataplatform.contracts;

import java.time.Duration;
import java.util.Objects;

public record FreshnessSlo(Duration maxLag, String measurement) {
    public FreshnessSlo {
        maxLag = Objects.requireNonNull(maxLag, "maxLag");
        if (maxLag.isZero() || maxLag.isNegative()) {
            throw new IllegalArgumentException("maxLag must be positive");
        }
        measurement = measurement == null || measurement.isBlank() ? "event-time-to-publish-time" : measurement;
    }
}
