package com.hkg.dataplatform.contracts;

import java.util.List;

public record CompatibilityResult(boolean compatible, List<String> violations) {
    public CompatibilityResult {
        violations = List.copyOf(violations);
    }

    public static CompatibilityResult success() {
        return new CompatibilityResult(true, List.of());
    }

    public static CompatibilityResult incompatible(List<String> violations) {
        return new CompatibilityResult(false, violations);
    }
}
