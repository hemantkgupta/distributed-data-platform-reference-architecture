package com.hkg.dataplatform.contracts;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class SourceContractTest {

    @Test
    void createsVersionedSourceContractWithOwnershipAndGrain() {
        SourceContract contract = ordersContract(1, columns(
                column("order_id", "bigint", false),
                column("customer_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));

        assertThat(contract.owner()).isEqualTo("commerce-data");
        assertThat(contract.grain()).isEqualTo("one row per source order");
        assertThat(contract.primaryKeyColumns()).containsExactly("order_id");
        assertThat(contract.column("amount")).isPresent();
    }

    @Test
    void detectsRemovalOfPublishedColumnAsBreakingChange() {
        SourceContract v1 = ordersContract(1, columns(
                column("order_id", "bigint", false),
                column("customer_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));
        SourceContract v2 = ordersContract(2, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));

        CompatibilityResult result = v2.checkBackwardCompatibility(v1);

        assertThat(result.compatible()).isFalse();
        assertThat(result.violations()).contains("Removed column: customer_id");
    }

    @Test
    void allowsAddingNullableColumnToPublishedContract() {
        SourceContract v1 = ordersContract(1, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));
        SourceContract v2 = ordersContract(2, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false),
                column("coupon_code", "string", true)));

        assertThat(v2.checkBackwardCompatibility(v1).compatible()).isTrue();
    }

    private static SourceContract ordersContract(int version, List<ColumnDefinition> columns) {
        return new SourceContract(
                "orders",
                version,
                "orders-postgres",
                "orders",
                "commerce-data",
                "one row per source order",
                List.of("order_id"),
                columns,
                new FreshnessSlo(Duration.ofMinutes(15), "source-commit-to-bronze-publish"),
                ContractStatus.ACTIVE,
                Instant.parse("2026-06-17T00:00:00Z"));
    }

    private static List<ColumnDefinition> columns(ColumnDefinition... columns) {
        return List.of(columns);
    }

    private static ColumnDefinition column(String name, String type, boolean nullable) {
        return new ColumnDefinition(name, type, nullable, DataSensitivity.INTERNAL, "");
    }
}
