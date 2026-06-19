package com.hkg.dataplatform.contracts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class SourceContractChangeTest {

    @Test
    void publishesCompatibleContractThroughExplicitOwnerApprovedStates() {
        SourceContract previous = ordersContract(1, ContractStatus.ACTIVE, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));
        SourceContract proposed = ordersContract(2, ContractStatus.PROPOSED, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false),
                column("coupon_code", "string", true)));

        SourceContractChange change = SourceContractChange
                .propose("req-1", 1, proposed, "commerce-data", at("2026-06-19T00:00:00Z"))
                .validateAgainst(previous, "commerce-data", at("2026-06-19T00:05:00Z"))
                .activate("commerce-data", at("2026-06-19T00:10:00Z"));

        assertThat(change.status()).isEqualTo(ContractStatus.ACTIVE);
        assertThat(change.contract().effectiveFrom()).isEqualTo(at("2026-06-19T00:10:00Z"));
        assertThat(change.history())
                .extracting(ContractTransition::toStatus)
                .containsExactly(ContractStatus.PROPOSED, ContractStatus.VALIDATED, ContractStatus.ACTIVE);
    }

    @Test
    void rejectsApprovalFromNonOwner() {
        SourceContract proposed = ordersContract(2, ContractStatus.PROPOSED, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));

        assertThatThrownBy(() -> SourceContractChange.propose(
                "req-2",
                1,
                proposed,
                "analytics-platform",
                at("2026-06-19T00:00:00Z")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Only the source owner");
    }

    @Test
    void refusesToValidateBreakingContractChange() {
        SourceContract previous = ordersContract(1, ContractStatus.ACTIVE, columns(
                column("order_id", "bigint", false),
                column("customer_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));
        SourceContract proposed = ordersContract(2, ContractStatus.PROPOSED, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));
        SourceContractChange change = SourceContractChange.propose(
                "req-3",
                1,
                proposed,
                "commerce-data",
                at("2026-06-19T00:00:00Z"));

        assertThatThrownBy(() -> change.validateAgainst(previous, "commerce-data", at("2026-06-19T00:05:00Z")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Removed column: customer_id");
    }

    @Test
    void requiresDeprecationBeforeRetirement() {
        SourceContract previous = ordersContract(1, ContractStatus.ACTIVE, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false)));
        SourceContract proposed = ordersContract(2, ContractStatus.PROPOSED, columns(
                column("order_id", "bigint", false),
                column("amount", "decimal(12,2)", false),
                column("coupon_code", "string", true)));
        SourceContractChange active = SourceContractChange
                .propose("req-4", 1, proposed, "commerce-data", at("2026-06-19T00:00:00Z"))
                .validateAgainst(previous, "commerce-data", at("2026-06-19T00:05:00Z"))
                .activate("commerce-data", at("2026-06-19T00:10:00Z"));

        assertThatThrownBy(() -> active.retire("commerce-data", at("2026-06-19T00:15:00Z")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Expected status DEPRECATED");

        SourceContractChange retired = active
                .deprecate("commerce-data", at("2026-06-19T00:20:00Z"))
                .retire("commerce-data", at("2026-06-19T00:30:00Z"));

        assertThat(retired.status()).isEqualTo(ContractStatus.RETIRED);
    }

    private static SourceContract ordersContract(int version, ContractStatus status, List<ColumnDefinition> columns) {
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
                status,
                at("2026-06-17T00:00:00Z"));
    }

    private static List<ColumnDefinition> columns(ColumnDefinition... columns) {
        return List.of(columns);
    }

    private static ColumnDefinition column(String name, String type, boolean nullable) {
        return new ColumnDefinition(name, type, nullable, DataSensitivity.INTERNAL, "");
    }

    private static Instant at(String instant) {
        return Instant.parse(instant);
    }
}
