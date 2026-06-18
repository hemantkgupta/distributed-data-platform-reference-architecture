package com.hkg.dataplatform.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import com.hkg.dataplatform.contracts.ColumnDefinition;
import com.hkg.dataplatform.contracts.ContractStatus;
import com.hkg.dataplatform.contracts.DataSensitivity;
import com.hkg.dataplatform.contracts.FreshnessSlo;
import com.hkg.dataplatform.contracts.SourceContract;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CdcEnvelopeTest {

    @Test
    void buildsStableIdempotencyKeyFromSourcePosition() {
        CdcEnvelope envelope = updateEnvelope(new CdcPosition("orders-0", 42));

        assertThat(envelope.idempotencyKey()).isEqualTo("orders:1:orders-0:42:evt-42");
    }

    @Test
    void validatesDeleteEventAgainstPrimaryKeyInBeforeImage() {
        CdcEnvelope envelope = new CdcEnvelope(
                "evt-99",
                "orders",
                1,
                "order_id=99",
                Operation.DELETE,
                Instant.parse("2026-06-18T08:00:00Z"),
                Instant.parse("2026-06-18T08:00:02Z"),
                new CdcPosition("orders-0", 99),
                Map.of("order_id", "99", "amount", "10.00"),
                Map.of());

        assertThat(envelope.validateAgainst(ordersContract())).isEmpty();
    }

    @Test
    void reportsContractVersionMismatch() {
        CdcEnvelope envelope = new CdcEnvelope(
                "evt-43",
                "orders",
                2,
                "order_id=43",
                Operation.UPDATE,
                Instant.parse("2026-06-18T08:00:00Z"),
                Instant.parse("2026-06-18T08:00:02Z"),
                new CdcPosition("orders-0", 43),
                Map.of("order_id", "43"),
                Map.of("order_id", "43", "amount", "11.00"));

        assertThat(envelope.validateAgainst(ordersContract()))
                .contains("Envelope contractVersion does not match contract");
    }

    private static CdcEnvelope updateEnvelope(CdcPosition position) {
        return new CdcEnvelope(
                "evt-42",
                "orders",
                1,
                "order_id=42",
                Operation.UPDATE,
                Instant.parse("2026-06-18T08:00:00Z"),
                Instant.parse("2026-06-18T08:00:02Z"),
                position,
                Map.of("order_id", "42", "amount", "10.00"),
                Map.of("order_id", "42", "amount", "12.00"));
    }

    private static SourceContract ordersContract() {
        return new SourceContract(
                "orders",
                1,
                "orders-postgres",
                "orders",
                "commerce-data",
                "one row per source order",
                List.of("order_id"),
                List.of(
                        new ColumnDefinition("order_id", "bigint", false, DataSensitivity.INTERNAL, ""),
                        new ColumnDefinition("amount", "decimal(12,2)", false, DataSensitivity.INTERNAL, "")),
                new FreshnessSlo(Duration.ofMinutes(15), "source-commit-to-bronze-publish"),
                ContractStatus.ACTIVE,
                Instant.parse("2026-06-17T00:00:00Z"));
    }
}
