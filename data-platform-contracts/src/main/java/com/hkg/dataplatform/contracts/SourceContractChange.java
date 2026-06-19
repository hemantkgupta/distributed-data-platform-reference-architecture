package com.hkg.dataplatform.contracts;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public record SourceContractChange(
        String requestId,
        int expectedPreviousVersion,
        SourceContract contract,
        List<ContractTransition> history) {

    public SourceContractChange {
        requestId = requireText(requestId, "requestId");
        if (expectedPreviousVersion < 0) {
            throw new IllegalArgumentException("expectedPreviousVersion must be non-negative");
        }
        contract = Objects.requireNonNull(contract, "contract");
        history = List.copyOf(history);
    }

    public static SourceContractChange propose(
            String requestId,
            int expectedPreviousVersion,
            SourceContract proposedContract,
            String actor,
            Instant decidedAt) {
        Objects.requireNonNull(proposedContract, "proposedContract");
        requireOwner(proposedContract, actor);
        SourceContract proposed = proposedContract.withStatus(ContractStatus.PROPOSED, decidedAt);
        return new SourceContractChange(
                requestId,
                expectedPreviousVersion,
                proposed,
                List.of(new ContractTransition(
                        requestId,
                        ContractStatus.PROPOSED,
                        ContractStatus.PROPOSED,
                        actor,
                        decidedAt,
                        "proposed source contract version")));
    }

    public ContractStatus status() {
        return contract.status();
    }

    public SourceContractChange validateAgainst(SourceContract previousContract, String actor, Instant decidedAt) {
        requireStatus(ContractStatus.PROPOSED);
        requireOwner(contract, actor);
        Objects.requireNonNull(previousContract, "previousContract");
        if (contract.version() != expectedPreviousVersion + 1) {
            throw new IllegalStateException("Contract version must be exactly expectedPreviousVersion + 1");
        }
        CompatibilityResult compatibility = contract.checkBackwardCompatibility(previousContract);
        if (!compatibility.compatible()) {
            throw new IllegalStateException("Contract is not backward compatible: " + compatibility.violations());
        }
        return transitionTo(ContractStatus.VALIDATED, actor, decidedAt, "validated compatibility and owner approval");
    }

    public SourceContractChange activate(String actor, Instant decidedAt) {
        requireStatus(ContractStatus.VALIDATED);
        requireOwner(contract, actor);
        return transitionTo(ContractStatus.ACTIVE, actor, decidedAt, "published active contract version");
    }

    public SourceContractChange deprecate(String actor, Instant decidedAt) {
        requireStatus(ContractStatus.ACTIVE);
        requireOwner(contract, actor);
        return transitionTo(ContractStatus.DEPRECATED, actor, decidedAt, "started deprecation window");
    }

    public SourceContractChange retire(String actor, Instant decidedAt) {
        requireStatus(ContractStatus.DEPRECATED);
        requireOwner(contract, actor);
        return transitionTo(ContractStatus.RETIRED, actor, decidedAt, "retired contract after deprecation window");
    }

    private SourceContractChange transitionTo(ContractStatus nextStatus, String actor, Instant decidedAt, String reason) {
        ContractStatus currentStatus = contract.status();
        List<ContractTransition> transitions = new ArrayList<>(history);
        transitions.add(new ContractTransition(requestId, currentStatus, nextStatus, actor, decidedAt, reason));
        return new SourceContractChange(
                requestId,
                expectedPreviousVersion,
                contract.withStatus(nextStatus, decidedAt),
                transitions);
    }

    private void requireStatus(ContractStatus expected) {
        if (contract.status() != expected) {
            throw new IllegalStateException("Expected status " + expected + " but was " + contract.status());
        }
    }

    private static void requireOwner(SourceContract contract, String actor) {
        String normalizedActor = requireText(actor, "actor");
        if (!contract.owner().equals(normalizedActor)) {
            throw new IllegalArgumentException("Only the source owner can approve this contract change");
        }
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
