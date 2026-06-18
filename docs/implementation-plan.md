# Implementation Plan

This repo follows the daily learning/build track for `distributed-data-platform-reference-architecture`.

## Phase 1: Source Contracts, CDC Envelopes, Ownership, Fact Grain

- CP1: Model source contracts as versioned compatibility surfaces.
- CP2: Model CDC envelopes with replay and idempotency metadata.
- CP3: Add ownership and approval state transitions.
- CP4: Add fact grain examples and compatibility checks.
- CP5: Add contract-driven medallion promotion notes.

## Near-Term Constraints

- Java 17 and Gradle multi-module.
- JUnit 5 and AssertJ for tests.
- One small checkpoint per day unless explicitly expanded.
- Build with `./gradlew build --console=plain` whenever code changes.
