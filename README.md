# Distributed Data Platform Reference Architecture

Java 17 companion implementation for the CSE topic `distributed-data-platform-reference-architecture`.

The repo starts with the first learning checkpoints from the 90-day plan:

- `data-platform-contracts`: source contract records, ownership, grain, schema, freshness, and compatibility checks.
- `data-platform-cdc`: replayable CDC envelope records with source position and idempotency keys.

## Build

Requires JDK 17+.

```sh
./gradlew build --console=plain
```

## Learning Log

Daily learning notes live under `docs/learning/`.

## Wiki Anchors

- `/Users/hemantkgupta/CSE-Raw/raw-blog/distributed-data-platform-reference-architecture/distributed-data-platform-reference-architecture.md`
- `/Users/hemantkgupta/CSE-Raw/wiki/sources/distributed-data-platform-reference-architecture-deep-research-report.md`
- `[[concepts/data-contract]]`
- `[[concepts/log-based-change-data-capture]]`
- `[[concepts/medallion-architecture]]`
