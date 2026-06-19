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

## Databricks Artifacts

Databricks notebooks are saved as source files under `notebooks/databricks/`.

Use the day-prefixed convention:

```text
notebooks/databricks/day_XX_checkpoint_name.py
```

Reusable SQL that is not tied to one notebook lives under `sql/databricks/`.

Use Databricks source format for mixed PySpark and SQL notebooks:

```python
# Databricks notebook source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1;
```

## Wiki Anchors

- `/Users/hemantkgupta/CSE-Raw/raw-blog/distributed-data-platform-reference-architecture/distributed-data-platform-reference-architecture.md`
- `/Users/hemantkgupta/CSE-Raw/wiki/sources/distributed-data-platform-reference-architecture-deep-research-report.md`
- `[[concepts/data-contract]]`
- `[[concepts/log-based-change-data-capture]]`
- `[[concepts/medallion-architecture]]`
