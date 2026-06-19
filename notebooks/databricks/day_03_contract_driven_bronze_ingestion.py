# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 3 - Contract-Driven Bronze Ingestion
# MAGIC
# MAGIC Goal: ingest source events into a bronze Delta table with enough contract metadata, validation, quarantine, and idempotency behavior to make reruns explainable.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Basics For This Notebook
# MAGIC
# MAGIC The SQL version of today's work would be: create an inline source table, add metadata columns, split valid and invalid rows, then merge into Delta tables.
# MAGIC
# MAGIC PySpark lets us express the validation part programmatically:
# MAGIC
# MAGIC - `spark.createDataFrame(...)` creates a DataFrame from Python rows.
# MAGIC - `withColumn(...)` adds or replaces a column.
# MAGIC - `F.col("amount")` references a column, like using `amount` in SQL.
# MAGIC - `F.lit("orders.events")` creates a constant value for every row.
# MAGIC - `F.when(condition, value)` is similar to SQL `CASE WHEN`.
# MAGIC - `createOrReplaceTempView(...)` gives SQL a temporary name for a DataFrame.

# COMMAND ----------

from pyspark.sql import functions as F

contract = {
    "contract_id": "orders.events",
    "contract_version": 1,
    "source_system": "orders-postgres",
    "grain": "one row per source order event",
    "idempotency_key": "event_id",
    "required_columns": [
        "event_id",
        "source_lsn",
        "operation",
        "order_id",
        "customer_id",
        "amount",
        "occurred_at",
    ],
}

raw_events = spark.createDataFrame(
    [
        ("evt-001", 1001, "create", 1, 101, 250.00, "2026-06-19T08:00:00Z"),
        ("evt-002", 1002, "create", 2, 102, 125.50, "2026-06-19T08:01:00Z"),
        ("evt-003", 1003, "update", 2, 102, 140.00, "2026-06-19T08:02:00Z"),
        ("evt-003", 1003, "update", 2, 102, 140.00, "2026-06-19T08:02:00Z"),  # duplicate delivery
        ("evt-004", 1004, "create", None, 103, 90.00, "2026-06-19T08:03:00Z"),  # bad key
        ("evt-005", 1005, "create", 5, 105, -20.00, "2026-06-19T08:04:00Z"),  # bad measure
    ],
    [
        "event_id",
        "source_lsn",
        "operation",
        "order_id",
        "customer_id",
        "amount",
        "occurred_at",
    ],
)

events = (
    raw_events
    .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
    .withColumn("occurred_at", F.to_timestamp("occurred_at"))
    .withColumn("contract_id", F.lit(contract["contract_id"]))
    .withColumn("contract_version", F.lit(contract["contract_version"]))
    .withColumn("source_system", F.lit(contract["source_system"]))
    .withColumn("ingested_at", F.current_timestamp())
)

display(events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Notes - Enrichment Block
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CAST(amount AS DECIMAL(10,2)) AS amount,
# MAGIC   TO_TIMESTAMP(occurred_at) AS occurred_at,
# MAGIC   'orders.events' AS contract_id,
# MAGIC   1 AS contract_version,
# MAGIC   'orders-postgres' AS source_system,
# MAGIC   CURRENT_TIMESTAMP() AS ingested_at
# MAGIC FROM raw_events;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - The DataFrame still represents source events; it now carries platform metadata.
# MAGIC - `withColumn` is like adding a derived column in `SELECT`.
# MAGIC - The chain is lazy until `display(events)` asks Spark to execute it.

# COMMAND ----------

validated = (
    events
    .withColumn(
        "dq_error",
        F.concat_ws(
            "; ",
            F.when(F.col("event_id").isNull(), "missing event_id"),
            F.when(F.col("source_lsn").isNull(), "missing source_lsn"),
            F.when(F.col("order_id").isNull(), "missing order_id"),
            F.when(F.col("customer_id").isNull(), "missing customer_id"),
            F.when(F.col("amount").isNull(), "missing amount"),
            F.when(F.col("amount") < 0, "negative amount"),
            F.when(~F.col("operation").isin("create", "update", "delete"), "invalid operation"),
            F.when(F.col("contract_id") != contract["contract_id"], "wrong contract_id"),
            F.when(F.col("contract_version") != contract["contract_version"], "wrong contract_version"),
        )
    )
)

valid_events = validated.where(F.col("dq_error") == "")
quarantine_events = validated.where(F.col("dq_error") != "")

valid_events.createOrReplaceTempView("valid_order_events_day3")
quarantine_events.createOrReplaceTempView("quarantine_order_events_day3")

display(valid_events)
display(quarantine_events)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Notes - Validation Block
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *,
# MAGIC   CONCAT_WS('; ',
# MAGIC     CASE WHEN order_id IS NULL THEN 'missing order_id' END,
# MAGIC     CASE WHEN amount < 0 THEN 'negative amount' END
# MAGIC   ) AS dq_error
# MAGIC FROM events;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `F.when(...)` produces a value only when the condition is true.
# MAGIC - `F.concat_ws('; ', ...)` joins the triggered error messages.
# MAGIC - `.where(F.col("dq_error") == "")` keeps valid rows.
# MAGIC - `.where(F.col("dq_error") != "")` keeps rejected rows for quarantine.
# MAGIC - Temporary views bridge PySpark DataFrames back into SQL cells.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day3 (
# MAGIC   event_id STRING,
# MAGIC   source_lsn BIGINT,
# MAGIC   operation STRING,
# MAGIC   order_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   occurred_at TIMESTAMP,
# MAGIC   contract_id STRING,
# MAGIC   contract_version INT,
# MAGIC   source_system STRING,
# MAGIC   ingested_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_quarantine_day3 (
# MAGIC   event_id STRING,
# MAGIC   source_lsn BIGINT,
# MAGIC   operation STRING,
# MAGIC   order_id BIGINT,
# MAGIC   customer_id BIGINT,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   occurred_at TIMESTAMP,
# MAGIC   contract_id STRING,
# MAGIC   contract_version INT,
# MAGIC   source_system STRING,
# MAGIC   ingested_at TIMESTAMP,
# MAGIC   dq_error STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_delta_day3 AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     event_id,
# MAGIC     source_lsn,
# MAGIC     operation,
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     amount,
# MAGIC     occurred_at,
# MAGIC     contract_id,
# MAGIC     contract_version,
# MAGIC     source_system,
# MAGIC     ingested_at
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY source_lsn DESC, ingested_at DESC) AS rn
# MAGIC     FROM valid_order_events_day3
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.event_id = source.event_id
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   event_id,
# MAGIC   source_lsn,
# MAGIC   operation,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   amount,
# MAGIC   occurred_at,
# MAGIC   contract_id,
# MAGIC   contract_version,
# MAGIC   source_system,
# MAGIC   ingested_at
# MAGIC ) VALUES (
# MAGIC   source.event_id,
# MAGIC   source.source_lsn,
# MAGIC   source.operation,
# MAGIC   source.order_id,
# MAGIC   source.customer_id,
# MAGIC   source.amount,
# MAGIC   source.occurred_at,
# MAGIC   source.contract_id,
# MAGIC   source.contract_version,
# MAGIC   source.source_system,
# MAGIC   source.ingested_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_quarantine_day3 AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     event_id,
# MAGIC     source_lsn,
# MAGIC     operation,
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     amount,
# MAGIC     occurred_at,
# MAGIC     contract_id,
# MAGIC     contract_version,
# MAGIC     source_system,
# MAGIC     ingested_at,
# MAGIC     dq_error
# MAGIC   FROM quarantine_order_events_day3
# MAGIC ) AS source
# MAGIC ON target.event_id = source.event_id AND target.dq_error = source.dq_error
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC Run the previous two MERGE cells again. The bronze row count should stay stable because `event_id` is the idempotency key.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze_delta' AS table_name, COUNT(*) AS row_count FROM orders_delta_day3
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine', COUNT(*) FROM orders_quarantine_day3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, COUNT(*) AS copies
# MAGIC FROM orders_delta_day3
# MAGIC GROUP BY event_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   contract_id,
# MAGIC   contract_version,
# MAGIC   source_system,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   MIN(source_lsn) AS min_lsn,
# MAGIC   MAX(source_lsn) AS max_lsn
# MAGIC FROM orders_delta_day3
# MAGIC GROUP BY contract_id, contract_version, source_system;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_quarantine_day3 ORDER BY source_lsn;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta_day3;
