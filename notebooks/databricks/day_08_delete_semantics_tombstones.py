# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 8 - Delete Semantics And Tombstones
# MAGIC
# MAGIC Goal: model source deletes as durable tombstones so replay and backfill jobs do not resurrect records.
# MAGIC
# MAGIC Lab flow:
# MAGIC
# MAGIC - Create CDC events with creates, updates, deletes, and duplicate delete delivery.
# MAGIC - Show the bad result when a pipeline ignores deletes.
# MAGIC - Build a tombstone-aware current-state table.
# MAGIC - Publish active silver and gold from non-deleted rows only.
# MAGIC - Prove a late stale update does not resurrect a deleted order.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create CDC Events With Deletes
# MAGIC
# MAGIC Purpose: create source-fidelity CDC rows where deletes are first-class events, not missing data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_cdc_day8 (
# MAGIC   event_id STRING,
# MAGIC   source_lsn BIGINT,
# MAGIC   operation STRING,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   event_time TIMESTAMP,
# MAGIC   order_date DATE,
# MAGIC   amount_after DECIMAL(10,2),
# MAGIC   status_after STRING,
# MAGIC   source_table STRING,
# MAGIC   contract_version INT,
# MAGIC   ingested_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per source CDC event',
# MAGIC   'idempotency_key' = 'event_id',
# MAGIC   'ordering_key' = 'source_lsn'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_cdc_day8 VALUES
# MAGIC   ('evt-801', 2001, 'create', 1, 101, TIMESTAMP '2026-06-01 08:00:00', DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed', 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-802', 2002, 'create', 2, 102, TIMESTAMP '2026-06-01 08:01:00', DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed', 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-803', 2003, 'create', 3, 103, TIMESTAMP '2026-06-02 09:00:00', DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed', 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-804', 2004, 'update', 2, 102, TIMESTAMP '2026-06-01 08:10:00', DATE '2026-06-01', CAST(155.00 AS DECIMAL(10,2)), 'completed', 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-805', 2005, 'delete', 3, 103, TIMESTAMP '2026-06-02 09:30:00', DATE '2026-06-02', CAST(NULL AS DECIMAL(10,2)), CAST(NULL AS STRING), 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-806', 2006, 'create', 4, 104, TIMESTAMP '2026-06-02 10:00:00', DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'pending', 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-807', 2007, 'delete', 4, 104, TIMESTAMP '2026-06-02 10:30:00', DATE '2026-06-02', CAST(NULL AS DECIMAL(10,2)), CAST(NULL AS STRING), 'orders', 1, TIMESTAMP '2026-06-25 06:00:00'),
# MAGIC   ('evt-807', 2007, 'delete', 4, 104, TIMESTAMP '2026-06-02 10:30:00', DATE '2026-06-02', CAST(NULL AS DECIMAL(10,2)), CAST(NULL AS STRING), 'orders', 1, TIMESTAMP '2026-06-25 06:00:01');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_cdc_day8
# MAGIC ORDER BY source_lsn, ingested_at;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 8 CDC rows.
# MAGIC - Order 3 has a delete event.
# MAGIC - Order 4 has a duplicate delete delivery with the same `event_id`.
# MAGIC
# MAGIC Operational meaning: bronze/CDC keeps the delete event because it is the only evidence that downstream tables must remove or hide the record.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Show The Bad Pipeline That Ignores Deletes
# MAGIC
# MAGIC Purpose: demonstrate how a current-state pipeline can resurrect deleted records if it only considers create/update events.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

cdc_df = spark.table("de_learning.orders_cdc_day8")

latest_non_delete_window = Window.partitionBy("order_id").orderBy(
    F.col("source_lsn").desc(),
    F.col("ingested_at").desc(),
)

naive_silver_df = (
    cdc_df
    .where(F.col("operation").isin("create", "update"))
    .withColumn("rn", F.row_number().over(latest_non_delete_window))
    .where(F.col("rn") == 1)
    .select(
        "order_id",
        "customer_id",
        "order_date",
        F.col("amount_after").alias("current_amount"),
        F.col("status_after").alias("current_status"),
        F.col("event_id").alias("last_event_id"),
        F.col("source_lsn").alias("last_source_lsn"),
    )
)

naive_silver_df.createOrReplaceTempView("orders_silver_naive_view_day8")

display(naive_silver_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC This DataFrame represents a wrong current-order table built from create/update events only.
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_lsn DESC, ingested_at DESC) AS rn
# MAGIC   FROM orders_cdc_day8
# MAGIC   WHERE operation IN ('create', 'update')
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `where(F.col("operation").isin(...))` is SQL `WHERE operation IN (...)`.
# MAGIC - `Window.partitionBy("order_id")` creates one ranking group per order.
# MAGIC - `F.row_number().over(...)` is SQL `ROW_NUMBER() OVER (...)`.
# MAGIC - `createOrReplaceTempView(...)` makes the PySpark result queryable from SQL cells.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_naive_day8
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'delete_handling' = 'wrong: ignores deletes'
# MAGIC )
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM orders_silver_naive_view_day8;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_gold_naive_day8
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*) AS active_order_count,
# MAGIC   SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END) AS completed_revenue
# MAGIC FROM orders_silver_naive_day8
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_naive_day8 ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC ```text
# MAGIC 2026-06-01 | active_order_count 2 | completed_revenue 405.00
# MAGIC 2026-06-02 | active_order_count 2 | completed_revenue 400.00
# MAGIC ```
# MAGIC
# MAGIC Operational meaning: this is wrong. Order 3 was deleted, but the naive pipeline kept its old completed amount.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Build Tombstone-Aware Current State
# MAGIC
# MAGIC Purpose: deduplicate CDC delivery, then choose the latest event per order including deletes.

# COMMAND ----------

def build_tombstone_current_state():
    cdc_events_df = spark.table("de_learning.orders_cdc_day8")

    dedup_event_window = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
    latest_order_window = Window.partitionBy("order_id").orderBy(
        F.col("source_lsn").desc(),
        F.col("ingested_at").desc(),
    )

    deduped_events_df = (
        cdc_events_df
        .withColumn("event_rn", F.row_number().over(dedup_event_window))
        .where(F.col("event_rn") == 1)
    )

    current_state_df = (
        deduped_events_df
        .withColumn("order_rn", F.row_number().over(latest_order_window))
        .where(F.col("order_rn") == 1)
        .select(
            "order_id",
            "customer_id",
            "order_date",
            F.when(F.col("operation") == "delete", F.lit(None).cast("decimal(10,2)"))
             .otherwise(F.col("amount_after")).alias("current_amount"),
            F.when(F.col("operation") == "delete", F.lit(None).cast("string"))
             .otherwise(F.col("status_after")).alias("current_status"),
            (F.col("operation") == "delete").alias("is_deleted"),
            F.when(F.col("operation") == "delete", F.col("event_time")).alias("deleted_at"),
            F.col("event_id").alias("last_event_id"),
            F.col("source_lsn").alias("last_source_lsn"),
        )
    )

    current_state_df.createOrReplaceTempView("orders_silver_tombstone_view_day8")
    return current_state_df


tombstone_state_df = build_tombstone_current_state()
display(tombstone_state_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC This DataFrame represents the correct current-order state: latest source event wins, including delete events.
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC WITH deduped AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS event_rn
# MAGIC     FROM orders_cdc_day8
# MAGIC   )
# MAGIC   WHERE event_rn = 1
# MAGIC ),
# MAGIC latest_per_order AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_lsn DESC, ingested_at DESC) AS order_rn
# MAGIC     FROM deduped
# MAGIC   )
# MAGIC   WHERE order_rn = 1
# MAGIC )
# MAGIC SELECT *, operation = 'delete' AS is_deleted
# MAGIC FROM latest_per_order;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - The first window deduplicates repeated delivery by `event_id`.
# MAGIC - The second window chooses current state by source order, not arrival time.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - The tombstone keeps the key and delete metadata even though business values are null.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_tombstone_day8
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per order including tombstones',
# MAGIC   'delete_handling' = 'latest CDC event can be a tombstone'
# MAGIC )
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM orders_silver_tombstone_view_day8;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver_tombstone_day8
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 4 current-state rows.
# MAGIC - Order 1 and order 2 are active.
# MAGIC - Order 3 and order 4 have `is_deleted = true`.
# MAGIC - Duplicate delete delivery for order 4 appears only once.
# MAGIC
# MAGIC Operational meaning: a tombstone is the durable "this key is deleted" state. It prevents old creates/updates from becoming visible again.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Publish Active Silver And Correct Gold
# MAGIC
# MAGIC Purpose: expose only active records to consumers while retaining tombstones for audit and replay.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_active_day8
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per active current order',
# MAGIC   'derived_from' = 'orders_silver_tombstone_day8'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   current_amount,
# MAGIC   current_status,
# MAGIC   last_event_id,
# MAGIC   last_source_lsn
# MAGIC FROM orders_silver_tombstone_day8
# MAGIC WHERE is_deleted = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_gold_daily_day8
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per order_date',
# MAGIC   'derived_from' = 'orders_silver_active_day8'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*) AS active_order_count,
# MAGIC   SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END) AS completed_revenue
# MAGIC FROM orders_silver_active_day8
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_active_day8 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_daily_day8 ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Active silver has 2 rows: order 1 and order 2.
# MAGIC - Correct gold has one row: `2026-06-01`, active order count `2`, completed revenue `405.00`.
# MAGIC - No `2026-06-02` gold row remains because both orders from that date are deleted or not active.
# MAGIC
# MAGIC Operational meaning: published consumer tables should usually hide tombstones, but internal state should keep them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Record Delete Audit Evidence
# MAGIC
# MAGIC Purpose: prove that every source delete has a matching tombstone in the current-state table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delete_audit_day8
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH delete_events AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
# MAGIC     FROM orders_cdc_day8
# MAGIC     WHERE operation = 'delete'
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   d.order_id,
# MAGIC   d.event_id AS delete_event_id,
# MAGIC   d.source_lsn AS delete_source_lsn,
# MAGIC   d.event_time AS delete_event_time,
# MAGIC   t.is_deleted AS tombstone_present,
# MAGIC   t.deleted_at,
# MAGIC   CASE WHEN t.is_deleted = true THEN 'PASS' ELSE 'FAIL' END AS audit_result
# MAGIC FROM delete_events d
# MAGIC LEFT JOIN orders_silver_tombstone_day8 t
# MAGIC   ON d.order_id = t.order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_delete_audit_day8
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 2 audit rows: one for order 3, one for order 4.
# MAGIC - Both rows have `audit_result = PASS`.
# MAGIC
# MAGIC Operational meaning: delete handling should be testable. "We filtered it out" is not enough evidence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Prove A Late Stale Update Does Not Resurrect A Delete
# MAGIC
# MAGIC Purpose: insert an old source update that arrives late and verify source ordering still keeps the tombstone.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_cdc_day8 VALUES
# MAGIC   ('evt-808', 2004, 'update', 3, 103, TIMESTAMP '2026-06-02 09:10:00', DATE '2026-06-02', CAST(410.00 AS DECIMAL(10,2)), 'completed', 'orders', 1, TIMESTAMP '2026-06-25 06:10:00');

# COMMAND ----------

tombstone_state_df = build_tombstone_current_state()
tombstone_state_df.createOrReplaceTempView("orders_silver_tombstone_view_day8")
display(tombstone_state_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_tombstone_day8
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM orders_silver_tombstone_view_day8;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_silver_tombstone_day8
# MAGIC WHERE order_id = 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC The same function is rerun after a late stale update arrives.
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM orders_cdc_day8
# MAGIC WHERE order_id = 3
# MAGIC ORDER BY source_lsn DESC, ingested_at DESC;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - The stale update arrives later by `ingested_at`, but its `source_lsn` is lower than the delete.
# MAGIC - The current-state window orders by `source_lsn DESC` first.
# MAGIC - This is why source ordering metadata is part of the CDC contract.

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Order 3 remains `is_deleted = true`.
# MAGIC - `last_source_lsn` for order 3 remains `2005`, not the late stale update `2004`.
# MAGIC
# MAGIC Operational meaning: replay order must be based on source position, not arrival time. Otherwise a backfill can resurrect deleted records.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Inspect Delta History And Final State
# MAGIC
# MAGIC Purpose: connect table history with delete audit evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_cdc_day8 ORDER BY order_id, source_lsn, ingested_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_tombstone_day8 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delete_audit_day8 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_silver_tombstone_day8;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - CDC shows the full event trail.
# MAGIC - Tombstone table shows current active/deleted state.
# MAGIC - Audit table shows delete coverage.
# MAGIC - Delta history shows table writes, but delete audit explains semantic correctness.
# MAGIC
# MAGIC Operational meaning: history tells you what operation ran. Tombstone evidence tells you whether delete semantics were preserved.
