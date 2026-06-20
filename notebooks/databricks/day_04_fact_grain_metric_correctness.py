# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 4 - Fact Grain and Metric Correctness
# MAGIC
# MAGIC Theme: the same business domain can be valid at multiple grains, but metrics break when you query the wrong grain.
# MAGIC
# MAGIC Objectives:
# MAGIC
# MAGIC - Create an order event table: one row per source order event.
# MAGIC - Derive a current order table: one row per order.
# MAGIC - Derive a daily aggregate table: one row per order date.
# MAGIC - Show how row counts and revenue change when the query uses the wrong grain.
# MAGIC - Use PySpark window syntax to derive latest state from events.
# MAGIC - Learn what grain checks should catch before a table is published.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept: Grain Is The Meaning Of One Row
# MAGIC
# MAGIC These are not interchangeable:
# MAGIC
# MAGIC - `order_events_delta_day4`: one row per order event.
# MAGIC - `orders_delta_day4`: one row per current order.
# MAGIC - `order_day_delta_day4`: one row per order date.
# MAGIC
# MAGIC A table can have the same columns and still mean something different. `order_id` repeats in an event table but should be unique in a current-state order table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create The Event-Grain Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order_events_delta_day4 (
# MAGIC   event_id STRING,
# MAGIC   source_lsn BIGINT,
# MAGIC   operation STRING,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   event_time TIMESTAMP,
# MAGIC   order_date DATE,
# MAGIC   amount_after DECIMAL(10,2),
# MAGIC   status_after STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per source order event',
# MAGIC   'entity_key' = 'event_id',
# MAGIC   'business_entity' = 'order'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO order_events_delta_day4 VALUES
# MAGIC   ('evt-001', 1001, 'create', 1, 101, TIMESTAMP '2026-06-01 08:00:00', DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   ('evt-002', 1002, 'create', 2, 102, TIMESTAMP '2026-06-01 08:01:00', DATE '2026-06-01', CAST(125.50 AS DECIMAL(10,2)), 'pending'),
# MAGIC   ('evt-003', 1003, 'update', 2, 102, TIMESTAMP '2026-06-01 08:05:00', DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   ('evt-004', 1004, 'create', 3, 103, TIMESTAMP '2026-06-02 09:00:00', DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   ('evt-005', 1005, 'create', 4, 104, TIMESTAMP '2026-06-02 09:10:00', DATE '2026-06-02', CAST(80.00 AS DECIMAL(10,2)), 'pending'),
# MAGIC   ('evt-006', 1006, 'update', 4, 104, TIMESTAMP '2026-06-02 09:20:00', DATE '2026-06-02', CAST(85.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   ('evt-007', 1007, 'correction', 4, 104, TIMESTAMP '2026-06-02 09:30:00', DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'completed');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order_events_delta_day4 ORDER BY source_lsn;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Check The Event Grain
# MAGIC
# MAGIC `event_id` should be unique. `order_id` should not be unique because an order can have multiple events.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, COUNT(*) AS rows_at_event_grain
# MAGIC FROM order_events_delta_day4
# MAGIC GROUP BY event_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, COUNT(*) AS events_for_order
# MAGIC FROM order_events_delta_day4
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected:
# MAGIC
# MAGIC - Duplicate `event_id` query returns no rows.
# MAGIC - Repeated `order_id` query returns order `2` and order `4`.
# MAGIC
# MAGIC Operational reading: an event table is safe to dedupe by `event_id`, but unsafe to treat as one row per order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Derive Current Order State With PySpark
# MAGIC
# MAGIC We now derive the latest event per order. This is a common silver-table operation: use the event grain as input, then publish a different table with order-level grain.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

events_df = spark.table("de_learning.order_events_delta_day4")

latest_order_window = Window.partitionBy("order_id").orderBy(F.col("source_lsn").desc())

current_orders_df = (
    events_df
    .withColumn("rn", F.row_number().over(latest_order_window))
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

current_orders_df.createOrReplaceTempView("orders_current_from_events_day4")

display(current_orders_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Notes - Latest Row Per Business Key
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_lsn DESC) AS rn
# MAGIC   FROM order_events_delta_day4
# MAGIC )
# MAGIC WHERE rn = 1;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.table("de_learning.order_events_delta_day4")` reads a SQL table as a DataFrame.
# MAGIC - `Window.partitionBy("order_id")` means "restart the calculation for each order."
# MAGIC - `orderBy(F.col("source_lsn").desc())` puts the newest event first for each order.
# MAGIC - `F.row_number().over(...)` is the PySpark version of SQL `ROW_NUMBER() OVER (...)`.
# MAGIC - `.where(F.col("rn") == 1)` keeps only the latest event per order.
# MAGIC - `.select(...)` projects and renames columns for the new table grain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Publish The Current-Order Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day4
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'entity_key' = 'order_id',
# MAGIC   'derived_from' = 'order_events_delta_day4'
# MAGIC )
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM orders_current_from_events_day4;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day4 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, COUNT(*) AS rows_at_order_grain
# MAGIC FROM orders_delta_day4
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected:
# MAGIC
# MAGIC - `orders_delta_day4` has 4 rows.
# MAGIC - Duplicate `order_id` query returns no rows.
# MAGIC
# MAGIC Operational reading: this table is safe for one-row-per-order metrics, but it has intentionally lost event history.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Publish A Daily Aggregate Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order_day_delta_day4
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per order_date',
# MAGIC   'entity_key' = 'order_date',
# MAGIC   'derived_from' = 'orders_delta_day4'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END) AS completed_revenue
# MAGIC FROM orders_delta_day4
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM order_day_delta_day4 ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Compute The Metric Wrong And Right
# MAGIC
# MAGIC The wrong query sums completed event rows. It double-counts order `4` because order `4` has both an update and a later correction event with completed status.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'wrong: sum completed event rows' AS metric_definition,
# MAGIC   COUNT(*) AS contributing_rows,
# MAGIC   SUM(amount_after) AS completed_revenue
# MAGIC FROM order_events_delta_day4
# MAGIC WHERE status_after = 'completed'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'right: sum current completed orders' AS metric_definition,
# MAGIC   COUNT(*) AS contributing_rows,
# MAGIC   SUM(current_amount) AS completed_revenue
# MAGIC FROM orders_delta_day4
# MAGIC WHERE current_status = 'completed'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'right: sum daily aggregate' AS metric_definition,
# MAGIC   SUM(order_count) AS contributing_rows,
# MAGIC   SUM(completed_revenue) AS completed_revenue
# MAGIC FROM order_day_delta_day4;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected:
# MAGIC
# MAGIC ```text
# MAGIC wrong event-grain revenue: 965.00
# MAGIC right order-grain revenue: 880.00
# MAGIC right day-grain revenue:   880.00
# MAGIC ```
# MAGIC
# MAGIC The event-grain query counted both `evt-006` and `evt-007` for order `4`. Both are real events. They are not both real final orders.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Make The Grain Contract Queryable

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW grain_contracts_day4 AS
# MAGIC SELECT 'order_events_delta_day4' AS table_name, 'one row per source order event' AS grain, 'event_id' AS key_column, 'auditing, replay, latest-state derivation' AS safe_for
# MAGIC UNION ALL
# MAGIC SELECT 'orders_delta_day4', 'one row per current order', 'order_id', 'order-level metrics, customer order counts'
# MAGIC UNION ALL
# MAGIC SELECT 'order_day_delta_day4', 'one row per order date', 'order_date', 'daily dashboards, date-level rollups';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM grain_contracts_day4 ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL order_events_delta_day4;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_delta_day4;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL order_day_delta_day4;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Observations
# MAGIC
# MAGIC - The event table has 7 rows and 4 distinct orders.
# MAGIC - The current-order table has 4 rows and 4 distinct orders.
# MAGIC - The daily aggregate table has 2 rows because the data covers 2 order dates.
# MAGIC - A row-count check is not enough; you need a grain-aware key check.
# MAGIC - The same `SUM(amount)` idea is wrong or right depending on which table grain it reads.
# MAGIC
# MAGIC Principal takeaway: grain is not documentation polish. It is part of the data contract. If the platform does not enforce grain, it can publish tables that are syntactically valid and semantically wrong.
