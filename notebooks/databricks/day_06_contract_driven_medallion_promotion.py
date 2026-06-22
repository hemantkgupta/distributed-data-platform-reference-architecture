# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 6 - Contract-Driven Medallion Promotion
# MAGIC
# MAGIC Goal: promote data through bronze, silver, and gold only when each boundary has validation evidence.
# MAGIC
# MAGIC Lab flow:
# MAGIC
# MAGIC - Bronze keeps source events and replay metadata.
# MAGIC - Silver keeps one clean current row per order.
# MAGIC - Gold keeps daily business metrics.
# MAGIC - Promotion evidence records which checks passed or blocked publication.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Bronze Raw Events
# MAGIC
# MAGIC Purpose: create source-fidelity rows with duplicates and bad records, the kind bronze is supposed to preserve.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_bronze_day6 (
# MAGIC   event_id STRING,
# MAGIC   source_lsn BIGINT,
# MAGIC   operation STRING,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   event_time TIMESTAMP,
# MAGIC   order_date DATE,
# MAGIC   amount_after DECIMAL(10,2),
# MAGIC   status_after STRING,
# MAGIC   contract_id STRING,
# MAGIC   contract_version INT,
# MAGIC   ingested_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'bronze',
# MAGIC   'grain' = 'one row per source order event',
# MAGIC   'contract_id' = 'orders.events',
# MAGIC   'contract_version' = '1'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_bronze_day6 VALUES
# MAGIC   ('evt-001', 1001, 'create', 1, 101, TIMESTAMP '2026-06-01 08:00:00', DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-002', 1002, 'create', 2, 102, TIMESTAMP '2026-06-01 08:01:00', DATE '2026-06-01', CAST(125.50 AS DECIMAL(10,2)), 'pending', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-003', 1003, 'update', 2, 102, TIMESTAMP '2026-06-01 08:05:00', DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-003', 1003, 'update', 2, 102, TIMESTAMP '2026-06-01 08:05:00', DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:01'),
# MAGIC   ('evt-004', 1004, 'create', 3, 103, TIMESTAMP '2026-06-02 09:00:00', DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-005', 1005, 'create', 4, 104, TIMESTAMP '2026-06-02 09:10:00', DATE '2026-06-02', CAST(80.00 AS DECIMAL(10,2)), 'pending', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-006', 1006, 'update', 4, 104, TIMESTAMP '2026-06-02 09:20:00', DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'completed', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-007', 1007, 'create', NULL, 105, TIMESTAMP '2026-06-03 10:00:00', DATE '2026-06-03', CAST(300.00 AS DECIMAL(10,2)), 'pending', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00'),
# MAGIC   ('evt-008', 1008, 'create', 6, 106, TIMESTAMP '2026-06-03 10:10:00', DATE '2026-06-03', CAST(-10.00 AS DECIMAL(10,2)), 'pending', 'orders.events', 1, TIMESTAMP '2026-06-22 06:00:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze_day6 ORDER BY source_lsn, ingested_at;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 9 bronze rows.
# MAGIC - `evt-003` appears twice because bronze preserves duplicate delivery.
# MAGIC - `evt-007` has missing `order_id`.
# MAGIC - `evt-008` has negative amount.
# MAGIC
# MAGIC Operational meaning: bronze should preserve source truth and enough replay metadata to explain bad inputs later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Validate Bronze Rows With PySpark
# MAGIC
# MAGIC Purpose: split raw bronze rows into silver-eligible rows and quarantine rows, while keeping evidence.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_df = spark.table("de_learning.orders_bronze_day6")

validated_bronze_df = (
    bronze_df
    .withColumn(
        "dq_error",
        F.concat_ws(
            "; ",
            F.when(F.col("event_id").isNull(), F.lit("missing event_id")),
            F.when(F.col("source_lsn").isNull(), F.lit("missing source_lsn")),
            F.when(F.col("order_id").isNull(), F.lit("missing order_id")),
            F.when(F.col("customer_id").isNull(), F.lit("missing customer_id")),
            F.when(F.col("amount_after").isNull(), F.lit("missing amount_after")),
            F.when(F.col("amount_after") < 0, F.lit("negative amount_after")),
            F.when(~F.col("operation").isin("create", "update", "delete"), F.lit("invalid operation")),
            F.when(F.col("contract_id") != F.lit("orders.events"), F.lit("wrong contract_id")),
            F.when(F.col("contract_version") != F.lit(1), F.lit("wrong contract_version")),
        ),
    )
)

valid_bronze_events_df = validated_bronze_df.where(F.col("dq_error") == "")
quarantine_events_df = validated_bronze_df.where(F.col("dq_error") != "")

valid_bronze_events_df.createOrReplaceTempView("valid_bronze_events_day6")
quarantine_events_df.createOrReplaceTempView("quarantine_events_day6")

display(valid_bronze_events_df.orderBy("source_lsn", "ingested_at"))
display(quarantine_events_df.orderBy("source_lsn"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *,
# MAGIC   CONCAT_WS('; ',
# MAGIC     CASE WHEN order_id IS NULL THEN 'missing order_id' END,
# MAGIC     CASE WHEN amount_after < 0 THEN 'negative amount_after' END
# MAGIC   ) AS dq_error
# MAGIC FROM orders_bronze_day6;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.table(...)` reads a SQL table into a DataFrame.
# MAGIC - `withColumn("dq_error", ...)` adds a validation-result column.
# MAGIC - `F.when(...)` is the PySpark form of SQL `CASE WHEN`.
# MAGIC - `where(F.col("dq_error") == "")` keeps rows that passed all checks.

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 7 valid bronze rows.
# MAGIC - 2 quarantine rows: missing `order_id`, negative `amount_after`.
# MAGIC
# MAGIC Operational meaning: failing rows are evidence. Do not silently drop them.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Persist Quarantine And Silver Candidate
# MAGIC
# MAGIC Purpose: keep rejected rows separately and build a current-order silver candidate from valid events.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_quarantine_day6
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'bronze-quarantine',
# MAGIC   'derived_from' = 'orders_bronze_day6'
# MAGIC )
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM quarantine_events_day6;

# COMMAND ----------

dedup_event_window = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())
latest_order_window = Window.partitionBy("order_id").orderBy(F.col("source_lsn").desc(), F.col("ingested_at").desc())

silver_candidate_df = (
    valid_bronze_events_df
    .withColumn("event_rn", F.row_number().over(dedup_event_window))
    .where(F.col("event_rn") == 1)
    .withColumn("order_rn", F.row_number().over(latest_order_window))
    .where(F.col("order_rn") == 1)
    .select(
        "order_id",
        "customer_id",
        "order_date",
        F.col("amount_after").alias("current_amount"),
        F.col("status_after").alias("current_status"),
        F.col("event_id").alias("last_event_id"),
        F.col("source_lsn").alias("last_source_lsn"),
        F.lit("orders.events").alias("source_contract_id"),
        F.lit(1).alias("source_contract_version"),
    )
)

silver_candidate_df.createOrReplaceTempView("orders_silver_candidate_view_day6")

display(silver_candidate_df.orderBy("order_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS event_rn,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY source_lsn DESC, ingested_at DESC) AS order_rn
# MAGIC   FROM valid_bronze_events_day6
# MAGIC )
# MAGIC WHERE event_rn = 1 AND order_rn = 1;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `Window.partitionBy("event_id")` deduplicates repeated deliveries.
# MAGIC - `Window.partitionBy("order_id")` finds the latest current state per order.
# MAGIC - `F.row_number().over(...)` is SQL `ROW_NUMBER() OVER (...)`.
# MAGIC - `select(...)` changes the table shape from event grain to current-order grain.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_candidate_day6
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'publication_status' = 'CANDIDATE',
# MAGIC   'derived_from' = 'orders_bronze_day6'
# MAGIC )
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM orders_silver_candidate_view_day6;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_quarantine_day6` has 2 rows.
# MAGIC - `orders_silver_candidate_day6` has 4 rows: order ids 1, 2, 3, 4.
# MAGIC - Duplicate `evt-003` is removed.
# MAGIC - Order 2 uses the latest amount `140.00`.
# MAGIC - Order 4 uses the latest amount `90.00`.
# MAGIC
# MAGIC Operational meaning: silver changes grain and quality level. It is not just "bronze with fewer rows."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Record Silver Promotion Evidence
# MAGIC
# MAGIC Purpose: make promotion checks queryable before publishing silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE medallion_promotion_evidence_day6 (
# MAGIC   promotion_id STRING,
# MAGIC   from_layer STRING,
# MAGIC   to_layer STRING,
# MAGIC   check_name STRING,
# MAGIC   severity STRING,
# MAGIC   outcome STRING,
# MAGIC   observed_value STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO medallion_promotion_evidence_day6
# MAGIC SELECT 'silver-pub-001', 'bronze', 'silver', 'bronze_row_count_positive', 'BLOCKER',
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        CAST(COUNT(*) AS STRING)
# MAGIC FROM orders_bronze_day6
# MAGIC UNION ALL
# MAGIC SELECT 'silver-pub-001', 'bronze', 'silver', 'quarantine_rows_expected', 'WARN',
# MAGIC        CASE WHEN COUNT(*) = 2 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        CAST(COUNT(*) AS STRING)
# MAGIC FROM orders_quarantine_day6
# MAGIC UNION ALL
# MAGIC SELECT 'silver-pub-001', 'bronze', 'silver', 'silver_order_id_unique', 'BLOCKER',
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        CAST(COUNT(*) AS STRING)
# MAGIC FROM (
# MAGIC   SELECT order_id
# MAGIC   FROM orders_silver_candidate_day6
# MAGIC   GROUP BY order_id
# MAGIC   HAVING COUNT(*) > 1
# MAGIC )
# MAGIC UNION ALL
# MAGIC SELECT 'silver-pub-001', 'bronze', 'silver', 'silver_amount_not_null', 'BLOCKER',
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        CAST(COUNT(*) AS STRING)
# MAGIC FROM orders_silver_candidate_day6
# MAGIC WHERE current_amount IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM medallion_promotion_evidence_day6
# MAGIC WHERE promotion_id = 'silver-pub-001'
# MAGIC ORDER BY severity, check_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All BLOCKER checks pass.
# MAGIC - The quarantine WARN check passes with observed value `2`.
# MAGIC
# MAGIC Operational meaning: warnings can be published if the platform policy allows it; blockers cannot.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Publish Silver Only If Blockers Passed
# MAGIC
# MAGIC Purpose: make the stable silver table visible only after evidence passes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver_promotion_decision_day6 AS
# MAGIC SELECT
# MAGIC   promotion_id,
# MAGIC   SUM(CASE WHEN severity = 'BLOCKER' AND outcome = 'FAIL' THEN 1 ELSE 0 END) AS blocker_failures,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN severity = 'BLOCKER' AND outcome = 'FAIL' THEN 1 ELSE 0 END) = 0
# MAGIC     THEN 'READY_TO_PROMOTE'
# MAGIC     ELSE 'BLOCKED'
# MAGIC   END AS promotion_decision
# MAGIC FROM medallion_promotion_evidence_day6
# MAGIC WHERE promotion_id = 'silver-pub-001'
# MAGIC GROUP BY promotion_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_promotion_decision_day6;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_day6
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'publication_status' = 'PUBLISHED',
# MAGIC   'published_from_promotion_id' = 'silver-pub-001'
# MAGIC )
# MAGIC AS
# MAGIC SELECT s.*
# MAGIC FROM orders_silver_candidate_day6 s
# MAGIC CROSS JOIN silver_promotion_decision_day6 d
# MAGIC WHERE d.promotion_decision = 'READY_TO_PROMOTE';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_day6 ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Decision is `READY_TO_PROMOTE`.
# MAGIC - `orders_silver_day6` has 4 clean current-order rows.
# MAGIC
# MAGIC Operational meaning: table visibility follows the decision record, not the existence of the candidate.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Build Gold Candidate Metrics
# MAGIC
# MAGIC Purpose: publish a business-facing daily metric from the silver table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_gold_candidate_day6
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'grain' = 'one row per order_date',
# MAGIC   'publication_status' = 'CANDIDATE',
# MAGIC   'derived_from' = 'orders_silver_day6'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END) AS completed_revenue
# MAGIC FROM orders_silver_day6
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_candidate_day6 ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC ```text
# MAGIC 2026-06-01 | order_count 2 | completed_revenue 390.00
# MAGIC 2026-06-02 | order_count 2 | completed_revenue 490.00
# MAGIC ```
# MAGIC
# MAGIC Operational meaning: gold is consumer-ready and metric-shaped. It is intentionally less detailed than silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Validate And Publish Gold
# MAGIC
# MAGIC Purpose: reconcile gold back to silver before making it consumer-visible.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO medallion_promotion_evidence_day6
# MAGIC SELECT 'gold-pub-001', 'silver', 'gold', 'gold_row_count_positive', 'BLOCKER',
# MAGIC        CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        CAST(COUNT(*) AS STRING)
# MAGIC FROM orders_gold_candidate_day6
# MAGIC UNION ALL
# MAGIC SELECT 'gold-pub-001', 'silver', 'gold', 'gold_revenue_matches_silver', 'BLOCKER',
# MAGIC        CASE
# MAGIC          WHEN (
# MAGIC            SELECT SUM(completed_revenue) FROM orders_gold_candidate_day6
# MAGIC          ) = (
# MAGIC            SELECT SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END)
# MAGIC            FROM orders_silver_day6
# MAGIC          )
# MAGIC          THEN 'PASS'
# MAGIC          ELSE 'FAIL'
# MAGIC        END,
# MAGIC        CAST((SELECT SUM(completed_revenue) FROM orders_gold_candidate_day6) AS STRING)
# MAGIC UNION ALL
# MAGIC SELECT 'gold-pub-001', 'silver', 'gold', 'gold_date_unique', 'BLOCKER',
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        CAST(COUNT(*) AS STRING)
# MAGIC FROM (
# MAGIC   SELECT order_date
# MAGIC   FROM orders_gold_candidate_day6
# MAGIC   GROUP BY order_date
# MAGIC   HAVING COUNT(*) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW gold_promotion_decision_day6 AS
# MAGIC SELECT
# MAGIC   promotion_id,
# MAGIC   SUM(CASE WHEN severity = 'BLOCKER' AND outcome = 'FAIL' THEN 1 ELSE 0 END) AS blocker_failures,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN severity = 'BLOCKER' AND outcome = 'FAIL' THEN 1 ELSE 0 END) = 0
# MAGIC     THEN 'READY_TO_PROMOTE'
# MAGIC     ELSE 'BLOCKED'
# MAGIC   END AS promotion_decision
# MAGIC FROM medallion_promotion_evidence_day6
# MAGIC WHERE promotion_id = 'gold-pub-001'
# MAGIC GROUP BY promotion_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM medallion_promotion_evidence_day6
# MAGIC WHERE promotion_id = 'gold-pub-001'
# MAGIC ORDER BY check_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_gold_day6
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'grain' = 'one row per order_date',
# MAGIC   'publication_status' = 'PUBLISHED',
# MAGIC   'published_from_promotion_id' = 'gold-pub-001'
# MAGIC )
# MAGIC AS
# MAGIC SELECT g.*
# MAGIC FROM orders_gold_candidate_day6 g
# MAGIC CROSS JOIN gold_promotion_decision_day6 d
# MAGIC WHERE d.promotion_decision = 'READY_TO_PROMOTE';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_day6 ORDER BY order_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All gold BLOCKER checks pass.
# MAGIC - `orders_gold_day6` has 2 daily metric rows.
# MAGIC
# MAGIC Operational meaning: every medallion boundary needs evidence. Promotion is not just `CREATE TABLE AS SELECT`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Inspect Promotion History
# MAGIC
# MAGIC Purpose: answer what was promoted, with which evidence, and which table versions Delta recorded.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   promotion_id,
# MAGIC   from_layer,
# MAGIC   to_layer,
# MAGIC   check_name,
# MAGIC   severity,
# MAGIC   outcome,
# MAGIC   observed_value
# MAGIC FROM medallion_promotion_evidence_day6
# MAGIC ORDER BY promotion_id, severity, check_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_silver_day6;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_gold_day6;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Evidence shows each promotion check and outcome.
# MAGIC - Delta history shows table creation and writes.
# MAGIC
# MAGIC Operational meaning: Delta history explains table operations; promotion evidence explains why the table was safe to expose.
