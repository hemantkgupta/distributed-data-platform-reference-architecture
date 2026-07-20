# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 21 - Delta Maintenance And Retention Safety
# MAGIC
# MAGIC Goal: practice table lifecycle checks: `DESCRIBE DETAIL`, `DESCRIBE HISTORY`, layout choices, `OPTIMIZE`, `VACUUM ... DRY RUN`, retention guardrails, and maintenance decision gates.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Delta table operations, troubleshooting, monitoring, optimization, and platform fundamentals.
# MAGIC - Professional stretch: production table maintenance policy, retention safety, time travel risk, layout strategy, predictive optimization, and incident-ready evidence.
# MAGIC
# MAGIC This notebook runs real Delta table operations where safe. Potentially destructive cleanup is limited to `VACUUM ... DRY RUN`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create A Delta Table With Retention Properties
# MAGIC
# MAGIC Purpose: establish a managed Delta table and explicitly document retention assumptions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_maintenance_day21 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   region STRING,
# MAGIC   source_batch_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 7 days',
# MAGIC   'delta.logRetentionDuration' = 'interval 30 days',
# MAGIC   'maintenance.owner' = 'data-platform',
# MAGIC   'maintenance.table_tier' = 'silver',
# MAGIC   'maintenance.time_travel_required_days' = '7'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_maintenance_day21 VALUES
# MAGIC   (2101, 101, DATE'2026-07-15', CAST(250.00 AS DECIMAL(10,2)), 'completed', 'US', 'batch-001'),
# MAGIC   (2102, 102, DATE'2026-07-15', CAST(125.50 AS DECIMAL(10,2)), 'pending', 'US', 'batch-001'),
# MAGIC   (2103, 103, DATE'2026-07-16', CAST(400.00 AS DECIMAL(10,2)), 'completed', 'EU', 'batch-001'),
# MAGIC   (2104, 104, DATE'2026-07-16', CAST(80.00 AS DECIMAL(10,2)), 'cancelled', 'APAC', 'batch-001');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_maintenance_day21 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_maintenance_day21;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_maintenance_day21;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 4 rows in `orders_maintenance_day21`.
# MAGIC - `DESCRIBE DETAIL` shows Delta table metadata such as format, location, size, files, table properties, and statistics.
# MAGIC - `DESCRIBE HISTORY` shows at least create/write operations.
# MAGIC
# MAGIC Operational meaning: table maintenance starts with observed table state, not guesses. Detail tells you physical shape; history tells you operational change history.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Generate Multiple Versions
# MAGIC
# MAGIC Purpose: create realistic table history with insert, update, merge, and delete operations.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_maintenance_day21 VALUES
# MAGIC   (2105, 105, DATE'2026-07-17', CAST(60.00 AS DECIMAL(10,2)), 'pending', 'US', 'batch-002');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_maintenance_day21
# MAGIC SET amount = CAST(130.00 AS DECIMAL(10,2)),
# MAGIC     status = 'completed',
# MAGIC     source_batch_id = 'batch-003'
# MAGIC WHERE order_id = 2102;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_changes_day21 AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2103, 103, DATE'2026-07-16', CAST(410.00 AS DECIMAL(10,2)), 'completed', 'EU', 'batch-004', 'UPDATE'),
# MAGIC   (2106, 106, DATE'2026-07-17', CAST(95.00 AS DECIMAL(10,2)), 'completed', 'US', 'batch-004', 'INSERT')
# MAGIC AS t(order_id, customer_id, order_date, amount, status, region, source_batch_id, change_type);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_maintenance_day21 AS target
# MAGIC USING orders_changes_day21 AS source
# MAGIC ON target.order_id = source.order_id
# MAGIC WHEN MATCHED AND source.change_type = 'UPDATE' THEN UPDATE SET
# MAGIC   target.customer_id = source.customer_id,
# MAGIC   target.order_date = source.order_date,
# MAGIC   target.amount = source.amount,
# MAGIC   target.status = source.status,
# MAGIC   target.region = source.region,
# MAGIC   target.source_batch_id = source.source_batch_id
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   order_id, customer_id, order_date, amount, status, region, source_batch_id
# MAGIC ) VALUES (
# MAGIC   source.order_id, source.customer_id, source.order_date, source.amount, source.status, source.region, source.source_batch_id
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_maintenance_day21
# MAGIC WHERE status = 'cancelled';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_maintenance_day21 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_maintenance_day21;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Final table has 5 active rows.
# MAGIC - `order_id = 2102` is now completed with amount `130.00`.
# MAGIC - `order_id = 2103` has amount `410.00`.
# MAGIC - `order_id = 2104` is deleted.
# MAGIC - `DESCRIBE HISTORY` shows versioned operations for write/update/merge/delete.
# MAGIC
# MAGIC Operational meaning: each write creates a new Delta version. Maintenance decisions must respect time travel, rollback, audit, and downstream consumers that may depend on older versions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Query Current Metrics And Time Travel
# MAGIC
# MAGIC Purpose: prove that current state and historical state are different operational surfaces.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(*) AS current_order_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue
# MAGIC FROM orders_maintenance_day21
# MAGIC GROUP BY order_date
# MAGIC ORDER BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS version_zero_row_count
# MAGIC FROM orders_maintenance_day21 VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS current_row_count
# MAGIC FROM orders_maintenance_day21;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Current row count is 5.
# MAGIC - Version 0 row count is usually 0 because the table was created before inserts.
# MAGIC - Depending on your workspace history behavior, the first data-bearing version is visible in `DESCRIBE HISTORY`.
# MAGIC
# MAGIC Operational meaning: time travel depends on retained transaction logs and data files. Aggressive cleanup can break historical queries and rollback workflows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Run Safe Layout Maintenance
# MAGIC
# MAGIC Purpose: practice `OPTIMIZE` and inspect the maintenance operation in history.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE orders_maintenance_day21;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_maintenance_day21;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_maintenance_day21;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `OPTIMIZE` completes successfully.
# MAGIC - On a tiny learning table, file-count changes may be small or zero.
# MAGIC - `DESCRIBE HISTORY` includes an `OPTIMIZE` operation.
# MAGIC
# MAGIC Operational meaning: on real tables, `OPTIMIZE` reduces small-file overhead and improves scan efficiency. On tiny tables, the point is to learn the operation and how to verify it, not to expect a dramatic physical change.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Model Layout And Maintenance Policy
# MAGIC
# MAGIC Purpose: separate partitioning, liquid clustering, legacy ZORDER, and retention decisions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE table_layout_candidates_day21 (
# MAGIC   table_name STRING,
# MAGIC   table_tier STRING,
# MAGIC   size_gb INT,
# MAGIC   daily_write_gb INT,
# MAGIC   common_filter_columns STRING,
# MAGIC   query_pattern STRING,
# MAGIC   write_pattern STRING,
# MAGIC   current_layout STRING,
# MAGIC   recommended_layout STRING,
# MAGIC   reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO table_layout_candidates_day21 VALUES
# MAGIC   ('orders_maintenance_day21', 'silver', 128, 20, 'customer_id, order_date', 'point lookup plus date-range scans', 'daily merge/upsert', 'none', 'liquid_clustering', 'Large enough for layout management and mixed filters; prefer liquid clustering for new Databricks tables'),
# MAGIC   ('orders_events_raw_day21', 'bronze', 2048, 300, 'ingest_date', 'date-bounded replay and audit', 'append only', 'partitioned_by_ingest_date', 'partition_by_low_cardinality_date', 'Raw replay often benefits from coarse date partitioning when retention and replay are date-scoped'),
# MAGIC   ('orders_gold_daily_day21', 'gold', 24, 2, 'metric_date', 'dashboard date range', 'overwrite recent dates', 'none', 'no_manual_layout_yet', 'Small table; monitor before adding layout complexity'),
# MAGIC   ('orders_legacy_zorder_day21', 'silver', 640, 40, 'customer_id, region', 'legacy point lookup', 'daily merge/upsert', 'zorder_customer_region', 'migrate_to_liquid_when_rewriting', 'Existing ZORDER can remain, but new design should evaluate liquid clustering'),
# MAGIC   ('orders_high_cardinality_bad_partition_day21', 'silver', 500, 50, 'order_id', 'single order lookup', 'daily merge/upsert', 'partitioned_by_order_id', 'remove_high_cardinality_partitioning', 'High-cardinality partitioning creates too many tiny partitions and operational pain');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE table_maintenance_policy_day21 (
# MAGIC   table_name STRING,
# MAGIC   table_tier STRING,
# MAGIC   managed_by_uc BOOLEAN,
# MAGIC   predictive_optimization_enabled BOOLEAN,
# MAGIC   liquid_clustering_enabled BOOLEAN,
# MAGIC   small_file_count INT,
# MAGIC   stale_file_count INT,
# MAGIC   last_optimize_days_ago INT,
# MAGIC   last_vacuum_days_ago INT,
# MAGIC   time_travel_required_days INT,
# MAGIC   requested_vacuum_retention_hours INT,
# MAGIC   business_criticality STRING,
# MAGIC   owner_group STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO table_maintenance_policy_day21 VALUES
# MAGIC   ('orders_maintenance_day21', 'silver', true, false, false, 180, 40, 12, 30, 7, 168, 'HIGH', 'data-platform'),
# MAGIC   ('orders_events_raw_day21', 'bronze', true, true, false, 40, 900, 3, 45, 30, 168, 'HIGH', 'ingestion-platform'),
# MAGIC   ('orders_gold_daily_day21', 'gold', true, true, false, 12, 4, 20, 20, 14, 336, 'MEDIUM', 'analytics-platform'),
# MAGIC   ('orders_high_cardinality_bad_partition_day21', 'silver', true, false, false, 5000, 600, 60, 60, 30, 24, 'HIGH', 'data-platform');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_layout_candidates_day21 ORDER BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_maintenance_policy_day21 ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - One real Day 21 table plus modeled neighboring tables.
# MAGIC - Layout candidates show when to prefer liquid clustering, coarse date partitioning, no manual layout, or partition redesign.
# MAGIC - Maintenance policy captures small files, stale files, last optimize/vacuum age, time-travel requirement, and requested retention.
# MAGIC
# MAGIC Operational meaning: table layout is workload-specific. Do not partition by high-cardinality columns just because they are common filters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Evaluate Maintenance Gates With PySpark
# MAGIC
# MAGIC Purpose: convert table detail, history, layout policy, and retention policy into a production maintenance decision.

# COMMAND ----------

from pyspark.sql import functions as F

detail_df = spark.sql("DESCRIBE DETAIL de_learning.orders_maintenance_day21")
history_df = spark.sql("DESCRIBE HISTORY de_learning.orders_maintenance_day21")

policy_df = spark.table("de_learning.table_maintenance_policy_day21")
layout_df = spark.table("de_learning.table_layout_candidates_day21")

detail_summary_df = detail_df.select(
    F.lit("orders_maintenance_day21").alias("table_name"),
    F.col("numFiles").cast("int").alias("actual_num_files"),
    F.col("sizeInBytes").cast("long").alias("actual_size_bytes"),
    F.col("format").alias("actual_format")
)

history_summary_df = history_df.agg(
    F.count("*").alias("history_version_count"),
    F.sum(F.when(F.col("operation") == "OPTIMIZE", F.lit(1)).otherwise(F.lit(0))).alias("optimize_operation_count"),
    F.array_join(F.collect_set("operation"), ", ").alias("observed_operations")
).withColumn("table_name", F.lit("orders_maintenance_day21"))

base_df = (
    policy_df
    .join(layout_df.select("table_name", "current_layout", "recommended_layout", "reason"), on="table_name", how="left")
    .join(detail_summary_df, on="table_name", how="left")
    .join(history_summary_df, on="table_name", how="left")
)

optimize_check_df = base_df.select(
    "table_name",
    F.lit("optimize_needed").alias("check_name"),
    F.lit("MEDIUM").alias("severity"),
    F.when(
        (F.col("small_file_count") >= 100) | (F.col("last_optimize_days_ago") >= 7),
        F.lit("REVIEW")
    ).otherwise(F.lit("PASS")).alias("outcome"),
    F.concat(
        F.lit("small_file_count="), F.col("small_file_count"),
        F.lit(", last_optimize_days_ago="), F.col("last_optimize_days_ago"),
        F.lit(", observed_optimize_ops="), F.coalesce(F.col("optimize_operation_count"), F.lit(0))
    ).alias("evidence"),
    F.when(
        (F.col("small_file_count") >= 100) | (F.col("last_optimize_days_ago") >= 7),
        F.lit("Run or schedule OPTIMIZE; prefer predictive optimization where available")
    ).otherwise(F.lit("No immediate optimize action")).alias("recommended_action")
)

retention_check_df = base_df.select(
    "table_name",
    F.lit("vacuum_retention_safe").alias("check_name"),
    F.lit("HIGH").alias("severity"),
    F.when(
        F.col("requested_vacuum_retention_hours") >= F.col("time_travel_required_days") * F.lit(24),
        F.lit("PASS")
    ).otherwise(F.lit("FAIL")).alias("outcome"),
    F.concat(
        F.lit("requested_hours="), F.col("requested_vacuum_retention_hours"),
        F.lit(", required_hours="), F.col("time_travel_required_days") * F.lit(24)
    ).alias("evidence"),
    F.when(
        F.col("requested_vacuum_retention_hours") >= F.col("time_travel_required_days") * F.lit(24),
        F.lit("Allow VACUUM only after DRY RUN review")
    ).otherwise(F.lit("Block VACUUM; requested retention breaks time-travel requirement")).alias("recommended_action")
)

layout_check_df = base_df.select(
    "table_name",
    F.lit("layout_strategy").alias("check_name"),
    F.lit("MEDIUM").alias("severity"),
    F.when(
        F.col("recommended_layout").isin("remove_high_cardinality_partitioning", "liquid_clustering", "migrate_to_liquid_when_rewriting"),
        F.lit("REVIEW")
    ).otherwise(F.lit("PASS")).alias("outcome"),
    F.concat(
        F.lit("current_layout="), F.col("current_layout"),
        F.lit(", recommended_layout="), F.col("recommended_layout")
    ).alias("evidence"),
    F.col("reason").alias("recommended_action")
)

predictive_check_df = base_df.select(
    "table_name",
    F.lit("predictive_optimization").alias("check_name"),
    F.lit("LOW").alias("severity"),
    F.when(
        F.col("managed_by_uc") & (~F.col("predictive_optimization_enabled")),
        F.lit("REVIEW")
    ).otherwise(F.lit("PASS")).alias("outcome"),
    F.concat(
        F.lit("managed_by_uc="), F.col("managed_by_uc"),
        F.lit(", predictive_enabled="), F.col("predictive_optimization_enabled")
    ).alias("evidence"),
    F.when(
        F.col("managed_by_uc") & (~F.col("predictive_optimization_enabled")),
        F.lit("Evaluate enabling predictive optimization for managed Unity Catalog tables")
    ).otherwise(F.lit("No predictive optimization change needed")).alias("recommended_action")
)

maintenance_gate_df = (
    optimize_check_df
    .unionByName(retention_check_df)
    .unionByName(layout_check_df)
    .unionByName(predictive_check_df)
)

history_df.createOrReplaceTempView("orders_history_view_day21")
detail_df.createOrReplaceTempView("orders_detail_view_day21")
maintenance_gate_df.createOrReplaceTempView("maintenance_gate_results_day21")

display(maintenance_gate_df.orderBy("table_name", "severity", "check_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `detail_df = spark.sql("DESCRIBE DETAIL ...")` captures table metadata as a DataFrame.
# MAGIC - `history_df = spark.sql("DESCRIBE HISTORY ...")` captures Delta commit history as a DataFrame.
# MAGIC - `agg(...)` summarizes history, like SQL aggregation.
# MAGIC - `F.sum(F.when(...))` is conditional counting, like `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`.
# MAGIC - `join(..., how="left")` adds optional layout/history/detail context to policy rows.
# MAGIC - `unionByName(...)` stacks check-result DataFrames with matching column names.
# MAGIC - `createOrReplaceTempView(...)` lets later SQL query PySpark results.
# MAGIC - PySpark stays lazy until `display(...)` or another action runs.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   p.table_name,
# MAGIC   'vacuum_retention_safe' AS check_name,
# MAGIC   CASE
# MAGIC     WHEN p.requested_vacuum_retention_hours >= p.time_travel_required_days * 24 THEN 'PASS'
# MAGIC     ELSE 'FAIL'
# MAGIC   END AS outcome
# MAGIC FROM table_maintenance_policy_day21 p;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM maintenance_gate_results_day21
# MAGIC ORDER BY table_name, severity, check_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE maintenance_decision_summary_day21
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   COUNT(*) AS total_checks,
# MAGIC   SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) AS failed_checks,
# MAGIC   SUM(CASE WHEN outcome = 'REVIEW' THEN 1 ELSE 0 END) AS review_checks,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) > 0 THEN 'BLOCK_MAINTENANCE'
# MAGIC     WHEN SUM(CASE WHEN outcome = 'REVIEW' THEN 1 ELSE 0 END) > 0 THEN 'REVIEW_BEFORE_MAINTENANCE'
# MAGIC     ELSE 'READY_FOR_STANDARD_MAINTENANCE'
# MAGIC   END AS maintenance_decision
# MAGIC FROM maintenance_gate_results_day21
# MAGIC GROUP BY table_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM maintenance_decision_summary_day21 ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_maintenance_day21` should require review for optimize/layout/predictive optimization, but retention should pass.
# MAGIC - `orders_high_cardinality_bad_partition_day21` should fail retention safety because requested retention is only 24 hours while time travel requires 30 days.
# MAGIC - The summary blocks any table with failed retention checks.
# MAGIC
# MAGIC Operational meaning: maintenance automation should not blindly run `VACUUM` or layout changes. It needs gates that protect rollback, audit, time travel, and consumer expectations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Run VACUUM Dry Run Only
# MAGIC
# MAGIC Purpose: practice safe cleanup review without deleting files.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM orders_maintenance_day21 RETAIN 168 HOURS DRY RUN;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - On this new learning table, the dry run will usually return no removable files.
# MAGIC - That is correct: files must be older than the retention window before `VACUUM` can remove them.
# MAGIC
# MAGIC Operational meaning: `VACUUM ... DRY RUN` is the review step. Production cleanup should be dry-run reviewed before deleting files, especially where time travel or rollback is expected.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Final Checks
# MAGIC
# MAGIC Purpose: verify the table, history, maintenance gates, and cleanup posture.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'current_row_count' AS check_name, COUNT(*) AS observed_value FROM orders_maintenance_day21
# MAGIC UNION ALL
# MAGIC SELECT 'history_versions', COUNT(*) FROM orders_history_view_day21
# MAGIC UNION ALL
# MAGIC SELECT 'optimize_operations', COUNT(*) FROM orders_history_view_day21 WHERE operation = 'OPTIMIZE'
# MAGIC UNION ALL
# MAGIC SELECT 'layout_policy_rows', COUNT(*) FROM table_layout_candidates_day21
# MAGIC UNION ALL
# MAGIC SELECT 'maintenance_policy_rows', COUNT(*) FROM table_maintenance_policy_day21
# MAGIC UNION ALL
# MAGIC SELECT 'maintenance_gate_rows', COUNT(*) FROM maintenance_gate_results_day21
# MAGIC UNION ALL
# MAGIC SELECT 'blocked_tables', COUNT(*) FROM maintenance_decision_summary_day21 WHERE maintenance_decision = 'BLOCK_MAINTENANCE';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT operation, COUNT(*) AS operation_count
# MAGIC FROM orders_history_view_day21
# MAGIC GROUP BY operation
# MAGIC ORDER BY operation_count DESC, operation;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_maintenance_day21;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 5 current rows.
# MAGIC - At least one `OPTIMIZE` operation.
# MAGIC - 5 layout policy rows.
# MAGIC - 4 maintenance policy rows.
# MAGIC - 16 maintenance gate rows.
# MAGIC - At least one blocked table due to unsafe retention.
# MAGIC
# MAGIC Operational meaning: production table maintenance should leave behind queryable evidence: detail, history, policy rows, gate output, and dry-run cleanup posture.
