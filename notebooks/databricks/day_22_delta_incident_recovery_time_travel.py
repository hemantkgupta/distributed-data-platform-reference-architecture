# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 22 - Delta Incident Recovery And Time Travel
# MAGIC
# MAGIC Goal: recover from a bad Delta write by using `DESCRIBE HISTORY`, time travel, recovery decision gates, `RESTORE TABLE`, and forward-fix evidence.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Delta table history, time travel, troubleshooting, monitoring, and table operations.
# MAGIC - Professional stretch: incident diagnosis, restore vs forward-fix decisioning, rollback evidence, auditability, and production recovery runbooks.
# MAGIC
# MAGIC This notebook intentionally creates a bad write on a small learning table, diagnoses it, restores the table, and models when forward-fix is safer than restore.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create A Known-Good Delta Table
# MAGIC
# MAGIC Purpose: establish a baseline table with a clean first version.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_recovery_day22
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 7 days',
# MAGIC   'delta.logRetentionDuration' = 'interval 30 days',
# MAGIC   'incident.owner' = 'data-platform',
# MAGIC   'incident.recovery_slo_minutes' = '30'
# MAGIC )
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2201, 101, DATE'2026-07-18', CAST(250.00 AS DECIMAL(10,2)), 'completed', 'US', 'batch-001'),
# MAGIC   (2202, 102, DATE'2026-07-18', CAST(125.50 AS DECIMAL(10,2)), 'pending', 'US', 'batch-001'),
# MAGIC   (2203, 103, DATE'2026-07-19', CAST(400.00 AS DECIMAL(10,2)), 'completed', 'EU', 'batch-001'),
# MAGIC   (2204, 104, DATE'2026-07-19', CAST(80.00 AS DECIMAL(10,2)), 'cancelled', 'APAC', 'batch-001')
# MAGIC AS t(order_id, customer_id, order_date, amount, status, region, source_batch_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_recovery_day22 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_recovery_day22;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 4 rows.
# MAGIC - `DESCRIBE HISTORY` shows the table creation at version 0.
# MAGIC
# MAGIC Operational meaning: recovery starts by knowing which version was good. In production, this comes from history, quality checks, and business metrics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Apply A Good Write
# MAGIC
# MAGIC Purpose: create a known-good current version before the incident.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_good_changes_day22 AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2202, 102, DATE'2026-07-18', CAST(130.00 AS DECIMAL(10,2)), 'completed', 'US', 'batch-002', 'UPDATE'),
# MAGIC   (2205, 105, DATE'2026-07-19', CAST(95.00 AS DECIMAL(10,2)), 'completed', 'US', 'batch-002', 'INSERT')
# MAGIC AS t(order_id, customer_id, order_date, amount, status, region, source_batch_id, change_type);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_recovery_day22 AS target
# MAGIC USING orders_good_changes_day22 AS source
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
# MAGIC SELECT * FROM orders_recovery_day22 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
# MAGIC   MAX(amount) AS max_amount
# MAGIC FROM orders_recovery_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_recovery_day22;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 5 rows after the good merge.
# MAGIC - `order_id = 2202` is completed with amount `130.00`.
# MAGIC - `order_id = 2205` is inserted.
# MAGIC - This should be version 1 in a fresh run.
# MAGIC
# MAGIC Operational meaning: the version just before the bad write is your primary rollback candidate.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Create A Bad Write
# MAGIC
# MAGIC Purpose: simulate a production incident where a transformation multiplies US order amounts by 100.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_recovery_day22
# MAGIC SET amount = amount * 100,
# MAGIC     source_batch_id = 'batch-003-bad'
# MAGIC WHERE region = 'US';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_recovery_day22 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
# MAGIC   MAX(amount) AS max_amount,
# MAGIC   SUM(CASE WHEN amount > 1000 THEN 1 ELSE 0 END) AS suspicious_amount_rows
# MAGIC FROM orders_recovery_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_recovery_day22;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - US rows have amounts multiplied by 100.
# MAGIC - `suspicious_amount_rows` is greater than 0.
# MAGIC - `DESCRIBE HISTORY` shows an `UPDATE` after the good `MERGE`.
# MAGIC - In a fresh run, the bad write should be version 2.
# MAGIC
# MAGIC Operational meaning: a bad write is usually visible as a metric anomaly plus a suspicious table-history operation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Time Travel Before The Bad Write
# MAGIC
# MAGIC Purpose: query the known-good version without changing the current table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_recovery_day22 VERSION AS OF 1 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'current_bad_state' AS state_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
# MAGIC   MAX(amount) AS max_amount
# MAGIC FROM orders_recovery_day22
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'known_good_version_1' AS state_name,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
# MAGIC   MAX(amount) AS max_amount
# MAGIC FROM orders_recovery_day22 VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Version 1 has normal amounts.
# MAGIC - Current state has inflated amounts.
# MAGIC
# MAGIC Operational meaning: time travel is the diagnosis surface. It lets you prove what changed before deciding how to recover.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Preserve Bad State For Forward-Fix Practice
# MAGIC
# MAGIC Purpose: keep a copy of the bad state before restoring the main table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_bad_snapshot_day22
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM orders_recovery_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bad_snapshot_day22 ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_bad_snapshot_day22` contains the inflated bad amounts.
# MAGIC
# MAGIC Operational meaning: keeping an investigation snapshot can help audit and forward-fix testing without leaving the production table corrupted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Evaluate Restore Vs Forward-Fix With PySpark
# MAGIC
# MAGIC Purpose: classify incident recovery choices instead of reflexively restoring every bad write.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE recovery_incidents_day22 (
# MAGIC   incident_id STRING,
# MAGIC   table_name STRING,
# MAGIC   symptom STRING,
# MAGIC   bad_version INT,
# MAGIC   known_good_version INT,
# MAGIC   bad_write_is_latest BOOLEAN,
# MAGIC   downstream_published BOOLEAN,
# MAGIC   good_writes_after_bad BOOLEAN,
# MAGIC   affected_scope STRING,
# MAGIC   requires_audit_preservation BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO recovery_incidents_day22 VALUES
# MAGIC   ('ri-001', 'orders_recovery_day22', 'US amounts multiplied by 100', 2, 1, true, false, false, 'broad_table_state', false),
# MAGIC   ('ri-002', 'orders_customer_features_day22', 'bad enrichment column published to downstream model', 9, 8, false, true, true, 'single_column', true),
# MAGIC   ('ri-003', 'orders_gold_dashboard_day22', 'wrong metric already exported to finance', 15, 14, false, true, false, 'published_metric', true),
# MAGIC   ('ri-004', 'orders_silver_updates_day22', 'bad write followed by valid late-arriving corrections', 21, 20, false, false, true, 'subset_rows', false);

# COMMAND ----------

from pyspark.sql import functions as F

history_df = spark.sql("DESCRIBE HISTORY de_learning.orders_recovery_day22")
incidents_df = spark.table("de_learning.recovery_incidents_day22")

history_context_df = (
    history_df
    .select(
        F.col("version").cast("int").alias("history_version"),
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics"
    )
)

recovery_decision_df = (
    incidents_df.alias("i")
    .join(history_context_df.alias("h"), F.col("i.bad_version") == F.col("h.history_version"), "left")
    .withColumn(
        "recovery_action",
        F.when(
            F.col("bad_write_is_latest") & (~F.col("downstream_published")) & (~F.col("good_writes_after_bad")),
            F.lit("RESTORE_TABLE_TO_KNOWN_GOOD_VERSION")
        )
        .when(
            F.col("good_writes_after_bad"),
            F.lit("FORWARD_FIX_WITH_REPLAY_ANALYSIS")
        )
        .when(
            F.col("downstream_published") | F.col("requires_audit_preservation"),
            F.lit("FORWARD_FIX_WITH_AUDIT_NOTE")
        )
        .otherwise(F.lit("MANUAL_REVIEW"))
    )
    .withColumn(
        "required_evidence",
        F.when(
            F.col("recovery_action") == "RESTORE_TABLE_TO_KNOWN_GOOD_VERSION",
            F.lit("Capture bad version, known-good version, metric diff, RESTORE output, and post-restore validation")
        )
        .when(
            F.col("recovery_action") == "FORWARD_FIX_WITH_REPLAY_ANALYSIS",
            F.lit("Replay good writes after bad version, isolate affected rows, and apply correction merge")
        )
        .when(
            F.col("recovery_action") == "FORWARD_FIX_WITH_AUDIT_NOTE",
            F.lit("Publish correcting write, preserve audit trail, and notify downstream consumers")
        )
        .otherwise(F.lit("Escalate to incident owner"))
    )
    .select(
        "incident_id",
        "table_name",
        "symptom",
        "bad_version",
        "known_good_version",
        "operation",
        "bad_write_is_latest",
        "downstream_published",
        "good_writes_after_bad",
        "affected_scope",
        "recovery_action",
        "required_evidence"
    )
)

history_df.createOrReplaceTempView("orders_recovery_history_view_day22")
recovery_decision_df.createOrReplaceTempView("recovery_decisions_view_day22")
display(recovery_decision_df.orderBy("incident_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `history_df = spark.sql("DESCRIBE HISTORY ...")` turns table history into a DataFrame.
# MAGIC - `incidents_df = spark.table(...)` loads incident records from a SQL table.
# MAGIC - `join(..., "left")` attaches history context when the bad version exists in this table.
# MAGIC - `F.col("bad_write_is_latest") & ...` is boolean logic, like SQL `AND`.
# MAGIC - `~F.col("downstream_published")` means `NOT downstream_published`.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - `createOrReplaceTempView(...)` lets the next SQL cells query the PySpark output.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   incident_id,
# MAGIC   CASE
# MAGIC     WHEN bad_write_is_latest AND NOT downstream_published AND NOT good_writes_after_bad
# MAGIC       THEN 'RESTORE_TABLE_TO_KNOWN_GOOD_VERSION'
# MAGIC     WHEN good_writes_after_bad
# MAGIC       THEN 'FORWARD_FIX_WITH_REPLAY_ANALYSIS'
# MAGIC     ELSE 'FORWARD_FIX_WITH_AUDIT_NOTE'
# MAGIC   END AS recovery_action
# MAGIC FROM recovery_incidents_day22;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE recovery_decisions_day22
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM recovery_decisions_view_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT incident_id, table_name, bad_version, known_good_version, recovery_action, required_evidence
# MAGIC FROM recovery_decisions_day22
# MAGIC ORDER BY incident_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `ri-001` recommends `RESTORE_TABLE_TO_KNOWN_GOOD_VERSION`.
# MAGIC - Incidents with downstream publication or later good writes recommend forward-fix patterns.
# MAGIC
# MAGIC Operational meaning: restore is clean when the bad write is latest and unpublished. Forward-fix is safer when consumers already saw the bad data or valid writes happened after the bad version.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Restore The Main Table
# MAGIC
# MAGIC Purpose: recover the actual table to the known-good version.

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE orders_recovery_day22 TO VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_recovery_day22 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS restored_row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS restored_completed_revenue,
# MAGIC   MAX(amount) AS restored_max_amount,
# MAGIC   SUM(CASE WHEN amount > 1000 THEN 1 ELSE 0 END) AS restored_suspicious_amount_rows
# MAGIC FROM orders_recovery_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_recovery_day22;

# COMMAND ----------

from pyspark.sql import functions as F

refreshed_history_df = spark.sql("DESCRIBE HISTORY de_learning.orders_recovery_day22")
refreshed_history_df.createOrReplaceTempView("orders_recovery_history_view_day22")
display(refreshed_history_df.orderBy(F.col("version").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Amounts return to normal.
# MAGIC - `restored_suspicious_amount_rows = 0`.
# MAGIC - `DESCRIBE HISTORY` includes a `RESTORE` operation.
# MAGIC - The refreshed `orders_recovery_history_view_day22` includes the restore commit for later evidence checks.
# MAGIC
# MAGIC Operational meaning: restore creates a new table version. It does not erase the fact that the bad write happened; it adds a recovery commit to the timeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Practice Forward-Fix On The Bad Snapshot
# MAGIC
# MAGIC Purpose: repair bad data without using table restore.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_bad_snapshot_day22
# MAGIC SET amount = amount / 100,
# MAGIC     source_batch_id = 'batch-003-forward-fix'
# MAGIC WHERE source_batch_id = 'batch-003-bad';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bad_snapshot_day22 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS forward_fix_row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS forward_fix_completed_revenue,
# MAGIC   MAX(amount) AS forward_fix_max_amount,
# MAGIC   SUM(CASE WHEN amount > 1000 THEN 1 ELSE 0 END) AS forward_fix_suspicious_amount_rows
# MAGIC FROM orders_bad_snapshot_day22;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - The snapshot amounts are corrected.
# MAGIC - Suspicious amount count returns to 0.
# MAGIC
# MAGIC Operational meaning: forward-fix is usually better when later valid writes must be preserved or when external consumers need an explicit correcting transaction.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 9 - Write Recovery Evidence
# MAGIC
# MAGIC Purpose: leave an auditable record of the incident, decision, and validation results.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE recovery_evidence_day22
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   'ri-001' AS incident_id,
# MAGIC   'orders_recovery_day22' AS table_name,
# MAGIC   2 AS bad_version,
# MAGIC   1 AS known_good_version,
# MAGIC   'RESTORE_TABLE_TO_KNOWN_GOOD_VERSION' AS recovery_action,
# MAGIC   (SELECT SUM(CASE WHEN amount > 1000 THEN 1 ELSE 0 END) FROM orders_recovery_day22) AS post_recovery_suspicious_amount_rows,
# MAGIC   (SELECT COUNT(*) FROM orders_recovery_day22) AS post_recovery_row_count,
# MAGIC   current_timestamp() AS evidence_recorded_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM recovery_evidence_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT operation, COUNT(*) AS operation_count
# MAGIC FROM orders_recovery_history_view_day22
# MAGIC GROUP BY operation
# MAGIC ORDER BY operation_count DESC, operation;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Evidence row records bad version, known-good version, action, row count, and post-recovery suspicious row count.
# MAGIC
# MAGIC Operational meaning: an incident is not done when the table looks fixed. It is done when the recovery decision and validation evidence are durable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 10 - Final Checks
# MAGIC
# MAGIC Purpose: verify the recovered table, decisions, snapshot, evidence, and history.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'restored_current_rows' AS check_name, COUNT(*) AS observed_value FROM orders_recovery_day22
# MAGIC UNION ALL
# MAGIC SELECT 'restored_suspicious_rows', COUNT(*) FROM orders_recovery_day22 WHERE amount > 1000
# MAGIC UNION ALL
# MAGIC SELECT 'bad_snapshot_suspicious_rows_after_forward_fix', COUNT(*) FROM orders_bad_snapshot_day22 WHERE amount > 1000
# MAGIC UNION ALL
# MAGIC SELECT 'recovery_incident_rows', COUNT(*) FROM recovery_incidents_day22
# MAGIC UNION ALL
# MAGIC SELECT 'recovery_decision_rows', COUNT(*) FROM recovery_decisions_day22
# MAGIC UNION ALL
# MAGIC SELECT 'recovery_evidence_rows', COUNT(*) FROM recovery_evidence_day22;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_recovery_day22;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Restored current rows: 5.
# MAGIC - Restored suspicious rows: 0.
# MAGIC - Forward-fixed snapshot suspicious rows: 0.
# MAGIC - 4 incident rows.
# MAGIC - 4 decision rows.
# MAGIC - 1 evidence row.
# MAGIC
# MAGIC Operational meaning: production recovery should prove table state, recovery decision, forward-fix alternative, and evidence record.
