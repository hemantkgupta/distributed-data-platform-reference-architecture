# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 23 - Unity Catalog Drop Recovery And UNDROP
# MAGIC
# MAGIC Goal: practice Unity Catalog table lifecycle recovery with accidental drop diagnosis, `SHOW TABLES DROPPED`, `UNDROP TABLE`, clone-based investigation, and ownership/privilege evidence.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Unity Catalog tables, governance/security, troubleshooting, and table lifecycle operations.
# MAGIC - Professional stretch: production recovery runbooks, managed vs external caveats, shallow/deep clone tradeoffs, privilege evidence, and incident blast-radius classification.
# MAGIC
# MAGIC This notebook runs the managed-table recovery path directly. External-table and pipeline-backed relation cases are modeled as decision gates because they depend on workspace-admin objects such as external locations, storage credentials, and Lakeflow pipelines.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create A Recoverable Managed Table
# MAGIC
# MAGIC Purpose: create a Unity Catalog managed Delta table that can be dropped and recovered.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_undrop_archive_day23;
# MAGIC DROP TABLE IF EXISTS lifecycle_recovery_cases_day23;
# MAGIC DROP TABLE IF EXISTS lifecycle_recovery_decisions_day23;
# MAGIC DROP TABLE IF EXISTS lifecycle_recovery_evidence_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_undrop_day23
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'criticality' = 'high',
# MAGIC   'owner_domain' = 'orders',
# MAGIC   'recovery_playbook' = 'undrop-managed-table'
# MAGIC )
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2301, 101, DATE'2026-07-20', CAST(250.00 AS DECIMAL(10,2)), 'completed', 'US'),
# MAGIC   (2302, 102, DATE'2026-07-20', CAST(130.00 AS DECIMAL(10,2)), 'completed', 'US'),
# MAGIC   (2303, 103, DATE'2026-07-20', CAST(400.00 AS DECIMAL(10,2)), 'completed', 'EU'),
# MAGIC   (2304, 104, DATE'2026-07-21', CAST(90.00 AS DECIMAL(10,2)), 'pending', 'APAC'),
# MAGIC   (2305, 105, DATE'2026-07-21', CAST(75.00 AS DECIMAL(10,2)), 'completed', 'US')
# MAGIC AS t(order_id, customer_id, order_date, amount, status, region);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_undrop_day23 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES orders_undrop_day23;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 5 rows in `orders_undrop_day23`.
# MAGIC - `DESCRIBE DETAIL` shows a Delta table in the current catalog and schema.
# MAGIC - Table properties show `criticality`, `owner_domain`, and `recovery_playbook`.
# MAGIC
# MAGIC Operational meaning: before you recover a table, you need to know whether it is managed or external, who owns it, and what business surface it supports.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create An Investigation Clone
# MAGIC
# MAGIC Purpose: preserve an independent copy of the table before doing a risky lifecycle operation.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_undrop_archive_day23
# MAGIC DEEP CLONE orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_undrop_archive_day23 SET TBLPROPERTIES (
# MAGIC   'clone_purpose' = 'drop-recovery-investigation',
# MAGIC   'source_table' = 'orders_undrop_day23'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'source_table' AS table_role, COUNT(*) AS row_count, SUM(amount) AS gross_amount
# MAGIC FROM orders_undrop_day23
# MAGIC UNION ALL
# MAGIC SELECT 'deep_clone_archive' AS table_role, COUNT(*) AS row_count, SUM(amount) AS gross_amount
# MAGIC FROM orders_undrop_archive_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_undrop_archive_day23;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Source and clone both show 5 rows and the same gross amount.
# MAGIC - Clone history shows the clone operation.
# MAGIC
# MAGIC Operational meaning: deep clone gives you an investigation copy that does not depend on recovering the original table's files. Shallow clone is cheaper, but managed shallow clones can break if the source is dropped and not recovered within the recovery window.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Simulate Accidental DROP TABLE
# MAGIC
# MAGIC Purpose: drop the managed table and inspect the dropped-table recovery surface.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES DROPPED IN de_learning LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_undrop_day23` appears in the dropped tables list if your current schema is in Unity Catalog.
# MAGIC - The result includes a `tableId`, `deletedAt`, table type, creator, and owner.
# MAGIC
# MAGIC Operational meaning: after an accidental drop, do not immediately recreate the table with the same name. First list dropped tables and capture the table ID and owner evidence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Recover With UNDROP TABLE
# MAGIC
# MAGIC Purpose: restore the most recently dropped table with this name.

# COMMAND ----------

# MAGIC %sql
# MAGIC UNDROP TABLE orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_undrop_day23 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS recovered_row_count,
# MAGIC   SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS recovered_completed_revenue,
# MAGIC   MAX(amount) AS recovered_max_amount
# MAGIC FROM orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_undrop_day23;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - The table name is active again.
# MAGIC - The recovered table has 5 rows.
# MAGIC - Table history is available after recovery.
# MAGIC
# MAGIC Operational meaning: `UNDROP TABLE` restores the dropped relation within the configured recovery period. It recovers table metadata such as properties and privileges, but you still validate data and constraints after recovery.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Capture Ownership And Privilege Evidence
# MAGIC
# MAGIC Purpose: inspect who is running the recovery and what grants exist after recovery.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_catalog() AS catalog_name,
# MAGIC   current_schema() AS schema_name,
# MAGIC   current_user() AS current_principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE orders_undrop_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES orders_undrop_day23;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Current catalog, schema, and principal are visible.
# MAGIC - Grants show who can access or manage the recovered table.
# MAGIC - Table properties still show the recovery metadata from Part 1.
# MAGIC
# MAGIC Operational meaning: recovery is a security event. The runbook should capture the principal, owner/grant state, and table properties after recovery.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Classify Recovery Cases With PySpark
# MAGIC
# MAGIC Purpose: decide whether an object can be undropped or requires escalation, rebuild, or namespace cleanup.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lifecycle_recovery_cases_day23 (
# MAGIC   recovery_case_id STRING,
# MAGIC   relation_name STRING,
# MAGIC   relation_type STRING,
# MAGIC   storage_model STRING,
# MAGIC   parent_catalog_exists BOOLEAN,
# MAGIC   parent_schema_exists BOOLEAN,
# MAGIC   within_recovery_period BOOLEAN,
# MAGIC   same_name_active BOOLEAN,
# MAGIC   external_location_exists BOOLEAN,
# MAGIC   backing_pipeline_exists BOOLEAN,
# MAGIC   requester_has_required_privileges BOOLEAN,
# MAGIC   downstream_consumers INT,
# MAGIC   direct_cloud_access BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lifecycle_recovery_cases_day23 VALUES
# MAGIC   ('lr-001', 'orders_undrop_day23', 'TABLE', 'managed', true, true, true, false, true, true, true, 2, false),
# MAGIC   ('lr-002', 'orders_external_day23', 'TABLE', 'external', true, true, true, false, false, true, true, 1, true),
# MAGIC   ('lr-003', 'orders_collision_day23', 'TABLE', 'managed', true, true, true, true, true, true, true, 0, false),
# MAGIC   ('lr-004', 'orders_old_drop_day23', 'TABLE', 'managed', true, true, false, false, true, true, true, 4, false),
# MAGIC   ('lr-005', 'orders_mv_day23', 'MATERIALIZED_VIEW', 'managed', true, true, true, false, true, false, true, 3, false),
# MAGIC   ('lr-006', 'orders_secure_day23', 'TABLE', 'managed', true, true, true, false, true, true, false, 5, false);

# COMMAND ----------

from pyspark.sql import functions as F

cases_df = spark.table("de_learning.lifecycle_recovery_cases_day23")

recovery_decisions_df = (
    cases_df
    .withColumn(
        "recovery_action",
        F.when(
            (~F.col("parent_catalog_exists")) | (~F.col("parent_schema_exists")),
            F.lit("BLOCK_PARENT_NAMESPACE_MISSING")
        )
        .when(
            ~F.col("requester_has_required_privileges"),
            F.lit("ESCALATE_TO_OWNER_OR_MANAGE_PRIVILEGE")
        )
        .when(
            ~F.col("within_recovery_period"),
            F.lit("REBUILD_FROM_BACKUP_OR_SOURCE")
        )
        .when(
            F.col("relation_type").isin("MATERIALIZED_VIEW", "STREAMING_TABLE") & (~F.col("backing_pipeline_exists")),
            F.lit("BLOCK_BACKING_PIPELINE_MISSING")
        )
        .when(
            F.col("same_name_active"),
            F.lit("RENAME_ACTIVE_RELATION_THEN_UNDROP_WITH_ID")
        )
        .when(
            (F.col("storage_model") == "external") & (~F.col("external_location_exists")),
            F.lit("BLOCK_EXTERNAL_LOCATION_MISSING")
        )
        .when(
            (F.col("storage_model") == "external") & F.col("direct_cloud_access"),
            F.lit("UNDROP_AND_REVIEW_DIRECT_CLOUD_ACCESS")
        )
        .otherwise(F.lit("UNDROP_TABLE"))
    )
    .withColumn(
        "blast_radius",
        F.when(F.col("downstream_consumers") >= 4, F.lit("HIGH"))
        .when(F.col("downstream_consumers") >= 1, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )
    .withColumn(
        "evidence_required",
        F.when(
            F.col("recovery_action") == "UNDROP_TABLE",
            F.lit("SHOW TABLES DROPPED output, UNDROP command result, row count, grants, owner, and history")
        )
        .when(
            F.col("recovery_action") == "RENAME_ACTIVE_RELATION_THEN_UNDROP_WITH_ID",
            F.lit("Dropped tableId, active table rename evidence, UNDROP WITH ID output, consumer notification")
        )
        .when(
            F.col("recovery_action") == "UNDROP_AND_REVIEW_DIRECT_CLOUD_ACCESS",
            F.lit("External location privileges, direct cloud access review, row validation, owner approval")
        )
        .when(
            F.col("recovery_action").startswith("BLOCK"),
            F.lit("Blocking reason, owning team, missing dependency, and rebuild or escalation plan")
        )
        .otherwise(F.lit("Owner approval, rebuild source, downstream replay plan, and validation metrics"))
    )
    .select(
        "recovery_case_id",
        "relation_name",
        "relation_type",
        "storage_model",
        "downstream_consumers",
        "blast_radius",
        "recovery_action",
        "evidence_required"
    )
)

recovery_decisions_df.createOrReplaceTempView("lifecycle_recovery_decisions_view_day23")
display(recovery_decisions_df.orderBy("recovery_case_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `cases_df` represents recovery scenarios: managed table, external table, name collision, expired recovery window, pipeline-backed relation, and missing privilege.
# MAGIC - SQL equivalent: `SELECT ..., CASE WHEN ... THEN recovery_action END FROM lifecycle_recovery_cases_day23`.
# MAGIC - `F.col("within_recovery_period")` references a boolean column.
# MAGIC - `~F.col(...)` means SQL `NOT`.
# MAGIC - `isin(...)` is SQL `IN (...)`.
# MAGIC - `withColumn(...)` adds derived decision columns without changing the source table.
# MAGIC - PySpark is lazy here until `display(...)` runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lifecycle_recovery_decisions_day23
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM lifecycle_recovery_decisions_view_day23;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT recovery_case_id, relation_name, storage_model, blast_radius, recovery_action
# MAGIC FROM lifecycle_recovery_decisions_day23
# MAGIC ORDER BY recovery_case_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Managed in-window table case chooses `UNDROP_TABLE`.
# MAGIC - External case blocks or adds external-location/direct-cloud-access review.
# MAGIC - Same-name collision case recommends rename plus `UNDROP TABLE WITH ID`.
# MAGIC - Expired recovery window case recommends rebuild from backup or source.
# MAGIC - Pipeline-backed relation without its backing pipeline is blocked.
# MAGIC
# MAGIC Operational meaning: `UNDROP` is not a blind command. The safe action depends on namespace state, relation type, privilege, recovery window, external location state, and downstream blast radius.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Record A Specific UNDROP Runbook
# MAGIC
# MAGIC Purpose: make the manual incident steps queryable and reusable.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE undrop_runbook_day23
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Confirm namespace', 'SELECT current_catalog(), current_schema(), current_user()', 'Parent catalog and schema must exist before UNDROP'),
# MAGIC   (2, 'List dropped tables', 'SHOW TABLES DROPPED IN de_learning', 'Capture tableId, deletedAt, owner, and table type'),
# MAGIC   (3, 'Resolve name collision', 'ALTER TABLE active_name RENAME TO active_name_replacement', 'Only needed if a new active table already uses the original name'),
# MAGIC   (4, 'Recover table', 'UNDROP TABLE orders_undrop_day23', 'Use UNDROP TABLE WITH ID when multiple dropped relations have the same name'),
# MAGIC   (5, 'Validate table', 'SELECT COUNT(*), SUM(amount) FROM orders_undrop_day23', 'Compare with clone, lineage, or last trusted metric'),
# MAGIC   (6, 'Capture security evidence', 'SHOW GRANTS ON TABLE orders_undrop_day23', 'Confirm owner and grants after recovery')
# MAGIC AS t(step_number, step_name, command_shape, validation_note);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM undrop_runbook_day23 ORDER BY step_number;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 ordered runbook steps.
# MAGIC
# MAGIC Operational meaning: table recovery should be repeatable. A runbook prevents the two common mistakes: recreating a table before checking dropped-table IDs, and forgetting security evidence after recovery.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Write Recovery Evidence
# MAGIC
# MAGIC Purpose: persist the incident decision and post-recovery validation.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lifecycle_recovery_evidence_day23
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   'uc-drop-incident-day23-001' AS incident_id,
# MAGIC   current_catalog() AS catalog_name,
# MAGIC   current_schema() AS schema_name,
# MAGIC   current_user() AS recovery_principal,
# MAGIC   'orders_undrop_day23' AS recovered_relation_name,
# MAGIC   'managed' AS storage_model,
# MAGIC   (SELECT recovery_action FROM lifecycle_recovery_decisions_day23 WHERE recovery_case_id = 'lr-001') AS recovery_action,
# MAGIC   (SELECT COUNT(*) FROM orders_undrop_day23) AS recovered_row_count,
# MAGIC   (SELECT SUM(amount) FROM orders_undrop_day23) AS recovered_gross_amount,
# MAGIC   (SELECT COUNT(*) FROM orders_undrop_archive_day23) AS clone_row_count,
# MAGIC   (SELECT SUM(amount) FROM orders_undrop_archive_day23) AS clone_gross_amount,
# MAGIC   current_timestamp() AS evidence_recorded_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lifecycle_recovery_evidence_day23;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - One evidence row.
# MAGIC - Recovered row count and clone row count both equal 5.
# MAGIC - Recovered gross amount equals clone gross amount.
# MAGIC
# MAGIC Operational meaning: durable evidence turns table recovery from "the command succeeded" into an auditable incident closure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 9 - Final Checks
# MAGIC
# MAGIC Purpose: verify recovered table, clone, decisions, runbook, and evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'recovered_table_rows' AS check_name, CAST(COUNT(*) AS STRING) AS observed_value, '5' AS expected_value
# MAGIC FROM orders_undrop_day23
# MAGIC UNION ALL
# MAGIC SELECT 'archive_clone_rows', CAST(COUNT(*) AS STRING), '5'
# MAGIC FROM orders_undrop_archive_day23
# MAGIC UNION ALL
# MAGIC SELECT 'recovery_case_rows', CAST(COUNT(*) AS STRING), '6'
# MAGIC FROM lifecycle_recovery_cases_day23
# MAGIC UNION ALL
# MAGIC SELECT 'recovery_decision_rows', CAST(COUNT(*) AS STRING), '6'
# MAGIC FROM lifecycle_recovery_decisions_day23
# MAGIC UNION ALL
# MAGIC SELECT 'runbook_steps', CAST(COUNT(*) AS STRING), '6'
# MAGIC FROM undrop_runbook_day23
# MAGIC UNION ALL
# MAGIC SELECT 'evidence_rows', CAST(COUNT(*) AS STRING), '1'
# MAGIC FROM lifecycle_recovery_evidence_day23
# MAGIC UNION ALL
# MAGIC SELECT 'managed_case_action', MAX(recovery_action), 'UNDROP_TABLE'
# MAGIC FROM lifecycle_recovery_decisions_day23
# MAGIC WHERE recovery_case_id = 'lr-001';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_undrop_day23;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All count checks match expected values.
# MAGIC - Managed recovery decision is `UNDROP_TABLE`.
# MAGIC - Recovered table history is available.
# MAGIC
# MAGIC Operational meaning: final validation checks the recovered data surface, clone evidence, decision logic, runbook, and durable incident record.
