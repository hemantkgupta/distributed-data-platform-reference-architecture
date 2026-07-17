# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 19 - Managed Vs External Storage Lifecycle
# MAGIC
# MAGIC Goal: practice Unity Catalog storage lifecycle decisions: managed tables, external tables, managed volumes, external volumes, storage credentials, external locations, drop behavior, migration checks, and cleanup gates.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Unity Catalog objects, managed vs external tables, volumes, governance and security.
# MAGIC - Professional stretch: external-location risk, storage lifecycle ownership, deletion semantics, migration readiness, direct-cloud-access risk, and audit evidence.
# MAGIC
# MAGIC This notebook keeps the cloud-admin parts runnable by modeling storage credentials and external locations as Delta metadata tables. The managed Delta table is real.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Model Storage Credentials And External Locations
# MAGIC
# MAGIC Purpose: understand the external storage governance objects before creating tables or volumes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE storage_credentials_day19 (
# MAGIC   credential_name STRING,
# MAGIC   cloud_provider STRING,
# MAGIC   cloud_role_or_identity STRING,
# MAGIC   owner_principal STRING,
# MAGIC   direct_cloud_access_allowed BOOLEAN,
# MAGIC   purpose STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO storage_credentials_day19 VALUES
# MAGIC   ('cred_orders_lake_day19', 'aws', 'arn:aws:iam::111122223333:role/databricks-orders-lake', 'platform-security@example.com', false, 'Unity Catalog access to orders lake prefixes'),
# MAGIC   ('cred_legacy_raw_day19', 'aws', 'arn:aws:iam::111122223333:role/legacy-raw-shared', 'legacy-platform@example.com', true, 'legacy shared raw bucket with direct non-Databricks access risk'),
# MAGIC   ('cred_managed_root_day19', 'aws', 'arn:aws:iam::111122223333:role/databricks-managed-root', 'platform-admin@example.com', false, 'managed storage root for Unity Catalog managed data');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE external_locations_day19 (
# MAGIC   external_location_name STRING,
# MAGIC   storage_url STRING,
# MAGIC   credential_name STRING,
# MAGIC   owner_principal STRING,
# MAGIC   read_only BOOLEAN,
# MAGIC   intended_use STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO external_locations_day19 VALUES
# MAGIC   ('loc_orders_external_silver_day19', 's3://company-data-lake/orders/silver/', 'cred_orders_lake_day19', 'orders-domain@example.com', false, 'external Delta tables for orders domain'),
# MAGIC   ('loc_orders_raw_files_day19', 's3://company-landing-zone/orders/raw/', 'cred_orders_lake_day19', 'ingestion-owner@example.com', false, 'external volume for raw order files'),
# MAGIC   ('loc_legacy_raw_day19', 's3://legacy-shared-raw/orders/', 'cred_legacy_raw_day19', 'legacy-platform@example.com', true, 'read-only legacy external table registration');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM storage_credentials_day19 ORDER BY credential_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM external_locations_day19 ORDER BY external_location_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 3 storage credentials.
# MAGIC - 3 external locations.
# MAGIC - `cred_legacy_raw_day19` has direct cloud access risk.
# MAGIC - `loc_legacy_raw_day19` is read-only.
# MAGIC
# MAGIC Operational meaning: external storage governance starts at the credential and location. External tables and volumes inherit operational risk from the location and underlying cloud access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create A Real Managed Table And Model Storage Assets
# MAGIC
# MAGIC Purpose: compare real managed Delta behavior with modeled external assets.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_managed_storage_day19 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   region STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'storage_model' = 'managed',
# MAGIC   'owner_domain' = 'orders',
# MAGIC   'recovery_window_days' = '7'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_managed_storage_day19 VALUES
# MAGIC   (1901, 901, DATE'2026-07-15', CAST(240.00 AS DECIMAL(10,2)), 'completed', 'US'),
# MAGIC   (1902, 902, DATE'2026-07-15', CAST(120.50 AS DECIMAL(10,2)), 'pending', 'US'),
# MAGIC   (1903, 903, DATE'2026-07-16', CAST(410.75 AS DECIMAL(10,2)), 'completed', 'EU'),
# MAGIC   (1904, 904, DATE'2026-07-16', CAST(70.25 AS DECIMAL(10,2)), 'cancelled', 'APAC');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_storage_assets_day19 (
# MAGIC   asset_name STRING,
# MAGIC   asset_type STRING,
# MAGIC   storage_model STRING,
# MAGIC   external_location_name STRING,
# MAGIC   data_format STRING,
# MAGIC   data_lifecycle_owner STRING,
# MAGIC   unity_catalog_controls_metadata BOOLEAN,
# MAGIC   unity_catalog_controls_file_lifecycle BOOLEAN,
# MAGIC   drop_table_file_behavior STRING,
# MAGIC   recovery_window_days INT,
# MAGIC   direct_external_access_risk BOOLEAN,
# MAGIC   consumer_count INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO uc_storage_assets_day19 VALUES
# MAGIC   ('orders_managed_storage_day19', 'managed_table', 'managed', NULL, 'delta', 'databricks_unity_catalog', true, true, 'metadata_and_files_deleted_after_recovery_window', 7, false, 4),
# MAGIC   ('orders_external_silver_day19', 'external_table', 'external', 'loc_orders_external_silver_day19', 'delta', 'orders-domain@example.com', true, false, 'metadata_deleted_files_remain', NULL, false, 5),
# MAGIC   ('orders_raw_volume_day19', 'external_volume', 'external', 'loc_orders_raw_files_day19', 'files', 'ingestion-owner@example.com', true, false, 'volume_metadata_deleted_files_remain', NULL, false, 2),
# MAGIC   ('orders_legacy_raw_day19', 'external_table', 'external', 'loc_legacy_raw_day19', 'json', 'legacy-platform@example.com', true, false, 'metadata_deleted_files_remain', NULL, true, 1),
# MAGIC   ('orders_workload_support_day19', 'managed_volume', 'managed', NULL, 'files', 'databricks_unity_catalog', true, true, 'volume_metadata_and_files_deleted_after_recovery_window', 7, false, 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_managed_storage_day19 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_managed_storage_day19;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT asset_name, asset_type, storage_model, data_lifecycle_owner, drop_table_file_behavior, direct_external_access_risk
# MAGIC FROM uc_storage_assets_day19
# MAGIC ORDER BY asset_type, asset_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Managed table has 4 rows and Delta metadata.
# MAGIC - Managed assets show Unity Catalog controls metadata and file lifecycle.
# MAGIC - External assets show Unity Catalog controls metadata but not file lifecycle.
# MAGIC - Legacy external table has direct external access risk.
# MAGIC
# MAGIC Operational meaning: managed vs external is not just path choice. It changes who owns deletion, optimization, cleanup, and risk when non-Databricks clients access files directly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Model Dependencies And Lifecycle Requests
# MAGIC
# MAGIC Purpose: make drop and migration decisions depend on consumers, storage ownership, and location risk.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE asset_dependencies_day19 (
# MAGIC   asset_name STRING,
# MAGIC   dependent_name STRING,
# MAGIC   dependent_type STRING,
# MAGIC   criticality STRING,
# MAGIC   owner_principal STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO asset_dependencies_day19 VALUES
# MAGIC   ('orders_managed_storage_day19', 'daily_orders_dashboard', 'dashboard', 'high', 'analytics-owner@example.com'),
# MAGIC   ('orders_managed_storage_day19', 'orders_quality_job', 'job', 'high', 'data-platform@example.com'),
# MAGIC   ('orders_external_silver_day19', 'partner_extract_job', 'job', 'medium', 'partner-platform@example.com'),
# MAGIC   ('orders_external_silver_day19', 'finance_reconciliation_view', 'view', 'high', 'finance-owner@example.com'),
# MAGIC   ('orders_raw_volume_day19', 'bronze_ingestion_job', 'job', 'high', 'ingestion-owner@example.com'),
# MAGIC   ('orders_legacy_raw_day19', 'legacy_audit_notebook', 'notebook', 'low', 'legacy-platform@example.com');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lifecycle_requests_day19 (
# MAGIC   request_id STRING,
# MAGIC   requested_by STRING,
# MAGIC   asset_name STRING,
# MAGIC   requested_action STRING,
# MAGIC   target_storage_model STRING,
# MAGIC   request_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lifecycle_requests_day19 VALUES
# MAGIC   ('sl-001', 'orders-domain@example.com', 'orders_external_silver_day19', 'MIGRATE_TO_MANAGED', 'managed', 'bring silver table under Unity Catalog managed lifecycle'),
# MAGIC   ('sl-002', 'analyst@example.com', 'orders_managed_storage_day19', 'DROP', NULL, 'cleanup table after dashboard migration'),
# MAGIC   ('sl-003', 'legacy-platform@example.com', 'orders_legacy_raw_day19', 'DROP', NULL, 'remove old legacy raw registration'),
# MAGIC   ('sl-004', 'ingestion-owner@example.com', 'orders_raw_volume_day19', 'DROP', NULL, 'cleanup raw landing path'),
# MAGIC   ('sl-005', 'data-platform@example.com', 'orders_workload_support_day19', 'DROP', NULL, 'delete unused workload support files');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM asset_dependencies_day19 ORDER BY asset_name, dependent_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lifecycle_requests_day19 ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Managed table has high-criticality consumers.
# MAGIC - External table and external volume have operational dependencies.
# MAGIC - Requests include managed migration and drop scenarios.
# MAGIC
# MAGIC Operational meaning: storage lifecycle decisions are unsafe without dependency inventory. Dropping metadata can still break consumers; dropping managed assets can also remove files after recovery windows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Evaluate Lifecycle Risk With PySpark
# MAGIC
# MAGIC Purpose: combine asset metadata, external-location metadata, credentials, dependencies, and requests into a decision.

# COMMAND ----------

from pyspark.sql import functions as F

assets_df = spark.table("de_learning.uc_storage_assets_day19")
locations_df = spark.table("de_learning.external_locations_day19")
credentials_df = spark.table("de_learning.storage_credentials_day19")
dependencies_df = spark.table("de_learning.asset_dependencies_day19")
requests_df = spark.table("de_learning.lifecycle_requests_day19")

dependency_summary_df = (
    dependencies_df
    .groupBy("asset_name")
    .agg(
        F.count("*").alias("dependency_count"),
        F.sum(F.when(F.col("criticality") == "high", F.lit(1)).otherwise(F.lit(0))).alias("high_dependency_count"),
        F.array_join(F.collect_set("dependent_name"), ", ").alias("dependency_names")
    )
)

location_context_df = (
    locations_df.alias("loc")
    .join(credentials_df.alias("cred"), on="credential_name", how="left")
    .select(
        "external_location_name",
        "storage_url",
        F.col("loc.read_only").alias("location_read_only"),
        F.col("loc.owner_principal").alias("location_owner"),
        F.col("cred.direct_cloud_access_allowed").alias("credential_direct_cloud_access_allowed"),
        F.col("cred.owner_principal").alias("credential_owner")
    )
)

lifecycle_decision_df = (
    requests_df.alias("req")
    .join(assets_df.alias("asset"), on="asset_name", how="inner")
    .join(dependency_summary_df.alias("dep"), on="asset_name", how="left")
    .join(location_context_df.alias("loc"), on="external_location_name", how="left")
    .na.fill({"dependency_count": 0, "high_dependency_count": 0})
    .withColumn(
        "storage_lifecycle_risk",
        F.when(F.col("storage_model") == "managed", F.lit("UC_MANAGES_FILES_AND_METADATA"))
         .when(F.col("credential_direct_cloud_access_allowed") == F.lit(True), F.lit("EXTERNAL_WITH_DIRECT_CLOUD_ACCESS_RISK"))
         .when(F.col("storage_model") == "external", F.lit("EXTERNAL_FILES_REMAIN_AFTER_METADATA_DROP"))
         .otherwise(F.lit("UNKNOWN_STORAGE_MODEL"))
    )
    .withColumn(
        "decision",
        F.when(
            (F.col("requested_action") == "DROP") & (F.col("high_dependency_count") > 0),
            F.lit("BLOCK_DEPENDENCIES_EXIST")
        )
        .when(
            (F.col("requested_action") == "DROP") & (F.col("storage_model") == "external") & (F.col("credential_direct_cloud_access_allowed") == F.lit(True)),
            F.lit("BLOCK_DIRECT_CLOUD_ACCESS_REVIEW")
        )
        .when(
            (F.col("requested_action") == "DROP") & (F.col("storage_model") == "external"),
            F.lit("NEEDS_STORAGE_OWNER_REVIEW")
        )
        .when(
            (F.col("requested_action") == "DROP") & (F.col("storage_model") == "managed"),
            F.lit("APPROVE_AFTER_RECOVERY_WINDOW_CONFIRMATION")
        )
        .when(
            (F.col("requested_action") == "MIGRATE_TO_MANAGED") & (F.col("storage_model") == "external") & (F.col("data_format") == "delta"),
            F.lit("READY_FOR_MIGRATION_PLAN")
        )
        .otherwise(F.lit("NEEDS_MANUAL_REVIEW"))
    )
    .withColumn(
        "required_action",
        F.when(F.col("decision") == "BLOCK_DEPENDENCIES_EXIST", F.concat(F.lit("Resolve high-criticality dependencies: "), F.col("dependency_names")))
         .when(F.col("decision") == "BLOCK_DIRECT_CLOUD_ACCESS_REVIEW", F.lit("Remove or document direct cloud access before drop"))
         .when(F.col("decision") == "NEEDS_STORAGE_OWNER_REVIEW", F.lit("Confirm cloud files, retention, and external consumers with storage owner"))
         .when(F.col("decision") == "APPROVE_AFTER_RECOVERY_WINDOW_CONFIRMATION", F.lit("Confirm recovery window and downstream replacement before drop"))
         .when(F.col("decision") == "READY_FOR_MIGRATION_PLAN", F.lit("Create managed target, copy data, validate counts/history, swap consumers"))
         .otherwise(F.lit("Manual review"))
    )
    .select(
        "request_id",
        "asset_name",
        "asset_type",
        "storage_model",
        "requested_action",
        "data_format",
        "dependency_count",
        "high_dependency_count",
        "storage_lifecycle_risk",
        "decision",
        "required_action"
    )
)

lifecycle_decision_df.createOrReplaceTempView("storage_lifecycle_decisions_day19")
display(lifecycle_decision_df.orderBy("request_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `assets_df`, `locations_df`, `credentials_df`, `dependencies_df`, and `requests_df` are SQL tables loaded as DataFrames.
# MAGIC - `groupBy(...).agg(...)` creates dependency counts, similar to SQL `GROUP BY asset_name`.
# MAGIC - `join(..., how="left")` keeps lifecycle requests even when optional metadata is missing.
# MAGIC - `.na.fill(...)` replaces null dependency counts with zero after a left join.
# MAGIC - `withColumn(...)` adds risk, decision, and required-action columns.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - `createOrReplaceTempView(...)` makes the PySpark decision output queryable in SQL.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   r.request_id,
# MAGIC   r.asset_name,
# MAGIC   CASE
# MAGIC     WHEN r.requested_action = 'DROP' AND d.high_dependency_count > 0 THEN 'BLOCK_DEPENDENCIES_EXIST'
# MAGIC     WHEN r.requested_action = 'DROP' AND a.storage_model = 'external' THEN 'NEEDS_STORAGE_OWNER_REVIEW'
# MAGIC     ELSE 'NEEDS_MANUAL_REVIEW'
# MAGIC   END AS decision
# MAGIC FROM lifecycle_requests_day19 r
# MAGIC JOIN uc_storage_assets_day19 a USING (asset_name)
# MAGIC LEFT JOIN dependency_summary d USING (asset_name);
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT request_id, asset_name, requested_action, storage_lifecycle_risk, decision, required_action
# MAGIC FROM storage_lifecycle_decisions_day19
# MAGIC ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `sl-001` is ready for a migration plan because the external table is Delta.
# MAGIC - `sl-002` is blocked because the managed table has high-criticality dependencies.
# MAGIC - `sl-003` is blocked for direct cloud access review.
# MAGIC - `sl-004` needs storage-owner review because external volume files remain.
# MAGIC - `sl-005` can be approved after recovery-window confirmation.
# MAGIC
# MAGIC Operational meaning: managed/external lifecycle decisions need different gates. Managed drops are file-destructive after recovery windows; external drops leave files and direct-access risks behind.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Build A Migration Runbook
# MAGIC
# MAGIC Purpose: turn a migration decision into ordered validation steps.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE migration_runbook_day19 (
# MAGIC   migration_id STRING,
# MAGIC   source_asset_name STRING,
# MAGIC   target_asset_name STRING,
# MAGIC   step_number INT,
# MAGIC   step_name STRING,
# MAGIC   validation_query_or_check STRING,
# MAGIC   required_before_cutover BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO migration_runbook_day19 VALUES
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 1, 'Freeze source schema contract', 'DESCRIBE TABLE source and capture schema version', true),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 2, 'Create managed target table', 'CREATE TABLE target USING DELTA AS SELECT ...', true),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 3, 'Validate row counts', 'source_count = target_count', true),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 4, 'Validate business checksums', 'SUM(amount), COUNT(DISTINCT order_id), min/max dates match', true),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 5, 'Replay final delta', 'merge changes since freeze point', true),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 6, 'Swap consumers', 'update views/jobs to managed target', true),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 7, 'Retain external source', 'keep external files for rollback window', false),
# MAGIC   ('mig-001', 'orders_external_silver_day19', 'orders_managed_storage_v2_day19', 8, 'Document cleanup owner', 'assign final external file cleanup to storage owner', false);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM migration_runbook_day19 ORDER BY migration_id, step_number;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   migration_id,
# MAGIC   COUNT(*) AS total_steps,
# MAGIC   SUM(CASE WHEN required_before_cutover THEN 1 ELSE 0 END) AS required_cutover_steps,
# MAGIC   SUM(CASE WHEN step_name LIKE '%Validate%' THEN 1 ELSE 0 END) AS validation_steps
# MAGIC FROM migration_runbook_day19
# MAGIC GROUP BY migration_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 8 migration steps.
# MAGIC - 6 steps required before cutover.
# MAGIC - 2 explicit validation steps.
# MAGIC
# MAGIC Operational meaning: migration is not a copy operation. It is schema freeze, copy, validation, replay, consumer swap, rollback retention, and cleanup ownership.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Final Checks
# MAGIC
# MAGIC Purpose: verify storage lifecycle evidence and table history.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'storage_credential_rows' AS check_name, COUNT(*) AS observed_value FROM storage_credentials_day19
# MAGIC UNION ALL
# MAGIC SELECT 'external_location_rows', COUNT(*) FROM external_locations_day19
# MAGIC UNION ALL
# MAGIC SELECT 'storage_asset_rows', COUNT(*) FROM uc_storage_assets_day19
# MAGIC UNION ALL
# MAGIC SELECT 'dependency_rows', COUNT(*) FROM asset_dependencies_day19
# MAGIC UNION ALL
# MAGIC SELECT 'lifecycle_request_rows', COUNT(*) FROM lifecycle_requests_day19
# MAGIC UNION ALL
# MAGIC SELECT 'lifecycle_decision_rows', COUNT(*) FROM storage_lifecycle_decisions_day19
# MAGIC UNION ALL
# MAGIC SELECT 'blocked_lifecycle_requests', COUNT(*) FROM storage_lifecycle_decisions_day19 WHERE decision LIKE 'BLOCK%'
# MAGIC UNION ALL
# MAGIC SELECT 'migration_runbook_steps', COUNT(*) FROM migration_runbook_day19;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed_storage_day19;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 3 storage credentials.
# MAGIC - 3 external locations.
# MAGIC - 5 storage assets.
# MAGIC - 6 dependency rows.
# MAGIC - 5 lifecycle requests.
# MAGIC - 5 lifecycle decisions.
# MAGIC - 2 blocked lifecycle requests.
# MAGIC - 8 migration runbook steps.
# MAGIC
# MAGIC Operational meaning: storage lifecycle governance needs evidence for credentials, locations, assets, dependencies, requests, decisions, runbooks, and Delta history.
