# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 16 - Unity Catalog Foundations: Object Hierarchy, Managed Tables, External Assets, And Lifecycle
# MAGIC
# MAGIC Goal: practice the Databricks platform and Unity Catalog object model: catalog, schema, table, view, volume, managed vs external storage, ownership, grants, and lifecycle decisions.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Unity Catalog basics, tables/views, governance/security.
# MAGIC - Professional stretch: least privilege, lifecycle ownership, external-location risk, deletion semantics, and auditability.
# MAGIC
# MAGIC Note: the managed table and view are real runnable objects. External locations and volumes often require admin/cloud setup, so this notebook models those with Delta metadata tables that you can run in a personal workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Build The Unity Catalog Object Map
# MAGIC
# MAGIC Purpose: model how objects nest and what each object owns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_object_hierarchy_day16 (
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   parent_object STRING,
# MAGIC   namespace_path STRING,
# MAGIC   storage_kind STRING,
# MAGIC   owner_principal STRING,
# MAGIC   contains_data BOOLEAN,
# MAGIC   consumer_visible BOOLEAN,
# MAGIC   lifecycle_responsibility STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO uc_object_hierarchy_day16 VALUES
# MAGIC   ('metastore', 'metastore', NULL, 'metastore', 'metadata', 'account-admin@example.com', false, false, 'maps workspaces to governed metadata'),
# MAGIC   ('main', 'catalog', 'metastore', 'main', 'metadata', 'platform-admin@example.com', false, false, 'top-level data namespace and grant boundary'),
# MAGIC   ('de_learning', 'schema', 'main', 'main.de_learning', 'metadata', 'data-platform@example.com', false, false, 'groups related tables, views, functions, and volumes'),
# MAGIC   ('orders_managed_day16', 'managed_table', 'de_learning', 'main.de_learning.orders_managed_day16', 'managed', 'orders-domain@example.com', true, false, 'Databricks manages table metadata and data lifecycle'),
# MAGIC   ('orders_external_day16', 'external_table', 'de_learning', 'main.de_learning.orders_external_day16', 'external', 'orders-domain@example.com', true, false, 'Databricks manages metadata; cloud storage owner manages files'),
# MAGIC   ('orders_completed_view_day16', 'view', 'de_learning', 'main.de_learning.orders_completed_view_day16', 'logical', 'analytics-owner@example.com', false, true, 'logical consumer contract over governed base data'),
# MAGIC   ('orders_raw_volume_day16', 'volume', 'de_learning', 'main.de_learning.orders_raw_volume_day16', 'external', 'ingestion-owner@example.com', true, false, 'governed path for files used by ingestion');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   object_type,
# MAGIC   storage_kind,
# MAGIC   COUNT(*) AS object_count,
# MAGIC   SUM(CASE WHEN contains_data THEN 1 ELSE 0 END) AS data_bearing_objects,
# MAGIC   SUM(CASE WHEN consumer_visible THEN 1 ELSE 0 END) AS consumer_visible_objects
# MAGIC FROM uc_object_hierarchy_day16
# MAGIC GROUP BY object_type, storage_kind
# MAGIC ORDER BY object_type, storage_kind;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   object_name,
# MAGIC   object_type,
# MAGIC   parent_object,
# MAGIC   namespace_path,
# MAGIC   lifecycle_responsibility
# MAGIC FROM uc_object_hierarchy_day16
# MAGIC ORDER BY
# MAGIC   CASE object_type
# MAGIC     WHEN 'metastore' THEN 1
# MAGIC     WHEN 'catalog' THEN 2
# MAGIC     WHEN 'schema' THEN 3
# MAGIC     WHEN 'managed_table' THEN 4
# MAGIC     WHEN 'external_table' THEN 5
# MAGIC     WHEN 'view' THEN 6
# MAGIC     WHEN 'volume' THEN 7
# MAGIC     ELSE 99
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Object hierarchy is metastore -> catalog -> schema -> tables/views/volumes.
# MAGIC - Managed table, external table, and volume are data-bearing.
# MAGIC - View is consumer-visible but does not own physical data.
# MAGIC
# MAGIC Operational meaning: Unity Catalog governance starts with object identity and ownership. You cannot reason about grants, deletion, or audit until you know which object owns what.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create A Real Managed Delta Table And View
# MAGIC
# MAGIC Purpose: create runnable objects for managed table and consumer-facing view behavior.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_managed_day16 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   contains_pii BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'storage_class' = 'managed',
# MAGIC   'owner_domain' = 'orders',
# MAGIC   'table_purpose' = 'platform-foundation-lab'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_managed_day16 VALUES
# MAGIC   (1601, 501, DATE'2026-07-13', CAST(250.00 AS DECIMAL(10,2)), 'completed', false),
# MAGIC   (1602, 502, DATE'2026-07-13', CAST(90.00 AS DECIMAL(10,2)), 'pending', false),
# MAGIC   (1603, 503, DATE'2026-07-14', CAST(410.00 AS DECIMAL(10,2)), 'completed', false),
# MAGIC   (1604, 504, DATE'2026-07-14', CAST(75.50 AS DECIMAL(10,2)), 'cancelled', false);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_completed_view_day16 AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status
# MAGIC FROM orders_managed_day16
# MAGIC WHERE status = 'completed';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_managed_day16 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_completed_view_day16 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_managed_day16;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed_day16;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Managed table has 4 rows.
# MAGIC - View exposes only the 2 completed orders.
# MAGIC - `DESCRIBE DETAIL` shows Delta table metadata.
# MAGIC - `DESCRIBE HISTORY` shows create/replace and write operations.
# MAGIC
# MAGIC Operational meaning: managed tables are physical governed data objects; views are logical access contracts over base data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Model External Tables And Volumes
# MAGIC
# MAGIC Purpose: practice external asset reasoning without requiring cloud-admin setup.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE external_assets_day16 (
# MAGIC   asset_name STRING,
# MAGIC   asset_type STRING,
# MAGIC   storage_location STRING,
# MAGIC   backing_credential STRING,
# MAGIC   owner_principal STRING,
# MAGIC   data_deleted_when_object_dropped BOOLEAN,
# MAGIC   write_allowed BOOLEAN,
# MAGIC   recommended_use STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO external_assets_day16 VALUES
# MAGIC   ('orders_external_day16', 'external_table', 's3://company-data-lake/orders/silver/', 'storage-credential-orders', 'orders-domain@example.com', false, true, 'query existing Delta data while keeping files outside managed storage'),
# MAGIC   ('orders_raw_volume_day16', 'external_volume', 's3://company-landing-zone/orders/raw/', 'storage-credential-ingestion', 'ingestion-owner@example.com', false, true, 'govern file ingestion paths for raw files'),
# MAGIC   ('reference_managed_day16', 'managed_table', 'managed by Databricks', 'n/a', 'reference-data@example.com', true, true, 'small trusted reference tables fully controlled by Databricks');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   asset_name,
# MAGIC   asset_type,
# MAGIC   storage_location,
# MAGIC   data_deleted_when_object_dropped,
# MAGIC   recommended_use
# MAGIC FROM external_assets_day16
# MAGIC ORDER BY asset_type, asset_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - External table and external volume do not delete cloud files when the catalog object is dropped.
# MAGIC - Managed table lifecycle is controlled by Databricks.
# MAGIC
# MAGIC Operational meaning: external assets separate metadata lifecycle from file lifecycle. This is powerful, but it requires clearer ownership of cloud storage.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Model Grants And Least Privilege
# MAGIC
# MAGIC Purpose: understand which privileges belong at catalog, schema, table, view, and volume levels.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE privilege_model_day16 (
# MAGIC   principal STRING,
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   privilege STRING,
# MAGIC   grant_scope STRING,
# MAGIC   reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO privilege_model_day16 VALUES
# MAGIC   ('orders-domain@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', 'domain owner can create and manage orders objects'),
# MAGIC   ('orders-domain@example.com', 'orders_managed_day16', 'managed_table', 'MODIFY', 'table', 'domain owner can write managed table'),
# MAGIC   ('analytics-readers@example.com', 'orders_completed_view_day16', 'view', 'SELECT', 'view', 'consumers should read curated completed-order view'),
# MAGIC   ('analytics-readers@example.com', 'orders_managed_day16', 'managed_table', 'SELECT', 'table', 'intentionally too broad for this lab'),
# MAGIC   ('ingestion-job-sp@example.com', 'orders_raw_volume_day16', 'volume', 'READ FILES', 'volume', 'job reads raw landing files'),
# MAGIC   ('ingestion-job-sp@example.com', 'orders_raw_volume_day16', 'volume', 'WRITE FILES', 'volume', 'job writes checkpoint and rescue files');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM privilege_model_day16 ORDER BY principal, object_type, object_name, privilege;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   principal,
# MAGIC   SUM(CASE WHEN object_type = 'managed_table' AND privilege = 'SELECT' AND principal LIKE '%readers%' THEN 1 ELSE 0 END) AS broad_base_table_selects,
# MAGIC   SUM(CASE WHEN object_type = 'view' AND privilege = 'SELECT' THEN 1 ELSE 0 END) AS governed_view_selects,
# MAGIC   SUM(CASE WHEN object_type = 'volume' AND privilege LIKE '%FILES' THEN 1 ELSE 0 END) AS file_privileges
# MAGIC FROM privilege_model_day16
# MAGIC GROUP BY principal
# MAGIC ORDER BY principal;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Analytics readers have one good view-level grant and one intentionally broad base-table grant.
# MAGIC - Ingestion service principal has file privileges on a volume.
# MAGIC
# MAGIC Operational meaning: least privilege usually means consumers get `SELECT` on views, jobs get only needed file/table permissions, and base tables stay restricted.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Evaluate Lifecycle Decisions With PySpark
# MAGIC
# MAGIC Purpose: classify object lifecycle risk and recommend action.

# COMMAND ----------

from pyspark.sql import functions as F

objects_df = spark.table("de_learning.uc_object_hierarchy_day16")
assets_df = spark.table("de_learning.external_assets_day16")
grants_df = spark.table("de_learning.privilege_model_day16")

broad_reader_grants_df = (
    grants_df
    .where(
        (F.col("principal").like("%readers%"))
        & (F.col("object_type") == F.lit("managed_table"))
        & (F.col("privilege") == F.lit("SELECT"))
    )
    .groupBy("object_name")
    .agg(F.count("*").alias("broad_reader_grant_count"))
)

lifecycle_df = (
    objects_df
    .join(
        assets_df.select(
            F.col("asset_name").alias("object_name"),
            "data_deleted_when_object_dropped",
            "write_allowed"
        ),
        on="object_name",
        how="left"
    )
    .join(broad_reader_grants_df, on="object_name", how="left")
    .na.fill({"broad_reader_grant_count": 0})
    .withColumn(
        "drop_risk",
        F.when(F.col("storage_kind") == "managed", F.lit("DROPS_METADATA_AND_DATA"))
         .when(F.col("storage_kind") == "external", F.lit("DROPS_METADATA_ONLY"))
         .otherwise(F.lit("NO_PHYSICAL_DATA"))
    )
    .withColumn(
        "recommended_action",
        F.when(F.col("broad_reader_grant_count") > 0, F.lit("RESTRICT_BASE_TABLE_GRANT"))
         .when(F.col("object_type") == "external_table", F.lit("VERIFY_CLOUD_STORAGE_OWNER_BEFORE_DROP"))
         .when(F.col("object_type") == "volume", F.lit("VERIFY_FILE_PRIVILEGES_AND_RETENTION"))
         .when(F.col("object_type") == "view", F.lit("KEEP_AS_CONSUMER_CONTRACT"))
         .otherwise(F.lit("STANDARD_LIFECYCLE_REVIEW"))
    )
    .select(
        "object_name",
        "object_type",
        "storage_kind",
        "owner_principal",
        "broad_reader_grant_count",
        "drop_risk",
        "recommended_action"
    )
)

lifecycle_df.createOrReplaceTempView("object_lifecycle_review_day16")
display(lifecycle_df.orderBy("object_type", "object_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `objects_df` is the object map; `assets_df` adds external/managed file lifecycle; `grants_df` adds privileges.
# MAGIC - `where(...)` filters rows. SQL equivalent: `WHERE principal LIKE '%readers%'`.
# MAGIC - `groupBy(...).agg(F.count("*"))` is SQL `GROUP BY object_name, COUNT(*)`.
# MAGIC - `join(..., how="left")` keeps every object even if it has no external asset metadata or broad grant.
# MAGIC - `.na.fill(...)` turns missing grant counts into zero.
# MAGIC - `withColumn(...)` adds `drop_risk` and `recommended_action`.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   recommended_action,
# MAGIC   COUNT(*) AS object_count
# MAGIC FROM object_lifecycle_review_day16
# MAGIC GROUP BY recommended_action
# MAGIC ORDER BY recommended_action;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Managed table with broad reader grant gets `RESTRICT_BASE_TABLE_GRANT`.
# MAGIC - External table gets `VERIFY_CLOUD_STORAGE_OWNER_BEFORE_DROP`.
# MAGIC - Volume gets `VERIFY_FILE_PRIVILEGES_AND_RETENTION`.
# MAGIC - View gets `KEEP_AS_CONSUMER_CONTRACT`.
# MAGIC
# MAGIC Operational meaning: lifecycle review combines object type, storage type, owner, grants, and deletion semantics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Practice Drop And Ownership Decisions
# MAGIC
# MAGIC Purpose: create safe decision records before changing object lifecycle.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lifecycle_change_requests_day16 (
# MAGIC   request_id STRING,
# MAGIC   requested_by STRING,
# MAGIC   object_name STRING,
# MAGIC   requested_action STRING,
# MAGIC   business_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lifecycle_change_requests_day16 VALUES
# MAGIC   ('lc-001', 'analytics-owner@example.com', 'orders_completed_view_day16', 'DROP', 'replace old completed-order view with v2'),
# MAGIC   ('lc-002', 'analyst@example.com', 'orders_managed_day16', 'GRANT_SELECT', 'ad hoc analysis'),
# MAGIC   ('lc-003', 'orders-domain@example.com', 'orders_external_day16', 'DROP', 'decommission old external table pointer'),
# MAGIC   ('lc-004', 'ingestion-owner@example.com', 'orders_raw_volume_day16', 'DROP', 'cleanup old raw file path');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW lifecycle_decisions_day16 AS
# MAGIC SELECT
# MAGIC   r.request_id,
# MAGIC   r.object_name,
# MAGIC   r.requested_action,
# MAGIC   o.object_type,
# MAGIC   o.storage_kind,
# MAGIC   o.owner_principal,
# MAGIC   r.requested_by,
# MAGIC   CASE
# MAGIC     WHEN r.requested_action = 'GRANT_SELECT' AND o.object_type = 'managed_table' THEN 'DENY_USE_VIEW'
# MAGIC     WHEN r.requested_action = 'DROP' AND o.storage_kind = 'external' THEN 'NEEDS_STORAGE_OWNER_REVIEW'
# MAGIC     WHEN r.requested_action = 'DROP' AND o.object_type = 'view' AND r.requested_by = o.owner_principal THEN 'APPROVE'
# MAGIC     WHEN r.requested_by <> o.owner_principal THEN 'DENY_NOT_OWNER'
# MAGIC     ELSE 'NEEDS_REVIEW'
# MAGIC   END AS decision,
# MAGIC   CASE
# MAGIC     WHEN r.requested_action = 'GRANT_SELECT' AND o.object_type = 'managed_table' THEN 'Consumers should use governed views instead of broad base-table access.'
# MAGIC     WHEN r.requested_action = 'DROP' AND o.storage_kind = 'external' THEN 'External object drop may leave files behind; confirm cloud storage ownership and consumers.'
# MAGIC     WHEN r.requested_action = 'DROP' AND o.object_type = 'view' AND r.requested_by = o.owner_principal THEN 'View owner can replace logical consumer contract after dependency check.'
# MAGIC     WHEN r.requested_by <> o.owner_principal THEN 'Requester is not the object owner.'
# MAGIC     ELSE 'Manual review required.'
# MAGIC   END AS reason
# MAGIC FROM lifecycle_change_requests_day16 r
# MAGIC JOIN uc_object_hierarchy_day16 o
# MAGIC   ON r.object_name = o.object_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lifecycle_decisions_day16 ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - View drop by owner is approved.
# MAGIC - Direct select on managed base table is denied; use view.
# MAGIC - External table and volume drops require storage owner review.
# MAGIC
# MAGIC Operational meaning: object lifecycle changes should be decisions with reasons, not one-off commands.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Platform Checks
# MAGIC
# MAGIC Purpose: verify object coverage and lifecycle risks.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'object_hierarchy_rows' AS check_name, COUNT(*) AS observed_value FROM uc_object_hierarchy_day16
# MAGIC UNION ALL
# MAGIC SELECT 'managed_order_rows', COUNT(*) FROM orders_managed_day16
# MAGIC UNION ALL
# MAGIC SELECT 'completed_view_rows', COUNT(*) FROM orders_completed_view_day16
# MAGIC UNION ALL
# MAGIC SELECT 'external_asset_rows', COUNT(*) FROM external_assets_day16
# MAGIC UNION ALL
# MAGIC SELECT 'privilege_rows', COUNT(*) FROM privilege_model_day16
# MAGIC UNION ALL
# MAGIC SELECT 'lifecycle_decision_rows', COUNT(*) FROM lifecycle_decisions_day16
# MAGIC UNION ALL
# MAGIC SELECT 'objects_needing_restriction', COUNT(*) FROM object_lifecycle_review_day16 WHERE recommended_action = 'RESTRICT_BASE_TABLE_GRANT';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_managed_day16;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 7 object hierarchy rows.
# MAGIC - 4 managed order rows.
# MAGIC - 2 completed view rows.
# MAGIC - 3 external asset rows.
# MAGIC - 6 privilege rows.
# MAGIC - 4 lifecycle decision rows.
# MAGIC - 1 object needing grant restriction.
# MAGIC
# MAGIC Operational meaning: platform governance starts with inventory, ownership, privilege review, and lifecycle decision records.
