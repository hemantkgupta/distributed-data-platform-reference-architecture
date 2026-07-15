# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 17 - Unity Catalog Privilege Inheritance And Effective Access
# MAGIC
# MAGIC Goal: practice Unity Catalog privilege prerequisites, inherited grants, object-level access, volume access, and effective access review.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Unity Catalog privileges, governance and security.
# MAGIC - Professional stretch: least privilege, inherited grant risk, access review evidence, and production remediation decisions.
# MAGIC
# MAGIC This notebook simulates grants as Delta tables so it can run in a personal workspace without admin privileges. The access-control reasoning mirrors how you should think about real Unity Catalog grants.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Governed Objects
# MAGIC
# MAGIC Purpose: create a base table, a consumer view, a masking helper function, and an object inventory.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_secure_base_day17 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   customer_email STRING,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   region STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contains_pii' = 'true',
# MAGIC   'governance_surface' = 'base_table',
# MAGIC   'owner_domain' = 'orders'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_secure_base_day17 VALUES
# MAGIC   (1701, 701, 'aarya@example.com', DATE'2026-07-13', CAST(220.00 AS DECIMAL(10,2)), 'completed', 'US'),
# MAGIC   (1702, 702, 'ben@example.com', DATE'2026-07-13', CAST(85.50 AS DECIMAL(10,2)), 'pending', 'US'),
# MAGIC   (1703, 703, 'chen@example.com', DATE'2026-07-14', CAST(410.00 AS DECIMAL(10,2)), 'completed', 'EU'),
# MAGIC   (1704, 704, 'diya@example.com', DATE'2026-07-14', CAST(64.75 AS DECIMAL(10,2)), 'cancelled', 'APAC'),
# MAGIC   (1705, 705, 'elena@example.com', DATE'2026-07-15', CAST(510.20 AS DECIMAL(10,2)), 'completed', 'EU');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_customer_email_day17(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN email IS NULL THEN NULL
# MAGIC   ELSE '***MASKED***'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_masked_view_day17 AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   mask_customer_email_day17(customer_email) AS customer_email,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status,
# MAGIC   region
# MAGIC FROM orders_secure_base_day17
# MAGIC WHERE status = 'completed';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_securable_objects_day17 (
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   catalog_name STRING,
# MAGIC   schema_name STRING,
# MAGIC   owner_principal STRING,
# MAGIC   sensitivity_class STRING,
# MAGIC   preferred_consumer_surface STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO uc_securable_objects_day17 VALUES
# MAGIC   ('main', 'catalog', 'main', NULL, 'platform-admin@example.com', 'metadata', 'n/a'),
# MAGIC   ('de_learning', 'schema', 'main', 'de_learning', 'data-platform@example.com', 'metadata', 'n/a'),
# MAGIC   ('orders_secure_base_day17', 'managed_table', 'main', 'de_learning', 'orders-domain@example.com', 'restricted_pii', 'orders_masked_view_day17'),
# MAGIC   ('orders_masked_view_day17', 'view', 'main', 'de_learning', 'analytics-owner@example.com', 'masked_pii', 'orders_masked_view_day17'),
# MAGIC   ('orders_raw_volume_day17', 'volume', 'main', 'de_learning', 'ingestion-owner@example.com', 'raw_files', 'job-only'),
# MAGIC   ('mask_customer_email_day17', 'function', 'main', 'de_learning', 'data-platform@example.com', 'pii-helper', 'function-execute-only');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_secure_base_day17 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_masked_view_day17 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT object_name, object_type, sensitivity_class, preferred_consumer_surface
# MAGIC FROM uc_securable_objects_day17
# MAGIC ORDER BY
# MAGIC   CASE object_type
# MAGIC     WHEN 'catalog' THEN 1
# MAGIC     WHEN 'schema' THEN 2
# MAGIC     WHEN 'managed_table' THEN 3
# MAGIC     WHEN 'view' THEN 4
# MAGIC     WHEN 'volume' THEN 5
# MAGIC     WHEN 'function' THEN 6
# MAGIC     ELSE 99
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Base table has 5 rows and includes `customer_email`.
# MAGIC - View has 3 completed rows and masks `customer_email`.
# MAGIC - Object inventory has 6 securable objects.
# MAGIC
# MAGIC Operational meaning: consumers should usually see a governed view, while the base table remains a restricted producer-domain object.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Model Grants And Inheritance
# MAGIC
# MAGIC Purpose: model prerequisite privileges, direct object grants, and schema-level inherited grants.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE privilege_grants_day17 (
# MAGIC   principal STRING,
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   privilege STRING,
# MAGIC   grant_scope STRING,
# MAGIC   applies_to_descendants BOOLEAN,
# MAGIC   grant_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO privilege_grants_day17 VALUES
# MAGIC   ('analytics-readers@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'required to see objects inside the catalog'),
# MAGIC   ('analytics-readers@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'required to see objects inside the schema'),
# MAGIC   ('analytics-readers@example.com', 'orders_masked_view_day17', 'view', 'SELECT', 'view', false, 'consumer reads masked view'),
# MAGIC   ('analytics-readers@example.com', 'mask_customer_email_day17', 'function', 'EXECUTE', 'function', false, 'consumer can execute helper if needed'),
# MAGIC
# MAGIC   ('orders-writers@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'writer can access catalog'),
# MAGIC   ('orders-writers@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'writer can access schema'),
# MAGIC   ('orders-writers@example.com', 'orders_secure_base_day17', 'managed_table', 'SELECT', 'table', false, 'writer can inspect base table'),
# MAGIC   ('orders-writers@example.com', 'orders_secure_base_day17', 'managed_table', 'MODIFY', 'table', false, 'writer can write base table'),
# MAGIC
# MAGIC   ('finance-readers@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'finance can access catalog'),
# MAGIC   ('finance-readers@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'finance can access schema'),
# MAGIC   ('finance-readers@example.com', 'orders_secure_base_day17', 'managed_table', 'SELECT', 'table', false, 'approved sensitive base-table access'),
# MAGIC
# MAGIC   ('ingestion-job-sp@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'job can access catalog'),
# MAGIC   ('ingestion-job-sp@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'job can access schema'),
# MAGIC   ('ingestion-job-sp@example.com', 'orders_raw_volume_day17', 'volume', 'READ VOLUME', 'volume', false, 'job reads landing files'),
# MAGIC   ('ingestion-job-sp@example.com', 'orders_raw_volume_day17', 'volume', 'WRITE VOLUME', 'volume', false, 'job writes checkpoint and rescue files'),
# MAGIC
# MAGIC   ('contractor-readers@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'catalog access exists'),
# MAGIC   ('contractor-readers@example.com', 'orders_masked_view_day17', 'view', 'SELECT', 'view', false, 'missing USE SCHEMA on purpose'),
# MAGIC
# MAGIC   ('data-platform-admins@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'platform admin catalog access'),
# MAGIC   ('data-platform-admins@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'platform admin schema access'),
# MAGIC   ('data-platform-admins@example.com', 'orders_secure_base_day17', 'managed_table', 'MANAGE', 'table', false, 'table administration'),
# MAGIC
# MAGIC   ('schema-analysts@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'schema analysts catalog access'),
# MAGIC   ('schema-analysts@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'schema analysts schema access'),
# MAGIC   ('schema-analysts@example.com', 'de_learning', 'schema', 'SELECT', 'schema', true, 'intentionally broad inherited select across schema'),
# MAGIC
# MAGIC   ('analyst-broad@example.com', 'main', 'catalog', 'USE CATALOG', 'catalog', false, 'analyst catalog access'),
# MAGIC   ('analyst-broad@example.com', 'de_learning', 'schema', 'USE SCHEMA', 'schema', false, 'analyst schema access'),
# MAGIC   ('analyst-broad@example.com', 'orders_secure_base_day17', 'managed_table', 'SELECT', 'table', false, 'intentionally broad direct base-table select');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT principal, object_name, object_type, privilege, applies_to_descendants, grant_reason
# MAGIC FROM privilege_grants_day17
# MAGIC ORDER BY principal, object_type, object_name, privilege;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   principal,
# MAGIC   SUM(CASE WHEN privilege = 'USE CATALOG' THEN 1 ELSE 0 END) AS use_catalog_grants,
# MAGIC   SUM(CASE WHEN privilege = 'USE SCHEMA' THEN 1 ELSE 0 END) AS use_schema_grants,
# MAGIC   SUM(CASE WHEN privilege = 'SELECT' AND object_type = 'managed_table' THEN 1 ELSE 0 END) AS direct_base_table_selects,
# MAGIC   SUM(CASE WHEN privilege = 'SELECT' AND object_type = 'schema' AND applies_to_descendants THEN 1 ELSE 0 END) AS inherited_schema_selects,
# MAGIC   SUM(CASE WHEN privilege LIKE '%VOLUME' THEN 1 ELSE 0 END) AS volume_grants
# MAGIC FROM privilege_grants_day17
# MAGIC GROUP BY principal
# MAGIC ORDER BY principal;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `analytics-readers@example.com` has catalog/schema usage plus view `SELECT`.
# MAGIC - `contractor-readers@example.com` has view `SELECT` but is missing `USE SCHEMA`.
# MAGIC - `schema-analysts@example.com` has inherited schema-level `SELECT`.
# MAGIC - `analyst-broad@example.com` has direct base-table `SELECT`.
# MAGIC
# MAGIC Operational meaning: object-level `SELECT` is not enough. `USE CATALOG` and `USE SCHEMA` are prerequisites, and inherited grants can be broader than intended.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Define Access Requests And Required Privileges
# MAGIC
# MAGIC Purpose: make access evaluation explicit instead of relying on memory.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE access_requests_day17 (
# MAGIC   request_id STRING,
# MAGIC   principal STRING,
# MAGIC   action_code STRING,
# MAGIC   object_name STRING,
# MAGIC   request_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO access_requests_day17 VALUES
# MAGIC   ('ar-001', 'analytics-readers@example.com', 'QUERY_VIEW', 'orders_masked_view_day17', 'dashboard reads completed orders'),
# MAGIC   ('ar-002', 'analytics-readers@example.com', 'QUERY_TABLE', 'orders_secure_base_day17', 'analyst tries direct base table read'),
# MAGIC   ('ar-003', 'orders-writers@example.com', 'WRITE_TABLE', 'orders_secure_base_day17', 'orders pipeline writes curated table'),
# MAGIC   ('ar-004', 'contractor-readers@example.com', 'QUERY_VIEW', 'orders_masked_view_day17', 'contractor tries view read without schema use'),
# MAGIC   ('ar-005', 'ingestion-job-sp@example.com', 'WRITE_VOLUME', 'orders_raw_volume_day17', 'landing job writes files'),
# MAGIC   ('ar-006', 'ingestion-job-sp@example.com', 'QUERY_TABLE', 'orders_secure_base_day17', 'job tries table read without select'),
# MAGIC   ('ar-007', 'data-platform-admins@example.com', 'MANAGE_TABLE', 'orders_secure_base_day17', 'admin changes table ownership or grants'),
# MAGIC   ('ar-008', 'schema-analysts@example.com', 'QUERY_TABLE', 'orders_secure_base_day17', 'schema-level inherited select reaches base table'),
# MAGIC   ('ar-009', 'analyst-broad@example.com', 'QUERY_TABLE', 'orders_secure_base_day17', 'direct base-table select should be reviewed'),
# MAGIC   ('ar-010', 'analytics-readers@example.com', 'EXECUTE_FUNCTION', 'mask_customer_email_day17', 'consumer tests masking helper');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE access_requirements_day17 (
# MAGIC   action_code STRING,
# MAGIC   required_privilege STRING,
# MAGIC   required_scope STRING,
# MAGIC   requirement_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO access_requirements_day17 VALUES
# MAGIC   ('QUERY_TABLE', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('QUERY_TABLE', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('QUERY_TABLE', 'SELECT', 'object', 'table read requires select'),
# MAGIC
# MAGIC   ('QUERY_VIEW', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('QUERY_VIEW', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('QUERY_VIEW', 'SELECT', 'object', 'view read requires select'),
# MAGIC
# MAGIC   ('WRITE_TABLE', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('WRITE_TABLE', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('WRITE_TABLE', 'MODIFY', 'object', 'table write requires modify'),
# MAGIC
# MAGIC   ('READ_VOLUME', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('READ_VOLUME', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('READ_VOLUME', 'READ VOLUME', 'object', 'volume read requires read volume'),
# MAGIC
# MAGIC   ('WRITE_VOLUME', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('WRITE_VOLUME', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('WRITE_VOLUME', 'WRITE VOLUME', 'object', 'volume write requires write volume'),
# MAGIC
# MAGIC   ('CREATE_TABLE', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('CREATE_TABLE', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('CREATE_TABLE', 'CREATE TABLE', 'schema', 'table creation requires create table on schema'),
# MAGIC
# MAGIC   ('MANAGE_TABLE', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('MANAGE_TABLE', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('MANAGE_TABLE', 'MANAGE', 'object', 'grant or ownership changes require manage'),
# MAGIC
# MAGIC   ('EXECUTE_FUNCTION', 'USE CATALOG', 'catalog', 'catalog usage is prerequisite'),
# MAGIC   ('EXECUTE_FUNCTION', 'USE SCHEMA', 'schema', 'schema usage is prerequisite'),
# MAGIC   ('EXECUTE_FUNCTION', 'EXECUTE', 'object', 'function execution requires execute');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM access_requests_day17 ORDER BY request_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM access_requirements_day17 ORDER BY action_code, required_scope, required_privilege;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 10 access requests.
# MAGIC - Each action has catalog, schema, and action-specific requirements.
# MAGIC
# MAGIC Operational meaning: production access reviews should be evaluated from explicit requirements, not a vague question like "does this group have access?"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Evaluate Effective Access With PySpark
# MAGIC
# MAGIC Purpose: compute whether each request is allowed, denied, or risky because of inherited/broad access.

# COMMAND ----------

from pyspark.sql import functions as F

objects_df = spark.table("de_learning.uc_securable_objects_day17")
grants_df = spark.table("de_learning.privilege_grants_day17")
requests_df = spark.table("de_learning.access_requests_day17")
requirements_df = spark.table("de_learning.access_requirements_day17")

exact_grants_df = grants_df.select(
    "principal",
    "privilege",
    F.col("object_name").alias("effective_object_name"),
    F.col("object_type").alias("grant_object_type"),
    F.lit("DIRECT").alias("grant_match_type")
)

schema_inherited_grants_df = (
    grants_df.alias("g")
    .where(
        (F.col("g.applies_to_descendants") == F.lit(True))
        & (F.col("g.object_type") == F.lit("schema"))
    )
    .join(
        objects_df.alias("o"),
        F.col("g.object_name") == F.col("o.schema_name"),
        "inner"
    )
    .where(~F.col("o.object_type").isin("catalog", "schema"))
    .select(
        F.col("g.principal"),
        F.col("g.privilege"),
        F.col("o.object_name").alias("effective_object_name"),
        F.col("g.object_type").alias("grant_object_type"),
        F.lit("INHERITED_FROM_SCHEMA").alias("grant_match_type")
    )
)

catalog_inherited_grants_df = (
    grants_df.alias("g")
    .where(
        (F.col("g.applies_to_descendants") == F.lit(True))
        & (F.col("g.object_type") == F.lit("catalog"))
    )
    .join(
        objects_df.alias("o"),
        F.col("g.object_name") == F.col("o.catalog_name"),
        "inner"
    )
    .where(~F.col("o.object_type").isin("catalog"))
    .select(
        F.col("g.principal"),
        F.col("g.privilege"),
        F.col("o.object_name").alias("effective_object_name"),
        F.col("g.object_type").alias("grant_object_type"),
        F.lit("INHERITED_FROM_CATALOG").alias("grant_match_type")
    )
)

effective_grants_df = (
    exact_grants_df
    .unionByName(schema_inherited_grants_df)
    .unionByName(catalog_inherited_grants_df)
    .dropDuplicates(["principal", "privilege", "effective_object_name"])
)

request_context_df = (
    requests_df.alias("r")
    .join(objects_df.alias("o"), on="object_name", how="inner")
    .select(
        "request_id",
        "principal",
        "action_code",
        "object_name",
        "request_reason",
        "object_type",
        "catalog_name",
        "schema_name",
        "sensitivity_class",
        "preferred_consumer_surface"
    )
)

expanded_requirements_df = (
    request_context_df
    .join(requirements_df, on="action_code", how="inner")
    .withColumn(
        "required_object_name",
        F.when(F.col("required_scope") == "catalog", F.col("catalog_name"))
         .when(F.col("required_scope") == "schema", F.col("schema_name"))
         .otherwise(F.col("object_name"))
    )
    .withColumn(
        "requirement_label",
        F.concat_ws(" ", F.col("required_privilege"), F.lit("on"), F.col("required_object_name"))
    )
)

checked_requirements_df = (
    expanded_requirements_df.alias("req")
    .join(
        effective_grants_df.alias("grant"),
        (F.col("req.principal") == F.col("grant.principal"))
        & (F.col("req.required_privilege") == F.col("grant.privilege"))
        & (F.col("req.required_object_name") == F.col("grant.effective_object_name")),
        "left"
    )
    .select(
        F.col("req.request_id"),
        F.col("req.principal"),
        F.col("req.action_code"),
        F.col("req.object_name"),
        F.col("req.request_reason"),
        F.col("req.object_type"),
        F.col("req.catalog_name"),
        F.col("req.schema_name"),
        F.col("req.sensitivity_class"),
        F.col("req.preferred_consumer_surface"),
        F.col("req.required_privilege"),
        F.col("req.required_scope"),
        F.col("req.required_object_name"),
        F.col("req.requirement_label"),
        F.col("grant.principal").alias("matched_principal"),
        F.col("grant.grant_match_type")
    )
    .withColumn(
        "requirement_status",
        F.when(F.col("matched_principal").isNotNull(), F.lit("GRANTED")).otherwise(F.lit("MISSING"))
    )
    .withColumn(
        "missing_requirement",
        F.when(F.col("requirement_status") == "MISSING", F.col("requirement_label"))
    )
    .withColumn(
        "grant_evidence",
        F.when(
            F.col("requirement_status") == "GRANTED",
            F.concat_ws(" via ", F.col("requirement_label"), F.col("grant.grant_match_type"))
        )
    )
)

decision_df = (
    checked_requirements_df
    .groupBy(
        "request_id",
        "principal",
        "action_code",
        "object_name",
        "object_type",
        "sensitivity_class",
        "preferred_consumer_surface",
        "request_reason"
    )
    .agg(
        F.count("*").alias("required_privilege_count"),
        F.sum(F.when(F.col("requirement_status") == "MISSING", F.lit(1)).otherwise(F.lit(0))).alias("missing_privilege_count"),
        F.array_join(F.collect_set("missing_requirement"), "; ").alias("missing_requirements"),
        F.array_join(F.collect_set("grant_evidence"), "; ").alias("grant_evidence")
    )
    .withColumn(
        "access_decision",
        F.when(F.col("missing_privilege_count") == 0, F.lit("ALLOW")).otherwise(F.lit("DENY"))
    )
    .withColumn(
        "risk_note",
        F.when(
            (F.col("access_decision") == "ALLOW")
            & (F.col("action_code") == "QUERY_TABLE")
            & (F.col("sensitivity_class") == "restricted_pii"),
            F.concat(F.lit("ALLOW_BUT_REVIEW_BASE_TABLE_ACCESS; prefer "), F.col("preferred_consumer_surface"))
        )
        .when(F.col("access_decision") == "DENY", F.lit("FIX_MISSING_PREREQUISITES_OR_ROUTE_TO_VIEW"))
        .otherwise(F.lit("OK"))
    )
)

decision_df.createOrReplaceTempView("effective_access_decisions_day17")
display(decision_df.orderBy("request_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `objects_df`, `grants_df`, `requests_df`, and `requirements_df` are SQL tables loaded as DataFrames.
# MAGIC - `unionByName(...)` stacks direct, schema-inherited, and catalog-inherited grants like SQL `UNION ALL`.
# MAGIC - `join(..., how="left")` checks whether each required privilege has matching grant evidence.
# MAGIC - `withColumn(...)` derives required object names, missing requirements, decisions, and risk notes.
# MAGIC - `groupBy(...).agg(...)` collapses many requirement rows back into one decision per access request.
# MAGIC - The whole transformation is lazy until `display(...)` runs.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT request_id,
# MAGIC        CASE WHEN SUM(missing_flag) = 0 THEN 'ALLOW' ELSE 'DENY' END AS access_decision
# MAGIC FROM expanded_requirements
# MAGIC LEFT JOIN effective_grants
# MAGIC   ON principal = principal
# MAGIC  AND required_privilege = privilege
# MAGIC  AND required_object_name = effective_object_name
# MAGIC GROUP BY request_id;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   request_id,
# MAGIC   principal,
# MAGIC   action_code,
# MAGIC   object_name,
# MAGIC   access_decision,
# MAGIC   missing_privilege_count,
# MAGIC   missing_requirements,
# MAGIC   risk_note
# MAGIC FROM effective_access_decisions_day17
# MAGIC ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `ar-001` is allowed: analytics readers query the masked view.
# MAGIC - `ar-002` is denied: analytics readers lack direct base-table `SELECT`.
# MAGIC - `ar-004` is denied even with view `SELECT`, because `USE SCHEMA` is missing.
# MAGIC - `ar-008` is allowed through inherited schema `SELECT`, but flagged for review.
# MAGIC - `ar-009` is allowed through direct base-table `SELECT`, but flagged for review.
# MAGIC
# MAGIC Operational meaning: effective access is the combination of prerequisites, object grants, inherited grants, and sensitivity of the target object.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Turn Decisions Into Remediation
# MAGIC
# MAGIC Purpose: convert access decisions into concrete grant/revoke/reroute actions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE access_remediation_plan_day17
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   request_id,
# MAGIC   principal,
# MAGIC   action_code,
# MAGIC   object_name,
# MAGIC   access_decision,
# MAGIC   missing_requirements,
# MAGIC   risk_note,
# MAGIC   CASE
# MAGIC     WHEN access_decision = 'DENY' AND missing_requirements LIKE '%USE SCHEMA%' THEN 'Review identity first; if approved, grant USE SCHEMA on de_learning.'
# MAGIC     WHEN access_decision = 'DENY' AND action_code = 'QUERY_TABLE' THEN 'Do not grant base-table SELECT by default; route to orders_masked_view_day17.'
# MAGIC     WHEN risk_note LIKE 'ALLOW_BUT_REVIEW_BASE_TABLE_ACCESS%' THEN 'Replace broad base-table SELECT with SELECT on orders_masked_view_day17 unless a sensitive-data exception is approved.'
# MAGIC     WHEN access_decision = 'ALLOW' THEN 'No immediate change; keep evidence for audit.'
# MAGIC     ELSE 'Manual access review required.'
# MAGIC   END AS recommended_remediation
# MAGIC FROM effective_access_decisions_day17;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM access_remediation_plan_day17 ORDER BY request_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   access_decision,
# MAGIC   risk_note,
# MAGIC   COUNT(*) AS request_count
# MAGIC FROM access_remediation_plan_day17
# MAGIC GROUP BY access_decision, risk_note
# MAGIC ORDER BY access_decision, risk_note;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Denied requests get concrete missing privilege or reroute guidance.
# MAGIC - Allowed-but-risky base-table access gets a remediation toward the governed view.
# MAGIC - Clean allowed requests are retained as audit evidence.
# MAGIC
# MAGIC Operational meaning: an access review is incomplete unless it creates a next action: grant, revoke, reroute, approve exception, or keep evidence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Final Checks
# MAGIC
# MAGIC Purpose: validate the lab outputs and review table history.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'object_rows' AS check_name, COUNT(*) AS observed_value FROM uc_securable_objects_day17
# MAGIC UNION ALL
# MAGIC SELECT 'grant_rows', COUNT(*) FROM privilege_grants_day17
# MAGIC UNION ALL
# MAGIC SELECT 'access_request_rows', COUNT(*) FROM access_requests_day17
# MAGIC UNION ALL
# MAGIC SELECT 'access_decision_rows', COUNT(*) FROM effective_access_decisions_day17
# MAGIC UNION ALL
# MAGIC SELECT 'denied_requests', COUNT(*) FROM effective_access_decisions_day17 WHERE access_decision = 'DENY'
# MAGIC UNION ALL
# MAGIC SELECT 'base_table_access_reviews', COUNT(*) FROM effective_access_decisions_day17 WHERE risk_note LIKE 'ALLOW_BUT_REVIEW_BASE_TABLE_ACCESS%';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_secure_base_day17;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 object rows.
# MAGIC - 26 grant rows.
# MAGIC - 10 access requests.
# MAGIC - 10 access decisions.
# MAGIC - 3 denied requests.
# MAGIC - 2 base-table access reviews.
# MAGIC
# MAGIC Operational meaning: governance should produce queryable evidence: objects, grants, requests, decisions, remediation, and Delta history.
