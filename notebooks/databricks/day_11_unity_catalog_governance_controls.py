# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 11 - Unity Catalog-Style Governance Controls
# MAGIC
# MAGIC Goal: practice managed/external object reasoning, row filters, masking, and access audit evidence using runnable Delta tables and views.
# MAGIC
# MAGIC Lab flow:
# MAGIC
# MAGIC - Model managed tables, external volumes, views, and lifecycle responsibilities.
# MAGIC - Build a sensitive customer table and consumer context.
# MAGIC - Apply row-filter and masking logic through secure views.
# MAGIC - Evaluate access policy decisions with PySpark.
# MAGIC - Produce governance audit evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Model Governed Objects And Lifecycle
# MAGIC
# MAGIC Purpose: practice Unity Catalog-style object vocabulary: catalog, schema, managed table, external volume, and view.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_objects_day11 (
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   parent_object STRING,
# MAGIC   storage_class STRING,
# MAGIC   lifecycle_owner STRING,
# MAGIC   contains_pii BOOLEAN,
# MAGIC   consumer_visible BOOLEAN,
# MAGIC   expected_control STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO uc_objects_day11 VALUES
# MAGIC   ('main', 'catalog', NULL, 'metastore', 'platform-admin@example.com', false, false, 'catalog ownership and grants'),
# MAGIC   ('de_learning', 'schema', 'main', 'metastore', 'data-platform@example.com', false, false, 'schema ownership and grants'),
# MAGIC   ('orders_raw_volume_day11', 'volume', 'de_learning', 'external', 'ingestion-owner@example.com', false, false, 'READ FILES / WRITE FILES scoped to ingestion principals'),
# MAGIC   ('customers_secure_base_day11', 'table', 'de_learning', 'managed', 'customer-domain-owner@example.com', true, false, 'restricted direct access'),
# MAGIC   ('customers_masked_view_day11', 'view', 'de_learning', 'logical', 'customer-domain-owner@example.com', true, true, 'row filters and masking'),
# MAGIC   ('customer_access_audit_day11', 'table', 'de_learning', 'managed', 'security-owner@example.com', false, false, 'audit retention and review');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uc_objects_day11 ORDER BY object_type, object_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   object_type,
# MAGIC   storage_class,
# MAGIC   COUNT(*) AS object_count,
# MAGIC   SUM(CASE WHEN contains_pii THEN 1 ELSE 0 END) AS pii_object_count,
# MAGIC   SUM(CASE WHEN consumer_visible THEN 1 ELSE 0 END) AS consumer_visible_count
# MAGIC FROM uc_objects_day11
# MAGIC GROUP BY object_type, storage_class
# MAGIC ORDER BY object_type, storage_class;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - A clear catalog -> schema -> table/view/volume object map.
# MAGIC - The base PII table is not consumer-visible.
# MAGIC - The masked view is consumer-visible.
# MAGIC
# MAGIC Operational meaning: in Unity Catalog, object type and lifecycle determine who can own, access, share, and clean up data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create Sensitive Customer Data And Access Context
# MAGIC
# MAGIC Purpose: create a base table with PII plus a small context table that simulates current consumer role.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_secure_base_day11 (
# MAGIC   customer_id INT,
# MAGIC   customer_name STRING,
# MAGIC   email STRING,
# MAGIC   region STRING,
# MAGIC   segment STRING,
# MAGIC   lifetime_value DECIMAL(10,2),
# MAGIC   consent_to_marketing BOOLEAN,
# MAGIC   pii_classification STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contains_pii' = 'true',
# MAGIC   'direct_access' = 'restricted'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customers_secure_base_day11 VALUES
# MAGIC   (101, 'Asha Rao', 'asha.rao@example.com', 'APAC', 'enterprise', CAST(12000.00 AS DECIMAL(10,2)), true, 'EMAIL'),
# MAGIC   (102, 'Ben Smith', 'ben.smith@example.com', 'NA', 'midmarket', CAST(5000.00 AS DECIMAL(10,2)), false, 'EMAIL'),
# MAGIC   (103, 'Carla Diaz', 'carla.diaz@example.com', 'EU', 'enterprise', CAST(17000.00 AS DECIMAL(10,2)), true, 'EMAIL'),
# MAGIC   (104, 'Dev Patel', 'dev.patel@example.com', 'APAC', 'smb', CAST(900.00 AS DECIMAL(10,2)), false, 'EMAIL'),
# MAGIC   (105, 'Elena Rossi', 'elena.rossi@example.com', 'EU', 'midmarket', CAST(3200.00 AS DECIMAL(10,2)), true, 'EMAIL');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE consumer_context_day11 (
# MAGIC   principal STRING,
# MAGIC   role_name STRING,
# MAGIC   allowed_region STRING,
# MAGIC   can_view_email BOOLEAN,
# MAGIC   can_view_lifetime_value BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO consumer_context_day11 VALUES
# MAGIC   ('analyst_apac@example.com', 'regional_analyst', 'APAC', false, false),
# MAGIC   ('finance_global@example.com', 'finance_analyst', 'ALL', false, true),
# MAGIC   ('privacy_admin@example.com', 'privacy_admin', 'ALL', true, true);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_secure_base_day11 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM consumer_context_day11 ORDER BY principal;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 5 customer rows with email and lifetime value.
# MAGIC - 3 consumer contexts with different region and masking rights.
# MAGIC
# MAGIC Operational meaning: masking and row filters are based on who is asking and what they are allowed to see.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Apply Row Filters And Masking With A Secure View
# MAGIC
# MAGIC Purpose: simulate Unity Catalog row-filter and column-mask behavior with a runnable SQL view.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_consumer_day11
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM consumer_context_day11
# MAGIC WHERE principal = 'analyst_apac@example.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customers_masked_view_day11 AS
# MAGIC SELECT
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   CASE
# MAGIC     WHEN ctx.can_view_email THEN c.email
# MAGIC     ELSE concat('masked+', cast(c.customer_id AS STRING), '@example.com')
# MAGIC   END AS email,
# MAGIC   c.region,
# MAGIC   c.segment,
# MAGIC   CASE
# MAGIC     WHEN ctx.can_view_lifetime_value THEN c.lifetime_value
# MAGIC     ELSE CAST(NULL AS DECIMAL(10,2))
# MAGIC   END AS lifetime_value,
# MAGIC   c.consent_to_marketing
# MAGIC FROM customers_secure_base_day11 c
# MAGIC CROSS JOIN current_consumer_day11 ctx
# MAGIC WHERE ctx.allowed_region = 'ALL'
# MAGIC    OR c.region = ctx.allowed_region;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_masked_view_day11 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Only APAC customers are visible: customer 101 and 104.
# MAGIC - Email is masked.
# MAGIC - `lifetime_value` is null.
# MAGIC
# MAGIC Operational meaning: row filters reduce which records are visible; masking changes which column values are visible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Switch Consumer Contexts And Compare Results
# MAGIC
# MAGIC Purpose: show how the same governed table produces different views for different roles.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_consumer_day11
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM consumer_context_day11
# MAGIC WHERE principal = 'finance_global@example.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_masked_view_day11 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE current_consumer_day11
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM consumer_context_day11
# MAGIC WHERE principal = 'privacy_admin@example.com';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_masked_view_day11 ORDER BY customer_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Finance sees all regions and `lifetime_value`, but email remains masked.
# MAGIC - Privacy admin sees all regions, real email, and `lifetime_value`.
# MAGIC
# MAGIC Operational meaning: access policy should be role-sensitive, not hard-coded into separate copied tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Evaluate Access Requests With PySpark
# MAGIC
# MAGIC Purpose: turn access requests into explicit allow/deny decisions with reasons.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE access_requests_day11 (
# MAGIC   request_id STRING,
# MAGIC   principal STRING,
# MAGIC   requested_object STRING,
# MAGIC   requested_privilege STRING,
# MAGIC   requested_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO access_requests_day11 VALUES
# MAGIC   ('access-001', 'analyst_apac@example.com', 'customers_masked_view_day11', 'SELECT', 'regional dashboard'),
# MAGIC   ('access-002', 'analyst_apac@example.com', 'customers_secure_base_day11', 'SELECT', 'ad hoc investigation'),
# MAGIC   ('access-003', 'finance_global@example.com', 'customers_masked_view_day11', 'SELECT', 'revenue analysis'),
# MAGIC   ('access-004', 'finance_global@example.com', 'customers_secure_base_day11', 'SELECT', 'needs raw emails'),
# MAGIC   ('access-005', 'privacy_admin@example.com', 'customers_secure_base_day11', 'SELECT', 'privacy review');

# COMMAND ----------

from pyspark.sql import functions as F

requests_df = spark.table("de_learning.access_requests_day11")
objects_df = spark.table("de_learning.uc_objects_day11")
context_df = spark.table("de_learning.consumer_context_day11")

access_decision_df = (
    requests_df
    .join(objects_df, requests_df.requested_object == objects_df.object_name, "left")
    .join(context_df, on="principal", how="left")
    .withColumn(
        "object_exists",
        F.col("object_type").isNotNull(),
    )
    .withColumn(
        "base_pii_table",
        (F.col("object_type") == "table") & (F.col("contains_pii") == True),
    )
    .withColumn(
        "allowed",
        F.col("object_exists")
        & (
            (F.col("requested_object") == "customers_masked_view_day11")
            | (
                (F.col("requested_object") == "customers_secure_base_day11")
                & (F.col("role_name") == "privacy_admin")
            )
        )
    )
    .withColumn(
        "decision",
        F.when(F.col("allowed"), F.lit("APPROVED")).otherwise(F.lit("DENIED")),
    )
    .withColumn(
        "decision_reason",
        F.concat_ws(
            "; ",
            F.when(~F.col("object_exists"), F.lit("requested object does not exist")),
            F.when(
                (F.col("requested_object") == "customers_secure_base_day11")
                & (F.col("role_name") != "privacy_admin"),
                F.lit("direct PII table access requires privacy_admin role"),
            ),
            F.when(
                F.col("requested_object") == "customers_masked_view_day11",
                F.lit("masked governed view is approved consumer surface"),
            ),
            F.when(
                (F.col("requested_object") == "customers_secure_base_day11")
                & (F.col("role_name") == "privacy_admin"),
                F.lit("privacy_admin can access base PII table for review"),
            ),
        ),
    )
    .select(
        "request_id",
        "principal",
        "role_name",
        "requested_object",
        "requested_privilege",
        "decision",
        "decision_reason",
    )
)

access_decision_df.createOrReplaceTempView("access_decisions_day11")
display(access_decision_df.orderBy("request_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC This DataFrame represents access-review evidence: each request is joined to object metadata and consumer role context.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT r.*, o.object_type, c.role_name,
# MAGIC   CASE
# MAGIC     WHEN r.requested_object = 'customers_masked_view_day11' THEN 'APPROVED'
# MAGIC     WHEN r.requested_object = 'customers_secure_base_day11' AND c.role_name = 'privacy_admin' THEN 'APPROVED'
# MAGIC     ELSE 'DENIED'
# MAGIC   END AS decision
# MAGIC FROM access_requests_day11 r
# MAGIC LEFT JOIN uc_objects_day11 o ON r.requested_object = o.object_name
# MAGIC LEFT JOIN consumer_context_day11 c ON r.principal = c.principal;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `join(..., how="left")` keeps every access request even if metadata is missing.
# MAGIC - `withColumn(...)` adds policy evaluation fields.
# MAGIC - Boolean expressions with `&` and `|` model policy rules.
# MAGIC - `createOrReplaceTempView(...)` lets SQL persist the PySpark decision output.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE access_decisions_delta_day11
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM access_decisions_day11;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM access_decisions_delta_day11 ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `access-001` and `access-003` are approved for the masked view.
# MAGIC - `access-002` and `access-004` are denied for direct PII table access.
# MAGIC - `access-005` is approved because `privacy_admin` can access the base PII table.
# MAGIC
# MAGIC Operational meaning: access decisions should be explainable, queryable, and reviewable after the fact.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Produce Governance Audit Summary
# MAGIC
# MAGIC Purpose: summarize masking, row filtering, object classification, and access decisions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE governance_audit_summary_day11
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   'pii_objects' AS metric_name,
# MAGIC   COUNT(*) AS metric_value
# MAGIC FROM uc_objects_day11
# MAGIC WHERE contains_pii = true
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'consumer_visible_pii_surfaces',
# MAGIC   COUNT(*)
# MAGIC FROM uc_objects_day11
# MAGIC WHERE contains_pii = true
# MAGIC   AND consumer_visible = true
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'denied_access_requests',
# MAGIC   COUNT(*)
# MAGIC FROM access_decisions_delta_day11
# MAGIC WHERE decision = 'DENIED'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'approved_access_requests',
# MAGIC   COUNT(*)
# MAGIC FROM access_decisions_delta_day11
# MAGIC WHERE decision = 'APPROVED';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governance_audit_summary_day11 ORDER BY metric_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   requested_object,
# MAGIC   decision,
# MAGIC   COUNT(*) AS request_count
# MAGIC FROM access_decisions_delta_day11
# MAGIC GROUP BY requested_object, decision
# MAGIC ORDER BY requested_object, decision;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - PII objects are visible in the object inventory.
# MAGIC - Direct base-table access denials are visible in access decisions.
# MAGIC - Approved access goes through the masked governed view or privacy admin path.
# MAGIC
# MAGIC Operational meaning: governance controls are only useful when the platform can prove what was protected, who asked, and why access was allowed or denied.
