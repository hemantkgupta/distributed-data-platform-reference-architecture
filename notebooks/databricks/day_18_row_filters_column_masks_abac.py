# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 18 - Row Filters, Column Masks, Dynamic Views, And ABAC Policy Thinking
# MAGIC
# MAGIC Goal: practice fine-grained governance patterns: row filters, column masks, dynamic views, ABAC-style policy coverage, and policy validation.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Unity Catalog governance and security, SQL/Python data processing.
# MAGIC - Professional stretch: security/compliance, policy coverage, fail-closed reasoning, performance-aware policy design, and audit evidence.
# MAGIC
# MAGIC This notebook keeps the hands-on parts runnable in a personal workspace by simulating users and policy context with Delta tables. It also shows the real Databricks SQL shape for table-level row filters and column masks as reference.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Sensitive Base Data And Viewer Context
# MAGIC
# MAGIC Purpose: build a base table with regional and PII fields, then define simulated viewer roles.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_sensitive_day18 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   customer_email STRING,
# MAGIC   customer_phone STRING,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   region STRING,
# MAGIC   sales_channel STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contains_pii' = 'true',
# MAGIC   'row_scope_column' = 'region',
# MAGIC   'governance_surface' = 'restricted_base_table'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_sensitive_day18 VALUES
# MAGIC   (1801, 801, 'aarya@example.com', '+1-555-0101', DATE'2026-07-13', CAST(220.00 AS DECIMAL(10,2)), 'completed', 'US', 'web'),
# MAGIC   (1802, 802, 'ben@example.com', '+1-555-0102', DATE'2026-07-13', CAST(85.50 AS DECIMAL(10,2)), 'pending', 'US', 'mobile'),
# MAGIC   (1803, 803, 'chen@example.com', '+49-555-0103', DATE'2026-07-14', CAST(410.00 AS DECIMAL(10,2)), 'completed', 'EU', 'web'),
# MAGIC   (1804, 804, 'diya@example.com', '+65-555-0104', DATE'2026-07-14', CAST(64.75 AS DECIMAL(10,2)), 'completed', 'APAC', 'store'),
# MAGIC   (1805, 805, 'elena@example.com', '+49-555-0105', DATE'2026-07-15', CAST(510.20 AS DECIMAL(10,2)), 'cancelled', 'EU', 'mobile'),
# MAGIC   (1806, 806, 'farah@example.com', '+91-555-0106', DATE'2026-07-15', CAST(130.30 AS DECIMAL(10,2)), 'completed', 'IN', 'web');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE viewer_policy_context_day18 (
# MAGIC   viewer_principal STRING,
# MAGIC   business_role STRING,
# MAGIC   allowed_regions STRING,
# MAGIC   can_view_pii BOOLEAN,
# MAGIC   policy_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viewer_policy_context_day18 VALUES
# MAGIC   ('analyst-us@example.com', 'REGIONAL_ANALYST', 'US', false, 'US analyst can see US rows with PII masked'),
# MAGIC   ('analyst-eu@example.com', 'REGIONAL_ANALYST', 'EU', false, 'EU analyst can see EU rows with PII masked'),
# MAGIC   ('contractor-apac@example.com', 'CONTRACTOR', 'APAC', false, 'Contractor can see APAC rows with PII masked'),
# MAGIC   ('fraud-team@example.com', 'FRAUD_INVESTIGATOR', 'ALL', true, 'Approved fraud team can see all rows with PII'),
# MAGIC   ('executive@example.com', 'EXECUTIVE', 'ALL', false, 'Executive can see all regions but PII remains masked');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_sensitive_day18 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM viewer_policy_context_day18 ORDER BY viewer_principal;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 sensitive order rows.
# MAGIC - 5 simulated viewers.
# MAGIC - Regional viewers have one allowed region.
# MAGIC - Global viewers use `ALL`.
# MAGIC - Only the fraud team can see PII.
# MAGIC
# MAGIC Operational meaning: row-level and column-level policies need data attributes and identity attributes. Without both, policy behavior is guesswork.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Build Policy Functions And A Dynamic View Simulation
# MAGIC
# MAGIC Purpose: simulate a dynamic view that filters rows and masks columns based on the viewer context.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION can_view_region_day18(allowed_regions STRING, row_region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN allowed_regions = 'ALL' OR array_contains(split(allowed_regions, ','), row_region);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_email_day18(email STRING, can_view_pii BOOLEAN)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN can_view_pii THEN email
# MAGIC   WHEN email IS NULL THEN NULL
# MAGIC   ELSE '***MASKED_EMAIL***'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_phone_day18(phone STRING, can_view_pii BOOLEAN)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN can_view_pii THEN phone
# MAGIC   WHEN phone IS NULL THEN NULL
# MAGIC   ELSE '***MASKED_PHONE***'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_dynamic_view_sim_day18 AS
# MAGIC SELECT
# MAGIC   v.viewer_principal,
# MAGIC   v.business_role,
# MAGIC   o.order_id,
# MAGIC   o.customer_id,
# MAGIC   mask_email_day18(o.customer_email, v.can_view_pii) AS customer_email,
# MAGIC   mask_phone_day18(o.customer_phone, v.can_view_pii) AS customer_phone,
# MAGIC   o.order_date,
# MAGIC   o.amount,
# MAGIC   o.status,
# MAGIC   o.region,
# MAGIC   o.sales_channel,
# MAGIC   CASE WHEN v.can_view_pii THEN 'PII_VISIBLE' ELSE 'PII_MASKED' END AS pii_policy_result
# MAGIC FROM orders_sensitive_day18 o
# MAGIC CROSS JOIN viewer_policy_context_day18 v
# MAGIC WHERE can_view_region_day18(v.allowed_regions, o.region);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   viewer_principal,
# MAGIC   COUNT(*) AS visible_rows,
# MAGIC   SUM(CASE WHEN pii_policy_result = 'PII_MASKED' THEN 1 ELSE 0 END) AS masked_rows,
# MAGIC   SUM(CASE WHEN pii_policy_result = 'PII_VISIBLE' THEN 1 ELSE 0 END) AS unmasked_rows
# MAGIC FROM orders_dynamic_view_sim_day18
# MAGIC GROUP BY viewer_principal
# MAGIC ORDER BY viewer_principal;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM orders_dynamic_view_sim_day18
# MAGIC WHERE viewer_principal IN ('analyst-us@example.com', 'fraud-team@example.com')
# MAGIC ORDER BY viewer_principal, order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `analyst-us@example.com` sees 2 US rows with masked PII.
# MAGIC - `analyst-eu@example.com` sees 2 EU rows with masked PII.
# MAGIC - `contractor-apac@example.com` sees 1 APAC row with masked PII.
# MAGIC - `fraud-team@example.com` sees 6 rows with unmasked PII.
# MAGIC - `executive@example.com` sees 6 rows with masked PII.
# MAGIC
# MAGIC Operational meaning: a dynamic view can combine row filtering and column masking in one consumer-facing contract.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Compare Dynamic Views, Table-Level Policies, And ABAC
# MAGIC
# MAGIC Purpose: learn when each fine-grained governance option fits.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE policy_design_options_day18 (
# MAGIC   policy_approach STRING,
# MAGIC   applies_to STRING,
# MAGIC   managed_by STRING,
# MAGIC   best_for STRING,
# MAGIC   main_risk STRING,
# MAGIC   choose_when STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO policy_design_options_day18 VALUES
# MAGIC   ('dynamic_view', 'view over one or more tables', 'view owner', 'curated consumer surface with joins, transformations, row filtering, and masking', 'logic can drift from base table if not tested', 'you want consumers to query a governed view instead of the base table'),
# MAGIC   ('table_row_filter_column_mask', 'one table and selected columns', 'table owner or principal with manage privilege', 'table-specific row and column rules', 'hard to scale consistently across many tables', 'one table needs specific policy logic'),
# MAGIC   ('abac_policy', 'catalog/schema/table objects matched by governed tags', 'central policy owner', 'consistent rules across many tagged tables and columns', 'depends on tag quality and policy ownership discipline', 'many tables need the same tag-driven rule');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM policy_design_options_day18 ORDER BY policy_approach;

# COMMAND ----------

# MAGIC %md
# MAGIC Reference SQL shape for real table-level controls:
# MAGIC
# MAGIC ```sql
# MAGIC -- Reference only: run only when your workspace and privileges support it.
# MAGIC CREATE FUNCTION region_filter(region STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN is_account_group_member('global-readers') OR region = 'US';
# MAGIC
# MAGIC ALTER TABLE orders_sensitive_day18
# MAGIC SET ROW FILTER region_filter ON (region);
# MAGIC
# MAGIC CREATE FUNCTION email_mask(email STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE
# MAGIC   WHEN is_account_group_member('pii-approved') THEN email
# MAGIC   ELSE '***MASKED***'
# MAGIC END;
# MAGIC
# MAGIC ALTER TABLE orders_sensitive_day18
# MAGIC ALTER COLUMN customer_email SET MASK email_mask;
# MAGIC ```
# MAGIC
# MAGIC Expected result:
# MAGIC
# MAGIC - Dynamic views are best for curated surfaces.
# MAGIC - Table-level row filters and masks are best for one table with table-specific policy logic.
# MAGIC - ABAC is best when the same policy should apply across many tagged objects.
# MAGIC
# MAGIC Operational meaning: the principal design decision is scope. One view, one table, or many tagged objects.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Create Policy Inventory And Intentional Gap
# MAGIC
# MAGIC Purpose: model assets, sensitive columns, active policies, and one missing mask for validation practice.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE governed_assets_day18 (
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   sensitivity_class STRING,
# MAGIC   row_scope_required BOOLEAN,
# MAGIC   mask_required BOOLEAN,
# MAGIC   recommended_policy_approach STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO governed_assets_day18 VALUES
# MAGIC   ('orders_sensitive_day18', 'managed_table', 'restricted_pii', true, true, 'dynamic_view_or_table_policy'),
# MAGIC   ('orders_dynamic_view_sim_day18', 'view', 'masked_pii_surface', false, false, 'dynamic_view'),
# MAGIC   ('orders_gold_metrics_day18', 'managed_table', 'aggregated_non_pii', false, false, 'standard_grants'),
# MAGIC   ('customers_external_day18', 'external_table', 'restricted_pii', true, true, 'abac_policy');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sensitive_columns_day18 (
# MAGIC   object_name STRING,
# MAGIC   column_name STRING,
# MAGIC   data_classification STRING,
# MAGIC   mask_required BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO sensitive_columns_day18 VALUES
# MAGIC   ('orders_sensitive_day18', 'customer_email', 'email_pii', true),
# MAGIC   ('orders_sensitive_day18', 'customer_phone', 'phone_pii', true),
# MAGIC   ('orders_sensitive_day18', 'customer_id', 'internal_identifier', false),
# MAGIC   ('orders_sensitive_day18', 'region', 'row_scope_driver', false),
# MAGIC   ('orders_sensitive_day18', 'amount', 'financial_measure', false),
# MAGIC   ('customers_external_day18', 'email', 'email_pii', true),
# MAGIC   ('customers_external_day18', 'phone', 'phone_pii', true),
# MAGIC   ('customers_external_day18', 'loyalty_id', 'direct_identifier', true);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE policy_rules_day18 (
# MAGIC   policy_id STRING,
# MAGIC   policy_type STRING,
# MAGIC   target_object_name STRING,
# MAGIC   target_column_name STRING,
# MAGIC   policy_owner STRING,
# MAGIC   active BOOLEAN,
# MAGIC   policy_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO policy_rules_day18 VALUES
# MAGIC   ('pol-001', 'DYNAMIC_VIEW_ROW_FILTER', 'orders_sensitive_day18', NULL, 'analytics-owner@example.com', true, 'dynamic view filters by allowed region'),
# MAGIC   ('pol-002', 'DYNAMIC_VIEW_COLUMN_MASK', 'orders_sensitive_day18', 'customer_email', 'analytics-owner@example.com', true, 'dynamic view masks email'),
# MAGIC   ('pol-003', 'DYNAMIC_VIEW_COLUMN_MASK', 'orders_sensitive_day18', 'customer_phone', 'analytics-owner@example.com', true, 'dynamic view masks phone'),
# MAGIC   ('pol-004', 'ABAC_ROW_FILTER', 'customers_external_day18', NULL, 'security-policy@example.com', true, 'tag-driven regional row policy'),
# MAGIC   ('pol-005', 'ABAC_COLUMN_MASK', 'customers_external_day18', 'email', 'security-policy@example.com', true, 'tag-driven email mask'),
# MAGIC   ('pol-006', 'ABAC_COLUMN_MASK', 'customers_external_day18', 'phone', 'security-policy@example.com', true, 'tag-driven phone mask');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM governed_assets_day18 ORDER BY object_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensitive_columns_day18 ORDER BY object_name, column_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM policy_rules_day18 ORDER BY policy_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 4 governed assets.
# MAGIC - 8 classified columns.
# MAGIC - 6 active policy rules.
# MAGIC - `customers_external_day18.loyalty_id` intentionally has no mask policy.
# MAGIC
# MAGIC Operational meaning: a policy catalog lets you prove coverage and find gaps before publishing sensitive data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Validate Policy Coverage With PySpark
# MAGIC
# MAGIC Purpose: check row-filter coverage and column-mask coverage from metadata.

# COMMAND ----------

from pyspark.sql import functions as F

assets_df = spark.table("de_learning.governed_assets_day18")
columns_df = spark.table("de_learning.sensitive_columns_day18")
rules_df = spark.table("de_learning.policy_rules_day18")

active_rules_df = rules_df.where(F.col("active") == F.lit(True))

row_policy_df = (
    assets_df
    .where(F.col("row_scope_required") == F.lit(True))
    .alias("asset")
    .join(
        active_rules_df
        .where(F.col("policy_type").isin("DYNAMIC_VIEW_ROW_FILTER", "TABLE_ROW_FILTER", "ABAC_ROW_FILTER"))
        .select(F.col("target_object_name").alias("object_name"), "policy_id", "policy_type")
        .alias("rule"),
        on="object_name",
        how="left"
    )
    .select(
        F.lit("ROW_FILTER_COVERAGE").alias("check_name"),
        F.col("object_name"),
        F.lit(None).cast("string").alias("column_name"),
        F.when(F.col("policy_id").isNotNull(), F.lit("PASS")).otherwise(F.lit("FAIL")).alias("outcome"),
        F.coalesce(F.col("policy_id"), F.lit("MISSING_ROW_POLICY")).alias("evidence"),
        F.when(
            F.col("policy_id").isNull(),
            F.concat(F.lit("Add row filter or ABAC row policy for "), F.col("object_name"))
        ).otherwise(F.lit("Row policy coverage present")).alias("remediation")
    )
)

column_policy_df = (
    columns_df
    .where(F.col("mask_required") == F.lit(True))
    .alias("col")
    .join(
        active_rules_df
        .where(F.col("policy_type").isin("DYNAMIC_VIEW_COLUMN_MASK", "TABLE_COLUMN_MASK", "ABAC_COLUMN_MASK"))
        .select(
            F.col("target_object_name").alias("object_name"),
            F.col("target_column_name").alias("column_name"),
            "policy_id",
            "policy_type"
        )
        .alias("rule"),
        on=["object_name", "column_name"],
        how="left"
    )
    .select(
        F.lit("COLUMN_MASK_COVERAGE").alias("check_name"),
        F.col("object_name"),
        F.col("column_name"),
        F.when(F.col("policy_id").isNotNull(), F.lit("PASS")).otherwise(F.lit("FAIL")).alias("outcome"),
        F.coalesce(F.col("policy_id"), F.lit("MISSING_MASK_POLICY")).alias("evidence"),
        F.when(
            F.col("policy_id").isNull(),
            F.concat(F.lit("Add column mask policy for "), F.col("object_name"), F.lit("."), F.col("column_name"))
        ).otherwise(F.lit("Mask coverage present")).alias("remediation")
    )
)

policy_validation_df = row_policy_df.unionByName(column_policy_df)
policy_validation_df.createOrReplaceTempView("policy_validation_results_day18")
display(policy_validation_df.orderBy("outcome", "check_name", "object_name", "column_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `assets_df`, `columns_df`, and `rules_df` are policy metadata tables loaded as DataFrames.
# MAGIC - `where(...)` filters to assets/columns that require row filters or masks.
# MAGIC - `join(..., how="left")` keeps required controls even when no matching active policy exists.
# MAGIC - `withColumn` is not needed here because `select(...)` builds the derived output columns directly.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - `unionByName(...)` stacks row-filter checks and column-mask checks into one validation result.
# MAGIC - Execution is lazy until `display(...)`.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT c.object_name, c.column_name,
# MAGIC        CASE WHEN r.policy_id IS NULL THEN 'FAIL' ELSE 'PASS' END AS outcome
# MAGIC FROM sensitive_columns_day18 c
# MAGIC LEFT JOIN policy_rules_day18 r
# MAGIC   ON c.object_name = r.target_object_name
# MAGIC  AND c.column_name = r.target_column_name
# MAGIC  AND r.active = true
# MAGIC WHERE c.mask_required = true;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT outcome, check_name, COUNT(*) AS check_count
# MAGIC FROM policy_validation_results_day18
# MAGIC GROUP BY outcome, check_name
# MAGIC ORDER BY outcome, check_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM policy_validation_results_day18
# MAGIC WHERE outcome = 'FAIL'
# MAGIC ORDER BY check_name, object_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Row filter coverage passes for `orders_sensitive_day18` and `customers_external_day18`.
# MAGIC - Column mask coverage passes for email and phone columns.
# MAGIC - Column mask coverage fails for `customers_external_day18.loyalty_id`.
# MAGIC
# MAGIC Operational meaning: governance needs automated coverage checks. Manual review does not scale once tables and tags grow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Convert Gaps Into Publish Decisions
# MAGIC
# MAGIC Purpose: decide whether governed assets can be published to consumers.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE policy_publish_decisions_day18
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   object_name,
# MAGIC   SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) AS failed_policy_checks,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN outcome = 'FAIL' THEN 1 ELSE 0 END) = 0 THEN 'READY_TO_PUBLISH'
# MAGIC     ELSE 'BLOCKED_POLICY_GAP'
# MAGIC   END AS publish_decision,
# MAGIC   array_join(collect_set(CASE WHEN outcome = 'FAIL' THEN remediation END), '; ') AS required_fix
# MAGIC FROM policy_validation_results_day18
# MAGIC GROUP BY object_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM policy_publish_decisions_day18 ORDER BY object_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_sensitive_day18` is ready from the policy-coverage perspective.
# MAGIC - `customers_external_day18` is blocked because `loyalty_id` lacks a mask.
# MAGIC
# MAGIC Operational meaning: sensitive data publication should be blocked by missing controls, not fixed after consumers already have access.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Checks
# MAGIC
# MAGIC Purpose: validate row counts, policy outputs, and Delta history.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'base_order_rows' AS check_name, COUNT(*) AS observed_value FROM orders_sensitive_day18
# MAGIC UNION ALL
# MAGIC SELECT 'viewer_context_rows', COUNT(*) FROM viewer_policy_context_day18
# MAGIC UNION ALL
# MAGIC SELECT 'dynamic_view_visible_rows', COUNT(*) FROM orders_dynamic_view_sim_day18
# MAGIC UNION ALL
# MAGIC SELECT 'policy_asset_rows', COUNT(*) FROM governed_assets_day18
# MAGIC UNION ALL
# MAGIC SELECT 'sensitive_column_rows', COUNT(*) FROM sensitive_columns_day18
# MAGIC UNION ALL
# MAGIC SELECT 'policy_rule_rows', COUNT(*) FROM policy_rules_day18
# MAGIC UNION ALL
# MAGIC SELECT 'policy_validation_rows', COUNT(*) FROM policy_validation_results_day18
# MAGIC UNION ALL
# MAGIC SELECT 'policy_validation_failures', COUNT(*) FROM policy_validation_results_day18 WHERE outcome = 'FAIL'
# MAGIC UNION ALL
# MAGIC SELECT 'blocked_publish_objects', COUNT(*) FROM policy_publish_decisions_day18 WHERE publish_decision = 'BLOCKED_POLICY_GAP';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_sensitive_day18;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 base order rows.
# MAGIC - 5 viewer context rows.
# MAGIC - 17 dynamic view visible rows.
# MAGIC - 4 policy asset rows.
# MAGIC - 8 sensitive column rows.
# MAGIC - 6 policy rule rows.
# MAGIC - 7 policy validation rows.
# MAGIC - 1 policy validation failure.
# MAGIC - 1 blocked publish object.
# MAGIC
# MAGIC Operational meaning: policy-controlled systems need evidence tables: source data, policy context, rules, validation results, decisions, and table history.
