# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 25 - Platform Governance Readiness Review
# MAGIC
# MAGIC Goal: close the Unity Catalog foundation segment with an executable readiness review across object hierarchy, privileges, lifecycle, fine-grained security, compute, maintenance, recovery, lineage, monitoring, and cost ownership.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks Intelligence Platform, governance/security, troubleshooting/monitoring/optimization, Lakeflow Jobs, and CI/CD readiness.
# MAGIC - Professional stretch: platform governance operating model, evidence retention, compliance, cost/performance, debugging/deployment, and production readiness gates.
# MAGIC
# MAGIC This review lab uses runnable Delta tables plus simulated control-plane metadata. Commands that require workspace-admin or metastore-admin privileges are stored as query templates rather than executed directly.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Build A Governed Serving Asset
# MAGIC
# MAGIC Purpose: create a compact table and consumer view that represent a governed data product with PII, quality state, ownership, and serving behavior.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS orders_governance_public_view_day25;
# MAGIC DROP TABLE IF EXISTS segment_next_drills_day25;
# MAGIC DROP TABLE IF EXISTS segment_review_summary_day25;
# MAGIC DROP TABLE IF EXISTS readiness_scorecard_day25;
# MAGIC DROP TABLE IF EXISTS segment_control_coverage_day25;
# MAGIC DROP TABLE IF EXISTS system_query_templates_day25;
# MAGIC DROP TABLE IF EXISTS platform_operational_signals_day25;
# MAGIC DROP TABLE IF EXISTS platform_assets_day25;
# MAGIC DROP TABLE IF EXISTS uc_foundation_controls_day25;
# MAGIC DROP TABLE IF EXISTS orders_governance_base_day25;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_governance_base_day25
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'owner_domain' = 'orders',
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'data_classification' = 'contains_pii',
# MAGIC   'quality_slo' = 'fresh_by_07_00_ist',
# MAGIC   'lifecycle_gate' = 'owner-quality-lineage-approval'
# MAGIC )
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2501, 301, DATE'2026-07-22', 'US', CAST(210.00 AS DECIMAL(10,2)), 'COMPLETED', 'ada@example.com', true),
# MAGIC   (2502, 302, DATE'2026-07-22', 'EU', CAST(125.00 AS DECIMAL(10,2)), 'COMPLETED', 'grace@example.com', true),
# MAGIC   (2503, 303, DATE'2026-07-22', 'APAC', CAST(315.00 AS DECIMAL(10,2)), 'COMPLETED', 'katherine@example.com', true),
# MAGIC   (2504, 304, DATE'2026-07-23', 'US', CAST(80.00 AS DECIMAL(10,2)), 'PENDING', 'margaret@example.com', true),
# MAGIC   (2505, 305, DATE'2026-07-23', 'EU', CAST(50.00 AS DECIMAL(10,2)), 'CANCELLED', 'dorothy@example.com', true),
# MAGIC   (2506, 306, DATE'2026-07-23', 'US', CAST(0.00 AS DECIMAL(10,2)), 'COMPLETED', 'invalid@example.com', false)
# MAGIC AS t(order_id, customer_id, order_date, region, amount, normalized_status, customer_email, quality_passed);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_governance_public_view_day25 AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   amount,
# MAGIC   normalized_status,
# MAGIC   concat(substr(customer_email, 1, 1), '***', substr(customer_email, instr(customer_email, '@'))) AS masked_customer_email
# MAGIC FROM orders_governance_base_day25
# MAGIC WHERE quality_passed = true
# MAGIC   AND region IN ('US', 'EU');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'base_table' AS asset, COUNT(*) AS row_count FROM orders_governance_base_day25
# MAGIC UNION ALL
# MAGIC SELECT 'public_view', COUNT(*) FROM orders_governance_public_view_day25;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Base table has 6 rows.
# MAGIC - Public view has only passing-quality US/EU rows with masked email values.
# MAGIC
# MAGIC Operational meaning: a governed serving table should expose ownership, classification, quality status, and a consumer-safe access path before it becomes a stable data product.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Build The Unity Catalog Foundation Coverage Matrix
# MAGIC
# MAGIC Purpose: turn Days 16-24 into concrete readiness controls, each mapped to Associate objectives and a Professional operating angle.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE uc_foundation_controls_day25 (
# MAGIC   control_id STRING,
# MAGIC   control_area STRING,
# MAGIC   associate_objective STRING,
# MAGIC   professional_extension STRING,
# MAGIC   required_artifact STRING,
# MAGIC   passing_signal STRING,
# MAGIC   days_covered ARRAY<STRING>
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO uc_foundation_controls_day25 VALUES
# MAGIC   (
# MAGIC     'UC-01',
# MAGIC     'Object hierarchy and naming',
# MAGIC     'Databricks Intelligence Platform',
# MAGIC     'Production namespace design and ownership boundaries',
# MAGIC     'Catalog, schema, table/view/volume naming convention',
# MAGIC     'Every production asset has a three-level namespace and owner',
# MAGIC     array('Day 16', 'Day 17')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-02',
# MAGIC     'Usage privilege chain',
# MAGIC     'Governance and Security',
# MAGIC     'Least-privilege access review',
# MAGIC     'USE CATALOG, USE SCHEMA, and object privilege evidence',
# MAGIC     'SELECT alone is not treated as sufficient without parent usage',
# MAGIC     array('Day 17')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-03',
# MAGIC     'Ownership and MANAGE boundary',
# MAGIC     'Governance and Security',
# MAGIC     'Admin-safe delegation model',
# MAGIC     'Owner principal and delegated manager evidence',
# MAGIC     'Data owner and platform operator responsibilities are separated',
# MAGIC     array('Day 16', 'Day 17')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-04',
# MAGIC     'Managed, external, and volume lifecycle',
# MAGIC     'Databricks Intelligence Platform',
# MAGIC     'Production deletion and migration safety',
# MAGIC     'Storage mode, location, and deletion semantics',
# MAGIC     'Drop behavior is known before the asset is deleted',
# MAGIC     array('Day 16', 'Day 19', 'Day 23')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-05',
# MAGIC     'Row filters and column masks',
# MAGIC     'Governance and Security',
# MAGIC     'Fine-grained security without consumer-specific copies',
# MAGIC     'Policy or view evidence for protected rows and columns',
# MAGIC     'PII assets have an enforceable row/column protection path',
# MAGIC     array('Day 18')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-06',
# MAGIC     'ABAC governed tags and policy scope',
# MAGIC     'Governance and Security',
# MAGIC     'Scalable tag-driven policy enforcement',
# MAGIC     'Governed tag taxonomy, policy scope, and owner evidence',
# MAGIC     'Policy coverage is inherited or attached at the right hierarchy level',
# MAGIC     array('Day 18')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-07',
# MAGIC     'Compute selection and isolation',
# MAGIC     'Troubleshooting, Monitoring, and Optimization',
# MAGIC     'Cost/performance and task isolation decisions',
# MAGIC     'Compute type chosen by workload and isolation need',
# MAGIC     'Jobs, SQL, notebooks, and pipelines use fit-for-purpose compute',
# MAGIC     array('Day 20')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-08',
# MAGIC     'Delta maintenance and retention',
# MAGIC     'Troubleshooting, Monitoring, and Optimization',
# MAGIC     'Retention-safe maintenance and cost control',
# MAGIC     'DESCRIBE DETAIL, history, OPTIMIZE/VACUUM plan, retention check',
# MAGIC     'Maintenance cannot break time travel, recovery, or streaming readers',
# MAGIC     array('Day 21')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-09',
# MAGIC     'Incident recovery and time travel',
# MAGIC     'Troubleshooting, Monitoring, and Optimization',
# MAGIC     'Restore vs forward-fix decision records',
# MAGIC     'Known-good version, bad write evidence, and recovery action',
# MAGIC     'Recovery decision can be explained after the incident',
# MAGIC     array('Day 22')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-10',
# MAGIC     'Drop recovery and UNDROP',
# MAGIC     'Governance and Security',
# MAGIC     'Lifecycle recovery and blast-radius control',
# MAGIC     'Dropped-object inspection, UNDROP path, clone/evidence plan',
# MAGIC     'Accidental drops have a documented recovery route',
# MAGIC     array('Day 23')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-11',
# MAGIC     'Lineage and impact analysis',
# MAGIC     'Troubleshooting, Monitoring, and Optimization',
# MAGIC     'Consumer blast-radius analysis before deployment',
# MAGIC     'Catalog Explorer or system lineage table evidence',
# MAGIC     'High-impact changes block until owners approve',
# MAGIC     array('Day 24')
# MAGIC   ),
# MAGIC   (
# MAGIC     'UC-12',
# MAGIC     'System-table monitoring and audit evidence',
# MAGIC     'Governance and Security',
# MAGIC     'Auditability, alerting, and compliance evidence retention',
# MAGIC     'System table query templates and restricted access pattern',
# MAGIC     'Operational evidence can be queried without exporting sensitive data',
# MAGIC     array('Day 20', 'Day 21', 'Day 22', 'Day 24')
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   control_id,
# MAGIC   control_area,
# MAGIC   associate_objective,
# MAGIC   concat_ws(', ', days_covered) AS days_covered,
# MAGIC   passing_signal
# MAGIC FROM uc_foundation_controls_day25
# MAGIC ORDER BY control_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 12 controls cover the Day 16-24 Unity Catalog foundation work.
# MAGIC - Each control has a concrete artifact or passing signal.
# MAGIC
# MAGIC Operational meaning: a topic is not production-ready because it was read once. It is ready when you can point to the artifact that proves the control.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Inventory Assets And Governance Evidence
# MAGIC
# MAGIC Purpose: create a production-style asset inventory with ownership, lineage, access policy, recovery, monitoring, and cost signals.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE platform_assets_day25 (
# MAGIC   asset_name STRING,
# MAGIC   asset_type STRING,
# MAGIC   namespace_depth INT,
# MAGIC   owner_domain STRING,
# MAGIC   data_classification STRING,
# MAGIC   storage_mode STRING,
# MAGIC   lifecycle_state STRING,
# MAGIC   has_usage_chain BOOLEAN,
# MAGIC   has_owner BOOLEAN,
# MAGIC   has_lineage BOOLEAN,
# MAGIC   has_quality_gate BOOLEAN,
# MAGIC   has_access_policy BOOLEAN,
# MAGIC   has_recovery_plan BOOLEAN,
# MAGIC   compute_binding STRING,
# MAGIC   monitoring_surface STRING,
# MAGIC   cost_owner STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO platform_assets_day25 VALUES
# MAGIC   ('prod.orders.orders_governance_base_day25', 'TABLE', 3, 'orders', 'CONTAINS_PII', 'MANAGED', 'PUBLISHED', true, true, true, true, true, true, 'SERVERLESS_JOBS', 'system.access.table_lineage', 'orders'),
# MAGIC   ('prod.orders.orders_governance_public_view_day25', 'VIEW', 3, 'analytics', 'MASKED', 'MANAGED', 'PUBLISHED', true, true, true, true, true, true, 'SERVERLESS_SQL_WAREHOUSE', 'system.query.history', 'analytics'),
# MAGIC   ('prod.orders.raw_files_volume_day25', 'VOLUME', 3, 'orders', 'CONTAINS_PII', 'EXTERNAL', 'ACTIVE', true, true, true, false, true, true, 'N/A', 'system.access.audit', 'orders'),
# MAGIC   ('prod.finance.orders_daily_gold_job_day25', 'JOB', 3, 'finance', 'INTERNAL', 'N/A', 'ACTIVE', true, true, true, true, false, true, 'SERVERLESS_JOBS', 'system.lakeflow.job_run_timeline', 'finance'),
# MAGIC   ('prod.finance.revenue_sql_warehouse_day25', 'SQL_WAREHOUSE', 3, 'finance', 'INTERNAL', 'N/A', 'ACTIVE', true, true, false, false, false, false, 'SERVERLESS_SQL_WAREHOUSE', 'system.query.history', 'finance'),
# MAGIC   ('prod.legacy.orders_legacy_ext_day25', 'TABLE', 3, 'unknown', 'CONTAINS_PII', 'EXTERNAL', 'DEPRECATED', false, false, false, false, false, false, 'CLASSIC_ALL_PURPOSE', 'NONE', NULL),
# MAGIC   ('prod.analytics.sandbox_orders_day25', 'TABLE', 3, 'analytics', 'INTERNAL', 'MANAGED', 'DRAFT', true, true, false, false, false, false, 'ALL_PURPOSE', 'system.query.history', 'analytics'),
# MAGIC   ('prod.orders.orders_recovery_clone_day25', 'TABLE', 3, 'orders', 'INTERNAL', 'MANAGED', 'RECOVERY', true, true, true, true, false, true, 'CLASSIC_JOBS', 'system.access.audit', 'orders');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   asset_type,
# MAGIC   lifecycle_state,
# MAGIC   COUNT(*) AS asset_count,
# MAGIC   SUM(CASE WHEN has_owner THEN 1 ELSE 0 END) AS with_owner,
# MAGIC   SUM(CASE WHEN has_lineage THEN 1 ELSE 0 END) AS with_lineage,
# MAGIC   SUM(CASE WHEN has_access_policy THEN 1 ELSE 0 END) AS with_access_policy
# MAGIC FROM platform_assets_day25
# MAGIC GROUP BY asset_type, lifecycle_state
# MAGIC ORDER BY asset_type, lifecycle_state;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Published governed table/view assets have stronger evidence.
# MAGIC - Legacy and sandbox assets have missing governance evidence.
# MAGIC
# MAGIC Operational meaning: inventory is the bridge between catalog metadata and operations. Unknown owner, missing lineage, missing policy, or missing recovery plan should become a remediation item.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Add Operational Signals For Monitoring And Cost
# MAGIC
# MAGIC Purpose: model the monitoring surfaces you should inspect for jobs, SQL, lineage, audit, compute, and cost signals.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE platform_operational_signals_day25 (
# MAGIC   signal_id STRING,
# MAGIC   workload_name STRING,
# MAGIC   workload_type STRING,
# MAGIC   compute_choice STRING,
# MAGIC   monitoring_surface STRING,
# MAGIC   recent_failure_count INT,
# MAGIC   queue_or_startup_wait_minutes DOUBLE,
# MAGIC   monthly_cost_index DOUBLE,
# MAGIC   owner_domain STRING,
# MAGIC   recommended_action STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO platform_operational_signals_day25 VALUES
# MAGIC   ('SIG-01', 'orders_daily_gold_job_day25', 'Lakeflow Job', 'SERVERLESS_JOBS', 'system.lakeflow.job_run_timeline', 0, 1.2, 3.4, 'finance', 'keep current compute and monitor freshness'),
# MAGIC   ('SIG-02', 'revenue_sql_warehouse_day25', 'SQL Warehouse', 'SERVERLESS_SQL_WAREHOUSE', 'system.query.history', 2, 7.5, 6.8, 'finance', 'review queued queries and query profile before scaling'),
# MAGIC   ('SIG-03', 'legacy_backfill_day25', 'Notebook Repair', 'CLASSIC_ALL_PURPOSE', 'cluster event log and system.compute.clusters', 3, 12.0, 9.7, 'unknown', 'move to job compute with owner and budget tag'),
# MAGIC   ('SIG-04', 'policy_audit_review_day25', 'Audit Review', 'SERVERLESS_SQL_WAREHOUSE', 'system.access.audit', 0, 0.5, 1.1, 'security', 'restrict audit access through dynamic views'),
# MAGIC   ('SIG-05', 'lineage_impact_review_day25', 'Lineage Review', 'SERVERLESS_SQL_WAREHOUSE', 'system.access.table_lineage', 0, 0.7, 1.4, 'platform', 'keep lineage evidence attached to change requests');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   workload_name,
# MAGIC   workload_type,
# MAGIC   compute_choice,
# MAGIC   monitoring_surface,
# MAGIC   recent_failure_count,
# MAGIC   queue_or_startup_wait_minutes,
# MAGIC   monthly_cost_index,
# MAGIC   recommended_action
# MAGIC FROM platform_operational_signals_day25
# MAGIC ORDER BY recent_failure_count DESC, monthly_cost_index DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - The legacy repair workload stands out as high risk.
# MAGIC - The SQL warehouse workload needs performance/cost review.
# MAGIC
# MAGIC Operational meaning: governance readiness includes operability. A data product is not ready if nobody owns its cost, failures, query latency, or monitoring surface.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Score Asset Readiness With PySpark
# MAGIC
# MAGIC Purpose: classify each asset as ready, conditional, or blocked based on critical evidence gaps.

# COMMAND ----------

from pyspark.sql import functions as F

assets_df = spark.table("de_learning.platform_assets_day25")
controls_df = spark.table("de_learning.uc_foundation_controls_day25")

protected_classes = ["CONTAINS_PII", "RESTRICTED"]

scored_assets_df = (
    assets_df
    .withColumn("policy_required", F.col("data_classification").isin(protected_classes))
    .withColumn("owner_points", F.when(F.col("has_owner"), F.lit(2)).otherwise(F.lit(0)))
    .withColumn("usage_points", F.when(F.col("has_usage_chain"), F.lit(1)).otherwise(F.lit(0)))
    .withColumn("lineage_points", F.when(F.col("has_lineage"), F.lit(2)).otherwise(F.lit(0)))
    .withColumn("quality_points", F.when(F.col("has_quality_gate"), F.lit(2)).otherwise(F.lit(0)))
    .withColumn(
        "access_policy_points",
        F.when(F.col("policy_required") & F.col("has_access_policy"), F.lit(2))
        .when(~F.col("policy_required"), F.lit(1))
        .otherwise(F.lit(0))
    )
    .withColumn("recovery_points", F.when(F.col("has_recovery_plan"), F.lit(2)).otherwise(F.lit(0)))
    .withColumn(
        "monitoring_points",
        F.when(
            F.col("monitoring_surface").isNotNull() & (F.col("monitoring_surface") != F.lit("NONE")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn("cost_points", F.when(F.col("cost_owner").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
    .withColumn(
        "readiness_points",
        F.col("owner_points")
        + F.col("usage_points")
        + F.col("lineage_points")
        + F.col("quality_points")
        + F.col("access_policy_points")
        + F.col("recovery_points")
        + F.col("monitoring_points")
        + F.col("cost_points")
    )
    .withColumn("readiness_score_percent", F.round(F.col("readiness_points") / F.lit(13.0) * F.lit(100.0), 1))
    .withColumn("missing_owner_reason", F.when(~F.col("has_owner"), F.lit("missing owner")))
    .withColumn("missing_usage_reason", F.when(~F.col("has_usage_chain"), F.lit("missing parent usage/grant evidence")))
    .withColumn("missing_lineage_reason", F.when(~F.col("has_lineage"), F.lit("missing lineage evidence")))
    .withColumn("missing_quality_reason", F.when(~F.col("has_quality_gate"), F.lit("missing quality gate")))
    .withColumn(
        "missing_policy_reason",
        F.when(F.col("policy_required") & ~F.col("has_access_policy"), F.lit("protected data without access policy"))
    )
    .withColumn("missing_recovery_reason", F.when(~F.col("has_recovery_plan"), F.lit("missing recovery plan")))
    .withColumn("missing_cost_reason", F.when(F.col("cost_owner").isNull(), F.lit("missing cost owner")))
    .withColumn(
        "critical_gap_count",
        F.when(~F.col("has_owner"), F.lit(1)).otherwise(F.lit(0))
        + F.when(~F.col("has_lineage"), F.lit(1)).otherwise(F.lit(0))
        + F.when(F.col("policy_required") & ~F.col("has_access_policy"), F.lit(1)).otherwise(F.lit(0))
        + F.when(~F.col("has_recovery_plan"), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "readiness_gaps",
        F.concat_ws(
            "; ",
            F.col("missing_owner_reason"),
            F.col("missing_usage_reason"),
            F.col("missing_lineage_reason"),
            F.col("missing_quality_reason"),
            F.col("missing_policy_reason"),
            F.col("missing_recovery_reason"),
            F.col("missing_cost_reason")
        )
    )
    .withColumn(
        "readiness_decision",
        F.when(F.col("critical_gap_count") > 0, F.lit("BLOCKED_REMEDIATE_CRITICAL_GAPS"))
        .when(F.col("readiness_score_percent") >= 85, F.lit("READY"))
        .when(F.col("readiness_score_percent") >= 70, F.lit("CONDITIONAL_READY"))
        .otherwise(F.lit("NOT_READY"))
    )
    .select(
        "asset_name",
        "asset_type",
        "owner_domain",
        "data_classification",
        "lifecycle_state",
        "compute_binding",
        "monitoring_surface",
        "readiness_points",
        "readiness_score_percent",
        "critical_gap_count",
        "readiness_decision",
        "readiness_gaps"
    )
)

control_coverage_df = (
    controls_df
    .withColumn("covered_day_count", F.size(F.col("days_covered")))
    .withColumn(
        "coverage_decision",
        F.when(F.col("covered_day_count") >= 2, F.lit("REINFORCED"))
        .otherwise(F.lit("COVERED_ONCE_NEEDS_DRILL"))
    )
    .select(
        "control_id",
        "control_area",
        "associate_objective",
        "professional_extension",
        "required_artifact",
        "passing_signal",
        "covered_day_count",
        "coverage_decision"
    )
)

scored_assets_df.createOrReplaceTempView("platform_asset_readiness_view_day25")
control_coverage_df.createOrReplaceTempView("segment_control_coverage_view_day25")

display(scored_assets_df.orderBy("readiness_score_percent", "asset_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `assets_df` is the asset inventory: tables, views, volumes, jobs, warehouses, and recovery assets.
# MAGIC - `controls_df` is the Day 16-24 control matrix.
# MAGIC - SQL equivalent: `SELECT asset_name, CASE WHEN has_owner AND has_lineage ... THEN 'READY' ELSE 'BLOCKED' END FROM platform_assets_day25`.
# MAGIC - `F.col(...)` references a DataFrame column inside scoring expressions.
# MAGIC - `withColumn(...)` adds point scores, gap reasons, and final readiness decisions.
# MAGIC - `isin(...)` is SQL `IN (...)`; it marks PII/restricted assets as requiring policy evidence.
# MAGIC - `concat_ws(...)` builds a readable gap list while skipping null reason columns.
# MAGIC - `createOrReplaceTempView(...)` lets SQL cells persist the PySpark scorecard.
# MAGIC - PySpark is lazy until `display(...)` materializes the scorecard.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE readiness_scorecard_day25
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM platform_asset_readiness_view_day25;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE segment_control_coverage_day25
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM segment_control_coverage_view_day25;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   readiness_decision,
# MAGIC   COUNT(*) AS asset_count,
# MAGIC   ROUND(AVG(readiness_score_percent), 1) AS avg_score
# MAGIC FROM readiness_scorecard_day25
# MAGIC GROUP BY readiness_decision
# MAGIC ORDER BY avg_score DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   asset_name,
# MAGIC   asset_type,
# MAGIC   readiness_score_percent,
# MAGIC   critical_gap_count,
# MAGIC   readiness_decision,
# MAGIC   readiness_gaps
# MAGIC FROM readiness_scorecard_day25
# MAGIC WHERE readiness_decision <> 'READY'
# MAGIC ORDER BY readiness_score_percent ASC, asset_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Governed published assets are ready or near-ready.
# MAGIC - Legacy/sandbox/all-purpose-compute assets are blocked or conditional.
# MAGIC
# MAGIC Operational meaning: readiness scoring forces you to translate platform vocabulary into operating evidence. Protected data without policy, owner, lineage, or recovery evidence is not production-ready.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Store Real Databricks Evidence Query Templates
# MAGIC
# MAGIC Purpose: keep the production query surfaces beside the simulated scorecard so the lab maps to a real workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE system_query_templates_day25 (
# MAGIC   template_id STRING,
# MAGIC   evidence_surface STRING,
# MAGIC   query_template STRING,
# MAGIC   operational_question STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO system_query_templates_day25 VALUES
# MAGIC   (
# MAGIC     'SYS-01',
# MAGIC     'information_schema.tables',
# MAGIC     'SELECT table_catalog, table_schema, table_name, table_type FROM system.information_schema.tables WHERE table_schema = ''de_learning''',
# MAGIC     'Which governed objects exist and are visible to this principal?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-02',
# MAGIC     'SHOW GRANTS',
# MAGIC     'SHOW GRANTS ON TABLE catalog.schema.table_name',
# MAGIC     'Which principals have object-level privileges?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-03',
# MAGIC     'information_schema.table_privileges',
# MAGIC     'SELECT grantee, privilege_type FROM system.information_schema.table_privileges WHERE table_schema = ''de_learning''',
# MAGIC     'Which table privileges can be queried as metadata?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-04',
# MAGIC     'system.access.table_lineage',
# MAGIC     'SELECT source_table_full_name, target_table_full_name, entity_type, entity_id FROM system.access.table_lineage WHERE event_date >= current_date() - INTERVAL 30 DAY',
# MAGIC     'Which upstream and downstream table dependencies affect a change?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-05',
# MAGIC     'system.access.column_lineage',
# MAGIC     'SELECT source_table_full_name, source_column_name, target_table_full_name, target_column_name FROM system.access.column_lineage WHERE event_date >= current_date() - INTERVAL 30 DAY',
# MAGIC     'Where do sensitive columns flow?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-06',
# MAGIC     'system.query.history',
# MAGIC     'SELECT statement_id, execution_status, compute.type, total_duration_ms, waiting_at_capacity_duration_ms FROM system.query.history WHERE start_time >= current_timestamp() - INTERVAL 7 DAY',
# MAGIC     'Which SQL queries are failing, slow, queued, or using the wrong compute?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-07',
# MAGIC     'system.lakeflow.job_run_timeline',
# MAGIC     'SELECT job_id, run_id, result_state, period_start_time, period_end_time FROM system.lakeflow.job_run_timeline WHERE period_start_time >= current_timestamp() - INTERVAL 7 DAY',
# MAGIC     'Which Lakeflow Job runs failed or changed behavior recently?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-08',
# MAGIC     'system.access.audit',
# MAGIC     'SELECT event_time, service_name, action_name, user_identity.email FROM system.access.audit WHERE event_time >= current_timestamp() - INTERVAL 7 DAY',
# MAGIC     'Which access, governance, lineage, or admin events support an investigation?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-09',
# MAGIC     'system.compute.clusters',
# MAGIC     'SELECT workspace_id, cluster_id, cluster_source, change_time FROM system.compute.clusters',
# MAGIC     'Which classic compute resources are being used by jobs or interactive workloads?'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SYS-10',
# MAGIC     'system.billing.usage',
# MAGIC     'SELECT usage_metadata, sku_name, usage_quantity FROM system.billing.usage WHERE usage_date >= current_date() - INTERVAL 30 DAY',
# MAGIC     'Which assets or workloads are driving platform cost?'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT template_id, evidence_surface, operational_question
# MAGIC FROM system_query_templates_day25
# MAGIC ORDER BY template_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 10 query templates connect the lab to real Unity Catalog, lineage, jobs, query history, audit, compute, and billing surfaces.
# MAGIC
# MAGIC Operational meaning: do not export raw system-table data casually. Keep query access controlled and store compact decision evidence where possible.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Produce Segment Summary And Next Drills
# MAGIC
# MAGIC Purpose: convert the review into a compact readiness summary and the next work items for the ingestion/loading segment.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE segment_review_summary_day25
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   current_timestamp() AS reviewed_at,
# MAGIC   COUNT(*) AS asset_count,
# MAGIC   ROUND(AVG(readiness_score_percent), 1) AS avg_asset_readiness,
# MAGIC   SUM(CASE WHEN readiness_decision = 'READY' THEN 1 ELSE 0 END) AS ready_assets,
# MAGIC   SUM(CASE WHEN readiness_decision LIKE 'BLOCKED%' THEN 1 ELSE 0 END) AS blocked_assets,
# MAGIC   SUM(critical_gap_count) AS critical_gap_count
# MAGIC FROM readiness_scorecard_day25;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE segment_next_drills_day25 (
# MAGIC   drill_id STRING,
# MAGIC   planned_day STRING,
# MAGIC   drill_focus STRING,
# MAGIC   associate_objective STRING,
# MAGIC   professional_angle STRING,
# MAGIC   starting_artifact STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO segment_next_drills_day25 VALUES
# MAGIC   ('NEXT-01', 'Day 26', 'COPY INTO vs Auto Loader ingestion method decision', 'Data Ingestion and Loading', 'checkpoint, replay, cost, and rescue-data tradeoffs', 'raw file inventory and ingestion decision table'),
# MAGIC   ('NEXT-02', 'Day 27', 'COPY INTO idempotent batch load with audit evidence', 'Data Ingestion and Loading', 'file-level dedupe, validation, and rerun safety', 'batch source files and load audit table'),
# MAGIC   ('NEXT-03', 'Day 28', 'Auto Loader-style schema evolution and rescue data', 'Data Ingestion and Loading', 'schema drift recovery and quarantine operations', 'streaming-style file discovery and rescue table'),
# MAGIC   ('NEXT-04', 'Day 29', 'Nested JSON and semi-structured bronze ingestion', 'Data Ingestion and Loading', 'variant extraction, schema enforcement, and malformed payload triage', 'nested raw event table');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM segment_review_summary_day25;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM segment_next_drills_day25 ORDER BY drill_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - One segment summary row with average readiness and blocked-asset count.
# MAGIC - Four concrete drills that move into the ingestion/loading segment.
# MAGIC
# MAGIC Operational meaning: a review day should end with a work queue. The next segment starts from evidence gaps and exam-weighted objectives, not from random feature browsing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'base_rows' AS check_name, CAST(COUNT(*) AS STRING) AS actual_value, '6' AS expected_value
# MAGIC FROM orders_governance_base_day25
# MAGIC UNION ALL
# MAGIC SELECT 'public_view_rows', CAST(COUNT(*) AS STRING), '4'
# MAGIC FROM orders_governance_public_view_day25
# MAGIC UNION ALL
# MAGIC SELECT 'foundation_controls', CAST(COUNT(*) AS STRING), '12'
# MAGIC FROM uc_foundation_controls_day25
# MAGIC UNION ALL
# MAGIC SELECT 'platform_assets', CAST(COUNT(*) AS STRING), '8'
# MAGIC FROM platform_assets_day25
# MAGIC UNION ALL
# MAGIC SELECT 'query_templates', CAST(COUNT(*) AS STRING), '10'
# MAGIC FROM system_query_templates_day25
# MAGIC UNION ALL
# MAGIC SELECT 'next_drills', CAST(COUNT(*) AS STRING), '4'
# MAGIC FROM segment_next_drills_day25;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   control_id,
# MAGIC   control_area,
# MAGIC   coverage_decision
# MAGIC FROM segment_control_coverage_day25
# MAGIC ORDER BY coverage_decision DESC, control_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Validation counts match the expected values.
# MAGIC - Controls covered only once are visible as drill candidates.
# MAGIC
# MAGIC Operational meaning: this closes the Unity Catalog foundation segment and prepares the transition to ingestion/loading labs with evidence-backed priorities.
