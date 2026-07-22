# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 24 - Unity Catalog Lineage And Impact Analysis
# MAGIC
# MAGIC Goal: practice lineage-driven impact analysis before lifecycle changes: identify downstream tables, jobs, dashboards, sensitive-column flows, and approval blockers before dropping, replacing, renaming, or changing data objects.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Unity Catalog governance, table/view dependencies, troubleshooting, and metadata discovery.
# MAGIC - Professional stretch: production change approval, lineage system table reasoning, blast-radius classification, consumer notification, and audit evidence.
# MAGIC
# MAGIC This notebook creates a runnable lineage graph with Delta tables and views. In a Unity Catalog workspace, the SQL transformations can also emit real lineage into Catalog Explorer and `system.access.table_lineage` after system tables are enabled.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Build A Small Data Product Graph
# MAGIC
# MAGIC Purpose: create source, silver, gold, and dashboard-view objects whose dependencies are easy to inspect.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS orders_lineage_dashboard_view_day24;
# MAGIC DROP TABLE IF EXISTS lineage_impact_evidence_day24;
# MAGIC DROP TABLE IF EXISTS lineage_consumer_notifications_day24;
# MAGIC DROP TABLE IF EXISTS lineage_change_decisions_day24;
# MAGIC DROP TABLE IF EXISTS lineage_change_requests_day24;
# MAGIC DROP TABLE IF EXISTS lineage_consumer_assets_day24;
# MAGIC DROP TABLE IF EXISTS lineage_edges_day24;
# MAGIC DROP TABLE IF EXISTS orders_lineage_gold_day24;
# MAGIC DROP TABLE IF EXISTS orders_lineage_silver_day24;
# MAGIC DROP TABLE IF EXISTS orders_lineage_raw_day24;
# MAGIC DROP TABLE IF EXISTS lineage_query_templates_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_lineage_raw_day24
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'owner_domain' = 'orders',
# MAGIC   'data_classification' = 'contains_pii',
# MAGIC   'lifecycle_gate' = 'lineage-impact-required'
# MAGIC )
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (2401, 101, DATE'2026-07-20', CAST(250.00 AS DECIMAL(10,2)), 'completed', 'US', 'ada@example.com'),
# MAGIC   (2402, 102, DATE'2026-07-20', CAST(130.00 AS DECIMAL(10,2)), 'completed', 'US', 'grace@example.com'),
# MAGIC   (2403, 103, DATE'2026-07-21', CAST(400.00 AS DECIMAL(10,2)), 'completed', 'EU', 'katherine@example.com'),
# MAGIC   (2404, 104, DATE'2026-07-21', CAST(90.00 AS DECIMAL(10,2)), 'pending', 'APAC', 'margaret@example.com'),
# MAGIC   (2405, 105, DATE'2026-07-21', CAST(75.00 AS DECIMAL(10,2)), 'cancelled', 'US', 'dorothy@example.com')
# MAGIC AS t(order_id, customer_id, order_date, amount, status, region, customer_email);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_lineage_silver_day24
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'owner_domain' = 'orders',
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'consumer_contract' = 'orders-current-state-v1'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   upper(status) AS normalized_status,
# MAGIC   region,
# MAGIC   customer_email,
# MAGIC   current_timestamp() AS conformed_at
# MAGIC FROM orders_lineage_raw_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_lineage_gold_day24
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'owner_domain' = 'finance',
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'serving_surface' = 'dashboard'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(CASE WHEN normalized_status = 'COMPLETED' THEN amount ELSE 0 END) AS completed_revenue,
# MAGIC   SUM(amount) AS gross_amount
# MAGIC FROM orders_lineage_silver_day24
# MAGIC GROUP BY order_date, region;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW orders_lineage_dashboard_view_day24 AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   order_count,
# MAGIC   completed_revenue
# MAGIC FROM orders_lineage_gold_day24
# MAGIC WHERE region IN ('US', 'EU');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'raw' AS object_role, COUNT(*) AS row_count FROM orders_lineage_raw_day24
# MAGIC UNION ALL
# MAGIC SELECT 'silver', COUNT(*) FROM orders_lineage_silver_day24
# MAGIC UNION ALL
# MAGIC SELECT 'gold', COUNT(*) FROM orders_lineage_gold_day24
# MAGIC UNION ALL
# MAGIC SELECT 'dashboard_view', COUNT(*) FROM orders_lineage_dashboard_view_day24;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Raw table: 5 rows.
# MAGIC - Silver table: 5 rows.
# MAGIC - Gold table: one row per `order_date, region`.
# MAGIC - Dashboard view: only US and EU gold rows.
# MAGIC
# MAGIC Operational meaning: impact analysis starts with the dependency graph. A source table change can affect tables, views, jobs, dashboards, and sensitive-column flows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Inspect Metadata And Security Surfaces
# MAGIC
# MAGIC Purpose: use table metadata and grants as the first dependency context.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_lineage_raw_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_lineage_gold_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES orders_lineage_raw_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GRANTS ON TABLE orders_lineage_gold_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   table_type
# MAGIC FROM information_schema.tables
# MAGIC WHERE table_schema = 'de_learning'
# MAGIC   AND table_name LIKE '%day24%'
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Table details and properties show lifecycle gate and classification metadata.
# MAGIC - Grants show the security surface for the gold table.
# MAGIC - Information schema lists Day 24 relations visible to the current principal.
# MAGIC
# MAGIC Operational meaning: lineage is permission-aware. If a principal cannot browse or select an object, impact analysis may be incomplete or masked.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Record Observed Lineage Edges
# MAGIC
# MAGIC Purpose: model the lineage edges that would normally come from Catalog Explorer or `system.access.table_lineage`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_edges_day24 (
# MAGIC   edge_id STRING,
# MAGIC   source_asset STRING,
# MAGIC   source_column STRING,
# MAGIC   target_asset STRING,
# MAGIC   target_column STRING,
# MAGIC   target_type STRING,
# MAGIC   entity_type STRING,
# MAGIC   entity_name STRING,
# MAGIC   consumer_owner STRING,
# MAGIC   criticality STRING,
# MAGIC   direct_access BOOLEAN,
# MAGIC   sensitive_flow BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lineage_edges_day24 VALUES
# MAGIC   ('le-001', 'orders_lineage_raw_day24', 'order_id', 'orders_lineage_silver_day24', 'order_id', 'TABLE', 'NOTEBOOK', 'day_24_silver_builder', 'data-platform@example.com', 'HIGH', true, false),
# MAGIC   ('le-002', 'orders_lineage_raw_day24', 'amount', 'orders_lineage_silver_day24', 'amount', 'TABLE', 'NOTEBOOK', 'day_24_silver_builder', 'data-platform@example.com', 'HIGH', true, false),
# MAGIC   ('le-003', 'orders_lineage_raw_day24', 'status', 'orders_lineage_silver_day24', 'normalized_status', 'TABLE', 'NOTEBOOK', 'day_24_silver_builder', 'data-platform@example.com', 'HIGH', true, false),
# MAGIC   ('le-004', 'orders_lineage_raw_day24', 'customer_email', 'orders_lineage_silver_day24', 'customer_email', 'TABLE', 'NOTEBOOK', 'day_24_silver_builder', 'data-platform@example.com', 'HIGH', true, true),
# MAGIC   ('le-005', 'orders_lineage_silver_day24', 'amount', 'orders_lineage_gold_day24', 'completed_revenue', 'TABLE', 'JOB', 'orders_daily_gold_job_day24', 'finance-analytics@example.com', 'HIGH', true, false),
# MAGIC   ('le-006', 'orders_lineage_silver_day24', 'order_date', 'orders_lineage_gold_day24', 'order_date', 'TABLE', 'JOB', 'orders_daily_gold_job_day24', 'finance-analytics@example.com', 'HIGH', true, false),
# MAGIC   ('le-007', 'orders_lineage_silver_day24', 'customer_email', 'ml_churn_feature_job_day24', 'customer_email', 'JOB', 'JOB', 'ml_churn_feature_job_day24', 'ml-platform@example.com', 'MEDIUM', true, true),
# MAGIC   ('le-008', 'orders_lineage_gold_day24', 'completed_revenue', 'orders_lineage_dashboard_view_day24', 'completed_revenue', 'VIEW', 'VIEW', 'orders_lineage_dashboard_view_day24', 'analytics-readers@example.com', 'HIGH', true, false),
# MAGIC   ('le-009', 'orders_lineage_gold_day24', 'completed_revenue', 'finance_revenue_dashboard_day24', 'completed_revenue', 'DASHBOARD', 'DASHBOARD', 'finance_revenue_dashboard_day24', 'finance-analytics@example.com', 'HIGH', true, false),
# MAGIC   ('le-010', 'orders_lineage_gold_day24', '*', 'executive_revenue_dashboard_day24', '*', 'DASHBOARD', 'DASHBOARD', 'executive_revenue_dashboard_day24', 'executive-office@example.com', 'HIGH', true, false),
# MAGIC   ('le-011', 'orders_lineage_raw_day24', '*', 'order_quality_query_day24', '*', 'QUERY', 'DBSQL_QUERY', 'order_quality_query_day24', 'data-quality@example.com', 'LOW', true, false);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lineage_edges_day24 ORDER BY edge_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 11 lineage edges.
# MAGIC - Edges cover table, view, dashboard, job, and query consumers.
# MAGIC - `customer_email` edges mark sensitive data flow.
# MAGIC
# MAGIC Operational meaning: a raw dependency count is not enough. You need target type, owner, criticality, direct/indirect access, and sensitive-flow flags.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Register Consumers And Change Requests
# MAGIC
# MAGIC Purpose: make consumer ownership and proposed lifecycle changes explicit.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_consumer_assets_day24 (
# MAGIC   asset_name STRING,
# MAGIC   asset_type STRING,
# MAGIC   owner_principal STRING,
# MAGIC   criticality STRING,
# MAGIC   active_users INT,
# MAGIC   approval_required BOOLEAN,
# MAGIC   notification_channel STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lineage_consumer_assets_day24 VALUES
# MAGIC   ('orders_lineage_silver_day24', 'TABLE', 'data-platform@example.com', 'HIGH', 3, true, '#data-platform'),
# MAGIC   ('orders_lineage_gold_day24', 'TABLE', 'finance-analytics@example.com', 'HIGH', 8, true, '#finance-data'),
# MAGIC   ('orders_lineage_dashboard_view_day24', 'VIEW', 'analytics-readers@example.com', 'HIGH', 18, true, '#analytics-consumers'),
# MAGIC   ('finance_revenue_dashboard_day24', 'DASHBOARD', 'finance-analytics@example.com', 'HIGH', 25, true, '#finance-data'),
# MAGIC   ('executive_revenue_dashboard_day24', 'DASHBOARD', 'executive-office@example.com', 'HIGH', 12, true, '#exec-reporting'),
# MAGIC   ('ml_churn_feature_job_day24', 'JOB', 'ml-platform@example.com', 'MEDIUM', 4, true, '#ml-platform'),
# MAGIC   ('order_quality_query_day24', 'QUERY', 'data-quality@example.com', 'LOW', 2, false, '#data-quality');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_change_requests_day24 (
# MAGIC   change_id STRING,
# MAGIC   change_action STRING,
# MAGIC   object_name STRING,
# MAGIC   changed_column STRING,
# MAGIC   requested_by STRING,
# MAGIC   reason STRING,
# MAGIC   desired_change_date DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lineage_change_requests_day24 VALUES
# MAGIC   ('cr-001', 'DROP_TABLE', 'orders_lineage_raw_day24', NULL, 'platform-cleanup@example.com', 'remove raw table after migration', DATE'2026-07-25'),
# MAGIC   ('cr-002', 'REPLACE_TABLE', 'orders_lineage_gold_day24', NULL, 'finance-analytics@example.com', 'change gold aggregate grain', DATE'2026-07-26'),
# MAGIC   ('cr-003', 'DROP_TABLE', 'orders_unused_experiment_day24', NULL, 'sandbox-owner@example.com', 'cleanup unused experiment', DATE'2026-07-24'),
# MAGIC   ('cr-004', 'DROP_COLUMN', 'orders_lineage_raw_day24', 'customer_email', 'privacy-office@example.com', 'remove raw email column', DATE'2026-07-27'),
# MAGIC   ('cr-005', 'RENAME_TABLE', 'orders_lineage_silver_day24', NULL, 'data-platform@example.com', 'rename silver table to align naming standard', DATE'2026-07-28');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lineage_change_requests_day24 ORDER BY change_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 7 consumer assets.
# MAGIC - 5 change requests covering drop, replace, column drop, and rename.
# MAGIC
# MAGIC Operational meaning: every lifecycle request should name the object, action, owner, requested date, and reason before impact analysis starts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Classify Transitive Impact With PySpark
# MAGIC
# MAGIC Purpose: compute downstream blast radius across multiple hops and decide whether each request is approved, blocked, or requires consumer signoff.

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F

edges_df = spark.table("de_learning.lineage_edges_day24")
requests_df = spark.table("de_learning.lineage_change_requests_day24")

frontier_df = (
    requests_df
    .select(
        "change_id",
        "change_action",
        "changed_column",
        F.col("object_name").alias("current_asset"),
        F.col("object_name").alias("lineage_path"),
        F.lit(0).alias("depth")
    )
)

impact_frames = []

for depth in range(1, 4):
    next_df = (
        frontier_df.alias("f")
        .join(edges_df.alias("e"), F.col("f.current_asset") == F.col("e.source_asset"), "inner")
        .where(
            F.col("f.changed_column").isNull()
            | (F.col("f.depth") > 0)
            | (F.col("e.source_column") == F.lit("*"))
            | (F.col("e.source_column") == F.col("f.changed_column"))
        )
        .select(
            F.col("f.change_id"),
            F.col("f.change_action"),
            F.col("f.changed_column"),
            F.lit(depth).alias("depth"),
            F.col("e.source_asset"),
            F.col("e.source_column"),
            F.col("e.target_asset"),
            F.col("e.target_column"),
            F.col("e.target_type"),
            F.col("e.entity_type"),
            F.col("e.entity_name"),
            F.col("e.consumer_owner"),
            F.col("e.criticality"),
            F.col("e.direct_access"),
            F.col("e.sensitive_flow"),
            F.concat_ws(" -> ", F.col("f.lineage_path"), F.col("e.target_asset")).alias("lineage_path")
        )
    )
    impact_frames.append(next_df)
    frontier_df = (
        next_df
        .select(
            "change_id",
            "change_action",
            "changed_column",
            F.col("target_asset").alias("current_asset"),
            "lineage_path",
            "depth"
        )
    )

impacts_df = (
    reduce(lambda left, right: left.unionByName(right), impact_frames)
    .dropDuplicates(["change_id", "target_asset", "target_column", "entity_type"])
    .withColumn(
        "criticality_score",
        F.when(F.col("criticality") == "HIGH", F.lit(3))
        .when(F.col("criticality") == "MEDIUM", F.lit(2))
        .when(F.col("criticality") == "LOW", F.lit(1))
        .otherwise(F.lit(0))
    )
)

summary_df = (
    impacts_df
    .groupBy("change_id")
    .agg(
        F.countDistinct("target_asset").alias("dependency_count"),
        F.sum(F.when(F.col("target_type") == "DASHBOARD", 1).otherwise(0)).alias("dashboard_dependencies"),
        F.sum(F.when(F.col("target_type") == "JOB", 1).otherwise(0)).alias("job_dependencies"),
        F.sum(F.when(F.col("sensitive_flow"), 1).otherwise(0)).alias("sensitive_flow_dependencies"),
        F.max("criticality_score").alias("max_criticality_score"),
        F.max("lineage_path").alias("sample_lineage_path")
    )
)

decisions_df = (
    requests_df
    .join(summary_df, "change_id", "left")
    .fillna({
        "dependency_count": 0,
        "dashboard_dependencies": 0,
        "job_dependencies": 0,
        "sensitive_flow_dependencies": 0,
        "max_criticality_score": 0,
        "sample_lineage_path": "no downstream lineage observed"
    })
    .withColumn(
        "blast_radius",
        F.when(F.col("max_criticality_score") >= 3, F.lit("HIGH"))
        .when(F.col("max_criticality_score") == 2, F.lit("MEDIUM"))
        .when(F.col("dependency_count") > 0, F.lit("LOW"))
        .otherwise(F.lit("NONE"))
    )
    .withColumn(
        "change_decision",
        F.when(F.col("dependency_count") == 0, F.lit("APPROVE_NO_LINEAGE_DEPENDENCIES"))
        .when(
            (F.col("change_action") == "DROP_COLUMN") & (F.col("sensitive_flow_dependencies") > 0),
            F.lit("BLOCK_PRIVACY_AND_CONSUMER_REDESIGN")
        )
        .when(
            F.col("change_action").isin("DROP_TABLE", "REPLACE_TABLE", "RENAME_TABLE")
            & (F.col("max_criticality_score") >= 3),
            F.lit("BLOCK_HIGH_CRITICALITY_CONSUMER_APPROVAL")
        )
        .when(F.col("dashboard_dependencies") > 0, F.lit("APPROVE_AFTER_DASHBOARD_OWNER_SIGNOFF"))
        .otherwise(F.lit("APPROVE_WITH_CONSUMER_NOTIFICATION"))
    )
    .withColumn(
        "evidence_required",
        F.when(
            F.col("change_decision").like("BLOCK%"),
            F.lit("Lineage graph, impacted owners, owner approval, rollback plan, and scheduled cutover")
        )
        .when(
            F.col("change_decision") == "APPROVE_NO_LINEAGE_DEPENDENCIES",
            F.lit("Information schema check, lineage query result, and owner approval")
        )
        .otherwise(F.lit("Consumer notification, validation query, and post-change monitoring"))
    )
    .select(
        "change_id",
        "change_action",
        "object_name",
        "changed_column",
        "dependency_count",
        "dashboard_dependencies",
        "job_dependencies",
        "sensitive_flow_dependencies",
        "blast_radius",
        "change_decision",
        "sample_lineage_path",
        "evidence_required"
    )
)

impacts_df.createOrReplaceTempView("lineage_impacts_view_day24")
decisions_df.createOrReplaceTempView("lineage_change_decisions_view_day24")
display(decisions_df.orderBy("change_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `edges_df` represents lineage edges: source object/column to downstream object/column.
# MAGIC - `requests_df` represents proposed lifecycle changes.
# MAGIC - The loop expands downstream dependencies up to 3 hops, similar to repeatedly joining a lineage table to itself.
# MAGIC - `F.col(...)` references a DataFrame column.
# MAGIC - `withColumn(...)` adds impact fields such as `blast_radius` and `change_decision`.
# MAGIC - `groupBy(...).agg(...)` is SQL `GROUP BY` plus aggregates.
# MAGIC - `createOrReplaceTempView(...)` lets the next SQL cells query the PySpark results.
# MAGIC - PySpark is lazy until `display(...)` runs.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   change_id,
# MAGIC   COUNT(DISTINCT target_asset) AS dependency_count,
# MAGIC   CASE
# MAGIC     WHEN COUNT(DISTINCT target_asset) = 0 THEN 'APPROVE_NO_LINEAGE_DEPENDENCIES'
# MAGIC     WHEN MAX(criticality) = 'HIGH' THEN 'BLOCK_HIGH_CRITICALITY_CONSUMER_APPROVAL'
# MAGIC     ELSE 'APPROVE_WITH_CONSUMER_NOTIFICATION'
# MAGIC   END AS change_decision
# MAGIC FROM lineage_change_requests_day24 r
# MAGIC LEFT JOIN lineage_edges_day24 e
# MAGIC   ON r.object_name = e.source_asset
# MAGIC GROUP BY r.change_id;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_change_decisions_day24
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM lineage_change_decisions_view_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT change_id, change_action, object_name, changed_column, dependency_count, blast_radius, change_decision
# MAGIC FROM lineage_change_decisions_day24
# MAGIC ORDER BY change_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `cr-001` is blocked because the raw table feeds high-criticality downstream objects.
# MAGIC - `cr-002` is blocked because the gold table feeds dashboards.
# MAGIC - `cr-003` is approved because no downstream lineage is observed.
# MAGIC - `cr-004` is blocked because `customer_email` flows to sensitive consumers.
# MAGIC - `cr-005` is blocked because renaming silver affects high-criticality downstream assets.
# MAGIC
# MAGIC Operational meaning: a lifecycle change should be blocked when lineage shows high-criticality consumers, sensitive flows, or dashboard/job dependencies that have not approved the cutover.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Build A Consumer Notification Plan
# MAGIC
# MAGIC Purpose: turn impacted lineage edges into owner-specific notification tasks.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_consumer_notifications_day24
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT DISTINCT
# MAGIC   i.change_id,
# MAGIC   d.change_decision,
# MAGIC   i.target_asset,
# MAGIC   COALESCE(c.asset_type, i.target_type) AS asset_type,
# MAGIC   COALESCE(c.owner_principal, i.consumer_owner) AS owner_principal,
# MAGIC   COALESCE(c.notification_channel, '#unknown-owner') AS notification_channel,
# MAGIC   r.desired_change_date,
# MAGIC   CASE
# MAGIC     WHEN d.change_decision LIKE 'BLOCK%' THEN 'OWNER_APPROVAL_REQUIRED'
# MAGIC     WHEN c.approval_required THEN 'OWNER_SIGNOFF_REQUIRED'
# MAGIC     ELSE 'NOTIFY_ONLY'
# MAGIC   END AS notification_action
# MAGIC FROM lineage_impacts_view_day24 i
# MAGIC JOIN lineage_change_requests_day24 r
# MAGIC   ON i.change_id = r.change_id
# MAGIC JOIN lineage_change_decisions_day24 d
# MAGIC   ON i.change_id = d.change_id
# MAGIC LEFT JOIN lineage_consumer_assets_day24 c
# MAGIC   ON i.target_asset = c.asset_name
# MAGIC WHERE d.change_decision <> 'APPROVE_NO_LINEAGE_DEPENDENCIES';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT change_id, target_asset, asset_type, owner_principal, notification_channel, notification_action
# MAGIC FROM lineage_consumer_notifications_day24
# MAGIC ORDER BY change_id, target_asset;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Blocked or approval-required changes generate notification tasks.
# MAGIC - Notifications are grouped by downstream target and owner.
# MAGIC
# MAGIC Operational meaning: lineage impact analysis should produce action. The output is not just a graph; it is a notification and approval queue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Store System-Table Query Templates
# MAGIC
# MAGIC Purpose: keep the real Databricks query shapes next to the simulated lab data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_query_templates_day24
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('table_lineage_downstream', 'system.access.table_lineage', 'SELECT source_table_full_name, target_table_full_name, entity_type, entity_id, event_time FROM system.access.table_lineage WHERE source_table_full_name = ''main.de_learning.orders_lineage_raw_day24'''),
# MAGIC   ('column_lineage_sensitive_flow', 'system.access.column_lineage', 'SELECT source_table_full_name, source_column_name, target_table_full_name, target_column_name, event_time FROM system.access.column_lineage WHERE source_column_name = ''customer_email'''),
# MAGIC   ('visible_relations', 'information_schema.tables', 'SELECT table_catalog, table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = ''de_learning'''),
# MAGIC   ('table_privilege_evidence', 'information_schema.table_privileges', 'SELECT grantee, privilege_type, table_name FROM information_schema.table_privileges WHERE table_schema = ''de_learning'''),
# MAGIC   ('object_grants', 'SHOW GRANTS', 'SHOW GRANTS ON TABLE de_learning.orders_lineage_gold_day24'),
# MAGIC   ('audit_events', 'system.access.audit', 'SELECT event_time, service_name, action_name, user_identity FROM system.access.audit WHERE request_params.table_name LIKE ''%orders_lineage%''')
# MAGIC AS t(template_name, source_surface, query_shape);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lineage_query_templates_day24 ORDER BY template_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 query templates covering table lineage, column lineage, information schema, table privileges, grants, and audit events.
# MAGIC
# MAGIC Operational meaning: in production, the simulated `lineage_edges_day24` table should be replaced by system lineage tables, Catalog Explorer, information schema, grants, and audit evidence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Persist Change Approval Evidence
# MAGIC
# MAGIC Purpose: close the loop with durable evidence for every proposed lifecycle request.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lineage_impact_evidence_day24
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.change_id,
# MAGIC   d.change_action,
# MAGIC   d.object_name,
# MAGIC   d.changed_column,
# MAGIC   d.dependency_count,
# MAGIC   d.dashboard_dependencies,
# MAGIC   d.job_dependencies,
# MAGIC   d.sensitive_flow_dependencies,
# MAGIC   d.blast_radius,
# MAGIC   d.change_decision,
# MAGIC   d.sample_lineage_path,
# MAGIC   current_user() AS evidence_recorded_by,
# MAGIC   current_timestamp() AS evidence_recorded_at
# MAGIC FROM lineage_change_decisions_day24 d;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lineage_impact_evidence_day24 ORDER BY change_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 5 evidence rows, one per change request.
# MAGIC - Blocked changes include dependency counts, blast radius, and sample path.
# MAGIC
# MAGIC Operational meaning: approval evidence becomes the artifact you attach to a production change request.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 9 - Final Checks
# MAGIC
# MAGIC Purpose: validate source objects, lineage graph, decisions, notifications, and evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'raw_rows' AS check_name, CAST(COUNT(*) AS STRING) AS observed_value, '5' AS expected_value
# MAGIC FROM orders_lineage_raw_day24
# MAGIC UNION ALL
# MAGIC SELECT 'silver_rows', CAST(COUNT(*) AS STRING), '5'
# MAGIC FROM orders_lineage_silver_day24
# MAGIC UNION ALL
# MAGIC SELECT 'lineage_edges', CAST(COUNT(*) AS STRING), '11'
# MAGIC FROM lineage_edges_day24
# MAGIC UNION ALL
# MAGIC SELECT 'change_requests', CAST(COUNT(*) AS STRING), '5'
# MAGIC FROM lineage_change_requests_day24
# MAGIC UNION ALL
# MAGIC SELECT 'change_decisions', CAST(COUNT(*) AS STRING), '5'
# MAGIC FROM lineage_change_decisions_day24
# MAGIC UNION ALL
# MAGIC SELECT 'blocked_changes', CAST(COUNT(*) AS STRING), '4'
# MAGIC FROM lineage_change_decisions_day24
# MAGIC WHERE change_decision LIKE 'BLOCK%'
# MAGIC UNION ALL
# MAGIC SELECT 'evidence_rows', CAST(COUNT(*) AS STRING), '5'
# MAGIC FROM lineage_impact_evidence_day24;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT change_decision, COUNT(*) AS decision_count
# MAGIC FROM lineage_change_decisions_day24
# MAGIC GROUP BY change_decision
# MAGIC ORDER BY decision_count DESC, change_decision;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Raw rows: 5.
# MAGIC - Silver rows: 5.
# MAGIC - Lineage edges: 11.
# MAGIC - Change requests: 5.
# MAGIC - Change decisions: 5.
# MAGIC - Blocked changes: 4.
# MAGIC - Evidence rows: 5.
# MAGIC
# MAGIC Operational meaning: the final state proves both the data-product graph and the governance decision path.
