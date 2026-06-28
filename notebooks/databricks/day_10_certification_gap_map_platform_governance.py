# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 10 - Certification Gap Map: Platform, Governance, Jobs, CI/CD
# MAGIC
# MAGIC Goal: bridge early hands-on gaps against the Databricks Data Engineer Associate objectives while adding Professional operational review habits.
# MAGIC
# MAGIC Lab flow:
# MAGIC
# MAGIC - Build a Days 1-9 coverage matrix against certification domains.
# MAGIC - Model Databricks object hierarchy and table lifecycle.
# MAGIC - Review least-privilege access decisions.
# MAGIC - Diagnose Lakeflow Jobs-style run history.
# MAGIC - Gate a deployment using CI/CD evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Build Certification Coverage Matrix
# MAGIC
# MAGIC Purpose: make the current learning coverage visible and find early certification gaps.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cert_coverage_days_1_9_day10 (
# MAGIC   learning_day INT,
# MAGIC   checkpoint STRING,
# MAGIC   associate_domain STRING,
# MAGIC   professional_extension STRING,
# MAGIC   hands_on_evidence STRING,
# MAGIC   coverage_strength STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cert_coverage_days_1_9_day10 VALUES
# MAGIC   (1, 'DataFrame vs Delta table vs SQL table', 'Databricks Intelligence Platform', 'Delta table operational semantics', 'Created DataFrame, Delta table, SQL table, DESCRIBE, DESCRIBE HISTORY', 'PARTIAL'),
# MAGIC   (2, 'Delta operability and time travel', 'Troubleshooting, Monitoring, and Optimization', 'Incident diagnosis with history/time travel', 'DESCRIBE HISTORY, VERSION AS OF, schema checks', 'STRONG'),
# MAGIC   (3, 'Contract-driven bronze ingestion', 'Data Ingestion and Loading', 'Replay-safe bronze ingestion and quarantine', 'Bronze table, validation, quarantine, idempotent MERGE', 'STRONG'),
# MAGIC   (4, 'Fact grain and metric correctness', 'Data Transformation and Modeling', 'Semantic correctness and grain checks', 'Event/current/day grain tables and PySpark latest-state derivation', 'STRONG'),
# MAGIC   (5, 'Publication state and ownership', 'Governance and Security', 'Ownership gates and publication control plane', 'Owner/version/quality gates and publication evidence', 'STRONG'),
# MAGIC   (6, 'Contract-driven medallion promotion', 'Data Transformation and Modeling', 'Promotion evidence across bronze/silver/gold', 'Bronze quarantine, silver promotion, gold reconciliation', 'STRONG'),
# MAGIC   (7, 'Replay and backfill safety', 'Data Ingestion and Loading', 'Backfill idempotency and input fingerprinting', 'Naive append failure, idempotent MERGE, run registry', 'STRONG'),
# MAGIC   (8, 'Delete semantics and tombstones', 'Data Ingestion and Loading', 'Delete correctness during replay', 'CDC deletes, tombstones, active silver, delete audit', 'STRONG'),
# MAGIC   (9, 'Schema evolution and compatibility', 'Data Transformation and Modeling', 'Schema compatibility evidence and publish gates', 'Nullable column, compatible/incompatible candidates, schema evidence', 'STRONG');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE cert_objectives_day10 (
# MAGIC   objective_id STRING,
# MAGIC   associate_domain STRING,
# MAGIC   expected_hands_on STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cert_objectives_day10 VALUES
# MAGIC   ('A1', 'Databricks Intelligence Platform', 'workspace, compute, catalog/schema/table hierarchy, Delta basics'),
# MAGIC   ('A2', 'Data Ingestion and Loading', 'COPY INTO, Auto Loader concepts, incremental loading, schema evolution'),
# MAGIC   ('A3', 'Data Transformation and Modeling', 'SQL/PySpark transformations, medallion modeling, views/tables'),
# MAGIC   ('A4', 'Working with Lakeflow Jobs', 'tasks, dependencies, schedules, retries, run history'),
# MAGIC   ('A5', 'Implementing CI/CD', 'Git folders, branches, Databricks CLI, Declarative Automation Bundles'),
# MAGIC   ('A6', 'Troubleshooting, Monitoring, and Optimization', 'run failures, metrics, history, performance symptoms'),
# MAGIC   ('A7', 'Governance and Security', 'Unity Catalog hierarchy, privileges, row filters, masking, ownership');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   o.objective_id,
# MAGIC   o.associate_domain,
# MAGIC   COUNT(c.learning_day) AS covered_days,
# MAGIC   CASE
# MAGIC     WHEN COUNT(c.learning_day) = 0 THEN 'GAP'
# MAGIC     WHEN SUM(CASE WHEN c.coverage_strength = 'STRONG' THEN 1 ELSE 0 END) = 0 THEN 'PARTIAL'
# MAGIC     ELSE 'COVERED'
# MAGIC   END AS coverage_status,
# MAGIC   o.expected_hands_on
# MAGIC FROM cert_objectives_day10 o
# MAGIC LEFT JOIN cert_coverage_days_1_9_day10 c
# MAGIC   ON o.associate_domain = c.associate_domain
# MAGIC GROUP BY o.objective_id, o.associate_domain, o.expected_hands_on
# MAGIC ORDER BY o.objective_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Strong coverage for ingestion, transformation/modeling, and several governance basics.
# MAGIC - Gaps or partial coverage for Lakeflow Jobs, CI/CD, and some platform/Unity Catalog terminology.
# MAGIC
# MAGIC Operational meaning: a certification plan should be evidence-driven. A coverage table prevents vague "I studied this" thinking.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Model Databricks Object Hierarchy And Lifecycle
# MAGIC
# MAGIC Purpose: practice catalog/schema/table/view/volume vocabulary and managed-vs-external lifecycle reasoning.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE platform_objects_day10 (
# MAGIC   object_name STRING,
# MAGIC   object_type STRING,
# MAGIC   parent_object STRING,
# MAGIC   storage_mode STRING,
# MAGIC   lifecycle_owner STRING,
# MAGIC   consumer_visible BOOLEAN,
# MAGIC   notes STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO platform_objects_day10 VALUES
# MAGIC   ('main', 'catalog', NULL, 'metastore-managed', 'platform-admin@example.com', false, 'top-level Unity Catalog namespace'),
# MAGIC   ('de_learning', 'schema', 'main', 'metastore-managed', 'data-platform@example.com', false, 'learning schema for this lab'),
# MAGIC   ('orders_bronze', 'table', 'de_learning', 'managed', 'ingestion-owner@example.com', false, 'raw or bronze records'),
# MAGIC   ('orders_silver', 'table', 'de_learning', 'managed', 'orders-owner@example.com', true, 'clean active order state'),
# MAGIC   ('orders_gold_daily', 'table', 'de_learning', 'managed', 'analytics-owner@example.com', true, 'BI-facing daily metrics'),
# MAGIC   ('orders_active_view', 'view', 'de_learning', 'logical', 'orders-owner@example.com', true, 'view that hides tombstones'),
# MAGIC   ('orders_raw_volume', 'volume', 'de_learning', 'external', 'ingestion-owner@example.com', false, 'landing area for raw files');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM platform_objects_day10 ORDER BY object_type, object_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   object_type,
# MAGIC   storage_mode,
# MAGIC   COUNT(*) AS object_count,
# MAGIC   SUM(CASE WHEN consumer_visible THEN 1 ELSE 0 END) AS consumer_visible_count
# MAGIC FROM platform_objects_day10
# MAGIC GROUP BY object_type, storage_mode
# MAGIC ORDER BY object_type, storage_mode;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Object hierarchy is explicit: catalog -> schema -> tables/views/volumes.
# MAGIC - Managed tables are consumer surfaces or internal tables; external volume is a landing zone.
# MAGIC
# MAGIC Operational meaning: platform questions often test whether you know where data and permissions live, not just how to query a table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Review Least-Privilege Access
# MAGIC
# MAGIC Purpose: detect privilege assignments that are too broad for the principal's role.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE privilege_assignments_day10 (
# MAGIC   principal STRING,
# MAGIC   principal_type STRING,
# MAGIC   object_name STRING,
# MAGIC   privilege STRING,
# MAGIC   business_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO privilege_assignments_day10 VALUES
# MAGIC   ('orders-ingestion-sp', 'service_principal', 'orders_raw_volume', 'READ FILES', 'ingest raw files'),
# MAGIC   ('orders-ingestion-sp', 'service_principal', 'orders_bronze', 'MODIFY', 'write bronze records'),
# MAGIC   ('orders-analyst-group', 'group', 'orders_gold_daily', 'SELECT', 'read daily order metrics'),
# MAGIC   ('orders-analyst-group', 'group', 'orders_silver', 'SELECT', 'debug metric inputs'),
# MAGIC   ('orders-analyst-group', 'group', 'orders_bronze', 'MODIFY', 'temporary investigation access'),
# MAGIC   ('platform-admins', 'group', 'de_learning', 'MANAGE', 'schema administration');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE privilege_policy_day10 (
# MAGIC   principal STRING,
# MAGIC   object_name STRING,
# MAGIC   allowed_privilege STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO privilege_policy_day10 VALUES
# MAGIC   ('orders-ingestion-sp', 'orders_raw_volume', 'READ FILES'),
# MAGIC   ('orders-ingestion-sp', 'orders_bronze', 'MODIFY'),
# MAGIC   ('orders-analyst-group', 'orders_gold_daily', 'SELECT'),
# MAGIC   ('orders-analyst-group', 'orders_silver', 'SELECT'),
# MAGIC   ('platform-admins', 'de_learning', 'MANAGE');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   a.principal,
# MAGIC   a.object_name,
# MAGIC   a.privilege,
# MAGIC   CASE WHEN p.allowed_privilege IS NULL THEN 'REVIEW_OVERPRIVILEGED' ELSE 'OK' END AS access_decision,
# MAGIC   a.business_reason
# MAGIC FROM privilege_assignments_day10 a
# MAGIC LEFT JOIN privilege_policy_day10 p
# MAGIC   ON a.principal = p.principal
# MAGIC  AND a.object_name = p.object_name
# MAGIC  AND a.privilege = p.allowed_privilege
# MAGIC ORDER BY access_decision DESC, a.principal, a.object_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders-analyst-group` having `MODIFY` on `orders_bronze` is flagged as `REVIEW_OVERPRIVILEGED`.
# MAGIC
# MAGIC Operational meaning: governance is not only grant syntax. It is proving that privileges match role and purpose.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Diagnose Lakeflow Jobs-Style Run History
# MAGIC
# MAGIC Purpose: practice job/task/run terminology, retry behavior, and failure triage.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeflow_job_runs_day10 (
# MAGIC   job_name STRING,
# MAGIC   task_name STRING,
# MAGIC   run_id STRING,
# MAGIC   attempt_number INT,
# MAGIC   status STRING,
# MAGIC   started_at TIMESTAMP,
# MAGIC   ended_at TIMESTAMP,
# MAGIC   error_class STRING,
# MAGIC   rows_written BIGINT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakeflow_job_runs_day10 VALUES
# MAGIC   ('orders_daily_pipeline', 'ingest_orders', 'run-100', 1, 'SUCCESS', TIMESTAMP '2026-06-27 05:00:00', TIMESTAMP '2026-06-27 05:03:00', NULL, 5000),
# MAGIC   ('orders_daily_pipeline', 'build_silver', 'run-100', 1, 'SUCCESS', TIMESTAMP '2026-06-27 05:03:10', TIMESTAMP '2026-06-27 05:07:00', NULL, 4988),
# MAGIC   ('orders_daily_pipeline', 'build_gold', 'run-100', 1, 'FAILED', TIMESTAMP '2026-06-27 05:07:10', TIMESTAMP '2026-06-27 05:08:30', 'SCHEMA_MISMATCH', 0),
# MAGIC   ('orders_daily_pipeline', 'build_gold', 'run-100', 2, 'SUCCESS', TIMESTAMP '2026-06-27 05:09:00', TIMESTAMP '2026-06-27 05:11:00', NULL, 3),
# MAGIC   ('orders_daily_pipeline', 'publish_metrics', 'run-100', 1, 'SUCCESS', TIMESTAMP '2026-06-27 05:11:10', TIMESTAMP '2026-06-27 05:12:00', NULL, 3);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_name,
# MAGIC   task_name,
# MAGIC   run_id,
# MAGIC   MAX(attempt_number) AS attempts,
# MAGIC   MAX(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS eventually_succeeded,
# MAGIC   COLLECT_SET(error_class) AS observed_error_classes
# MAGIC FROM lakeflow_job_runs_day10
# MAGIC GROUP BY job_name, task_name, run_id
# MAGIC ORDER BY task_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lakeflow_job_runs_day10
# MAGIC WHERE status = 'FAILED'
# MAGIC ORDER BY started_at;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `build_gold` failed once with `SCHEMA_MISMATCH`, then succeeded on attempt 2.
# MAGIC - The job eventually completed, but the failure still needs review.
# MAGIC
# MAGIC Operational meaning: Associate-level skill is reading job/task/run/retry state. Professional-level skill is deciding whether a retry hid a real schema incident.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Gate CI/CD Deployment Evidence With PySpark
# MAGIC
# MAGIC Purpose: model Git/Bundle/CLI validation evidence and block unsafe promotion.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE deployment_checks_day10 (
# MAGIC   deployment_id STRING,
# MAGIC   target_env STRING,
# MAGIC   git_branch STRING,
# MAGIC   bundle_validate_status STRING,
# MAGIC   unit_tests_status STRING,
# MAGIC   notebook_compile_status STRING,
# MAGIC   job_config_status STRING,
# MAGIC   requested_by STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO deployment_checks_day10 VALUES
# MAGIC   ('deploy-001', 'dev', 'feature/day10-cert-bridge', 'PASS', 'PASS', 'PASS', 'PASS', 'data-engineer@example.com'),
# MAGIC   ('deploy-002', 'prod', 'feature/day10-cert-bridge', 'PASS', 'PASS', 'PASS', 'PASS', 'data-engineer@example.com'),
# MAGIC   ('deploy-003', 'prod', 'main', 'PASS', 'PASS', 'PASS', 'PASS', 'release-manager@example.com'),
# MAGIC   ('deploy-004', 'prod', 'main', 'PASS', 'FAIL', 'PASS', 'PASS', 'release-manager@example.com');

# COMMAND ----------

from pyspark.sql import functions as F

deployment_df = spark.table("de_learning.deployment_checks_day10")

release_decision_df = (
    deployment_df
    .withColumn(
        "branch_ok",
        F.when(F.col("target_env") == "prod", F.col("git_branch") == F.lit("main"))
         .otherwise(F.lit(True)),
    )
    .withColumn(
        "all_checks_passed",
        (F.col("bundle_validate_status") == "PASS")
        & (F.col("unit_tests_status") == "PASS")
        & (F.col("notebook_compile_status") == "PASS")
        & (F.col("job_config_status") == "PASS"),
    )
    .withColumn(
        "deployment_decision",
        F.when(F.col("branch_ok") & F.col("all_checks_passed"), F.lit("READY_TO_DEPLOY"))
         .otherwise(F.lit("BLOCKED")),
    )
    .withColumn(
        "decision_reason",
        F.concat_ws(
            "; ",
            F.when(~F.col("branch_ok"), F.lit("prod deployments must come from main")),
            F.when(~F.col("all_checks_passed"), F.lit("one or more validation checks failed")),
        ),
    )
)

release_decision_df.createOrReplaceTempView("deployment_decisions_day10")
display(release_decision_df.orderBy("deployment_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC This DataFrame represents release-gate evidence for Databricks code deployment.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *,
# MAGIC   CASE
# MAGIC     WHEN target_env = 'prod' AND git_branch <> 'main' THEN 'BLOCKED'
# MAGIC     WHEN unit_tests_status <> 'PASS' THEN 'BLOCKED'
# MAGIC     ELSE 'READY_TO_DEPLOY'
# MAGIC   END AS deployment_decision
# MAGIC FROM deployment_checks_day10;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `withColumn(...)` adds derived review columns.
# MAGIC - Boolean expressions such as `F.col("git_branch") == F.lit("main")` become DataFrame columns.
# MAGIC - `F.concat_ws(...)` combines multiple failure reasons into one readable string.
# MAGIC - `createOrReplaceTempView(...)` lets SQL persist the PySpark decision output.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE deployment_decisions_delta_day10
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM deployment_decisions_day10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   deployment_id,
# MAGIC   target_env,
# MAGIC   git_branch,
# MAGIC   deployment_decision,
# MAGIC   decision_reason
# MAGIC FROM deployment_decisions_delta_day10
# MAGIC ORDER BY deployment_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `deploy-001` is ready for dev.
# MAGIC - `deploy-002` is blocked because prod deployment is not from `main`.
# MAGIC - `deploy-003` is ready for prod.
# MAGIC - `deploy-004` is blocked because tests failed.
# MAGIC
# MAGIC Operational meaning: CI/CD is a release safety system, not just a way to move notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Final Readiness Snapshot
# MAGIC
# MAGIC Purpose: summarize the bridge gaps that still need hands-on work.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   o.objective_id,
# MAGIC   o.associate_domain,
# MAGIC   COUNT(c.learning_day) AS covered_days,
# MAGIC   CASE
# MAGIC     WHEN COUNT(c.learning_day) = 0 THEN 'NEEDS_BRIDGE_LAB'
# MAGIC     WHEN SUM(CASE WHEN c.coverage_strength = 'STRONG' THEN 1 ELSE 0 END) = 0 THEN 'NEEDS_MORE_HANDS_ON'
# MAGIC     ELSE 'TRACKED'
# MAGIC   END AS next_action
# MAGIC FROM cert_objectives_day10 o
# MAGIC LEFT JOIN cert_coverage_days_1_9_day10 c
# MAGIC   ON o.associate_domain = c.associate_domain
# MAGIC GROUP BY o.objective_id, o.associate_domain
# MAGIC ORDER BY o.objective_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'overprivileged_access' AS issue_type,
# MAGIC   COUNT(*) AS issue_count
# MAGIC FROM (
# MAGIC   SELECT a.*
# MAGIC   FROM privilege_assignments_day10 a
# MAGIC   LEFT JOIN privilege_policy_day10 p
# MAGIC     ON a.principal = p.principal
# MAGIC    AND a.object_name = p.object_name
# MAGIC    AND a.privilege = p.allowed_privilege
# MAGIC   WHERE p.allowed_privilege IS NULL
# MAGIC )
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'failed_job_attempts',
# MAGIC   COUNT(*)
# MAGIC FROM lakeflow_job_runs_day10
# MAGIC WHERE status = 'FAILED'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'blocked_deployments',
# MAGIC   COUNT(*)
# MAGIC FROM deployment_decisions_delta_day10
# MAGIC WHERE deployment_decision = 'BLOCKED';

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Lakeflow Jobs and CI/CD were initial gaps before this bridge lab.
# MAGIC - Final issue counts show one overprivileged access, one failed job attempt, and two blocked deployments.
# MAGIC
# MAGIC Operational meaning: certification readiness should become operational readiness: know the terms, then use them to diagnose and gate real work.
