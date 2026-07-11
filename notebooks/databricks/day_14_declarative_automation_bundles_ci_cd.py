# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 14 - Declarative Automation Bundles: CI/CD, Targets, Drift, And Rollback
# MAGIC
# MAGIC Goal: practice bundle-style deployment thinking with runnable Delta tables: declared targets, job resources, validation gates, target-specific overrides, deployment drift, promotion approval, and rollback evidence.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: CI/CD, Lakeflow Jobs deployment, platform operations, troubleshooting.
# MAGIC - Professional stretch: production deployment controls, target isolation, service-principal run identity, deployment locks, drift detection, rollback evidence.
# MAGIC
# MAGIC Note: this notebook simulates Declarative Automation Bundles metadata with Delta tables so it can run in a personal Databricks workspace. In production, the equivalent artifacts live in `databricks.yml`, included resource YAML files, Databricks CLI bundle commands, workspace resources, and deployment history.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Model Bundle Targets
# MAGIC
# MAGIC Purpose: define dev, staging, and prod targets with different deployment controls.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bundle_targets_day14 (
# MAGIC   bundle_name STRING,
# MAGIC   target_name STRING,
# MAGIC   deployment_mode STRING,
# MAGIC   workspace_host STRING,
# MAGIC   root_path STRING,
# MAGIC   run_as_type STRING,
# MAGIC   run_as_identity STRING,
# MAGIC   deployment_lock_enabled BOOLEAN,
# MAGIC   fail_on_active_runs BOOLEAN,
# MAGIC   approval_required BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bundle_targets_day14 VALUES
# MAGIC   (
# MAGIC     'orders-platform-bundle-day14',
# MAGIC     'dev',
# MAGIC     'development',
# MAGIC     'https://dev-workspace.example.com',
# MAGIC     '/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}',
# MAGIC     'USER',
# MAGIC     '${workspace.current_user.userName}',
# MAGIC     false,
# MAGIC     false,
# MAGIC     false
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders-platform-bundle-day14',
# MAGIC     'staging',
# MAGIC     'production',
# MAGIC     'https://staging-workspace.example.com',
# MAGIC     '/Shared/.bundle/orders-platform-bundle-day14/staging',
# MAGIC     'SERVICE_PRINCIPAL',
# MAGIC     'sp-data-platform-staging@example.com',
# MAGIC     true,
# MAGIC     true,
# MAGIC     true
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders-platform-bundle-day14',
# MAGIC     'prod',
# MAGIC     'production',
# MAGIC     'https://prod-workspace.example.com',
# MAGIC     '/Shared/.bundle/orders-platform-bundle-day14/prod',
# MAGIC     'USER',
# MAGIC     'hemant@example.com',
# MAGIC     false,
# MAGIC     false,
# MAGIC     true
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   deployment_mode,
# MAGIC   root_path,
# MAGIC   run_as_type,
# MAGIC   run_as_identity,
# MAGIC   deployment_lock_enabled,
# MAGIC   fail_on_active_runs,
# MAGIC   approval_required
# MAGIC FROM bundle_targets_day14
# MAGIC ORDER BY
# MAGIC   CASE target_name WHEN 'dev' THEN 1 WHEN 'staging' THEN 2 ELSE 3 END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Dev runs as the current user and does not require approval.
# MAGIC - Staging uses a service principal, deployment lock, and active-run protection.
# MAGIC - Prod is intentionally unsafe: it runs as a user and has no deployment lock.
# MAGIC
# MAGIC Operational meaning: targets encode environment-specific behavior. Prod should be stricter than dev.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Model Bundle Job Resources And CLI Lifecycle
# MAGIC
# MAGIC Purpose: represent the job resource that would normally live in bundle YAML, plus the CLI lifecycle commands.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bundle_job_resources_day14 (
# MAGIC   target_name STRING,
# MAGIC   job_key STRING,
# MAGIC   job_name STRING,
# MAGIC   task_key STRING,
# MAGIC   notebook_path STRING,
# MAGIC   max_retries INT,
# MAGIC   timeout_seconds INT,
# MAGIC   schedule_status STRING,
# MAGIC   expected_output_table STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bundle_job_resources_day14 VALUES
# MAGIC   ('dev', 'orders_daily_job', '[dev] orders daily job', 'ingest_orders', './notebooks/day_12_ingestion_copy_into_auto_loader_patterns.py', 1, 900, 'PAUSED', 'orders_bronze_autoloader_day12'),
# MAGIC   ('dev', 'orders_daily_job', '[dev] orders daily job', 'triage_job', './notebooks/day_13_lakeflow_jobs_orchestration_triage.py', 1, 900, 'PAUSED', 'promotion_gate_after_drift_fix_day13'),
# MAGIC   ('staging', 'orders_daily_job', 'orders daily job staging', 'ingest_orders', './notebooks/day_12_ingestion_copy_into_auto_loader_patterns.py', 2, 1200, 'PAUSED', 'orders_bronze_autoloader_day12'),
# MAGIC   ('staging', 'orders_daily_job', 'orders daily job staging', 'triage_job', './notebooks/day_13_lakeflow_jobs_orchestration_triage.py', 1, 900, 'PAUSED', 'promotion_gate_after_drift_fix_day13'),
# MAGIC   ('prod', 'orders_daily_job', 'orders daily job', 'ingest_orders', './notebooks/day_12_ingestion_copy_into_auto_loader_patterns.py', 2, 1200, 'UNPAUSED', 'orders_bronze_autoloader_day12'),
# MAGIC   ('prod', 'orders_daily_job', 'orders daily job', 'triage_job', './notebooks/day_13_lakeflow_jobs_orchestration_triage.py', 1, 900, 'UNPAUSED', 'promotion_gate_after_drift_fix_day13');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bundle_cli_lifecycle_day14 (
# MAGIC   step_number INT,
# MAGIC   lifecycle_step STRING,
# MAGIC   command_text STRING,
# MAGIC   expected_result STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bundle_cli_lifecycle_day14 VALUES
# MAGIC   (1, 'validate dev', 'databricks bundle validate -t dev', 'bundle configuration is syntactically and semantically valid for dev'),
# MAGIC   (2, 'deploy dev', 'databricks bundle deploy -t dev', 'dev workspace resources are created or updated'),
# MAGIC   (3, 'run dev job', 'databricks bundle run -t dev orders_daily_job', 'dev workflow runs for smoke validation'),
# MAGIC   (4, 'validate staging', 'databricks bundle validate -t staging', 'staging configuration is deployable'),
# MAGIC   (5, 'deploy staging', 'databricks bundle deploy -t staging', 'staging resources are updated after review'),
# MAGIC   (6, 'validate prod', 'databricks bundle validate -t prod', 'prod configuration passes strict gates before deployment'),
# MAGIC   (7, 'deploy prod', 'databricks bundle deploy -t prod', 'prod resources are updated only after promotion approval');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bundle_cli_lifecycle_day14 ORDER BY step_number;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - You see the core lifecycle: validate, deploy, run, promote.
# MAGIC - Prod deployment is not first; it follows dev/staging validation.
# MAGIC
# MAGIC Operational meaning: bundles convert workspace changes into reviewed, repeatable deployment commands.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Validate Bundle Safety Rules
# MAGIC
# MAGIC Purpose: fail prod validation when production controls are missing.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW bundle_validation_findings_day14 AS
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'prod_runs_as_service_principal' AS rule_name,
# MAGIC   'BLOCKER' AS severity,
# MAGIC   CASE WHEN run_as_type = 'SERVICE_PRINCIPAL' THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('prod run_as_type = ', run_as_type, ', identity = ', run_as_identity) AS detail
# MAGIC FROM bundle_targets_day14
# MAGIC WHERE target_name = 'prod'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'prod_uses_deployment_lock' AS rule_name,
# MAGIC   'BLOCKER' AS severity,
# MAGIC   CASE WHEN deployment_lock_enabled THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('deployment_lock_enabled = ', cast(deployment_lock_enabled AS STRING)) AS detail
# MAGIC FROM bundle_targets_day14
# MAGIC WHERE target_name = 'prod'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'prod_fails_on_active_runs' AS rule_name,
# MAGIC   'WARN' AS severity,
# MAGIC   CASE WHEN fail_on_active_runs THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('fail_on_active_runs = ', cast(fail_on_active_runs AS STRING)) AS detail
# MAGIC FROM bundle_targets_day14
# MAGIC WHERE target_name = 'prod'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'notebook_paths_declared' AS rule_name,
# MAGIC   'BLOCKER' AS severity,
# MAGIC   CASE WHEN COUNT(*) = SUM(CASE WHEN notebook_path IS NOT NULL AND notebook_path <> '' THEN 1 ELSE 0 END) THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('task_count = ', cast(COUNT(*) AS STRING)) AS detail
# MAGIC FROM bundle_job_resources_day14
# MAGIC GROUP BY target_name
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'timeouts_positive' AS rule_name,
# MAGIC   'BLOCKER' AS severity,
# MAGIC   CASE WHEN MIN(timeout_seconds) > 0 THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('min_timeout_seconds = ', cast(MIN(timeout_seconds) AS STRING)) AS detail
# MAGIC FROM bundle_job_resources_day14
# MAGIC GROUP BY target_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bundle_validation_findings_day14 ORDER BY target_name, severity, rule_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   SUM(CASE WHEN outcome = 'FAIL' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS blocker_failures,
# MAGIC   SUM(CASE WHEN outcome = 'FAIL' AND severity = 'WARN' THEN 1 ELSE 0 END) AS warning_failures
# MAGIC FROM bundle_validation_findings_day14
# MAGIC GROUP BY target_name
# MAGIC ORDER BY target_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Prod has 2 blocker failures: it does not run as a service principal and it does not use a deployment lock.
# MAGIC - Prod also has 1 warning: it does not fail on active runs.
# MAGIC - Notebook-path and timeout checks pass.
# MAGIC
# MAGIC Operational meaning: `bundle validate` checks schema and deployability, but production teams usually add stricter release gates for identity, locks, and safety policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Detect Drift Between Desired And Workspace State
# MAGIC
# MAGIC Purpose: compare declared bundle resources with what is currently in the workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE desired_job_state_day14
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   job_key,
# MAGIC   task_key,
# MAGIC   max_retries AS desired_max_retries,
# MAGIC   timeout_seconds AS desired_timeout_seconds,
# MAGIC   schedule_status AS desired_schedule_status
# MAGIC FROM bundle_job_resources_day14
# MAGIC WHERE target_name = 'prod';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace_job_state_day14 (
# MAGIC   target_name STRING,
# MAGIC   job_key STRING,
# MAGIC   task_key STRING,
# MAGIC   workspace_max_retries INT,
# MAGIC   workspace_timeout_seconds INT,
# MAGIC   workspace_schedule_status STRING,
# MAGIC   last_updated_by STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workspace_job_state_day14 VALUES
# MAGIC   ('prod', 'orders_daily_job', 'ingest_orders', 2, 1200, 'UNPAUSED', 'bundle-deploy'),
# MAGIC   ('prod', 'orders_daily_job', 'triage_job', 0, 300, 'UNPAUSED', 'manual-ui-edit');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW deployment_drift_day14 AS
# MAGIC SELECT
# MAGIC   d.target_name,
# MAGIC   d.job_key,
# MAGIC   d.task_key,
# MAGIC   d.desired_max_retries,
# MAGIC   w.workspace_max_retries,
# MAGIC   d.desired_timeout_seconds,
# MAGIC   w.workspace_timeout_seconds,
# MAGIC   d.desired_schedule_status,
# MAGIC   w.workspace_schedule_status,
# MAGIC   w.last_updated_by,
# MAGIC   CASE
# MAGIC     WHEN d.desired_max_retries <> w.workspace_max_retries THEN true
# MAGIC     WHEN d.desired_timeout_seconds <> w.workspace_timeout_seconds THEN true
# MAGIC     WHEN d.desired_schedule_status <> w.workspace_schedule_status THEN true
# MAGIC     ELSE false
# MAGIC   END AS has_drift
# MAGIC FROM desired_job_state_day14 d
# MAGIC JOIN workspace_job_state_day14 w
# MAGIC   ON d.target_name = w.target_name
# MAGIC  AND d.job_key = w.job_key
# MAGIC  AND d.task_key = w.task_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM deployment_drift_day14 ORDER BY task_key;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `triage_job` has drift: retries and timeout differ from the bundle declaration.
# MAGIC - `last_updated_by = manual-ui-edit` explains why drift happened.
# MAGIC
# MAGIC Operational meaning: declarative deployment protects production only if you detect and correct manual workspace drift.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Evaluate Release Promotion With PySpark
# MAGIC
# MAGIC Purpose: combine validation findings, test runs, approvals, and drift into one release decision.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bundle_test_runs_day14 (
# MAGIC   release_version STRING,
# MAGIC   target_name STRING,
# MAGIC   test_name STRING,
# MAGIC   test_type STRING,
# MAGIC   outcome STRING,
# MAGIC   evidence_table STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bundle_test_runs_day14 VALUES
# MAGIC   ('release-2026-07-11-r1', 'dev', 'python source compile', 'STATIC', 'PASS', NULL),
# MAGIC   ('release-2026-07-11-r1', 'dev', 'notebook smoke run', 'SMOKE', 'PASS', 'promotion_gate_after_drift_fix_day13'),
# MAGIC   ('release-2026-07-11-r1', 'staging', 'job repair scenario', 'INTEGRATION', 'PASS', 'promotion_gate_after_drift_fix_day13'),
# MAGIC   ('release-2026-07-11-r1', 'prod', 'predeployment validation', 'STATIC', 'PASS', 'bundle_validation_findings_day14');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE release_approvals_day14 (
# MAGIC   release_version STRING,
# MAGIC   target_name STRING,
# MAGIC   approver STRING,
# MAGIC   approval_status STRING,
# MAGIC   approved_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO release_approvals_day14 VALUES
# MAGIC   ('release-2026-07-11-r1', 'prod', 'data-platform-lead@example.com', 'APPROVED', TIMESTAMP'2026-07-11T05:45:00Z');

# COMMAND ----------

from pyspark.sql import functions as F

validation_df = spark.sql("SELECT * FROM bundle_validation_findings_day14")
drift_df = spark.sql("SELECT * FROM deployment_drift_day14")
tests_df = spark.table("de_learning.bundle_test_runs_day14")
approvals_df = spark.table("de_learning.release_approvals_day14")

validation_summary_df = (
    validation_df
    .groupBy("target_name")
    .agg(
        F.sum(F.when((F.col("severity") == "BLOCKER") & (F.col("outcome") == "FAIL"), 1).otherwise(0)).alias("blocker_failures"),
        F.sum(F.when((F.col("severity") == "WARN") & (F.col("outcome") == "FAIL"), 1).otherwise(0)).alias("warning_failures")
    )
)

drift_summary_df = (
    drift_df
    .groupBy("target_name")
    .agg(F.sum(F.when(F.col("has_drift"), 1).otherwise(0)).alias("drift_count"))
)

test_summary_df = (
    tests_df
    .where(F.col("release_version") == "release-2026-07-11-r1")
    .groupBy("target_name")
    .agg(F.sum(F.when(F.col("outcome") != "PASS", 1).otherwise(0)).alias("failed_tests"))
)

approval_summary_df = (
    approvals_df
    .where((F.col("release_version") == "release-2026-07-11-r1") & (F.col("target_name") == "prod"))
    .groupBy("target_name")
    .agg(F.sum(F.when(F.col("approval_status") == "APPROVED", 1).otherwise(0)).alias("approval_count"))
)

release_gate_df = (
    validation_summary_df
    .join(drift_summary_df, on="target_name", how="left")
    .join(test_summary_df, on="target_name", how="left")
    .join(approval_summary_df, on="target_name", how="left")
    .where(F.col("target_name") == "prod")
    .na.fill({"drift_count": 0, "failed_tests": 0, "approval_count": 0})
    .withColumn(
        "release_decision",
        F.when(
            (F.col("blocker_failures") == 0)
            & (F.col("drift_count") == 0)
            & (F.col("failed_tests") == 0)
            & (F.col("approval_count") >= 1),
            F.lit("APPROVED")
        ).otherwise(F.lit("BLOCKED"))
    )
)

release_gate_df.createOrReplaceTempView("release_gate_day14")
display(release_gate_df)

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `validation_df`, `drift_df`, `tests_df`, and `approvals_df` are DataFrames representing different CI/CD evidence sources.
# MAGIC - `groupBy(...).agg(...)` is SQL `GROUP BY` with aggregate functions.
# MAGIC - `F.sum(F.when(...))` counts rows matching a condition.
# MAGIC - `join(..., how="left")` preserves the validation summary even if drift/test/approval data is missing.
# MAGIC - `.na.fill(...)` replaces null aggregate values with zero.
# MAGIC - `withColumn("release_decision", ...)` adds the final gate decision.
# MAGIC - SQL equivalent: aggregate each evidence table, left join the summaries, then use `CASE WHEN` for `APPROVED` vs `BLOCKED`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM release_gate_day14;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Release is `BLOCKED`.
# MAGIC - Reasons: prod has blocker validation failures and deployment drift.
# MAGIC - Approval alone is not enough.
# MAGIC
# MAGIC Operational meaning: promotion is a gate over evidence, not a calendar event.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Fix Prod Controls And Re-Evaluate
# MAGIC
# MAGIC Purpose: correct run identity, deployment lock, active-run protection, and drift.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bundle_targets_fixed_day14
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   bundle_name,
# MAGIC   target_name,
# MAGIC   deployment_mode,
# MAGIC   workspace_host,
# MAGIC   root_path,
# MAGIC   CASE WHEN target_name = 'prod' THEN 'SERVICE_PRINCIPAL' ELSE run_as_type END AS run_as_type,
# MAGIC   CASE WHEN target_name = 'prod' THEN 'sp-data-platform-prod@example.com' ELSE run_as_identity END AS run_as_identity,
# MAGIC   CASE WHEN target_name = 'prod' THEN true ELSE deployment_lock_enabled END AS deployment_lock_enabled,
# MAGIC   CASE WHEN target_name = 'prod' THEN true ELSE fail_on_active_runs END AS fail_on_active_runs,
# MAGIC   approval_required
# MAGIC FROM bundle_targets_day14;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace_job_state_day14
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.target_name,
# MAGIC   d.job_key,
# MAGIC   d.task_key,
# MAGIC   d.desired_max_retries AS workspace_max_retries,
# MAGIC   d.desired_timeout_seconds AS workspace_timeout_seconds,
# MAGIC   d.desired_schedule_status AS workspace_schedule_status,
# MAGIC   'bundle-deploy' AS last_updated_by
# MAGIC FROM desired_job_state_day14 d;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW bundle_validation_findings_after_fix_day14 AS
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'prod_runs_as_service_principal' AS rule_name,
# MAGIC   'BLOCKER' AS severity,
# MAGIC   CASE WHEN run_as_type = 'SERVICE_PRINCIPAL' THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('prod run_as_type = ', run_as_type, ', identity = ', run_as_identity) AS detail
# MAGIC FROM bundle_targets_fixed_day14
# MAGIC WHERE target_name = 'prod'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'prod_uses_deployment_lock' AS rule_name,
# MAGIC   'BLOCKER' AS severity,
# MAGIC   CASE WHEN deployment_lock_enabled THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('deployment_lock_enabled = ', cast(deployment_lock_enabled AS STRING)) AS detail
# MAGIC FROM bundle_targets_fixed_day14
# MAGIC WHERE target_name = 'prod'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   target_name,
# MAGIC   'prod_fails_on_active_runs' AS rule_name,
# MAGIC   'WARN' AS severity,
# MAGIC   CASE WHEN fail_on_active_runs THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   concat('fail_on_active_runs = ', cast(fail_on_active_runs AS STRING)) AS detail
# MAGIC FROM bundle_targets_fixed_day14
# MAGIC WHERE target_name = 'prod';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW deployment_drift_after_fix_day14 AS
# MAGIC SELECT
# MAGIC   d.target_name,
# MAGIC   d.job_key,
# MAGIC   d.task_key,
# MAGIC   CASE
# MAGIC     WHEN d.desired_max_retries <> w.workspace_max_retries THEN true
# MAGIC     WHEN d.desired_timeout_seconds <> w.workspace_timeout_seconds THEN true
# MAGIC     WHEN d.desired_schedule_status <> w.workspace_schedule_status THEN true
# MAGIC     ELSE false
# MAGIC   END AS has_drift
# MAGIC FROM desired_job_state_day14 d
# MAGIC JOIN workspace_job_state_day14 w
# MAGIC   ON d.target_name = w.target_name
# MAGIC  AND d.job_key = w.job_key
# MAGIC  AND d.task_key = w.task_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE release_gate_after_fix_day14
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   'release-2026-07-11-r1' AS release_version,
# MAGIC   'prod' AS target_name,
# MAGIC   (SELECT COUNT(*) FROM bundle_validation_findings_after_fix_day14 WHERE severity = 'BLOCKER' AND outcome = 'FAIL') AS blocker_failures,
# MAGIC   (SELECT COUNT(*) FROM deployment_drift_after_fix_day14 WHERE has_drift) AS drift_count,
# MAGIC   (SELECT COUNT(*) FROM bundle_test_runs_day14 WHERE target_name IN ('dev', 'staging', 'prod') AND outcome <> 'PASS') AS failed_tests,
# MAGIC   (SELECT COUNT(*) FROM release_approvals_day14 WHERE target_name = 'prod' AND approval_status = 'APPROVED') AS approval_count,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM bundle_validation_findings_after_fix_day14 WHERE severity = 'BLOCKER' AND outcome = 'FAIL') = 0
# MAGIC      AND (SELECT COUNT(*) FROM deployment_drift_after_fix_day14 WHERE has_drift) = 0
# MAGIC      AND (SELECT COUNT(*) FROM bundle_test_runs_day14 WHERE target_name IN ('dev', 'staging', 'prod') AND outcome <> 'PASS') = 0
# MAGIC      AND (SELECT COUNT(*) FROM release_approvals_day14 WHERE target_name = 'prod' AND approval_status = 'APPROVED') >= 1
# MAGIC       THEN 'APPROVED'
# MAGIC     ELSE 'BLOCKED'
# MAGIC   END AS release_decision;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM release_gate_after_fix_day14;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Final release decision is `APPROVED`.
# MAGIC - Blocker failures are 0.
# MAGIC - Drift count is 0.
# MAGIC - Approval count is at least 1.
# MAGIC
# MAGIC Operational meaning: fix the declared and deployed state first; then promote.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Record Deployment And Rollback Evidence
# MAGIC
# MAGIC Purpose: record what was deployed and how to return to the previous known-good version.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bundle_deployment_history_day14 (
# MAGIC   deployment_id STRING,
# MAGIC   release_version STRING,
# MAGIC   target_name STRING,
# MAGIC   git_commit STRING,
# MAGIC   deployed_at TIMESTAMP,
# MAGIC   deployed_by STRING,
# MAGIC   deployment_status STRING,
# MAGIC   previous_release_version STRING,
# MAGIC   rollback_command STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bundle_deployment_history_day14 VALUES
# MAGIC   (
# MAGIC     'deploy-1401',
# MAGIC     'release-2026-07-10-r0',
# MAGIC     'prod',
# MAGIC     '67f859b',
# MAGIC     TIMESTAMP'2026-07-10T06:30:00Z',
# MAGIC     'sp-data-platform-prod@example.com',
# MAGIC     'SUCCESS',
# MAGIC     NULL,
# MAGIC     NULL
# MAGIC   ),
# MAGIC   (
# MAGIC     'deploy-1402',
# MAGIC     'release-2026-07-11-r1',
# MAGIC     'prod',
# MAGIC     'day14abc',
# MAGIC     TIMESTAMP'2026-07-11T06:30:00Z',
# MAGIC     'sp-data-platform-prod@example.com',
# MAGIC     'SUCCESS',
# MAGIC     'release-2026-07-10-r0',
# MAGIC     'git checkout 67f859b && databricks bundle deploy -t prod'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bundle_deployment_history_day14 ORDER BY deployed_at;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Current prod release has a previous known-good release.
# MAGIC - Rollback command points to the previous commit and prod deployment target.
# MAGIC
# MAGIC Operational meaning: rollback is not a vague instruction. It is a recorded commit, target, deploy identity, and command.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Final CI/CD Checks
# MAGIC
# MAGIC Purpose: produce the compact checklist a release reviewer should inspect.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'prod_blocker_failures_after_fix' AS check_name, COUNT(*) AS observed_value
# MAGIC FROM bundle_validation_findings_after_fix_day14
# MAGIC WHERE severity = 'BLOCKER' AND outcome = 'FAIL'
# MAGIC UNION ALL
# MAGIC SELECT 'prod_drift_after_fix', COUNT(*)
# MAGIC FROM deployment_drift_after_fix_day14
# MAGIC WHERE has_drift
# MAGIC UNION ALL
# MAGIC SELECT 'failed_tests', COUNT(*)
# MAGIC FROM bundle_test_runs_day14
# MAGIC WHERE outcome <> 'PASS'
# MAGIC UNION ALL
# MAGIC SELECT 'prod_approvals', COUNT(*)
# MAGIC FROM release_approvals_day14
# MAGIC WHERE target_name = 'prod' AND approval_status = 'APPROVED'
# MAGIC UNION ALL
# MAGIC SELECT 'approved_release_gates', COUNT(*)
# MAGIC FROM release_gate_after_fix_day14
# MAGIC WHERE release_decision = 'APPROVED'
# MAGIC UNION ALL
# MAGIC SELECT 'rollback_records', COUNT(*)
# MAGIC FROM bundle_deployment_history_day14
# MAGIC WHERE rollback_command IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY release_gate_after_fix_day14;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 0 prod blocker failures after fix.
# MAGIC - 0 prod drift after fix.
# MAGIC - 0 failed tests.
# MAGIC - 1 prod approval.
# MAGIC - 1 approved release gate.
# MAGIC - 1 rollback record.
# MAGIC
# MAGIC Operational meaning: CI/CD is production control. A release is ready when validation, tests, approval, deployed state, and rollback evidence all agree.
