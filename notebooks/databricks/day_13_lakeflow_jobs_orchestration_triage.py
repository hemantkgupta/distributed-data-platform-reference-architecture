# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 13 - Lakeflow Jobs: Task Dependencies, Retries, Repair, And Promotion Gates
# MAGIC
# MAGIC Goal: practice Lakeflow Jobs concepts using runnable Delta tables: tasks, dependencies, run-if behavior, retries, repair scope, run history, and deployment/promotion gates.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Lakeflow Jobs, task orchestration, monitoring, troubleshooting, CI/CD basics.
# MAGIC - Professional stretch: failure triage, repair-run scope, deployment drift, promotion gating, and operational evidence.
# MAGIC
# MAGIC Note: this notebook simulates Lakeflow Jobs metadata with Delta tables so it works in a personal Databricks workspace. In production, use actual Lakeflow Jobs UI/API, job run output, system tables where available, and Declarative Automation Bundles.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Model A Lakeflow Job DAG
# MAGIC
# MAGIC Purpose: create a multi-task job definition with dependencies, retry policy, and run-if behavior.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeflow_job_tasks_day13 (
# MAGIC   job_name STRING,
# MAGIC   task_key STRING,
# MAGIC   task_type STRING,
# MAGIC   depends_on ARRAY<STRING>,
# MAGIC   run_if STRING,
# MAGIC   max_retries INT,
# MAGIC   min_retry_interval_seconds INT,
# MAGIC   timeout_seconds INT,
# MAGIC   owner STRING,
# MAGIC   output_table STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakeflow_job_tasks_day13 VALUES
# MAGIC   (
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'ingest_orders',
# MAGIC     'notebook',
# MAGIC     array(),
# MAGIC     'ALL_SUCCESS',
# MAGIC     2,
# MAGIC     60,
# MAGIC     900,
# MAGIC     'ingestion-owner@example.com',
# MAGIC     'orders_bronze_day13'
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'validate_bronze',
# MAGIC     'notebook',
# MAGIC     array('ingest_orders'),
# MAGIC     'ALL_SUCCESS',
# MAGIC     1,
# MAGIC     120,
# MAGIC     600,
# MAGIC     'data-quality@example.com',
# MAGIC     'orders_quality_evidence_day13'
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'build_silver',
# MAGIC     'notebook',
# MAGIC     array('validate_bronze'),
# MAGIC     'ALL_SUCCESS',
# MAGIC     1,
# MAGIC     120,
# MAGIC     1200,
# MAGIC     'transform-owner@example.com',
# MAGIC     'orders_silver_day13'
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'publish_gold',
# MAGIC     'notebook',
# MAGIC     array('build_silver'),
# MAGIC     'ALL_SUCCESS',
# MAGIC     0,
# MAGIC     0,
# MAGIC     600,
# MAGIC     'analytics-owner@example.com',
# MAGIC     'orders_gold_day13'
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'notify_failure',
# MAGIC     'notebook',
# MAGIC     array('ingest_orders', 'validate_bronze', 'build_silver', 'publish_gold'),
# MAGIC     'AT_LEAST_ONE_FAILED',
# MAGIC     0,
# MAGIC     0,
# MAGIC     300,
# MAGIC     'platform-oncall@example.com',
# MAGIC     'job_alerts_day13'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   task_key,
# MAGIC   depends_on,
# MAGIC   run_if,
# MAGIC   max_retries,
# MAGIC   min_retry_interval_seconds,
# MAGIC   owner,
# MAGIC   output_table
# MAGIC FROM lakeflow_job_tasks_day13
# MAGIC ORDER BY
# MAGIC   CASE task_key
# MAGIC     WHEN 'ingest_orders' THEN 1
# MAGIC     WHEN 'validate_bronze' THEN 2
# MAGIC     WHEN 'build_silver' THEN 3
# MAGIC     WHEN 'publish_gold' THEN 4
# MAGIC     ELSE 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `ingest_orders -> validate_bronze -> build_silver -> publish_gold`.
# MAGIC - `notify_failure` depends on the main tasks and runs when at least one dependency fails.
# MAGIC - Retry policy is task-specific, not job-wide.
# MAGIC
# MAGIC Operational meaning: a Lakeflow Job is a DAG. Downstream tasks should depend on upstream evidence, not on hope that a previous notebook worked.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create Run History With Success, Failure, And Repair
# MAGIC
# MAGIC Purpose: simulate what you inspect in Lakeflow Jobs run history after one healthy run, one failed run, and one repaired run.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeflow_job_runs_day13 (
# MAGIC   job_run_id STRING,
# MAGIC   job_name STRING,
# MAGIC   trigger_type STRING,
# MAGIC   run_started_at TIMESTAMP,
# MAGIC   run_finished_at TIMESTAMP,
# MAGIC   run_status STRING,
# MAGIC   run_attempt INT,
# MAGIC   repaired_from_run_id STRING,
# MAGIC   git_commit STRING,
# MAGIC   bundle_target STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakeflow_job_runs_day13 VALUES
# MAGIC   (
# MAGIC     'job-run-1301',
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'SCHEDULED',
# MAGIC     TIMESTAMP'2026-07-10T05:00:00Z',
# MAGIC     TIMESTAMP'2026-07-10T05:10:00Z',
# MAGIC     'SUCCESS',
# MAGIC     1,
# MAGIC     NULL,
# MAGIC     'abc1234',
# MAGIC     'prod'
# MAGIC   ),
# MAGIC   (
# MAGIC     'job-run-1302',
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'SCHEDULED',
# MAGIC     TIMESTAMP'2026-07-10T06:00:00Z',
# MAGIC     TIMESTAMP'2026-07-10T06:07:00Z',
# MAGIC     'FAILED',
# MAGIC     1,
# MAGIC     NULL,
# MAGIC     'abc1234',
# MAGIC     'prod'
# MAGIC   ),
# MAGIC   (
# MAGIC     'job-run-1302-repair-1',
# MAGIC     'orders_daily_lakeflow_day13',
# MAGIC     'REPAIR_RUN',
# MAGIC     TIMESTAMP'2026-07-10T06:20:00Z',
# MAGIC     TIMESTAMP'2026-07-10T06:28:00Z',
# MAGIC     'SUCCESS',
# MAGIC     2,
# MAGIC     'job-run-1302',
# MAGIC     'abc1234',
# MAGIC     'prod'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeflow_task_runs_day13 (
# MAGIC   job_run_id STRING,
# MAGIC   task_key STRING,
# MAGIC   task_run_started_at TIMESTAMP,
# MAGIC   task_run_finished_at TIMESTAMP,
# MAGIC   attempt_number INT,
# MAGIC   task_status STRING,
# MAGIC   error_class STRING,
# MAGIC   error_message STRING,
# MAGIC   rows_written BIGINT,
# MAGIC   quality_failures BIGINT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakeflow_task_runs_day13 VALUES
# MAGIC   ('job-run-1301', 'ingest_orders', TIMESTAMP'2026-07-10T05:00:00Z', TIMESTAMP'2026-07-10T05:03:00Z', 1, 'SUCCESS', NULL, NULL, 8, 0),
# MAGIC   ('job-run-1301', 'validate_bronze', TIMESTAMP'2026-07-10T05:03:00Z', TIMESTAMP'2026-07-10T05:05:00Z', 1, 'SUCCESS', NULL, NULL, 0, 0),
# MAGIC   ('job-run-1301', 'build_silver', TIMESTAMP'2026-07-10T05:05:00Z', TIMESTAMP'2026-07-10T05:08:00Z', 1, 'SUCCESS', NULL, NULL, 6, 0),
# MAGIC   ('job-run-1301', 'publish_gold', TIMESTAMP'2026-07-10T05:08:00Z', TIMESTAMP'2026-07-10T05:10:00Z', 1, 'SUCCESS', NULL, NULL, 2, 0),
# MAGIC   ('job-run-1301', 'notify_failure', NULL, NULL, 0, 'EXCLUDED', NULL, 'No upstream task failed', 0, 0),
# MAGIC
# MAGIC   ('job-run-1302', 'ingest_orders', TIMESTAMP'2026-07-10T06:00:00Z', TIMESTAMP'2026-07-10T06:03:00Z', 1, 'SUCCESS', NULL, NULL, 8, 0),
# MAGIC   ('job-run-1302', 'validate_bronze', TIMESTAMP'2026-07-10T06:03:00Z', TIMESTAMP'2026-07-10T06:05:00Z', 1, 'FAILED', 'DATA_QUALITY', '2 rows failed amount and null checks', 0, 2),
# MAGIC   ('job-run-1302', 'build_silver', NULL, NULL, 0, 'SKIPPED', NULL, 'Upstream dependency validate_bronze failed', 0, 0),
# MAGIC   ('job-run-1302', 'publish_gold', NULL, NULL, 0, 'SKIPPED', NULL, 'Upstream dependency build_silver skipped', 0, 0),
# MAGIC   ('job-run-1302', 'notify_failure', TIMESTAMP'2026-07-10T06:05:00Z', TIMESTAMP'2026-07-10T06:07:00Z', 1, 'SUCCESS', NULL, NULL, 1, 0),
# MAGIC
# MAGIC   ('job-run-1302-repair-1', 'validate_bronze', TIMESTAMP'2026-07-10T06:20:00Z', TIMESTAMP'2026-07-10T06:22:00Z', 2, 'SUCCESS', NULL, NULL, 0, 0),
# MAGIC   ('job-run-1302-repair-1', 'build_silver', TIMESTAMP'2026-07-10T06:22:00Z', TIMESTAMP'2026-07-10T06:26:00Z', 1, 'SUCCESS', NULL, NULL, 6, 0),
# MAGIC   ('job-run-1302-repair-1', 'publish_gold', TIMESTAMP'2026-07-10T06:26:00Z', TIMESTAMP'2026-07-10T06:28:00Z', 1, 'SUCCESS', NULL, NULL, 2, 0),
# MAGIC   ('job-run-1302-repair-1', 'notify_failure', NULL, NULL, 0, 'EXCLUDED', NULL, 'Repair succeeded; alert task not needed', 0, 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   job_run_id,
# MAGIC   task_key,
# MAGIC   attempt_number,
# MAGIC   task_status,
# MAGIC   error_class,
# MAGIC   error_message,
# MAGIC   rows_written,
# MAGIC   quality_failures
# MAGIC FROM lakeflow_task_runs_day13
# MAGIC ORDER BY job_run_id, task_run_started_at NULLS LAST, task_key;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `job-run-1301` succeeds.
# MAGIC - `job-run-1302` fails at `validate_bronze`.
# MAGIC - `build_silver` and `publish_gold` are skipped because their dependency failed.
# MAGIC - `notify_failure` runs on the failed run.
# MAGIC - `job-run-1302-repair-1` reruns the failed task and downstream tasks.
# MAGIC
# MAGIC Operational meaning: run history is the first incident timeline. It tells you where the DAG stopped and which downstream outputs are unsafe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Summarize Failed Runs And Skipped Outputs
# MAGIC
# MAGIC Purpose: identify which task failed, which outputs were not produced, and which owner should respond.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW failed_task_summary_day13 AS
# MAGIC SELECT
# MAGIC   r.job_run_id,
# MAGIC   r.run_started_at,
# MAGIC   t.task_key AS failed_task,
# MAGIC   cfg.owner AS failed_task_owner,
# MAGIC   t.error_class,
# MAGIC   t.error_message,
# MAGIC   t.quality_failures
# MAGIC FROM lakeflow_job_runs_day13 r
# MAGIC JOIN lakeflow_task_runs_day13 t
# MAGIC   ON r.job_run_id = t.job_run_id
# MAGIC JOIN lakeflow_job_tasks_day13 cfg
# MAGIC   ON t.task_key = cfg.task_key
# MAGIC WHERE r.run_status = 'FAILED'
# MAGIC   AND t.task_status = 'FAILED';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM failed_task_summary_day13;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.job_run_id,
# MAGIC   t.task_key,
# MAGIC   cfg.output_table,
# MAGIC   t.task_status,
# MAGIC   t.error_message
# MAGIC FROM lakeflow_task_runs_day13 t
# MAGIC JOIN lakeflow_job_tasks_day13 cfg
# MAGIC   ON t.task_key = cfg.task_key
# MAGIC WHERE t.job_run_id = 'job-run-1302'
# MAGIC   AND t.task_status IN ('FAILED', 'SKIPPED')
# MAGIC ORDER BY
# MAGIC   CASE t.task_key
# MAGIC     WHEN 'validate_bronze' THEN 1
# MAGIC     WHEN 'build_silver' THEN 2
# MAGIC     WHEN 'publish_gold' THEN 3
# MAGIC     ELSE 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Failed task: `validate_bronze`.
# MAGIC - Owner: `data-quality@example.com`.
# MAGIC - Unsafe or missing outputs: `orders_quality_evidence_day13`, `orders_silver_day13`, `orders_gold_day13`.
# MAGIC
# MAGIC Operational meaning: do not debug a job as one black box. Debug the first failed task, then reason about downstream output validity.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Evaluate Retry And Repair Scope With PySpark
# MAGIC
# MAGIC Purpose: compute whether each failed/skipped task should be retried, repaired, or left excluded.

# COMMAND ----------

from pyspark.sql import functions as F

tasks_df = spark.table("de_learning.lakeflow_job_tasks_day13")
task_runs_df = spark.table("de_learning.lakeflow_task_runs_day13")

incident_runs_df = (
    task_runs_df
    .where(F.col("job_run_id") == F.lit("job-run-1302"))
    .join(tasks_df.select("task_key", "depends_on", "run_if", "max_retries", "owner", "output_table"), on="task_key", how="left")
)

repair_plan_df = (
    incident_runs_df
    .withColumn(
        "retry_available",
        (F.col("task_status") == F.lit("FAILED")) & (F.col("attempt_number") <= F.col("max_retries"))
    )
    .withColumn(
        "repair_action",
        F.when(F.col("retry_available"), F.lit("RETRY_FAILED_TASK"))
         .when(F.col("task_status") == F.lit("SKIPPED"), F.lit("RERUN_AFTER_UPSTREAM_REPAIR"))
         .when(F.col("task_status") == F.lit("EXCLUDED"), F.lit("NO_ACTION"))
         .otherwise(F.lit("NO_ACTION"))
    )
    .withColumn(
        "promotion_blocker",
        F.col("task_key").isin("validate_bronze", "build_silver", "publish_gold")
        & F.col("task_status").isin("FAILED", "SKIPPED")
    )
    .select(
        "job_run_id",
        "task_key",
        "task_status",
        "attempt_number",
        "max_retries",
        "retry_available",
        "repair_action",
        "promotion_blocker",
        "owner",
        "output_table"
    )
)

repair_plan_df.createOrReplaceTempView("repair_plan_day13")
display(repair_plan_df.orderBy("task_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `tasks_df` is the job configuration table; `task_runs_df` is the actual run-history table.
# MAGIC - `where(...)` filters rows. SQL equivalent: `WHERE job_run_id = 'job-run-1302'`.
# MAGIC - `join(..., how="left")` keeps every run-history row and adds task config where available.
# MAGIC - `withColumn(...)` adds operational decisions like `retry_available` and `repair_action`.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - `isin(...)` checks membership in a list, like SQL `IN (...)`.
# MAGIC - `createOrReplaceTempView(...)` lets the next SQL cells query the PySpark repair plan.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   repair_action,
# MAGIC   COUNT(*) AS task_count,
# MAGIC   SUM(CASE WHEN promotion_blocker THEN 1 ELSE 0 END) AS promotion_blockers
# MAGIC FROM repair_plan_day13
# MAGIC GROUP BY repair_action
# MAGIC ORDER BY repair_action;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `validate_bronze` is retryable because it failed and still has retry budget.
# MAGIC - `build_silver` and `publish_gold` should rerun only after upstream repair.
# MAGIC - Promotion is blocked until validation and downstream build tasks succeed.
# MAGIC
# MAGIC Operational meaning: repair should be scoped. Rerun the failed task and dependent tasks; avoid rerunning healthy upstream ingestion unless the input itself was wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Create Failure Triage Evidence
# MAGIC
# MAGIC Purpose: map failure classes to likely cause, owner, and action.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE lakeflow_failure_triage_day13 (
# MAGIC   error_class STRING,
# MAGIC   likely_cause STRING,
# MAGIC   first_owner STRING,
# MAGIC   immediate_action STRING,
# MAGIC   can_repair_run BOOLEAN,
# MAGIC   needs_code_change BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO lakeflow_failure_triage_day13 VALUES
# MAGIC   (
# MAGIC     'DATA_QUALITY',
# MAGIC     'Input data violated a required quality rule',
# MAGIC     'data-quality@example.com',
# MAGIC     'Inspect failed checks, quarantine bad rows, then repair failed validation and downstream tasks',
# MAGIC     true,
# MAGIC     false
# MAGIC   ),
# MAGIC   (
# MAGIC     'MISSING_TABLE',
# MAGIC     'A required upstream table or view was not present',
# MAGIC     'platform-oncall@example.com',
# MAGIC     'Check deployment order, schema selection, and task dependency wiring',
# MAGIC     true,
# MAGIC     true
# MAGIC   ),
# MAGIC   (
# MAGIC     'PERMISSION_DENIED',
# MAGIC     'Job principal lacks required table, volume, or schema privilege',
# MAGIC     'platform-security@example.com',
# MAGIC     'Fix grants or service principal assignment before repair',
# MAGIC     true,
# MAGIC     false
# MAGIC   ),
# MAGIC   (
# MAGIC     'CLUSTER_STARTUP',
# MAGIC     'Compute failed to start or library environment failed',
# MAGIC     'platform-oncall@example.com',
# MAGIC     'Check compute policy, libraries, init scripts, and cluster event log',
# MAGIC     true,
# MAGIC     false
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   s.job_run_id,
# MAGIC   s.failed_task,
# MAGIC   s.failed_task_owner,
# MAGIC   s.error_class,
# MAGIC   f.likely_cause,
# MAGIC   f.immediate_action,
# MAGIC   f.can_repair_run,
# MAGIC   f.needs_code_change
# MAGIC FROM failed_task_summary_day13 s
# MAGIC LEFT JOIN lakeflow_failure_triage_day13 f
# MAGIC   ON s.error_class = f.error_class;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `DATA_QUALITY` maps to a repairable incident.
# MAGIC - First response is not "rerun everything"; it is inspect failed checks, quarantine, then repair validation and downstream tasks.
# MAGIC
# MAGIC Operational meaning: failure triage should be encoded, not improvised during every incident.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Detect Deployment Drift Before Promotion
# MAGIC
# MAGIC Purpose: compare the declared job configuration to the workspace-observed configuration before promoting a job.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE declared_job_config_day13 (
# MAGIC   task_key STRING,
# MAGIC   declared_max_retries INT,
# MAGIC   declared_timeout_seconds INT,
# MAGIC   declared_owner STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO declared_job_config_day13 VALUES
# MAGIC   ('ingest_orders', 2, 900, 'ingestion-owner@example.com'),
# MAGIC   ('validate_bronze', 1, 600, 'data-quality@example.com'),
# MAGIC   ('build_silver', 1, 1200, 'transform-owner@example.com'),
# MAGIC   ('publish_gold', 0, 600, 'analytics-owner@example.com'),
# MAGIC   ('notify_failure', 0, 300, 'platform-oncall@example.com');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace_job_config_day13
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   task_key,
# MAGIC   max_retries AS workspace_max_retries,
# MAGIC   CASE
# MAGIC     WHEN task_key = 'validate_bronze' THEN 300
# MAGIC     ELSE timeout_seconds
# MAGIC   END AS workspace_timeout_seconds,
# MAGIC   owner AS workspace_owner
# MAGIC FROM lakeflow_job_tasks_day13;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW job_config_drift_day13 AS
# MAGIC SELECT
# MAGIC   d.task_key,
# MAGIC   d.declared_max_retries,
# MAGIC   w.workspace_max_retries,
# MAGIC   d.declared_timeout_seconds,
# MAGIC   w.workspace_timeout_seconds,
# MAGIC   d.declared_owner,
# MAGIC   w.workspace_owner,
# MAGIC   CASE
# MAGIC     WHEN d.declared_max_retries <> w.workspace_max_retries THEN true
# MAGIC     WHEN d.declared_timeout_seconds <> w.workspace_timeout_seconds THEN true
# MAGIC     WHEN d.declared_owner <> w.workspace_owner THEN true
# MAGIC     ELSE false
# MAGIC   END AS has_drift
# MAGIC FROM declared_job_config_day13 d
# MAGIC JOIN workspace_job_config_day13 w
# MAGIC   ON d.task_key = w.task_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM job_config_drift_day13 ORDER BY task_key;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `validate_bronze` has drift because workspace timeout is `300`, while declared timeout is `600`.
# MAGIC
# MAGIC Operational meaning: CI/CD is not just pushing code. It must detect whether the workspace job definition matches the reviewed declaration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Gate Production Promotion
# MAGIC
# MAGIC Purpose: approve promotion only when repair is clean, no promotion blockers remain, and config drift is resolved.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE promotion_gate_day13
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   'orders_daily_lakeflow_day13' AS job_name,
# MAGIC   (SELECT COUNT(*) FROM repair_plan_day13 WHERE promotion_blocker) AS unresolved_promotion_blockers,
# MAGIC   (SELECT COUNT(*) FROM job_config_drift_day13 WHERE has_drift) AS drift_count,
# MAGIC   (SELECT COUNT(*) FROM lakeflow_job_runs_day13 WHERE repaired_from_run_id = 'job-run-1302' AND run_status = 'SUCCESS') AS successful_repair_runs,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM repair_plan_day13 WHERE promotion_blocker) = 0
# MAGIC      AND (SELECT COUNT(*) FROM job_config_drift_day13 WHERE has_drift) = 0
# MAGIC      AND (SELECT COUNT(*) FROM lakeflow_job_runs_day13 WHERE repaired_from_run_id = 'job-run-1302' AND run_status = 'SUCCESS') >= 1
# MAGIC       THEN 'APPROVED'
# MAGIC     ELSE 'BLOCKED'
# MAGIC   END AS promotion_decision;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM promotion_gate_day13;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace_job_config_day13
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   task_key,
# MAGIC   declared_max_retries AS workspace_max_retries,
# MAGIC   declared_timeout_seconds AS workspace_timeout_seconds,
# MAGIC   declared_owner AS workspace_owner
# MAGIC FROM declared_job_config_day13;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW job_config_drift_day13 AS
# MAGIC SELECT
# MAGIC   d.task_key,
# MAGIC   d.declared_max_retries,
# MAGIC   w.workspace_max_retries,
# MAGIC   d.declared_timeout_seconds,
# MAGIC   w.workspace_timeout_seconds,
# MAGIC   d.declared_owner,
# MAGIC   w.workspace_owner,
# MAGIC   CASE
# MAGIC     WHEN d.declared_max_retries <> w.workspace_max_retries THEN true
# MAGIC     WHEN d.declared_timeout_seconds <> w.workspace_timeout_seconds THEN true
# MAGIC     WHEN d.declared_owner <> w.workspace_owner THEN true
# MAGIC     ELSE false
# MAGIC   END AS has_drift
# MAGIC FROM declared_job_config_day13 d
# MAGIC JOIN workspace_job_config_day13 w
# MAGIC   ON d.task_key = w.task_key;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE promotion_gate_after_drift_fix_day13
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   'orders_daily_lakeflow_day13' AS job_name,
# MAGIC   0 AS unresolved_promotion_blockers_after_repair,
# MAGIC   (SELECT COUNT(*) FROM job_config_drift_day13 WHERE has_drift) AS drift_count,
# MAGIC   (SELECT COUNT(*) FROM lakeflow_job_runs_day13 WHERE repaired_from_run_id = 'job-run-1302' AND run_status = 'SUCCESS') AS successful_repair_runs,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM job_config_drift_day13 WHERE has_drift) = 0
# MAGIC      AND (SELECT COUNT(*) FROM lakeflow_job_runs_day13 WHERE repaired_from_run_id = 'job-run-1302' AND run_status = 'SUCCESS') >= 1
# MAGIC       THEN 'APPROVED'
# MAGIC     ELSE 'BLOCKED'
# MAGIC   END AS promotion_decision;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM promotion_gate_after_drift_fix_day13;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - First gate is `BLOCKED` because the failed run had promotion blockers and config drift exists.
# MAGIC - After repair success and drift fix, final gate is `APPROVED`.
# MAGIC
# MAGIC Operational meaning: production promotion needs evidence: successful repair, no unresolved blockers, and no job configuration drift.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Final Operational Checks
# MAGIC
# MAGIC Purpose: create the compact checks an on-call or release reviewer should inspect.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'job_runs' AS check_name, COUNT(*) AS observed_value FROM lakeflow_job_runs_day13
# MAGIC UNION ALL
# MAGIC SELECT 'task_runs', COUNT(*) FROM lakeflow_task_runs_day13
# MAGIC UNION ALL
# MAGIC SELECT 'failed_job_runs', COUNT(*) FROM lakeflow_job_runs_day13 WHERE run_status = 'FAILED'
# MAGIC UNION ALL
# MAGIC SELECT 'repair_runs', COUNT(*) FROM lakeflow_job_runs_day13 WHERE trigger_type = 'REPAIR_RUN'
# MAGIC UNION ALL
# MAGIC SELECT 'config_drift_after_fix', COUNT(*) FROM job_config_drift_day13 WHERE has_drift
# MAGIC UNION ALL
# MAGIC SELECT 'approved_final_promotion_gates', COUNT(*) FROM promotion_gate_after_drift_fix_day13 WHERE promotion_decision = 'APPROVED';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY promotion_gate_after_drift_fix_day13;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 3 job runs.
# MAGIC - 14 task-run rows.
# MAGIC - 1 failed job run.
# MAGIC - 1 repair run.
# MAGIC - 0 config drift after fix.
# MAGIC - 1 approved final promotion gate.
# MAGIC
# MAGIC Operational meaning: a job is not healthy because the last notebook cell ran. It is healthy when the DAG, run history, repair evidence, and deployed configuration all line up.
