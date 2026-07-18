# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 20 - Compute Selection And Cost/Performance Guardrails
# MAGIC
# MAGIC Goal: choose the right Databricks compute for common data engineering workloads and explain the operational tradeoffs.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, Lakeflow Jobs, SQL warehouses, troubleshooting, monitoring, optimization, and governance basics.
# MAGIC - Professional stretch: workload isolation, policy-driven guardrails, cost attribution, production job compute choices, queue/startup/shuffle symptoms, and incident triage.
# MAGIC
# MAGIC This notebook models compute inventory, policies, and incidents as Delta tables so the exercise runs in Free Edition without requiring admin privileges.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Workload Profiles
# MAGIC
# MAGIC Purpose: describe the workload before choosing compute. Compute choice should follow workload shape, not habit.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workload_profiles_day20 (
# MAGIC   workload_id STRING,
# MAGIC   workload_name STRING,
# MAGIC   workload_type STRING,
# MAGIC   execution_mode STRING,
# MAGIC   primary_language STRING,
# MAGIC   is_production BOOLEAN,
# MAGIC   concurrency_level INT,
# MAGIC   latency_sensitivity STRING,
# MAGIC   needs_custom_libraries BOOLEAN,
# MAGIC   needs_custom_networking BOOLEAN,
# MAGIC   expected_runtime_minutes INT,
# MAGIC   data_volume_gb INT,
# MAGIC   schedule_pattern STRING,
# MAGIC   owner_group STRING,
# MAGIC   cost_center STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO workload_profiles_day20 VALUES
# MAGIC   ('wl-001', 'contract debugging notebook', 'notebook_interactive', 'interactive', 'python_sql', false, 1, 'medium', false, false, 45, 5, 'ad_hoc', 'data-platform', 'de-learning'),
# MAGIC   ('wl-002', 'nightly bronze to silver job', 'automated_job', 'scheduled', 'pyspark', true, 1, 'medium', false, false, 35, 120, 'daily', 'data-platform', 'orders'),
# MAGIC   ('wl-003', 'finance dashboard SQL', 'sql_analytics', 'interactive', 'sql', true, 35, 'high', false, false, 5, 20, 'business_hours', 'finance-analytics', 'finance'),
# MAGIC   ('wl-004', 'month end historical backfill', 'automated_job', 'backfill', 'pyspark', true, 1, 'low', true, false, 180, 950, 'monthly', 'data-platform', 'orders'),
# MAGIC   ('wl-005', 'streaming quality pipeline', 'lakeflow_pipeline', 'continuous', 'python_sql', true, 1, 'high', false, false, 1440, 50, 'continuous', 'data-quality', 'orders'),
# MAGIC   ('wl-006', 'partner hourly SQL extract', 'sql_analytics', 'scheduled', 'sql', true, 10, 'medium', false, false, 8, 35, 'hourly', 'partner-platform', 'partners');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workload_profiles_day20 ORDER BY workload_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 workload profiles.
# MAGIC - Mix of interactive notebook, scheduled job, SQL analytics, backfill, and pipeline workloads.
# MAGIC - `wl-004` needs custom libraries and handles the largest data volume.
# MAGIC - `wl-003` is high-concurrency SQL.
# MAGIC
# MAGIC Operational meaning: production compute decisions start with workload facts: language, latency, concurrency, runtime, data volume, custom requirements, and ownership.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create Compute Option Catalog
# MAGIC
# MAGIC Purpose: compare serverless compute, classic jobs compute, all-purpose compute, SQL warehouses, and Lakeflow pipeline compute.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE compute_options_day20 (
# MAGIC   compute_option STRING,
# MAGIC   compute_family STRING,
# MAGIC   intended_for STRING,
# MAGIC   supports_python BOOLEAN,
# MAGIC   supports_sql BOOLEAN,
# MAGIC   supports_scheduled_jobs BOOLEAN,
# MAGIC   supports_interactive_notebooks BOOLEAN,
# MAGIC   supports_high_concurrency_sql BOOLEAN,
# MAGIC   supports_custom_cluster_config BOOLEAN,
# MAGIC   startup_profile STRING,
# MAGIC   operator_config_surface STRING,
# MAGIC   default_cost_posture STRING,
# MAGIC   production_job_fit STRING,
# MAGIC   notes STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO compute_options_day20 VALUES
# MAGIC   ('serverless_notebook_or_job', 'serverless', 'notebooks and automated jobs when supported', true, true, true, true, false, false, 'fast', 'low', 'efficient_on_demand', 'preferred', 'Recommended default for many notebooks and jobs when workload features are supported'),
# MAGIC   ('classic_jobs_compute', 'classic', 'scheduled non-SQL jobs needing custom cluster settings', true, true, true, false, false, true, 'cold_start', 'high', 'policy_controlled', 'conditional', 'Use when jobs need custom settings unavailable on serverless'),
# MAGIC   ('classic_all_purpose_compute', 'classic', 'interactive development and shared exploration', true, true, false, true, false, true, 'warm_if_running', 'high', 'idle_risk', 'avoid_for_production_jobs', 'Useful for exploration but risky for production jobs because idle/shared state can hide cost and reproducibility issues'),
# MAGIC   ('serverless_sql_warehouse', 'sql_warehouse', 'BI, dashboards, ad hoc SQL, and SQL tasks', false, true, true, false, true, false, 'fast', 'low', 'efficient_on_demand', 'preferred_for_sql', 'Preferred SQL warehouse type when available'),
# MAGIC   ('pro_sql_warehouse', 'sql_warehouse', 'SQL analytics requiring non-serverless workspace constraints', false, true, true, false, true, false, 'medium', 'medium', 'managed_capacity', 'conditional', 'Use when serverless SQL warehouse is unavailable or org policy requires non-serverless SQL compute'),
# MAGIC   ('classic_sql_warehouse', 'sql_warehouse', 'legacy SQL analytics or constrained environments', false, true, true, false, true, false, 'medium', 'medium', 'capacity_idle_risk', 'conditional', 'Use when classic SQL warehouse is required by environment constraints'),
# MAGIC   ('serverless_lakeflow_pipeline', 'serverless', 'Lakeflow pipeline workloads when supported', true, true, true, false, false, false, 'fast', 'low', 'efficient_on_demand', 'preferred_for_pipelines', 'Databricks-managed compute for pipeline workloads when feature support fits');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compute_options_day20 ORDER BY compute_family, compute_option;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 7 compute options.
# MAGIC - Serverless has lower operator configuration surface.
# MAGIC - Classic jobs compute remains useful when custom cluster settings are required.
# MAGIC - All-purpose compute is marked as avoid for production jobs.
# MAGIC - SQL warehouses are the fit for high-concurrency SQL workloads.
# MAGIC
# MAGIC Operational meaning: compute choice is a control decision. It changes startup behavior, cost controls, tuning surface, reproducibility, and operational ownership.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Build A Candidate Matrix In SQL
# MAGIC
# MAGIC Purpose: quickly eliminate compute options that do not support the workload language or execution mode.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE compute_candidate_matrix_day20
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   w.workload_id,
# MAGIC   w.workload_name,
# MAGIC   w.workload_type,
# MAGIC   w.primary_language,
# MAGIC   w.execution_mode,
# MAGIC   w.is_production,
# MAGIC   w.concurrency_level,
# MAGIC   w.latency_sensitivity,
# MAGIC   w.needs_custom_libraries,
# MAGIC   w.needs_custom_networking,
# MAGIC   w.data_volume_gb,
# MAGIC   c.compute_option,
# MAGIC   c.compute_family,
# MAGIC   c.startup_profile,
# MAGIC   c.operator_config_surface,
# MAGIC   c.production_job_fit,
# MAGIC   CASE
# MAGIC     WHEN w.primary_language = 'sql' AND c.supports_sql THEN 'LANGUAGE_OK'
# MAGIC     WHEN w.primary_language IN ('pyspark', 'python_sql') AND c.supports_python THEN 'LANGUAGE_OK'
# MAGIC     ELSE 'LANGUAGE_NOT_SUPPORTED'
# MAGIC   END AS language_check,
# MAGIC   CASE
# MAGIC     WHEN w.workload_type = 'sql_analytics' AND c.supports_high_concurrency_sql THEN 'WORKLOAD_OK'
# MAGIC     WHEN w.workload_type = 'automated_job' AND c.supports_scheduled_jobs THEN 'WORKLOAD_OK'
# MAGIC     WHEN w.workload_type = 'notebook_interactive' AND c.supports_interactive_notebooks THEN 'WORKLOAD_OK'
# MAGIC     WHEN w.workload_type = 'lakeflow_pipeline' AND c.compute_option = 'serverless_lakeflow_pipeline' THEN 'WORKLOAD_OK'
# MAGIC     ELSE 'WORKLOAD_NOT_IDEAL'
# MAGIC   END AS workload_check
# MAGIC FROM workload_profiles_day20 w
# MAGIC CROSS JOIN compute_options_day20 c
# MAGIC WHERE
# MAGIC   (w.primary_language = 'sql' AND c.supports_sql)
# MAGIC   OR (w.primary_language IN ('pyspark', 'python_sql') AND c.supports_python);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workload_id, workload_name, compute_option, language_check, workload_check, production_job_fit
# MAGIC FROM compute_candidate_matrix_day20
# MAGIC ORDER BY workload_id, compute_option;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - SQL workloads only consider SQL-capable options.
# MAGIC - PySpark/Python workloads only consider Python-capable options.
# MAGIC - Some language-compatible options still show `WORKLOAD_NOT_IDEAL`.
# MAGIC
# MAGIC Operational meaning: a compute option can run the code and still be the wrong production choice. You need a second pass for workload fit, cost, and operational risk.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Score Compute Decisions With PySpark
# MAGIC
# MAGIC Purpose: rank candidates by workload fit, production safety, custom requirements, latency, concurrency, and cost posture.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

workloads_df = spark.table("de_learning.workload_profiles_day20")
options_df = spark.table("de_learning.compute_options_day20")

candidate_df = (
    workloads_df.crossJoin(options_df)
    .where(
        ((F.col("primary_language") == "sql") & F.col("supports_sql"))
        | ((F.col("primary_language").isin("pyspark", "python_sql")) & F.col("supports_python"))
    )
)

scored_df = (
    candidate_df
    .withColumn(
        "workload_fit_score",
        F.when((F.col("workload_type") == "sql_analytics") & F.col("supports_high_concurrency_sql"), F.lit(40))
         .when((F.col("workload_type") == "automated_job") & (F.col("compute_option") == "serverless_notebook_or_job") & (~F.col("needs_custom_libraries")) & (~F.col("needs_custom_networking")), F.lit(38))
         .when((F.col("workload_type") == "automated_job") & (F.col("compute_option") == "classic_jobs_compute") & (F.col("needs_custom_libraries") | F.col("needs_custom_networking")), F.lit(42))
         .when((F.col("workload_type") == "lakeflow_pipeline") & (F.col("compute_option") == "serverless_lakeflow_pipeline"), F.lit(45))
         .when((F.col("workload_type") == "notebook_interactive") & (F.col("compute_option") == "serverless_notebook_or_job"), F.lit(35))
         .when((F.col("workload_type") == "notebook_interactive") & (F.col("compute_option") == "classic_all_purpose_compute"), F.lit(25))
         .otherwise(F.lit(5))
    )
    .withColumn(
        "production_safety_score",
        F.when(F.col("is_production") & (F.col("compute_option") == "classic_all_purpose_compute"), F.lit(-35))
         .when(F.col("is_production") & F.col("production_job_fit").isin("preferred", "preferred_for_sql", "preferred_for_pipelines"), F.lit(20))
         .when(F.col("is_production") & (F.col("production_job_fit") == "conditional"), F.lit(10))
         .otherwise(F.lit(0))
    )
    .withColumn(
        "custom_requirement_score",
        F.when((F.col("needs_custom_libraries") | F.col("needs_custom_networking")) & F.col("supports_custom_cluster_config"), F.lit(25))
         .when((F.col("needs_custom_libraries") | F.col("needs_custom_networking")) & (~F.col("supports_custom_cluster_config")), F.lit(-30))
         .otherwise(F.lit(5))
    )
    .withColumn(
        "latency_concurrency_score",
        F.when((F.col("latency_sensitivity") == "high") & (F.col("startup_profile") == "fast"), F.lit(10))
         .otherwise(F.lit(0))
        + F.when((F.col("concurrency_level") >= 10) & F.col("supports_high_concurrency_sql"), F.lit(20)).otherwise(F.lit(0))
    )
    .withColumn(
        "cost_control_score",
        F.when(F.col("default_cost_posture").isin("efficient_on_demand", "policy_controlled"), F.lit(15))
         .when(F.col("default_cost_posture") == "idle_risk", F.lit(-20))
         .otherwise(F.lit(5))
    )
    .withColumn(
        "total_score",
        F.col("workload_fit_score")
        + F.col("production_safety_score")
        + F.col("custom_requirement_score")
        + F.col("latency_concurrency_score")
        + F.col("cost_control_score")
    )
    .withColumn(
        "decision_reason",
        F.when(F.col("compute_option") == "serverless_sql_warehouse", F.lit("Best fit for SQL analytics and high concurrency when available"))
         .when(F.col("compute_option") == "serverless_notebook_or_job", F.lit("Best low-ops default for notebooks and jobs when features are supported"))
         .when(F.col("compute_option") == "classic_jobs_compute", F.lit("Use for production jobs that require custom cluster settings"))
         .when(F.col("compute_option") == "classic_all_purpose_compute", F.lit("Use for interactive development; avoid for production jobs"))
         .when(F.col("compute_option") == "serverless_lakeflow_pipeline", F.lit("Best fit for supported Lakeflow pipeline workloads"))
         .otherwise(F.lit("Conditional fit; check workspace constraints and policies"))
    )
)

rank_window = Window.partitionBy("workload_id").orderBy(F.col("total_score").desc(), F.col("compute_option").asc())

decision_df = (
    scored_df
    .withColumn("candidate_rank", F.row_number().over(rank_window))
    .where(F.col("candidate_rank") == 1)
    .select(
        "workload_id",
        "workload_name",
        "workload_type",
        "primary_language",
        "is_production",
        "compute_option",
        "compute_family",
        "total_score",
        "workload_fit_score",
        "production_safety_score",
        "custom_requirement_score",
        "latency_concurrency_score",
        "cost_control_score",
        "decision_reason"
    )
)

decision_df.createOrReplaceTempView("compute_selection_decisions_view_day20")
display(decision_df.orderBy("workload_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `workloads_df` is the SQL table `workload_profiles_day20` as a DataFrame.
# MAGIC - `options_df` is the SQL table `compute_options_day20` as a DataFrame.
# MAGIC - `crossJoin` creates every workload/compute pairing, like SQL `CROSS JOIN`.
# MAGIC - `.where(...)` filters candidate rows, like SQL `WHERE`.
# MAGIC - `F.col("primary_language")` references a column inside an expression.
# MAGIC - `withColumn("total_score", ...)` adds a calculated column.
# MAGIC - `F.when(...).otherwise(...)` is the PySpark version of SQL `CASE WHEN`.
# MAGIC - `Window.partitionBy(...).orderBy(...)` ranks candidates per workload.
# MAGIC - PySpark is lazily evaluated; most transformations build a plan. `display(...)` triggers execution.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     w.workload_id,
# MAGIC     c.compute_option,
# MAGIC     <score expression> AS total_score,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY w.workload_id ORDER BY <score expression> DESC) AS candidate_rank
# MAGIC   FROM workload_profiles_day20 w
# MAGIC   CROSS JOIN compute_options_day20 c
# MAGIC   WHERE language_is_supported
# MAGIC )
# MAGIC WHERE candidate_rank = 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE compute_selection_decisions_day20
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM compute_selection_decisions_view_day20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT workload_id, workload_name, compute_option, total_score, decision_reason
# MAGIC FROM compute_selection_decisions_day20
# MAGIC ORDER BY workload_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `wl-001` -> `serverless_notebook_or_job`.
# MAGIC - `wl-002` -> `serverless_notebook_or_job`.
# MAGIC - `wl-003` -> `serverless_sql_warehouse`.
# MAGIC - `wl-004` -> `classic_jobs_compute`.
# MAGIC - `wl-005` -> `serverless_lakeflow_pipeline`.
# MAGIC - `wl-006` -> `serverless_sql_warehouse`.
# MAGIC
# MAGIC Operational meaning: serverless is often the default, SQL warehouses serve SQL concurrency, classic jobs compute is justified by custom settings, and all-purpose compute should not silently become production infrastructure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Define Compute Policy Guardrails
# MAGIC
# MAGIC Purpose: model the controls that keep compute choices from becoming cost, security, or reliability problems.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE compute_policy_controls_day20 (
# MAGIC   policy_id STRING,
# MAGIC   applies_to_compute STRING,
# MAGIC   control_name STRING,
# MAGIC   control_type STRING,
# MAGIC   expected_value STRING,
# MAGIC   severity STRING,
# MAGIC   reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO compute_policy_controls_day20 VALUES
# MAGIC   ('policy-serverless-usage-day20', 'serverless_notebook_or_job', 'cost_center_tag_required', 'fixed_tag', 'cost_center', 'HIGH', 'Cost attribution is mandatory for serverless usage'),
# MAGIC   ('policy-serverless-usage-day20', 'serverless_lakeflow_pipeline', 'owner_tag_required', 'fixed_tag', 'owner_group', 'HIGH', 'Pipeline ownership must be visible during incident review'),
# MAGIC   ('policy-job-standard-day20', 'classic_jobs_compute', 'max_workers', 'range', '1-16', 'HIGH', 'Limit accidental large job clusters'),
# MAGIC   ('policy-job-standard-day20', 'classic_jobs_compute', 'runtime_lts_required', 'allowlist', 'LTS runtimes only', 'MEDIUM', 'Reduce runtime drift and dependency surprises'),
# MAGIC   ('policy-job-standard-day20', 'classic_jobs_compute', 'auto_termination_minutes', 'fixed', '20', 'HIGH', 'Avoid idle cluster cost'),
# MAGIC   ('policy-all-purpose-dev-day20', 'classic_all_purpose_compute', 'production_jobs_allowed', 'fixed', 'false', 'HIGH', 'Do not run production jobs on shared interactive clusters'),
# MAGIC   ('policy-all-purpose-dev-day20', 'classic_all_purpose_compute', 'max_workers', 'range', '1-4', 'MEDIUM', 'Keep development clusters bounded'),
# MAGIC   ('policy-sql-warehouse-day20', 'serverless_sql_warehouse', 'auto_stop_minutes', 'fixed', '10', 'MEDIUM', 'Avoid idle SQL warehouse cost'),
# MAGIC   ('policy-sql-warehouse-day20', 'serverless_sql_warehouse', 'max_clusters', 'range', '1-4', 'HIGH', 'Bound concurrency scale-out cost'),
# MAGIC   ('policy-sql-warehouse-day20', 'pro_sql_warehouse', 'query_history_review', 'process', 'weekly', 'MEDIUM', 'Detect queueing, expensive queries, and missing optimization');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compute_policy_controls_day20 ORDER BY policy_id, applies_to_compute, control_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.workload_id,
# MAGIC   d.workload_name,
# MAGIC   d.compute_option,
# MAGIC   COUNT(p.control_name) AS guardrail_count,
# MAGIC   SUM(CASE WHEN p.severity = 'HIGH' THEN 1 ELSE 0 END) AS high_severity_guardrails
# MAGIC FROM compute_selection_decisions_day20 d
# MAGIC LEFT JOIN compute_policy_controls_day20 p
# MAGIC   ON d.compute_option = p.applies_to_compute
# MAGIC GROUP BY d.workload_id, d.workload_name, d.compute_option
# MAGIC ORDER BY d.workload_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Every selected compute option should have at least one guardrail.
# MAGIC - Classic jobs compute should show worker/runtime/autotermination controls.
# MAGIC - SQL warehouse choices should show auto-stop and max-cluster controls.
# MAGIC - Serverless choices should show usage attribution and ownership controls.
# MAGIC
# MAGIC Operational meaning: compute policy is how platform teams convert architecture decisions into enforceable defaults.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Triage Compute Incidents
# MAGIC
# MAGIC Purpose: connect symptoms to likely compute actions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE compute_incidents_day20 (
# MAGIC   incident_id STRING,
# MAGIC   workload_id STRING,
# MAGIC   observed_queue_seconds INT,
# MAGIC   startup_seconds INT,
# MAGIC   runtime_minutes INT,
# MAGIC   estimated_cost_units DECIMAL(10,2),
# MAGIC   shuffle_spill_gb DECIMAL(10,2),
# MAGIC   skew_ratio DECIMAL(10,2),
# MAGIC   oom_events INT,
# MAGIC   failed_tasks INT,
# MAGIC   symptom STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO compute_incidents_day20 VALUES
# MAGIC   ('ci-001', 'wl-003', 240, 12, 7, CAST(18.50 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(1.20 AS DECIMAL(10,2)), 0, 0, 'dashboard users report slow query start'),
# MAGIC   ('ci-002', 'wl-002', 0, 540, 38, CAST(12.25 AS DECIMAL(10,2)), CAST(4.50 AS DECIMAL(10,2)), CAST(2.10 AS DECIMAL(10,2)), 0, 0, 'nightly job misses SLA because cluster startup dominates'),
# MAGIC   ('ci-003', 'wl-004', 0, 300, 260, CAST(96.00 AS DECIMAL(10,2)), CAST(280.00 AS DECIMAL(10,2)), CAST(38.00 AS DECIMAL(10,2)), 0, 12, 'backfill has large shuffle spill and task skew'),
# MAGIC   ('ci-004', 'wl-001', 0, 0, 45, CAST(42.00 AS DECIMAL(10,2)), CAST(0.00 AS DECIMAL(10,2)), CAST(1.00 AS DECIMAL(10,2)), 0, 0, 'interactive cluster left running after notebook use'),
# MAGIC   ('ci-005', 'wl-005', 0, 15, 1440, CAST(70.00 AS DECIMAL(10,2)), CAST(12.00 AS DECIMAL(10,2)), CAST(4.50 AS DECIMAL(10,2)), 3, 8, 'pipeline has repeated memory failures');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE compute_incident_triage_day20
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   i.incident_id,
# MAGIC   i.workload_id,
# MAGIC   w.workload_name,
# MAGIC   d.compute_option,
# MAGIC   i.symptom,
# MAGIC   CASE
# MAGIC     WHEN i.observed_queue_seconds >= 120 THEN 'WAREHOUSE_QUEUEING_OR_UNDERSIZED_CAPACITY'
# MAGIC     WHEN i.startup_seconds >= 300 THEN 'COLD_START_OR_WRONG_COMPUTE_FOR_SLA'
# MAGIC     WHEN i.shuffle_spill_gb >= 100 OR i.skew_ratio >= 20 THEN 'SPARK_SHUFFLE_SKEW_OR_PARTITIONING_PROBLEM'
# MAGIC     WHEN i.estimated_cost_units >= 40 AND w.workload_type = 'notebook_interactive' THEN 'IDLE_INTERACTIVE_COMPUTE_COST'
# MAGIC     WHEN i.oom_events > 0 THEN 'MEMORY_PRESSURE_OR_STATE_SIZE_PROBLEM'
# MAGIC     ELSE 'NEEDS_DEEPER_REVIEW'
# MAGIC   END AS likely_root_cause,
# MAGIC   CASE
# MAGIC     WHEN i.observed_queue_seconds >= 120 THEN 'Review SQL warehouse size, max clusters, query concurrency, and query history'
# MAGIC     WHEN i.startup_seconds >= 300 THEN 'Prefer serverless where supported or move scheduled jobs to job-appropriate compute'
# MAGIC     WHEN i.shuffle_spill_gb >= 100 OR i.skew_ratio >= 20 THEN 'Inspect Spark UI stages; fix joins, partitioning, skew, and file layout before only scaling up'
# MAGIC     WHEN i.estimated_cost_units >= 40 AND w.workload_type = 'notebook_interactive' THEN 'Enforce autotermination, usage tags, and serverless notebooks where supported'
# MAGIC     WHEN i.oom_events > 0 THEN 'Inspect state size, executor memory, partitions, and pipeline expectations'
# MAGIC     ELSE 'Collect run history, Spark UI evidence, and query profile'
# MAGIC   END AS next_action
# MAGIC FROM compute_incidents_day20 i
# MAGIC JOIN workload_profiles_day20 w
# MAGIC   ON i.workload_id = w.workload_id
# MAGIC LEFT JOIN compute_selection_decisions_day20 d
# MAGIC   ON i.workload_id = d.workload_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compute_incident_triage_day20 ORDER BY incident_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `ci-001` -> warehouse queueing or undersized capacity.
# MAGIC - `ci-002` -> cold start or wrong compute for SLA.
# MAGIC - `ci-003` -> Spark shuffle/skew/partitioning problem.
# MAGIC - `ci-004` -> idle interactive compute cost.
# MAGIC - `ci-005` -> memory pressure or state size problem.
# MAGIC
# MAGIC Operational meaning: do not solve every slow run by buying bigger compute. First classify queueing, startup, skew, spill, memory, and idle-cost symptoms.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Checks
# MAGIC
# MAGIC Purpose: verify the complete Day 20 evidence set.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'workload_profiles' AS check_name, COUNT(*) AS observed_value FROM workload_profiles_day20
# MAGIC UNION ALL
# MAGIC SELECT 'compute_options', COUNT(*) FROM compute_options_day20
# MAGIC UNION ALL
# MAGIC SELECT 'candidate_matrix_rows', COUNT(*) FROM compute_candidate_matrix_day20
# MAGIC UNION ALL
# MAGIC SELECT 'compute_decisions', COUNT(*) FROM compute_selection_decisions_day20
# MAGIC UNION ALL
# MAGIC SELECT 'policy_controls', COUNT(*) FROM compute_policy_controls_day20
# MAGIC UNION ALL
# MAGIC SELECT 'compute_incidents', COUNT(*) FROM compute_incidents_day20
# MAGIC UNION ALL
# MAGIC SELECT 'incident_triage_rows', COUNT(*) FROM compute_incident_triage_day20;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT compute_option, COUNT(*) AS selected_workloads
# MAGIC FROM compute_selection_decisions_day20
# MAGIC GROUP BY compute_option
# MAGIC ORDER BY selected_workloads DESC, compute_option;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY compute_selection_decisions_day20;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 workload profiles.
# MAGIC - 7 compute options.
# MAGIC - 6 compute decisions.
# MAGIC - 10 policy controls.
# MAGIC - 5 compute incidents.
# MAGIC - 5 incident triage rows.
# MAGIC
# MAGIC Operational meaning: a production compute recommendation should leave behind workload evidence, option comparison, decision reason, guardrails, incident model, and Delta history.
