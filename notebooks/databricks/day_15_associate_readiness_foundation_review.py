# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 15 - Associate Readiness Review: Foundation And Bridge Checkpoint
# MAGIC
# MAGIC Goal: review the first segment as an executable readiness lab: objective coverage, scenario decisions, correction loop, PySpark scoring, operational runbook, and next drills.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: Databricks platform, ingestion/loading, transformation/modeling, Lakeflow Jobs, CI/CD, troubleshooting/monitoring/optimization, governance/security.
# MAGIC - Professional stretch: production evidence, incident response, deployment gates, auditability, and durable runbooks.
# MAGIC
# MAGIC Note: this is a review day. It intentionally starts with two weak starter answers so you can see how readiness gaps are detected and corrected.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Build The Certification Coverage Matrix
# MAGIC
# MAGIC Purpose: map Days 1-14 to the Associate objectives and Professional operating angles.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE associate_objective_coverage_day15 (
# MAGIC   objective_id STRING,
# MAGIC   associate_objective STRING,
# MAGIC   exam_weight_percent INT,
# MAGIC   days_covered ARRAY<STRING>,
# MAGIC   hands_on_evidence STRING,
# MAGIC   professional_extension STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO associate_objective_coverage_day15 VALUES
# MAGIC   (
# MAGIC     'OBJ-01',
# MAGIC     'Databricks Intelligence Platform',
# MAGIC     6,
# MAGIC     array('Day 1', 'Day 2', 'Day 10', 'Day 11'),
# MAGIC     'DataFrame vs Delta vs SQL table; table history; platform object hierarchy; managed/external/governed surfaces',
# MAGIC     'Explain platform object lifecycle, auditability, and where contracts live'
# MAGIC   ),
# MAGIC   (
# MAGIC     'OBJ-02',
# MAGIC     'Data Ingestion and Loading',
# MAGIC     21,
# MAGIC     array('Day 3', 'Day 7', 'Day 8', 'Day 12'),
# MAGIC     'Bronze ingestion, CDC envelope, replay/backfill safety, tombstones, COPY INTO-style and Auto Loader-style checkpoints',
# MAGIC     'Design replay-safe ingestion with source offsets, file checkpoints, quarantine, and business-key idempotency'
# MAGIC   ),
# MAGIC   (
# MAGIC     'OBJ-03',
# MAGIC     'Data Transformation and Modeling',
# MAGIC     22,
# MAGIC     array('Day 4', 'Day 6', 'Day 7', 'Day 9'),
# MAGIC     'Fact grain, medallion promotion, silver/gold outputs, schema compatibility, replay-safe publication',
# MAGIC     'Separate event grain, current-state grain, aggregate grain, and publication contracts'
# MAGIC   ),
# MAGIC   (
# MAGIC     'OBJ-04',
# MAGIC     'Working with Lakeflow Jobs',
# MAGIC     16,
# MAGIC     array('Day 10', 'Day 13'),
# MAGIC     'Job DAG, task dependencies, run-if behavior, retries, run history, repair scope',
# MAGIC     'Triage first failed task, determine downstream validity, repair only needed tasks'
# MAGIC   ),
# MAGIC   (
# MAGIC     'OBJ-05',
# MAGIC     'Implementing CI/CD',
# MAGIC     10,
# MAGIC     array('Day 10', 'Day 14'),
# MAGIC     'Deployment decision gates, bundle targets, bundle validation, drift checks, rollback evidence',
# MAGIC     'Promote only with validation, tests, approval, no drift, and rollback path'
# MAGIC   ),
# MAGIC   (
# MAGIC     'OBJ-06',
# MAGIC     'Troubleshooting, Monitoring, and Optimization',
# MAGIC     10,
# MAGIC     array('Day 2', 'Day 5', 'Day 12', 'Day 13', 'Day 14'),
# MAGIC     'DESCRIBE HISTORY, publication gates, checkpoint counts, quarantine counts, task failures, deployment drift',
# MAGIC     'Use operational evidence before deciding repair, rollback, or forward fix'
# MAGIC   ),
# MAGIC   (
# MAGIC     'OBJ-07',
# MAGIC     'Governance and Security',
# MAGIC     15,
# MAGIC     array('Day 5', 'Day 10', 'Day 11', 'Day 14'),
# MAGIC     'Ownership gates, consumer visibility, row filtering, masking, PII access decisions, prod service-principal controls',
# MAGIC     'Implement least privilege, explainable access decisions, and auditable release ownership'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   objective_id,
# MAGIC   associate_objective,
# MAGIC   exam_weight_percent,
# MAGIC   concat_ws(', ', days_covered) AS days_covered,
# MAGIC   hands_on_evidence
# MAGIC FROM associate_objective_coverage_day15
# MAGIC ORDER BY objective_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All 7 Associate objective areas have at least one hands-on lab.
# MAGIC - Heaviest areas are ingestion/loading and transformation/modeling.
# MAGIC
# MAGIC Operational meaning: readiness is evidence-based. A topic is not covered until you can point to a lab artifact and production decision.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create Scenario Questions
# MAGIC
# MAGIC Purpose: convert the first 14 days into scenario decisions like the ones you need in certification and production reviews.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE readiness_scenarios_day15 (
# MAGIC   scenario_id STRING,
# MAGIC   objective_id STRING,
# MAGIC   scenario_text STRING,
# MAGIC   expected_decision STRING,
# MAGIC   expected_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO readiness_scenarios_day15 VALUES
# MAGIC   (
# MAGIC     'SC-001',
# MAGIC     'OBJ-03',
# MAGIC     'A source adds nullable coupon_code to an order event. Existing consumers do not use it yet.',
# MAGIC     'APPROVE_COMPATIBLE_SCHEMA',
# MAGIC     'Nullable additive fields are usually compatible when the core contract remains valid.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-002',
# MAGIC     'OBJ-02',
# MAGIC     'Two different files contain the same event_id for an order. File checkpoints show both files were loaded once.',
# MAGIC     'DEDUPE_IN_SILVER',
# MAGIC     'File idempotency prevents duplicate file loads, but business-key idempotency belongs in downstream processing.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-003',
# MAGIC     'OBJ-06',
# MAGIC     'A bad overwrite changed a Delta table. You need to know who changed it and whether you can query the previous version.',
# MAGIC     'USE_HISTORY_AND_TIME_TRAVEL',
# MAGIC     'DESCRIBE HISTORY identifies operations and versions; time travel can query an earlier table version within retention.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-004',
# MAGIC     'OBJ-05',
# MAGIC     'A prod bundle target runs as a named user and has no deployment lock.',
# MAGIC     'BLOCK_PROD_DEPLOY',
# MAGIC     'Prod should use stable service-principal identity and deployment safety controls.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-005',
# MAGIC     'OBJ-04',
# MAGIC     'A Lakeflow Job failed at validate_bronze. Ingest succeeded; silver and gold were skipped.',
# MAGIC     'REPAIR_FAILED_AND_DOWNSTREAM',
# MAGIC     'Repair validation and downstream tasks instead of blindly rerunning successful ingestion.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-006',
# MAGIC     'OBJ-07',
# MAGIC     'A regional analyst requests direct access to a base table containing email addresses.',
# MAGIC     'DENY_BASE_USE_MASKED_VIEW',
# MAGIC     'Use governed views with row filters and masking; deny direct base PII access unless justified.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-007',
# MAGIC     'OBJ-02',
# MAGIC     'Millions of files arrive continuously in cloud storage and directory listing is becoming expensive.',
# MAGIC     'CHOOSE_AUTO_LOADER',
# MAGIC     'Auto Loader-style discovery and checkpoints fit high-scale continuous file ingestion better than repeated batch listing.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'SC-008',
# MAGIC     'OBJ-03',
# MAGIC     'A backfill recomputes gold metrics for a prior month. Consumers must not see half-published results.',
# MAGIC     'PIN_INPUT_AND_ATOMICALLY_PUBLISH',
# MAGIC     'Backfills need pinned inputs, isolated outputs, quality checks, and atomic publication.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT scenario_id, associate_objective, scenario_text
# MAGIC FROM readiness_scenarios_day15 s
# MAGIC JOIN associate_objective_coverage_day15 o
# MAGIC   ON s.objective_id = o.objective_id
# MAGIC ORDER BY scenario_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 8 scenarios spanning ingestion, transformation, troubleshooting, jobs, CI/CD, and governance.
# MAGIC
# MAGIC Operational meaning: the certification question usually asks for the safest next action, not just a definition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Score Starter Answers
# MAGIC
# MAGIC Purpose: intentionally score a starter answer set with two mistakes so the readiness gap is visible.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE learner_answers_day15 (
# MAGIC   scenario_id STRING,
# MAGIC   learner_decision STRING,
# MAGIC   learner_reason STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO learner_answers_day15 VALUES
# MAGIC   ('SC-001', 'APPROVE_COMPATIBLE_SCHEMA', 'Nullable additive field is compatible.'),
# MAGIC   ('SC-002', 'TRUST_BRONZE_FILE_CHECKPOINT', 'Both files were loaded exactly once.'),
# MAGIC   ('SC-003', 'USE_HISTORY_AND_TIME_TRAVEL', 'Delta history and version queries answer this.'),
# MAGIC   ('SC-004', 'APPROVE_PROD_DEPLOY', 'The bundle has approval.'),
# MAGIC   ('SC-005', 'REPAIR_FAILED_AND_DOWNSTREAM', 'Ingest succeeded, so repair validation and downstream tasks.'),
# MAGIC   ('SC-006', 'DENY_BASE_USE_MASKED_VIEW', 'Regional analyst should use governed view.'),
# MAGIC   ('SC-007', 'CHOOSE_AUTO_LOADER', 'Continuous high-file-count ingestion needs scalable discovery.'),
# MAGIC   ('SC-008', 'PIN_INPUT_AND_ATOMICALLY_PUBLISH', 'Backfill outputs should not be half visible.');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW scenario_score_day15 AS
# MAGIC SELECT
# MAGIC   s.scenario_id,
# MAGIC   s.objective_id,
# MAGIC   o.associate_objective,
# MAGIC   a.learner_decision,
# MAGIC   s.expected_decision,
# MAGIC   CASE WHEN a.learner_decision = s.expected_decision THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   s.expected_reason
# MAGIC FROM readiness_scenarios_day15 s
# MAGIC JOIN associate_objective_coverage_day15 o
# MAGIC   ON s.objective_id = o.objective_id
# MAGIC LEFT JOIN learner_answers_day15 a
# MAGIC   ON s.scenario_id = a.scenario_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   scenario_id,
# MAGIC   associate_objective,
# MAGIC   learner_decision,
# MAGIC   expected_decision,
# MAGIC   outcome,
# MAGIC   expected_reason
# MAGIC FROM scenario_score_day15
# MAGIC ORDER BY scenario_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT outcome, COUNT(*) AS scenario_count
# MAGIC FROM scenario_score_day15
# MAGIC GROUP BY outcome
# MAGIC ORDER BY outcome;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 6 `PASS`.
# MAGIC - 2 `FAIL`: duplicate business event and unsafe prod deploy.
# MAGIC
# MAGIC Operational meaning: two common traps are confusing file idempotency with business-key idempotency, and confusing approval with safe production configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Correct The Weak Answers
# MAGIC
# MAGIC Purpose: fix the two mistakes and re-score the readiness scenarios.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO learner_answers_day15 t
# MAGIC USING (
# MAGIC   SELECT 'SC-002' AS scenario_id, 'DEDUPE_IN_SILVER' AS learner_decision, 'Bronze file checkpoints are not business-key dedupe.' AS learner_reason
# MAGIC   UNION ALL
# MAGIC   SELECT 'SC-004' AS scenario_id, 'BLOCK_PROD_DEPLOY' AS learner_decision, 'Prod target lacks service principal and deployment lock.' AS learner_reason
# MAGIC ) s
# MAGIC ON t.scenario_id = s.scenario_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.learner_decision = s.learner_decision,
# MAGIC   t.learner_reason = s.learner_reason;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW scenario_score_after_fix_day15 AS
# MAGIC SELECT
# MAGIC   s.scenario_id,
# MAGIC   s.objective_id,
# MAGIC   o.associate_objective,
# MAGIC   a.learner_decision,
# MAGIC   s.expected_decision,
# MAGIC   CASE WHEN a.learner_decision = s.expected_decision THEN 'PASS' ELSE 'FAIL' END AS outcome,
# MAGIC   s.expected_reason
# MAGIC FROM readiness_scenarios_day15 s
# MAGIC JOIN associate_objective_coverage_day15 o
# MAGIC   ON s.objective_id = o.objective_id
# MAGIC LEFT JOIN learner_answers_day15 a
# MAGIC   ON s.scenario_id = a.scenario_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT outcome, COUNT(*) AS scenario_count
# MAGIC FROM scenario_score_after_fix_day15
# MAGIC GROUP BY outcome
# MAGIC ORDER BY outcome;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 8 `PASS`.
# MAGIC - 0 `FAIL`.
# MAGIC
# MAGIC Operational meaning: review is useful only if it changes the next decision you make.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Compute Objective Readiness With PySpark
# MAGIC
# MAGIC Purpose: aggregate scenario results by certification objective and convert them into readiness status.

# COMMAND ----------

from pyspark.sql import functions as F

coverage_df = spark.table("de_learning.associate_objective_coverage_day15")
score_df = spark.sql("SELECT * FROM scenario_score_after_fix_day15")

objective_score_df = (
    score_df
    .groupBy("objective_id")
    .agg(
        F.count("*").alias("scenario_count"),
        F.sum(F.when(F.col("outcome") == "PASS", 1).otherwise(0)).alias("passed_count")
    )
    .withColumn("pass_rate", F.round(F.col("passed_count") / F.col("scenario_count"), 2))
)

readiness_df = (
    coverage_df
    .select("objective_id", "associate_objective", "exam_weight_percent", "hands_on_evidence", "professional_extension")
    .join(objective_score_df, on="objective_id", how="left")
    .na.fill({"scenario_count": 0, "passed_count": 0, "pass_rate": 0.0})
    .withColumn(
        "readiness_status",
        F.when((F.col("scenario_count") >= 1) & (F.col("pass_rate") >= 1.0), F.lit("GREEN"))
         .when((F.col("scenario_count") >= 1) & (F.col("pass_rate") >= 0.75), F.lit("AMBER"))
         .otherwise(F.lit("RED"))
    )
    .select(
        "objective_id",
        "associate_objective",
        "exam_weight_percent",
        "scenario_count",
        "passed_count",
        "pass_rate",
        "readiness_status",
        "hands_on_evidence",
        "professional_extension"
    )
)

readiness_df.createOrReplaceTempView("objective_readiness_day15")
display(readiness_df.orderBy(F.desc("exam_weight_percent")))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `coverage_df` is the objective coverage table; `score_df` is the fixed scenario score view.
# MAGIC - `groupBy(...).agg(...)` is SQL `GROUP BY`.
# MAGIC - `F.count("*")` counts scenarios per objective.
# MAGIC - `F.sum(F.when(...))` counts passed scenarios.
# MAGIC - `withColumn("pass_rate", ...)` adds a computed readiness metric.
# MAGIC - `join(..., how="left")` keeps every objective even if no scenario exists.
# MAGIC - `.na.fill(...)` converts missing score values to zero.
# MAGIC - `createOrReplaceTempView(...)` makes the PySpark readiness table available to SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   readiness_status,
# MAGIC   COUNT(*) AS objective_count,
# MAGIC   SUM(exam_weight_percent) AS exam_weight_percent
# MAGIC FROM objective_readiness_day15
# MAGIC GROUP BY readiness_status
# MAGIC ORDER BY readiness_status;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Objectives with scenario coverage and all corrected answers are `GREEN`.
# MAGIC - Any objective without a scenario is visible for follow-up.
# MAGIC
# MAGIC Operational meaning: readiness is not just confidence. It is coverage plus correct decisions under scenario pressure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Build The Operational Runbook
# MAGIC
# MAGIC Purpose: map production questions to the evidence you should inspect.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE operational_runbook_day15 (
# MAGIC   production_question STRING,
# MAGIC   first_evidence_to_check STRING,
# MAGIC   likely_command_or_table STRING,
# MAGIC   safe_decision_pattern STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO operational_runbook_day15 VALUES
# MAGIC   ('Who changed this table?', 'Delta operation history', 'DESCRIBE HISTORY <table>', 'identify operation/version/user, then decide forward-fix vs time travel'),
# MAGIC   ('Can I query before the bad write?', 'Delta version history and retention', 'SELECT * FROM <table> VERSION AS OF <n>', 'use time travel if retained; do not overwrite evidence blindly'),
# MAGIC   ('Did this rerun duplicate data?', 'file checkpoint plus business key counts', 'checkpoint table, duplicate event_id/order_id query', 'separate file idempotency from business-key idempotency'),
# MAGIC   ('Can this schema change publish?', 'contract compatibility result', 'schema comparison table or compatibility gate', 'approve nullable additive fields; block breaking semantic changes'),
# MAGIC   ('Why did the job fail?', 'first failed task', 'Lakeflow Jobs run history / task run output', 'repair failed task and downstream dependents'),
# MAGIC   ('Can this bundle deploy to prod?', 'validation, drift, tests, approval, rollback', 'bundle validate, drift table, release gate', 'promote only when all evidence is clean'),
# MAGIC   ('Can this analyst see PII?', 'role, region, and masking policy', 'Unity Catalog grants/views or access decision table', 'prefer governed masked view over direct base table access'),
# MAGIC   ('Can the backfill publish?', 'input snapshot, quality evidence, output isolation', 'publication gate, history, quality summary', 'publish atomically only after checks pass');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM operational_runbook_day15 ORDER BY production_question;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 8 common production questions mapped to concrete evidence.
# MAGIC
# MAGIC Operational meaning: production data engineering is answering operational questions with durable evidence, not memory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Create The Day 15 Readiness Checkpoint
# MAGIC
# MAGIC Purpose: summarize strengths, weak spots, and next drills.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE readiness_checkpoint_day15
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   objective_id,
# MAGIC   associate_objective,
# MAGIC   exam_weight_percent,
# MAGIC   readiness_status,
# MAGIC   CASE
# MAGIC     WHEN associate_objective = 'Data Transformation and Modeling' THEN 'Add more joins, windows, dedupe, explode, and aggregate PySpark drills'
# MAGIC     WHEN associate_objective = 'Databricks Intelligence Platform' THEN 'Start Days 16-25 with compute, catalogs, schemas, managed vs external tables'
# MAGIC     WHEN associate_objective = 'Working with Lakeflow Jobs' THEN 'Practice real job UI/API runs when available'
# MAGIC     WHEN associate_objective = 'Implementing CI/CD' THEN 'Create an actual minimal databricks.yml bundle outside Databricks next'
# MAGIC     ELSE 'Keep using scenario questions and evidence-first debugging'
# MAGIC   END AS next_drill
# MAGIC FROM objective_readiness_day15;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM readiness_checkpoint_day15 ORDER BY exam_weight_percent DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - A compact Day 15 checkpoint with next drills by objective.
# MAGIC
# MAGIC Operational meaning: after a review, the output should be a prioritized next practice list.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Final Review Checks
# MAGIC
# MAGIC Purpose: verify that coverage, correction, readiness, and runbook artifacts exist.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'associate_objectives' AS check_name, COUNT(*) AS observed_value FROM associate_objective_coverage_day15
# MAGIC UNION ALL
# MAGIC SELECT 'readiness_scenarios', COUNT(*) FROM readiness_scenarios_day15
# MAGIC UNION ALL
# MAGIC SELECT 'starter_failures', COUNT(*) FROM scenario_score_day15 WHERE outcome = 'FAIL'
# MAGIC UNION ALL
# MAGIC SELECT 'final_failures_after_fix', COUNT(*) FROM scenario_score_after_fix_day15 WHERE outcome = 'FAIL'
# MAGIC UNION ALL
# MAGIC SELECT 'operational_runbook_rows', COUNT(*) FROM operational_runbook_day15
# MAGIC UNION ALL
# MAGIC SELECT 'readiness_checkpoint_rows', COUNT(*) FROM readiness_checkpoint_day15;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY readiness_checkpoint_day15;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 7 Associate objectives.
# MAGIC - 8 readiness scenarios.
# MAGIC - 2 starter failures.
# MAGIC - 0 final failures after correction.
# MAGIC - 8 runbook rows.
# MAGIC - 7 readiness checkpoint rows.
# MAGIC
# MAGIC Operational meaning: Day 15 closes the first segment with evidence, not just a feeling that the basics are covered.
