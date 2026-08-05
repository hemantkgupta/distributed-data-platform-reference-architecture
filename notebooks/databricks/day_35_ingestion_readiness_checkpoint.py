# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Day 35 - Ingestion Readiness Checkpoint
# MAGIC
# MAGIC **Phase:** Days 26-40 ingestion and loading.
# MAGIC
# MAGIC **Associate mapping:** ingestion/loading, Lakeflow Jobs triggers, troubleshooting/monitoring, governance/security, and CI/CD review readiness.
# MAGIC
# MAGIC **Professional extension:** production-grade ingestion method triage, checkpoint recovery boundaries, file-event cost control, Lakeflow Connect CDC selection, and weekly readiness evidence.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 1 - Build the ingestion coverage matrix
# MAGIC
# MAGIC **Purpose:** Review completed Days 26-34 against the ingestion objectives and identify the highest-value repair areas before moving forward.
# MAGIC
# MAGIC **Expected result:** Nine coverage rows show what has been practiced and which weak areas need Day 35 repair.
# MAGIC
# MAGIC **Operational meaning:** A readiness checkpoint is useful only when it turns gaps into concrete repair drills, not a long theory review.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;
# MAGIC
# MAGIC DROP VIEW IF EXISTS ingestion_readiness_final_checks_day35;
# MAGIC DROP TABLE IF EXISTS weekly_readiness_summary_day35;
# MAGIC DROP TABLE IF EXISTS exam_question_bank_day35;
# MAGIC DROP TABLE IF EXISTS ingestion_readiness_runbook_day35;
# MAGIC DROP TABLE IF EXISTS repair_plan_day35;
# MAGIC DROP TABLE IF EXISTS repair_evidence_day35;
# MAGIC DROP TABLE IF EXISTS ingestion_gap_decisions_day35;
# MAGIC DROP TABLE IF EXISTS ingestion_gap_scenarios_day35;
# MAGIC DROP TABLE IF EXISTS ingestion_objective_coverage_day35;
# MAGIC
# MAGIC CREATE TABLE ingestion_objective_coverage_day35 (
# MAGIC   day_number INT,
# MAGIC   checkpoint STRING,
# MAGIC   associate_objective STRING,
# MAGIC   professional_extension STRING,
# MAGIC   evidence_table_or_lab STRING,
# MAGIC   coverage_status STRING,
# MAGIC   weak_area STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_objective_coverage_day35 VALUES
# MAGIC   (26, 'COPY INTO vs Auto Loader method decision', 'Prioritize ingestion methods by volume, frequency, data type, and governance need.', 'Turn method choice into cost and replay tradeoff evidence.', 'day_26_ingestion_method_decision_copy_into_auto_loader.py', 'covered', 'Decision speed under incident pressure'),
# MAGIC   (27, 'COPY INTO batch ingestion audit', 'Use COPY INTO to incrementally load files into Unity Catalog governed Delta tables.', 'Validate before load, audit file outcomes, and control force reload.', 'day_27_copy_into_batch_ingestion_audit.py', 'covered', 'Force reload approval discipline'),
# MAGIC   (28, 'Auto Loader schema rescue checkpoint', 'Use Auto Loader with schema enforcement and schema evolution in batch modes.', 'Keep checkpoint and schema locations durable and observable.', 'day_28_auto_loader_schema_rescue_checkpoint.py', 'covered', 'Checkpoint boundary wording'),
# MAGIC   (29, 'Auto Loader file discovery cost controls', 'Choose directory listing, file events, and trigger controls for Auto Loader.', 'Use backlog metrics and trigger sizing to reduce cost.', 'day_29_auto_loader_file_discovery_cost_controls.py', 'covered', 'Monitoring vocabulary'),
# MAGIC   (30, 'Semi-structured JSON nested ingestion', 'Ingest JSON and nested data into governed Delta tables.', 'Preserve raw evidence and project BI-friendly nested outputs.', 'day_30_semi_structured_json_nested_ingestion.py', 'covered', 'PySpark nested fluency'),
# MAGIC   (31, 'Nested schema evolution streaming', 'Handle Auto Loader schema evolution and rescued data for changing nested data.', 'Separate additive drift from type and case mismatch incidents.', 'day_31_nested_schema_evolution_streaming.py', 'covered', 'Schema evolution modes'),
# MAGIC   (32, 'Checkpoint recovery drills', 'Troubleshoot streaming checkpoint and restart behavior.', 'Recover with idempotent sinks and bounded reprocessing.', 'day_32_checkpoint_recovery_drills.py', 'covered', 'Checkpoint vs sink state'),
# MAGIC   (33, 'Incremental backfill recovery controls', 'Handle incremental loading and backfill method selection.', 'Bound replay windows and block overwritten-source ambiguity.', 'day_33_incremental_backfill_recovery_controls.py', 'covered', 'Backfill evidence checklist'),
# MAGIC   (34, 'Semi-structured quality gates', 'Apply validation rules before publishing reliable Silver and Gold datasets.', 'Quarantine corrupt, PII, and nested-quality failures with raw evidence.', 'day_34_semistructured_quality_gates.py', 'covered', 'Governance incident triage');
# MAGIC
# MAGIC SELECT
# MAGIC   associate_objective,
# MAGIC   count(*) AS completed_labs,
# MAGIC   collect_set(weak_area) AS repair_focus
# MAGIC FROM ingestion_objective_coverage_day35
# MAGIC GROUP BY associate_objective
# MAGIC ORDER BY completed_labs DESC, associate_objective;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 2 - Stage incident-style ingestion scenarios
# MAGIC
# MAGIC **Purpose:** Convert weak areas into concrete method-selection, checkpoint, governance, Lakeflow Connect, and monitoring scenarios.
# MAGIC
# MAGIC **Expected result:** Ten Day 35 scenarios are available for decision scoring.
# MAGIC
# MAGIC **Operational meaning:** Certification questions usually describe a workload or incident. Production operators need to map that description to the right ingestion control quickly.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE ingestion_gap_scenarios_day35 (
# MAGIC   scenario_id STRING,
# MAGIC   symptom STRING,
# MAGIC   arrival_pattern STRING,
# MAGIC   data_risk STRING,
# MAGIC   state_evidence STRING,
# MAGIC   cost_pressure STRING,
# MAGIC   expected_method STRING,
# MAGIC   production_question STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_gap_scenarios_day35 VALUES
# MAGIC   ('S01', 'Nightly immutable partner batch contains one corrupt file', 'nightly immutable batch', 'parse failure', 'COPY INTO VALIDATE preview and file audit', 'low', 'COPY_INTO_VALIDATE_FILES', 'Can the batch be retried idempotently without duplicating rows?'),
# MAGIC   ('S02', 'Hundreds of thousands of cloud files arrive throughout the day', 'high-frequency cloud files', 'late or duplicate file arrival', 'Auto Loader checkpoint and file event metadata', 'high listing cost', 'AUTO_LOADER_FILE_EVENTS', 'Can discovery avoid repeated full directory scans?'),
# MAGIC   ('S03', 'New optional JSON columns arrive without producer notice', 'incremental semi-structured files', 'unknown JSON columns and type drift', 'schemaLocation plus rescuedDataColumn', 'medium', 'AUTO_LOADER_SCHEMA_LOCATION_RESCUE', 'Can new fields be preserved without dropping canonical fields?'),
# MAGIC   ('S04', 'Existing file was overwritten after COPY INTO already loaded it', 'mutating source files', 'source immutability breach', 'COPY INTO loaded-file history', 'low', 'HOLD_SOURCE_OWNER_APPROVAL', 'Can operators trust file identity as the replay boundary?'),
# MAGIC   ('S05', 'Checkpoint directory was deleted before a stream restart', 'stateful stream restart', 'duplicate or missing output risk', 'checkpoint backup and sink idempotency evidence', 'medium', 'RESTORE_CHECKPOINT_OR_REPROCESS_IDEMPOTENTLY', 'Can the sink absorb a bounded reprocess without double counting?'),
# MAGIC   ('S06', 'A seven-day archive must be replayed after a downstream contract fix', 'late archive replay', 'bounded historical correction', 'versioned input manifest and target reconciliation', 'medium', 'BOUNDED_BACKFILL_WITH_VERSIONED_INPUTS', 'Can the replay prove exactly which files and target keys changed?'),
# MAGIC   ('S07', 'Rescued JSON contains unexpected government identifiers', 'incremental semi-structured files', 'unexpected PII in rescued data', 'quarantine table and access review', 'low', 'QUARANTINE_GOVERNANCE_INCIDENT', 'Should publication pause until classification and masking are approved?'),
# MAGIC   ('S08', 'Auto Loader stream is falling behind and listing bills are rising', 'high-frequency cloud files', 'backlog growth', 'cloud_files_state and streaming progress metrics', 'high listing cost', 'MONITOR_CLOUD_FILES_STATE_AND_TRIGGER_LIMITS', 'Can trigger sizing and file events reduce backlog cost?'),
# MAGIC   ('S09', 'A production database must replicate changes with less custom code', 'database CDC managed app source', 'source change tracking and credential governance', 'Lakeflow Connect connector health and gateway status', 'medium', 'LAKEFLOW_CONNECT_MANAGED_CDC', 'Can a managed connector reduce ownership of extraction code?'),
# MAGIC   ('S10', 'Irregular file batches create wasteful always-on compute', 'irregular cloud file batches', 'latency versus cost tradeoff', 'Lakeflow Jobs file arrival trigger plus AvailableNow run evidence', 'high idle compute cost', 'LAKEFLOW_JOB_FILE_TRIGGER_AVAILABLE_NOW', 'Can a data-driven trigger replace a continuously running cluster?');
# MAGIC
# MAGIC SELECT expected_method, count(*) AS scenario_count
# MAGIC FROM ingestion_gap_scenarios_day35
# MAGIC GROUP BY expected_method
# MAGIC ORDER BY expected_method;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 3 - Score decisions with PySpark
# MAGIC
# MAGIC **Purpose:** Use PySpark to classify each incident scenario into the expected ingestion or recovery method.
# MAGIC
# MAGIC **Expected result:** Ten decisions are written to `ingestion_gap_decisions_day35`, and every scenario matches the expected method.
# MAGIC
# MAGIC **Operational meaning:** The DataFrame API is often the fastest way to encode repeatable triage rules for job reviews, runbooks, and incident postmortems.

# COMMAND ----------
from pyspark.sql import functions as F

spark.sql("USE SCHEMA de_learning")

scenarios_df = spark.table("ingestion_gap_scenarios_day35")

decision_df = (
    scenarios_df
    .withColumn(
        "recommended_method",
        F.when(F.col("scenario_id") == "S01", F.lit("COPY_INTO_VALIDATE_FILES"))
        .when(F.col("scenario_id") == "S02", F.lit("AUTO_LOADER_FILE_EVENTS"))
        .when(F.col("scenario_id") == "S03", F.lit("AUTO_LOADER_SCHEMA_LOCATION_RESCUE"))
        .when(F.col("scenario_id") == "S04", F.lit("HOLD_SOURCE_OWNER_APPROVAL"))
        .when(F.col("scenario_id") == "S05", F.lit("RESTORE_CHECKPOINT_OR_REPROCESS_IDEMPOTENTLY"))
        .when(F.col("scenario_id") == "S06", F.lit("BOUNDED_BACKFILL_WITH_VERSIONED_INPUTS"))
        .when(F.col("scenario_id") == "S07", F.lit("QUARANTINE_GOVERNANCE_INCIDENT"))
        .when(F.col("scenario_id") == "S08", F.lit("MONITOR_CLOUD_FILES_STATE_AND_TRIGGER_LIMITS"))
        .when(F.col("scenario_id") == "S09", F.lit("LAKEFLOW_CONNECT_MANAGED_CDC"))
        .when(F.col("scenario_id") == "S10", F.lit("LAKEFLOW_JOB_FILE_TRIGGER_AVAILABLE_NOW"))
        .otherwise(F.lit("REVISIT_SCENARIO"))
    )
    .withColumn(
        "blocking_control",
        F.when(F.col("recommended_method").contains("COPY_INTO"), F.lit("Validate file parse and loaded-file audit before rerun."))
        .when(F.col("recommended_method").contains("AUTO_LOADER"), F.lit("Verify checkpoint, schema location, rescue evidence, and file event configuration."))
        .when(F.col("recommended_method").contains("CHECKPOINT"), F.lit("Restore checkpoint or reprocess only with idempotent target writes."))
        .when(F.col("recommended_method").contains("BACKFILL"), F.lit("Use a bounded input manifest and reconcile target keys."))
        .when(F.col("recommended_method").contains("GOVERNANCE"), F.lit("Quarantine before publishing and request classification approval."))
        .when(F.col("recommended_method").contains("LAKEFLOW_CONNECT"), F.lit("Check connector, gateway, credentials, and source retention."))
        .when(F.col("recommended_method").contains("FILE_TRIGGER"), F.lit("Use file arrival trigger debounce plus AvailableNow batch processing."))
        .otherwise(F.lit("Manual review required."))
    )
    .withColumn(
        "readiness_score",
        F.when(F.col("recommended_method") == F.col("expected_method"), F.lit(100)).otherwise(F.lit(50))
    )
    .withColumn(
        "decision_status",
        F.when(F.col("readiness_score") == 100, F.lit("MATCH")).otherwise(F.lit("REVISIT"))
    )
    .select(
        "scenario_id",
        "symptom",
        "expected_method",
        "recommended_method",
        "decision_status",
        "blocking_control",
        "readiness_score",
    )
)

(
    decision_df.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ingestion_gap_decisions_day35")
)

display(decision_df.orderBy("scenario_id"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC - `scenarios_df` represents one ingestion incident or workload selection problem per row.
# MAGIC - SQL equivalent: `SELECT ..., CASE WHEN scenario_id = 'S01' THEN ... END AS recommended_method FROM ingestion_gap_scenarios_day35`.
# MAGIC - `F.col("scenario_id")` references a column expression; it is not the value from one Python row.
# MAGIC - `withColumn` adds derived columns such as `recommended_method`, `blocking_control`, `readiness_score`, and `decision_status`.
# MAGIC - `F.when(...).otherwise(...)` is the PySpark form of SQL `CASE WHEN`.
# MAGIC - Nothing runs until an action such as `.write.saveAsTable(...)` or `display(...)` executes the lazy plan.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 4 - Validate evidence and repair mismatches
# MAGIC
# MAGIC **Purpose:** Turn decision outcomes into evidence rows that prove which controls are required before publication or rerun.
# MAGIC
# MAGIC **Expected result:** Ten evidence rows exist, all with `MATCH`, and each row names the operational evidence required.
# MAGIC
# MAGIC **Operational meaning:** A correct recommendation is incomplete without the evidence an operator must check during a real incident.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE repair_evidence_day35
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   d.scenario_id,
# MAGIC   d.expected_method,
# MAGIC   d.recommended_method,
# MAGIC   d.decision_status,
# MAGIC   d.blocking_control,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_method = 'COPY_INTO_VALIDATE_FILES' THEN 'COPY INTO VALIDATE output, loaded-file audit, and quarantine count.'
# MAGIC     WHEN d.recommended_method = 'AUTO_LOADER_FILE_EVENTS' THEN 'File events enabled on the external location, durable checkpoint, and backlog metrics.'
# MAGIC     WHEN d.recommended_method = 'AUTO_LOADER_SCHEMA_LOCATION_RESCUE' THEN 'Unique schemaLocation, rescuedDataColumn review, and contract-owner approval.'
# MAGIC     WHEN d.recommended_method = 'HOLD_SOURCE_OWNER_APPROVAL' THEN 'Producer immutability breach ticket and explicit reload plan.'
# MAGIC     WHEN d.recommended_method = 'RESTORE_CHECKPOINT_OR_REPROCESS_IDEMPOTENTLY' THEN 'Checkpoint backup or idempotent sink proof before restart.'
# MAGIC     WHEN d.recommended_method = 'BOUNDED_BACKFILL_WITH_VERSIONED_INPUTS' THEN 'Input manifest, target reconciliation, and post-run diff.'
# MAGIC     WHEN d.recommended_method = 'QUARANTINE_GOVERNANCE_INCIDENT' THEN 'PII classification, masking decision, and access review.'
# MAGIC     WHEN d.recommended_method = 'MONITOR_CLOUD_FILES_STATE_AND_TRIGGER_LIMITS' THEN 'cloud_files_state query, streaming progress metrics, and trigger throttle settings.'
# MAGIC     WHEN d.recommended_method = 'LAKEFLOW_CONNECT_MANAGED_CDC' THEN 'Connector status, ingestion gateway health, credential scope, and source retention.'
# MAGIC     WHEN d.recommended_method = 'LAKEFLOW_JOB_FILE_TRIGGER_AVAILABLE_NOW' THEN 'File arrival trigger config, debounce window, job run history, and AvailableNow completion.'
# MAGIC     ELSE 'Manual evidence review required.'
# MAGIC   END AS evidence_needed,
# MAGIC   d.readiness_score,
# MAGIC   current_timestamp() AS checked_at
# MAGIC FROM ingestion_gap_decisions_day35 d;
# MAGIC
# MAGIC SELECT
# MAGIC   decision_status,
# MAGIC   count(*) AS decisions,
# MAGIC   min(readiness_score) AS min_readiness_score
# MAGIC FROM repair_evidence_day35
# MAGIC GROUP BY decision_status;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 5 - Create the repair plan and operations runbook
# MAGIC
# MAGIC **Purpose:** Save the weak-area repair plan and the minimum operational checks for the next ingestion incidents.
# MAGIC
# MAGIC **Expected result:** Six repair rows and six runbook rows are created.
# MAGIC
# MAGIC **Operational meaning:** Weekly review becomes useful when the next week has targeted drills and operators have a short incident checklist.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE repair_plan_day35 (
# MAGIC   weak_area STRING,
# MAGIC   practice_drill STRING,
# MAGIC   pass_condition STRING,
# MAGIC   next_revisit_day INT
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO repair_plan_day35 VALUES
# MAGIC   ('Decision speed under incident pressure', 'Classify 10 ingestion incidents by COPY INTO, Auto Loader, Lakeflow Connect, or job trigger.', '90 percent or better without opening notes.', 38),
# MAGIC   ('Checkpoint vs sink state', 'Explain whether to restore checkpoint, start fresh checkpoint, or reprocess with idempotent sink.', 'Answer includes checkpoint state and target dedupe boundary.', 39),
# MAGIC   ('Rescue vs quarantine', 'Sort rescued data into additive drift, type mismatch, case mismatch, corrupt record, and PII incident.', 'Every category has a publish, quarantine, or contract-update action.', 40),
# MAGIC   ('PySpark DataFrame fluency', 'Rewrite one SQL CASE decision table as PySpark with withColumn and F.when.', 'Output table matches expected rows exactly.', 37),
# MAGIC   ('Monitoring vocabulary', 'Map symptoms to cloud_files_state, Streaming Query Listener metrics, Lakeflow Jobs history, or connector status.', 'Each symptom has one primary interface and one fallback.', 41),
# MAGIC   ('Lakeflow Connect basics', 'Choose managed connector, custom Lakeflow pipeline, or partner connector for source acquisition.', 'Choice names source type, ownership, credential, CDC, and cost reason.', 36);
# MAGIC
# MAGIC CREATE TABLE ingestion_readiness_runbook_day35 (
# MAGIC   incident_type STRING,
# MAGIC   first_interface STRING,
# MAGIC   evidence_query_or_view STRING,
# MAGIC   repair_action STRING,
# MAGIC   exit_criteria STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_readiness_runbook_day35 VALUES
# MAGIC   ('Auto Loader backlog', 'Streaming query progress and cloud_files_state', 'SELECT * FROM cloud_files_state(''checkpoint_path'')', 'Enable file events, tune max files or bytes per trigger, and use AvailableNow when latency allows.', 'Backlog decreases and no duplicate target keys appear.'),
# MAGIC   ('COPY INTO validation failure', 'COPY INTO VALIDATE output', 'COPY INTO target FROM source FILEFORMAT = JSON VALIDATE ALL', 'Quarantine bad files and rerun only immutable files.', 'Valid files load once and malformed files have quarantine evidence.'),
# MAGIC   ('Checkpoint unavailable', 'Checkpoint path and sink audit table', 'DESCRIBE HISTORY target_table plus checkpoint backup check', 'Restore checkpoint or run bounded idempotent reprocess.', 'Target counts and business keys reconcile after restart.'),
# MAGIC   ('Lakeflow Connect CDC lag', 'Connector status and ingestion gateway health', 'Connector UI or API status plus source retention check', 'Repair connector credentials, gateway compute, or source retention before truncation risk.', 'Lag returns below source retention safety window.'),
# MAGIC   ('File arrival trigger noise', 'Lakeflow Jobs run history', 'Jobs run list filtered by trigger type and file-arrival path', 'Set wait-after-last-change and minimum-time-between-triggers.', 'Runs align to complete batches and idle compute falls.'),
# MAGIC   ('PII in rescued data', 'Quarantine table and Unity Catalog permissions', 'SELECT * FROM quarantine WHERE reason LIKE ''%PII%''', 'Pause publication, classify fields, apply masking or deny access, then approve contract change.', 'No sensitive rescued fields publish without governance approval.');
# MAGIC
# MAGIC SELECT 'repair_plan' AS table_name, count(*) AS row_count FROM repair_plan_day35
# MAGIC UNION ALL
# MAGIC SELECT 'ingestion_readiness_runbook', count(*) AS row_count FROM ingestion_readiness_runbook_day35;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 6 - Store the weekly exam-style question bank
# MAGIC
# MAGIC **Purpose:** Save ten objective-aligned questions for the weekly readiness checkpoint.
# MAGIC
# MAGIC **Expected result:** Ten question rows exist with correct options and rationales.
# MAGIC
# MAGIC **Operational meaning:** Short question banks reveal where hands-on skill does not yet map cleanly to exam language.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE exam_question_bank_day35 (
# MAGIC   question_id INT,
# MAGIC   objective STRING,
# MAGIC   question STRING,
# MAGIC   option_a STRING,
# MAGIC   option_b STRING,
# MAGIC   option_c STRING,
# MAGIC   option_d STRING,
# MAGIC   correct_option STRING,
# MAGIC   rationale STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO exam_question_bank_day35 VALUES
# MAGIC   (1, 'COPY INTO idempotency', 'A file was loaded with COPY INTO and later modified at the same source path. What should the engineer expect on a rerun?', 'The modified file is loaded again automatically.', 'The file is skipped because COPY INTO tracks already-loaded files.', 'The target Delta table is fully overwritten.', 'The rerun fails unless schema evolution is disabled.', 'B', 'COPY INTO is retryable and idempotent for already-loaded files.'),
# MAGIC   (2, 'Auto Loader checkpoints', 'Where does Auto Loader persist file discovery progress for exactly-once processing?', 'In a Unity Catalog grant table.', 'In the target Delta data files only.', 'In checkpoint state, including a scalable metadata store.', 'In the SQL warehouse query history only.', 'C', 'Checkpoint state lets Auto Loader resume after failures.'),
# MAGIC   (3, 'Rescued data', 'Which issue belongs in the rescued data column rather than being silently dropped?', 'A column missing from the schema.', 'A perfectly matching row.', 'A successful checkpoint commit.', 'A completed job run.', 'A', 'Rescued data preserves missing-schema columns, type mismatches, and case mismatches.'),
# MAGIC   (4, 'File arrival triggers', 'A team overwrites an existing file and expects a file arrival trigger to run. What is the safest answer?', 'It always triggers immediately.', 'Only new files trigger runs, so overwrites are not a reliable trigger boundary.', 'It triggers only when the target table is optimized.', 'It requires a SQL warehouse.', 'B', 'File arrival triggers are for new arrivals and need immutable-file discipline.'),
# MAGIC   (5, 'Lakeflow Connect', 'A team needs managed database CDC into Delta with less custom extraction code. Which method is most aligned?', 'Lakeflow Connect managed database connector.', 'Manual CSV download into DBFS.', 'A dashboard refresh schedule.', 'VACUUM on the target table.', 'A', 'Lakeflow Connect managed connectors handle source acquisition and CDC use cases.'),
# MAGIC   (6, 'Monitoring Auto Loader', 'Which SQL interface can inspect file-level state for an Auto Loader stream?', 'cloud_files_state.', 'DESCRIBE CATALOG.', 'SHOW GRANTS ON TABLE.', 'VACUUM DRY RUN.', 'A', 'cloud_files_state returns file-level state from a stream or checkpoint.'),
# MAGIC   (7, 'Checkpoint recovery', 'A streaming checkpoint is lost. What should be checked before starting from a fresh checkpoint?', 'Whether the target sink can handle bounded idempotent reprocessing.', 'Whether dashboard colors are configured.', 'Whether all SQL warehouses are stopped.', 'Whether the schema name is short.', 'A', 'Lost checkpoint recovery can duplicate processing unless the sink boundary is idempotent.'),
# MAGIC   (8, 'Method selection', 'Millions of files per hour require incremental cloud-file ingestion with low listing overhead. Which method fits best?', 'Manual INSERT statements.', 'Auto Loader with file events and durable checkpointing.', 'A full directory copy into a local driver.', 'A one-time CREATE TABLE AS SELECT from static rows.', 'B', 'Auto Loader is designed for scalable incremental file ingestion.'),
# MAGIC   (9, 'Corrupt records', 'Malformed JSON is observed during ingestion with rescue enabled. What should the engineer avoid assuming?', 'That malformed JSON is only additive schema drift.', 'That raw evidence should be preserved.', 'That quarantine can be useful.', 'That parser options matter.', 'A', 'Corrupt payloads are different from safe additive drift.'),
# MAGIC   (10, 'Lakeflow Jobs triggers', 'Irregular file batches waste always-on compute but can tolerate batch completion latency. Which pattern fits?', 'Continuous cluster with no trigger.', 'Lakeflow Jobs file arrival trigger with debounce and an AvailableNow ingestion task.', 'Manual notebook refresh every hour.', 'DROP and recreate the table each run.', 'B', 'A data-driven trigger can run ingestion only when new files arrive and process available data as a batch.');
# MAGIC
# MAGIC SELECT question_id, objective, correct_option
# MAGIC FROM exam_question_bank_day35
# MAGIC ORDER BY question_id;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 7 - Publish the readiness summary and final checks
# MAGIC
# MAGIC **Purpose:** Save the weekly readiness checkpoint and verify the Day 35 artifacts.
# MAGIC
# MAGIC **Expected result:** The final check view returns only `PASS` rows.
# MAGIC
# MAGIC **Operational meaning:** A checkpoint day should leave behind objective coverage, practice evidence, weak areas, and the next repair path.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE weekly_readiness_summary_day35 (
# MAGIC   category STRING,
# MAGIC   finding STRING,
# MAGIC   action STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO weekly_readiness_summary_day35 VALUES
# MAGIC   ('strongest_concept', 'COPY INTO idempotency, validation, file audit, and immutable-source expectations are now repeatable.', 'Keep using validation and audit evidence before each batch rerun.'),
# MAGIC   ('strongest_concept', 'Auto Loader checkpoint, schema location, rescued data, and file-event tradeoffs have been practiced across several labs.', 'Tie each future Auto Loader answer to checkpoint plus schema state.'),
# MAGIC   ('strongest_concept', 'Semi-structured ingestion gates now distinguish additive drift, corrupt records, nested quality failures, and PII incidents.', 'Preserve raw bronze evidence before publishing silver projections.'),
# MAGIC   ('weakest_concept', 'Lakeflow Connect managed-connector selection is still lighter than COPY INTO and Auto Loader practice.', 'Run Day 36 on Lakeflow Connect source-acquisition contracts.'),
# MAGIC   ('weakest_concept', 'Monitoring interface language needs sharper recall: cloud_files_state, Streaming Query Listener metrics, Jobs history, connector health, and system tables.', 'Add one monitoring evidence row to every ingestion lab next week.'),
# MAGIC   ('guide_update', 'Official sources checked on 2026-08-05: Associate guide is May 4, 2026; the live Professional page now links a July 3, 2026 guide, newer than the previous November 30, 2025 automation anchor.', 'Use the live Professional objective map for future daily labs while preserving the historical anchor in notes.');
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW ingestion_readiness_final_checks_day35 AS
# MAGIC SELECT
# MAGIC   'coverage_rows' AS check_name,
# MAGIC   count(*) AS observed_value,
# MAGIC   9 AS expected_value,
# MAGIC   CASE WHEN count(*) = 9 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM ingestion_objective_coverage_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'scenario_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_gap_scenarios_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'decision_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_gap_decisions_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'matched_decisions',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_gap_decisions_day35
# MAGIC WHERE decision_status = 'MATCH'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'repair_evidence_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM repair_evidence_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'repair_plan_rows',
# MAGIC   count(*),
# MAGIC   6,
# MAGIC   CASE WHEN count(*) = 6 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM repair_plan_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'runbook_rows',
# MAGIC   count(*),
# MAGIC   6,
# MAGIC   CASE WHEN count(*) = 6 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_readiness_runbook_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'exam_questions',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM exam_question_bank_day35
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'weekly_summary_rows',
# MAGIC   count(*),
# MAGIC   6,
# MAGIC   CASE WHEN count(*) = 6 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM weekly_readiness_summary_day35;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ingestion_readiness_final_checks_day35
# MAGIC ORDER BY check_name;
