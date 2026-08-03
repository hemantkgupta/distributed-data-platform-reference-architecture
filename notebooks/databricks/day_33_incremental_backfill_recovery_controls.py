# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Day 33 - Incremental Backfill Recovery Controls
# MAGIC
# MAGIC **Phase:** Days 26-40 ingestion and loading.
# MAGIC
# MAGIC **Associate mapping:** ingestion/loading, transformation/modeling, troubleshooting/monitoring, Delta table reliability.
# MAGIC
# MAGIC **Professional extension:** choosing recovery-safe ingestion controls across `COPY INTO`, Auto Loader, and bounded backfills.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 1 - Create source inventory and recovery tables
# MAGIC
# MAGIC **Purpose:** Model file inventory, prior load audit, Auto Loader-style file state, a replay-safe bronze sink, and quarantine evidence.
# MAGIC
# MAGIC **Expected result:** Ten source-file candidates are staged across batch orders, clickstream files, and legacy archive backfill files.
# MAGIC
# MAGIC **Operational meaning:** Recovery method selection starts from evidence: source immutability, file modification time, business date, previous load state, and payload quality.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;
# MAGIC
# MAGIC DROP VIEW IF EXISTS recovery_final_checks_day33;
# MAGIC DROP TABLE IF EXISTS ingestion_recovery_runbook_day33;
# MAGIC DROP TABLE IF EXISTS ingestion_command_templates_day33;
# MAGIC DROP TABLE IF EXISTS ingestion_recovery_controls_day33;
# MAGIC DROP TABLE IF EXISTS orders_quarantine_day33;
# MAGIC DROP TABLE IF EXISTS orders_bronze_recovery_day33;
# MAGIC DROP TABLE IF EXISTS ingestion_recovery_decisions_day33;
# MAGIC DROP TABLE IF EXISTS autoloader_file_state_day33;
# MAGIC DROP TABLE IF EXISTS copy_into_file_audit_day33;
# MAGIC DROP TABLE IF EXISTS source_files_day33;
# MAGIC
# MAGIC CREATE TABLE source_files_day33 (
# MAGIC   source_file_path STRING,
# MAGIC   source_system STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   business_date DATE,
# MAGIC   file_size_mb DOUBLE,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   payload_quality STRING,
# MAGIC   source_files_immutable BOOLEAN,
# MAGIC   partition_date DATE,
# MAGIC   payload_hash STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO source_files_day33 VALUES
# MAGIC   ('dbfs:/landing/day33/partner/orders_3301.json', 'partner_orders_daily', timestamp('2026-08-03 05:00:00'), date('2026-08-02'), 1.1, 'evt-3301', 'ord-3301', 131.00, 'valid', true, date('2026-08-02'), 'hash_3301_v1'),
# MAGIC   ('dbfs:/landing/day33/partner/orders_3302.json', 'partner_orders_daily', timestamp('2026-08-03 05:01:00'), date('2026-08-02'), 0.9, 'evt-3302', 'ord-3302', 232.00, 'valid', true, date('2026-08-02'), 'hash_3302_v1'),
# MAGIC   ('dbfs:/landing/day33/partner/orders_3303.json', 'partner_orders_daily', timestamp('2026-08-03 05:02:00'), date('2026-08-03'), 1.0, 'evt-3303', 'ord-3303', 333.00, 'valid', true, date('2026-08-03'), 'hash_3303_v1'),
# MAGIC   ('dbfs:/landing/day33/partner/orders_3304_bad.json', 'partner_orders_daily', timestamp('2026-08-03 05:03:00'), date('2026-08-03'), 0.5, 'evt-3304', 'ord-3304', 444.00, 'corrupt', true, date('2026-08-03'), 'hash_3304_bad'),
# MAGIC   ('dbfs:/landing/day33/clickstream/click_3305.json', 'clickstream_events', timestamp('2026-08-03 05:10:00'), date('2026-08-03'), 0.2, 'clk-3305', 'ord-3305', 0.00, 'valid', true, date('2026-08-03'), 'hash_3305_v1'),
# MAGIC   ('dbfs:/landing/day33/archive/archive_3306.json', 'legacy_orders_archive', timestamp('2026-07-26 03:00:00'), date('2026-07-25'), 2.8, 'evt-3306', 'ord-3306', 636.00, 'valid', true, date('2026-07-25'), 'hash_3306_v1'),
# MAGIC   ('dbfs:/landing/day33/clickstream/click_3307.json', 'clickstream_events', timestamp('2026-08-03 05:12:00'), date('2026-08-03'), 0.2, 'clk-3307', 'ord-3307', 0.00, 'overwritten', false, date('2026-08-03'), 'hash_3307_v2'),
# MAGIC   ('dbfs:/landing/day33/partner/orders_3308_mispartitioned.json', 'partner_orders_daily', timestamp('2026-08-03 05:13:00'), date('2026-08-01'), 0.7, 'evt-3308', 'ord-3308', 838.00, 'partition_drift', true, date('2026-08-03'), 'hash_3308_v1'),
# MAGIC   ('dbfs:/landing/day33/clickstream/click_3309_late_recent.json', 'clickstream_events', timestamp('2026-08-02 23:50:00'), date('2026-08-02'), 0.1, 'clk-3309', 'ord-3309', 0.00, 'valid_late', true, date('2026-08-02'), 'hash_3309_v1'),
# MAGIC   ('dbfs:/landing/day33/archive/archive_3310.json', 'legacy_orders_archive', timestamp('2026-07-30 04:00:00'), date('2026-07-29'), 3.2, 'evt-3310', 'ord-3310', 1010.00, 'valid', true, date('2026-07-29'), 'hash_3310_v1');
# MAGIC
# MAGIC CREATE TABLE copy_into_file_audit_day33 (
# MAGIC   source_file_path STRING,
# MAGIC   copy_run_id STRING,
# MAGIC   validated_at TIMESTAMP,
# MAGIC   loaded_at TIMESTAMP,
# MAGIC   validation_status STRING,
# MAGIC   load_status STRING,
# MAGIC   result_metric STRING,
# MAGIC   operator_note STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE autoloader_file_state_day33 (
# MAGIC   source_file_path STRING,
# MAGIC   discovered_at TIMESTAMP,
# MAGIC   commit_time TIMESTAMP,
# MAGIC   checkpoint_location STRING,
# MAGIC   discovered_payload_hash STRING,
# MAGIC   state_status STRING,
# MAGIC   operator_note STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE ingestion_recovery_decisions_day33 (
# MAGIC   source_file_path STRING,
# MAGIC   source_system STRING,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   payload_quality STRING,
# MAGIC   candidate_idempotency_key STRING,
# MAGIC   copy_loaded BOOLEAN,
# MAGIC   autoloader_committed_path BOOLEAN,
# MAGIC   decision_action STRING,
# MAGIC   boundary_control STRING,
# MAGIC   recovery_reason STRING,
# MAGIC   decided_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_bronze_recovery_day33 (
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   source_file_path STRING,
# MAGIC   source_system STRING,
# MAGIC   business_date DATE,
# MAGIC   payload_hash STRING,
# MAGIC   ingestion_method STRING,
# MAGIC   idempotency_key STRING,
# MAGIC   ingested_at TIMESTAMP,
# MAGIC   recovery_run_id STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_quarantine_day33 (
# MAGIC   source_file_path STRING,
# MAGIC   source_system STRING,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   payload_quality STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   replay_policy STRING,
# MAGIC   quarantined_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC SELECT source_system, count(*) AS files, round(sum(file_size_mb), 2) AS total_mb
# MAGIC FROM source_files_day33
# MAGIC GROUP BY source_system
# MAGIC ORDER BY source_system;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 2 - Seed prior COPY INTO and Auto Loader evidence
# MAGIC
# MAGIC **Purpose:** Record that one batch file was already loaded by `COPY INTO`, one corrupt file failed validation, and one Auto Loader path was previously committed before a mutable overwrite.
# MAGIC
# MAGIC **Expected result:** The audit tables show two COPY INTO entries, one Auto Loader committed path, and one existing bronze row.
# MAGIC
# MAGIC **Operational meaning:** `COPY INTO` and Auto Loader both carry file-level state, but they use different mechanisms; recovery code must check the right evidence before replay.

# COMMAND ----------
# MAGIC %sql
# MAGIC INSERT INTO copy_into_file_audit_day33 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day33/partner/orders_3302.json',
# MAGIC     'copy-033-001',
# MAGIC     timestamp('2026-08-03 05:05:00'),
# MAGIC     timestamp('2026-08-03 05:06:00'),
# MAGIC     'VALIDATED',
# MAGIC     'LOADED',
# MAGIC     'num_loaded_files=1,num_skipped_corrupt_files=0',
# MAGIC     'Prior COPY INTO run loaded this immutable batch file.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day33/partner/orders_3304_bad.json',
# MAGIC     'copy-033-validate',
# MAGIC     timestamp('2026-08-03 05:07:00'),
# MAGIC     NULL,
# MAGIC     'VALIDATE_FAILED',
# MAGIC     'NOT_LOADED',
# MAGIC     'num_skipped_corrupt_files=1',
# MAGIC     'Validation detected a corrupt file before loading.'
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO autoloader_file_state_day33 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day33/clickstream/click_3307.json',
# MAGIC     timestamp('2026-08-03 04:50:00'),
# MAGIC     timestamp('2026-08-03 04:52:00'),
# MAGIC     'dbfs:/checkpoints/de_learning/clickstream_day33/',
# MAGIC     'hash_3307_v1',
# MAGIC     'COMMITTED',
# MAGIC     'Path was already committed before the producer overwrote the object with hash_3307_v2.'
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO orders_bronze_recovery_day33
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   order_id,
# MAGIC   order_amount,
# MAGIC   source_file_path,
# MAGIC   source_system,
# MAGIC   business_date,
# MAGIC   payload_hash,
# MAGIC   'COPY_INTO' AS ingestion_method,
# MAGIC   sha2(concat_ws('|', event_id, order_id, payload_hash), 256) AS idempotency_key,
# MAGIC   timestamp('2026-08-03 05:06:00') AS ingested_at,
# MAGIC   'copy-033-001' AS recovery_run_id
# MAGIC FROM source_files_day33
# MAGIC WHERE source_file_path = 'dbfs:/landing/day33/partner/orders_3302.json';
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM copy_into_file_audit_day33) AS copy_audit_rows,
# MAGIC   (SELECT count(*) FROM autoloader_file_state_day33) AS autoloader_state_rows,
# MAGIC   (SELECT count(*) FROM orders_bronze_recovery_day33) AS existing_bronze_rows;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 3 - Choose the recovery method with PySpark
# MAGIC
# MAGIC **Purpose:** Join source inventory to COPY INTO audit and Auto Loader state, then select a recovery action per file.
# MAGIC
# MAGIC **Expected result:** Ten decisions: two `COPY INTO` loads, two Auto Loader loads, two bounded backfills, one idempotent skip, and three quarantine/hold actions.
# MAGIC
# MAGIC **Operational meaning:** A production playbook should choose controls by failure mode: `VALIDATE` for batch files, checkpoints for streams, modified-time windows for backfills, and approvals for overwrites.

# COMMAND ----------
from pyspark.sql import functions as F

source_df = spark.table("de_learning.source_files_day33")
copy_audit_df = spark.table("de_learning.copy_into_file_audit_day33")
autoloader_state_df = spark.table("de_learning.autoloader_file_state_day33")

loaded_copy_df = (
    copy_audit_df
    .where(F.col("load_status") == F.lit("LOADED"))
    .select(F.col("source_file_path").alias("copy_loaded_path"))
    .distinct()
)

committed_auto_df = (
    autoloader_state_df
    .where(F.col("commit_time").isNotNull())
    .select(
        F.col("source_file_path").alias("autoloader_committed_source_file_path"),
        F.col("discovered_payload_hash").alias("previous_payload_hash"),
    )
    .distinct()
)

decision_df = (
    source_df
    .withColumn(
        "candidate_idempotency_key",
        F.sha2(F.concat_ws("|", F.col("event_id"), F.col("order_id"), F.col("payload_hash")), 256),
    )
    .join(
        loaded_copy_df,
        F.col("source_file_path") == F.col("copy_loaded_path"),
        "left",
    )
    .join(
        committed_auto_df,
        F.col("source_file_path") == F.col("autoloader_committed_source_file_path"),
        "left",
    )
    .withColumn("copy_loaded", F.col("copy_loaded_path").isNotNull())
    .withColumn("autoloader_committed_path", F.col("autoloader_committed_source_file_path").isNotNull())
    .withColumn(
        "decision_action",
        F.when(F.col("payload_quality") == F.lit("corrupt"), F.lit("QUARANTINE_VALIDATE_FIRST"))
        .when(F.col("payload_quality") == F.lit("partition_drift"), F.lit("QUARANTINE_PARTITION_DRIFT"))
        .when((F.col("source_files_immutable") == F.lit(False)) & F.col("autoloader_committed_path"), F.lit("HOLD_OVERWRITE_REPROCESS_APPROVAL"))
        .when(F.col("copy_loaded"), F.lit("SKIP_COPY_ALREADY_LOADED"))
        .when(F.col("source_system") == F.lit("partner_orders_daily"), F.lit("COPY_INTO_VALIDATED_BATCH"))
        .when(F.col("source_system") == F.lit("clickstream_events"), F.lit("AUTO_LOADER_AVAILABLE_NOW"))
        .when(F.col("source_system") == F.lit("legacy_orders_archive"), F.lit("BOUNDED_BACKFILL_MODIFIED_WINDOW"))
        .otherwise(F.lit("MANUAL_REVIEW")),
    )
    .withColumn(
        "boundary_control",
        F.when(F.col("decision_action") == F.lit("COPY_INTO_VALIDATED_BATCH"), F.lit("COPY INTO VALIDATE, then load immutable files by FILES or PATTERN."))
        .when(F.col("decision_action") == F.lit("AUTO_LOADER_AVAILABLE_NOW"), F.lit("Same checkpoint plus AvailableNow; includeExistingFiles only on first start."))
        .when(F.col("decision_action") == F.lit("BOUNDED_BACKFILL_MODIFIED_WINDOW"), F.lit("Use modified-time and business-date windows with an isolated recovery run id."))
        .when(F.col("decision_action") == F.lit("SKIP_COPY_ALREADY_LOADED"), F.lit("COPY INTO audit says the file was already loaded."))
        .otherwise(F.lit("Quarantine or hold until a human approves the replay boundary.")),
    )
    .withColumn(
        "recovery_reason",
        F.when(F.col("decision_action") == F.lit("QUARANTINE_VALIDATE_FIRST"), F.lit("Corrupt file should be validated and fixed before load."))
        .when(F.col("decision_action") == F.lit("QUARANTINE_PARTITION_DRIFT"), F.lit("Business date and partition date disagree."))
        .when(F.col("decision_action") == F.lit("HOLD_OVERWRITE_REPROCESS_APPROVAL"), F.lit("Mutable overwrite conflicts with committed Auto Loader path evidence."))
        .when(F.col("decision_action") == F.lit("SKIP_COPY_ALREADY_LOADED"), F.lit("Batch file is already loaded and COPY INTO would skip it."))
        .when(F.col("decision_action") == F.lit("COPY_INTO_VALIDATED_BATCH"), F.lit("Immutable scheduled batch is best handled by COPY INTO."))
        .when(F.col("decision_action") == F.lit("AUTO_LOADER_AVAILABLE_NOW"), F.lit("Frequent arrival pattern benefits from checkpointed Auto Loader discovery."))
        .when(F.col("decision_action") == F.lit("BOUNDED_BACKFILL_MODIFIED_WINDOW"), F.lit("Legacy archive file belongs to a bounded backfill window."))
        .otherwise(F.lit("No safe automated recovery action matched.")),
    )
    .select(
        "source_file_path",
        "source_system",
        "event_id",
        "order_id",
        "payload_quality",
        "candidate_idempotency_key",
        "copy_loaded",
        "autoloader_committed_path",
        "decision_action",
        "boundary_control",
        "recovery_reason",
        F.current_timestamp().alias("decided_at"),
    )
)

(
    decision_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("de_learning.ingestion_recovery_decisions_day33")
)

display(
    decision_df
    .groupBy("decision_action")
    .agg(F.count("*").alias("files"))
    .orderBy("decision_action")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC **DataFrame meaning:** `source_df` is the landing inventory; `copy_audit_df` is prior `COPY INTO` evidence; `autoloader_state_df` is checkpoint-derived file state.
# MAGIC
# MAGIC **SQL equivalent:** The PySpark block is a `LEFT JOIN` from source files to loaded COPY files and committed Auto Loader paths, followed by a `CASE WHEN` recovery decision.
# MAGIC
# MAGIC **Syntax notes:**
# MAGIC - `F.col("load_status") == F.lit("LOADED")` is the DataFrame version of `load_status = 'LOADED'`.
# MAGIC - `join(..., "left")` preserves every source file while adding optional audit evidence.
# MAGIC - `withColumn` derives the idempotency key, boolean evidence flags, and decision text.
# MAGIC - Spark remains lazy until `.write.saveAsTable(...)` and `display(...)` execute the plan.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 4 - Apply the recovery decisions
# MAGIC
# MAGIC **Purpose:** Load only approved records into bronze and send unsafe files to quarantine or manual hold.
# MAGIC
# MAGIC **Expected result:** Bronze contains seven distinct records including the preloaded row, while quarantine contains three unsafe files.
# MAGIC
# MAGIC **Operational meaning:** The same sink contract handles batch loads, streaming catch-up, and archive backfill as long as every path uses the same idempotency key.

# COMMAND ----------
# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day33
# MAGIC SELECT
# MAGIC   s.source_file_path,
# MAGIC   s.source_system,
# MAGIC   s.event_id,
# MAGIC   s.order_id,
# MAGIC   s.payload_quality,
# MAGIC   d.recovery_reason AS quarantine_reason,
# MAGIC   CASE
# MAGIC     WHEN d.decision_action = 'QUARANTINE_VALIDATE_FIRST' THEN 'Run COPY INTO VALIDATE after payload correction, then load by explicit FILES list.'
# MAGIC     WHEN d.decision_action = 'QUARANTINE_PARTITION_DRIFT' THEN 'Correct partition placement or business date before any load.'
# MAGIC     WHEN d.decision_action = 'HOLD_OVERWRITE_REPROCESS_APPROVAL' THEN 'Require data owner approval before enabling overwrite handling or replaying a corrected object.'
# MAGIC     ELSE 'Manual review required.'
# MAGIC   END AS replay_policy,
# MAGIC   current_timestamp() AS quarantined_at
# MAGIC FROM source_files_day33 s
# MAGIC INNER JOIN ingestion_recovery_decisions_day33 d
# MAGIC   ON s.source_file_path = d.source_file_path
# MAGIC WHERE d.decision_action IN (
# MAGIC   'QUARANTINE_VALIDATE_FIRST',
# MAGIC   'QUARANTINE_PARTITION_DRIFT',
# MAGIC   'HOLD_OVERWRITE_REPROCESS_APPROVAL'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO orders_bronze_recovery_day33
# MAGIC SELECT
# MAGIC   s.event_id,
# MAGIC   s.order_id,
# MAGIC   s.order_amount,
# MAGIC   s.source_file_path,
# MAGIC   s.source_system,
# MAGIC   s.business_date,
# MAGIC   s.payload_hash,
# MAGIC   CASE
# MAGIC     WHEN d.decision_action = 'COPY_INTO_VALIDATED_BATCH' THEN 'COPY_INTO'
# MAGIC     WHEN d.decision_action = 'AUTO_LOADER_AVAILABLE_NOW' THEN 'AUTO_LOADER'
# MAGIC     WHEN d.decision_action = 'BOUNDED_BACKFILL_MODIFIED_WINDOW' THEN 'BOUNDED_BACKFILL'
# MAGIC     ELSE 'UNKNOWN'
# MAGIC   END AS ingestion_method,
# MAGIC   d.candidate_idempotency_key AS idempotency_key,
# MAGIC   current_timestamp() AS ingested_at,
# MAGIC   'recovery-033-apply' AS recovery_run_id
# MAGIC FROM source_files_day33 s
# MAGIC INNER JOIN ingestion_recovery_decisions_day33 d
# MAGIC   ON s.source_file_path = d.source_file_path
# MAGIC LEFT ANTI JOIN orders_bronze_recovery_day33 b
# MAGIC   ON b.idempotency_key = d.candidate_idempotency_key
# MAGIC WHERE d.decision_action IN (
# MAGIC   'COPY_INTO_VALIDATED_BATCH',
# MAGIC   'AUTO_LOADER_AVAILABLE_NOW',
# MAGIC   'BOUNDED_BACKFILL_MODIFIED_WINDOW'
# MAGIC );
# MAGIC
# MAGIC SELECT
# MAGIC   ingestion_method,
# MAGIC   count(*) AS bronze_rows,
# MAGIC   count(DISTINCT idempotency_key) AS distinct_keys
# MAGIC FROM orders_bronze_recovery_day33
# MAGIC GROUP BY ingestion_method
# MAGIC ORDER BY ingestion_method;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 5 - Compare recovery controls by ingestion method
# MAGIC
# MAGIC **Purpose:** Store a concise operator matrix for deciding when to use `COPY INTO`, Auto Loader, bounded backfill, or manual repair.
# MAGIC
# MAGIC **Expected result:** Four method-control rows define boundary, duplicate, bad-record, backfill, and cost controls.
# MAGIC
# MAGIC **Operational meaning:** Ingestion tools overlap, but their recovery state lives in different places; the wrong control can create duplicate data or miss files.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE ingestion_recovery_controls_day33 (
# MAGIC   method_name STRING,
# MAGIC   best_fit STRING,
# MAGIC   boundary_control STRING,
# MAGIC   duplicate_control STRING,
# MAGIC   bad_record_control STRING,
# MAGIC   backfill_control STRING,
# MAGIC   cost_control STRING,
# MAGIC   operator_risk STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_recovery_controls_day33 VALUES
# MAGIC   (
# MAGIC     'COPY_INTO',
# MAGIC     'Scheduled immutable file batches and explicit file lists.',
# MAGIC     'Use VALIDATE, FILES, and PATTERN before writing.',
# MAGIC     'COPY INTO skips files already loaded, even if the source file later changes.',
# MAGIC     'Use VALIDATE and inspect skipped corrupt file metrics before load.',
# MAGIC     'Use explicit FILES or date partition folders; avoid broad root paths.',
# MAGIC     'Cheap for bounded batches; expensive if used as a blind directory scanner.',
# MAGIC     'Mutable source files can hide corrected payloads because loaded files are skipped.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'AUTO_LOADER',
# MAGIC     'Frequent file arrivals that need checkpointed incremental discovery.',
# MAGIC     'Use the original checkpoint, schema location, and AvailableNow or streaming trigger.',
# MAGIC     'Checkpoint and sink idempotency key together prevent duplicate processing.',
# MAGIC     'Use rescue/quarantine paths and monitor cloud_files_state.',
# MAGIC     'Use includeExistingFiles only on first start; use backfillInterval only where compatible.',
# MAGIC     'File events reduce listing costs; cleanSource can reduce source-listing pressure.',
# MAGIC     'Changing checkpoints or allowing overwrites without approval can reprocess or miss data.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'BOUNDED_BACKFILL',
# MAGIC     'Historical repair windows and legacy archive migration.',
# MAGIC     'Use modified-time and business-date windows plus an isolated recovery run id.',
# MAGIC     'MERGE or anti-join by idempotency key into the shared bronze sink.',
# MAGIC     'Validate source window before load and quarantine contract breaks.',
# MAGIC     'Use modifiedAfter/modifiedBefore-style windows and partition predicates.',
# MAGIC     'Bounded windows cap files scanned and compute consumed.',
# MAGIC     'Over-broad windows can reload months of data and swamp downstream consumers.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'MANUAL_REPAIR',
# MAGIC     'Overwrites, partition drift, corrupt files, or ownership disputes.',
# MAGIC     'Freeze writes until the data owner approves a precise replay boundary.',
# MAGIC     'Compare source payload hash against sink idempotency keys before replay.',
# MAGIC     'Keep original bad payloads as evidence and replay corrected replacements.',
# MAGIC     'Backfill only after the incident decision is recorded.',
# MAGIC     'Human review is slower but avoids expensive uncontrolled reprocessing.',
# MAGIC     'Skipping approval can turn a data correction into an audit incident.'
# MAGIC   );
# MAGIC
# MAGIC SELECT method_name, best_fit, operator_risk
# MAGIC FROM ingestion_recovery_controls_day33
# MAGIC ORDER BY method_name;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 6 - Save command templates
# MAGIC
# MAGIC **Purpose:** Capture executable shapes for validation, loading, checkpointed catch-up, bounded backfill, state inspection, and idempotent merging.
# MAGIC
# MAGIC **Expected result:** Seven templates are stored for Day 33 recovery drills.
# MAGIC
# MAGIC **Operational meaning:** Incident response improves when reviewed command shapes exist before the incident.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE ingestion_command_templates_day33 (
# MAGIC   template_name STRING,
# MAGIC   command_type STRING,
# MAGIC   command_text STRING,
# MAGIC   operator_note STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_command_templates_day33 VALUES
# MAGIC   (
# MAGIC     'copy_into_validate',
# MAGIC     'sql',
# MAGIC     'COPY INTO de_learning.orders_bronze_recovery_day33 FROM "/Volumes/catalog/schema/landing/orders/" FILEFORMAT = JSON VALIDATE ALL FORMAT_OPTIONS ("ignoreCorruptFiles" = "false")',
# MAGIC     'Validate parsing and schema compatibility before a batch repair load.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'copy_into_explicit_files',
# MAGIC     'sql',
# MAGIC     'COPY INTO de_learning.orders_bronze_recovery_day33 FROM "/Volumes/catalog/schema/landing/orders/" FILEFORMAT = JSON FILES = ("orders_3301.json", "orders_3303.json")',
# MAGIC     'Use FILES for a precise replay boundary after validation.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'autoloader_available_now',
# MAGIC     'pyspark',
# MAGIC     'spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", schema_location).option("cloudFiles.includeExistingFiles", "false").load(source_path).writeStream.option("checkpointLocation", checkpoint_location).trigger(availableNow=True).toTable(target_table)',
# MAGIC     'Use the original checkpoint and avoid re-including existing files after the first start.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'autoloader_state_inspection',
# MAGIC     'sql',
# MAGIC     'SELECT path, discovery_time, commit_time FROM cloud_files_state("dbfs:/checkpoints/de_learning/clickstream_day33/") ORDER BY discovery_time DESC',
# MAGIC     'Inspect file-level state for an Auto Loader checkpoint.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'bounded_backfill_window',
# MAGIC     'pyspark',
# MAGIC     'spark.read.format("json").option("modifiedAfter", "2026-07-25T00:00:00Z").option("modifiedBefore", "2026-08-01T00:00:00Z").load(archive_path)',
# MAGIC     'Bound the historical repair by modified time before applying business-date filters.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'idempotent_merge_sink',
# MAGIC     'sql',
# MAGIC     'MERGE INTO bronze b USING recovery_source r ON b.idempotency_key = r.idempotency_key WHEN NOT MATCHED THEN INSERT *',
# MAGIC     'All recovery methods should converge on the same sink idempotency contract.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'quarantine_replay_approval',
# MAGIC     'sql',
# MAGIC     'INSERT INTO quarantine SELECT source_file_path, payload_quality, recovery_reason FROM recovery_decisions WHERE decision_action LIKE "QUARANTINE%"',
# MAGIC     'Keep unsafe files out of bronze until the source owner approves correction.'
# MAGIC   );
# MAGIC
# MAGIC SELECT template_name, command_type, operator_note
# MAGIC FROM ingestion_command_templates_day33
# MAGIC ORDER BY template_name;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 7 - Run final recovery checks
# MAGIC
# MAGIC **Purpose:** Verify the Day 33 method-selection story and publish a small operator runbook.
# MAGIC
# MAGIC **Expected result:** Every final check returns `PASS`.
# MAGIC
# MAGIC **Operational meaning:** A backfill is not production-ready until counts, duplicate keys, quarantine rows, method controls, and command templates are reviewable.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE ingestion_recovery_runbook_day33 (
# MAGIC   step_number INT,
# MAGIC   runbook_step STRING,
# MAGIC   required_evidence STRING,
# MAGIC   done_criteria STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_recovery_runbook_day33 VALUES
# MAGIC   (1, 'Identify the source-file population.', 'Inventory with path, file modification time, business date, payload hash, and source immutability.', 'The candidate file set is bounded.'),
# MAGIC   (2, 'Choose method-specific recovery evidence.', 'COPY INTO audit, Auto Loader checkpoint state, or backfill window query.', 'No file is replayed without method evidence.'),
# MAGIC   (3, 'Validate before write.', 'COPY INTO VALIDATE, parser checks, partition checks, and quarantine rules.', 'Unsafe files are excluded from bronze.'),
# MAGIC   (4, 'Write through the shared idempotent sink.', 'Stable idempotency key and merge or anti-join evidence.', 'No duplicate sink keys exist.'),
# MAGIC   (5, 'Record operator controls.', 'Control matrix, command templates, and final count checks.', 'The recovery can be reviewed without rerunning the job.');
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW recovery_final_checks_day33 AS
# MAGIC SELECT 'source_files' AS check_name, count(*) AS actual_count, 10 AS expected_count FROM source_files_day33
# MAGIC UNION ALL SELECT 'copy_audit_rows', count(*), 2 FROM copy_into_file_audit_day33
# MAGIC UNION ALL SELECT 'autoloader_state_rows', count(*), 1 FROM autoloader_file_state_day33
# MAGIC UNION ALL SELECT 'recovery_decisions', count(*), 10 FROM ingestion_recovery_decisions_day33
# MAGIC UNION ALL SELECT 'copy_load_actions', count(*), 2 FROM ingestion_recovery_decisions_day33 WHERE decision_action = 'COPY_INTO_VALIDATED_BATCH'
# MAGIC UNION ALL SELECT 'autoloader_actions', count(*), 2 FROM ingestion_recovery_decisions_day33 WHERE decision_action = 'AUTO_LOADER_AVAILABLE_NOW'
# MAGIC UNION ALL SELECT 'bounded_backfill_actions', count(*), 2 FROM ingestion_recovery_decisions_day33 WHERE decision_action = 'BOUNDED_BACKFILL_MODIFIED_WINDOW'
# MAGIC UNION ALL SELECT 'copy_skip_actions', count(*), 1 FROM ingestion_recovery_decisions_day33 WHERE decision_action = 'SKIP_COPY_ALREADY_LOADED'
# MAGIC UNION ALL SELECT 'quarantine_rows', count(*), 3 FROM orders_quarantine_day33
# MAGIC UNION ALL SELECT 'bronze_rows', count(*), 7 FROM orders_bronze_recovery_day33
# MAGIC UNION ALL SELECT 'distinct_bronze_keys', count(DISTINCT idempotency_key), 7 FROM orders_bronze_recovery_day33
# MAGIC UNION ALL SELECT 'control_rows', count(*), 4 FROM ingestion_recovery_controls_day33
# MAGIC UNION ALL SELECT 'command_templates', count(*), 7 FROM ingestion_command_templates_day33
# MAGIC UNION ALL SELECT 'runbook_steps', count(*), 5 FROM ingestion_recovery_runbook_day33;
# MAGIC
# MAGIC SELECT
# MAGIC   check_name,
# MAGIC   actual_count,
# MAGIC   expected_count,
# MAGIC   CASE WHEN actual_count = expected_count THEN 'PASS' ELSE 'FAIL' END AS check_status
# MAGIC FROM recovery_final_checks_day33
# MAGIC ORDER BY check_name;
