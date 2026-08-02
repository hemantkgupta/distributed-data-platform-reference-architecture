# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Day 32 - Ingestion Checkpoint Recovery Drills
# MAGIC
# MAGIC **Phase:** Days 26-40 ingestion and loading.
# MAGIC
# MAGIC **Associate mapping:** ingestion/loading, troubleshooting and monitoring, Delta table reliability.
# MAGIC
# MAGIC **Professional extension:** checkpoint recovery, replay boundaries, idempotent sinks, bad-record replay, and incident runbooks.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 1 - Set up day-scoped checkpoint evidence
# MAGIC
# MAGIC **Purpose:** Create a small ingestion-control model that separates landing files, checkpoint state, stream run history, bronze sink rows, bad records, command templates, and operator runbook evidence.
# MAGIC
# MAGIC **Expected result:** Empty Day 32 tables are recreated and the landing/config tables contain one stream config plus eight source-file candidates.
# MAGIC
# MAGIC **Operational meaning:** Production recovery starts by preserving evidence: source inventory, checkpoint location, schema location, sink table, and run history must be explicit before replaying data.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;
# MAGIC
# MAGIC DROP VIEW IF EXISTS checkpoint_final_checks_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_recovery_runbook_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_command_templates_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_incident_decisions_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_incident_events_day32;
# MAGIC DROP TABLE IF EXISTS orders_curated_day32;
# MAGIC DROP TABLE IF EXISTS orders_bad_records_replay_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_replay_decisions_day32;
# MAGIC DROP TABLE IF EXISTS orders_bronze_checkpoint_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_file_state_day32;
# MAGIC DROP TABLE IF EXISTS checkpoint_run_history_day32;
# MAGIC DROP TABLE IF EXISTS landing_files_checkpoint_day32;
# MAGIC DROP TABLE IF EXISTS stream_checkpoint_config_day32;
# MAGIC
# MAGIC CREATE TABLE stream_checkpoint_config_day32 (
# MAGIC   stream_id STRING,
# MAGIC   source_path STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   schema_location STRING,
# MAGIC   sink_table STRING,
# MAGIC   checkpoint_owner STRING,
# MAGIC   stream_trigger STRING,
# MAGIC   replay_boundary_policy STRING,
# MAGIC   idempotency_key_policy STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO stream_checkpoint_config_day32 VALUES
# MAGIC   (
# MAGIC     'orders_autoloader_day32',
# MAGIC     's3://landing/orders/day32/',
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     's3://schemas/de_learning/orders_autoloader_day32/',
# MAGIC     'de_learning.orders_bronze_checkpoint_day32',
# MAGIC     'data-platform-oncall',
# MAGIC     'AvailableNow',
# MAGIC     'Replay from an uncommitted source-file boundary only; never delete a production checkpoint without an approved reprocess plan.',
# MAGIC     'sha2(event_id, order_id, payload_hash) protects the Delta sink from duplicate files and checkpoint relocation mistakes.'
# MAGIC   );
# MAGIC
# MAGIC CREATE TABLE landing_files_checkpoint_day32 (
# MAGIC   source_file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   file_size_mb DOUBLE,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   source_batch_id STRING,
# MAGIC   payload_quality STRING,
# MAGIC   payload_hash STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO landing_files_checkpoint_day32 VALUES
# MAGIC   ('s3://landing/orders/day32/orders_3201.json', timestamp('2026-08-02 05:00:00'), 1.1, 'evt-3201', 'ord-3201', 110.00, 'batch-032-a', 'valid', 'hash_3201_v1'),
# MAGIC   ('s3://landing/orders/day32/orders_3202.json', timestamp('2026-08-02 05:01:00'), 1.0, 'evt-3202', 'ord-3202', 220.00, 'batch-032-a', 'valid', 'hash_3202_v1'),
# MAGIC   ('s3://landing/orders/day32/orders_3203_bad.json', timestamp('2026-08-02 05:02:00'), 0.4, 'evt-3203', 'ord-3203', 330.00, 'batch-032-b', 'corrupt', 'hash_3203_bad'),
# MAGIC   ('s3://landing/orders/day32/orders_3204.json', timestamp('2026-08-02 05:03:00'), 1.3, 'evt-3204', 'ord-3204', 440.00, 'batch-032-b', 'valid', 'hash_3204_v1'),
# MAGIC   ('s3://landing/orders/day32/orders_3205.json', timestamp('2026-08-02 05:04:00'), 1.6, 'evt-3205', 'ord-3205', 550.00, 'batch-032-b', 'valid', 'hash_3205_v1'),
# MAGIC   ('s3://landing/orders/day32/orders_3202_replay_same.json', timestamp('2026-08-02 05:05:00'), 1.0, 'evt-3202', 'ord-3202', 220.00, 'batch-032-replay', 'valid_duplicate', 'hash_3202_v1'),
# MAGIC   ('s3://landing/orders/day32/orders_3206_fix.json', timestamp('2026-08-02 05:06:00'), 0.5, 'evt-3203', 'ord-3203', 333.00, 'batch-032-fix', 'corrected_bad_record', 'hash_3203_fix'),
# MAGIC   ('s3://landing/orders/day32/orders_3207_late.json', timestamp('2026-08-02 05:07:00'), 0.9, 'evt-3207', 'ord-3207', 770.00, 'batch-032-late', 'valid_late', 'hash_3207_v1');
# MAGIC
# MAGIC CREATE TABLE checkpoint_file_state_day32 (
# MAGIC   source_file_path STRING,
# MAGIC   discovered_at TIMESTAMP,
# MAGIC   commit_time TIMESTAMP,
# MAGIC   micro_batch_id BIGINT,
# MAGIC   checkpoint_location STRING,
# MAGIC   checkpoint_status STRING,
# MAGIC   state_reason STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE checkpoint_run_history_day32 (
# MAGIC   run_id STRING,
# MAGIC   stream_id STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   trigger_mode STRING,
# MAGIC   run_status STRING,
# MAGIC   started_at TIMESTAMP,
# MAGIC   ended_at TIMESTAMP,
# MAGIC   files_discovered INT,
# MAGIC   files_committed INT,
# MAGIC   failure_class STRING,
# MAGIC   recovery_action STRING,
# MAGIC   operator_note STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_bronze_checkpoint_day32 (
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   source_file_path STRING,
# MAGIC   source_batch_id STRING,
# MAGIC   payload_hash STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   micro_batch_id BIGINT,
# MAGIC   ingestion_run_id STRING,
# MAGIC   ingested_at TIMESTAMP,
# MAGIC   idempotency_key STRING,
# MAGIC   record_status STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE checkpoint_replay_decisions_day32 (
# MAGIC   source_file_path STRING,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   payload_quality STRING,
# MAGIC   candidate_idempotency_key STRING,
# MAGIC   checkpoint_seen BOOLEAN,
# MAGIC   sink_key_seen BOOLEAN,
# MAGIC   decision_action STRING,
# MAGIC   recovery_reason STRING,
# MAGIC   decided_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_bad_records_replay_day32 (
# MAGIC   source_file_path STRING,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   payload_quality STRING,
# MAGIC   payload_hash STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   replay_policy STRING,
# MAGIC   captured_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_curated_day32 (
# MAGIC   order_id STRING,
# MAGIC   event_id STRING,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   source_batch_id STRING,
# MAGIC   curated_reason STRING,
# MAGIC   curated_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM stream_checkpoint_config_day32) AS configs,
# MAGIC   (SELECT count(*) FROM landing_files_checkpoint_day32) AS landing_files;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 2 - Simulate a failed stream without losing checkpoint state
# MAGIC
# MAGIC **Purpose:** Record an initial successful micro-batch, then a failed micro-batch that discovered a corrupt file but did not commit it to the sink.
# MAGIC
# MAGIC **Expected result:** Two source files are committed, one corrupt file is discovered but uncommitted, and run history shows the failed run plus a same-checkpoint restart.
# MAGIC
# MAGIC **Operational meaning:** A same-checkpoint restart lets the stream skip files already committed in the checkpoint while preserving the exact failure boundary for investigation.

# COMMAND ----------
# MAGIC %sql
# MAGIC INSERT INTO checkpoint_file_state_day32 VALUES
# MAGIC   (
# MAGIC     's3://landing/orders/day32/orders_3201.json',
# MAGIC     timestamp('2026-08-02 05:00:30'),
# MAGIC     timestamp('2026-08-02 05:02:00'),
# MAGIC     0,
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     'COMMITTED',
# MAGIC     'Initial AvailableNow micro-batch committed this file to the Delta sink.'
# MAGIC   ),
# MAGIC   (
# MAGIC     's3://landing/orders/day32/orders_3202.json',
# MAGIC     timestamp('2026-08-02 05:01:30'),
# MAGIC     timestamp('2026-08-02 05:02:00'),
# MAGIC     0,
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     'COMMITTED',
# MAGIC     'Initial AvailableNow micro-batch committed this file to the Delta sink.'
# MAGIC   ),
# MAGIC   (
# MAGIC     's3://landing/orders/day32/orders_3203_bad.json',
# MAGIC     timestamp('2026-08-02 05:02:30'),
# MAGIC     NULL,
# MAGIC     1,
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     'FAILED_BAD_RECORD',
# MAGIC     'File was discovered but not committed because parsing failed before sink write.'
# MAGIC   );
# MAGIC
# MAGIC INSERT INTO orders_bronze_checkpoint_day32
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   order_id,
# MAGIC   order_amount,
# MAGIC   source_file_path,
# MAGIC   source_batch_id,
# MAGIC   payload_hash,
# MAGIC   's3://checkpoints/de_learning/orders_autoloader_day32/' AS checkpoint_location,
# MAGIC   0 AS micro_batch_id,
# MAGIC   'run-032-001' AS ingestion_run_id,
# MAGIC   timestamp('2026-08-02 05:02:00') AS ingested_at,
# MAGIC   sha2(concat_ws('|', event_id, order_id, payload_hash), 256) AS idempotency_key,
# MAGIC   'COMMITTED' AS record_status
# MAGIC FROM landing_files_checkpoint_day32
# MAGIC WHERE source_file_path IN (
# MAGIC   's3://landing/orders/day32/orders_3201.json',
# MAGIC   's3://landing/orders/day32/orders_3202.json'
# MAGIC );
# MAGIC
# MAGIC INSERT INTO checkpoint_run_history_day32 VALUES
# MAGIC   (
# MAGIC     'run-032-001',
# MAGIC     'orders_autoloader_day32',
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     'AvailableNow',
# MAGIC     'SUCCESS',
# MAGIC     timestamp('2026-08-02 05:00:20'),
# MAGIC     timestamp('2026-08-02 05:02:05'),
# MAGIC     2,
# MAGIC     2,
# MAGIC     NULL,
# MAGIC     'none',
# MAGIC     'First two valid files were committed.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'run-032-002',
# MAGIC     'orders_autoloader_day32',
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     'AvailableNow',
# MAGIC     'FAILED',
# MAGIC     timestamp('2026-08-02 05:02:10'),
# MAGIC     timestamp('2026-08-02 05:02:50'),
# MAGIC     1,
# MAGIC     0,
# MAGIC     'BAD_RECORD',
# MAGIC     'quarantine_then_forward_fix',
# MAGIC     'Corrupt file was discovered but no new sink write happened.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'run-032-003',
# MAGIC     'orders_autoloader_day32',
# MAGIC     's3://checkpoints/de_learning/orders_autoloader_day32/',
# MAGIC     'AvailableNow',
# MAGIC     'PAUSED_FOR_RECOVERY',
# MAGIC     timestamp('2026-08-02 05:03:00'),
# MAGIC     timestamp('2026-08-02 05:03:20'),
# MAGIC     0,
# MAGIC     0,
# MAGIC     'RECOVERY_REVIEW',
# MAGIC     'restart_same_checkpoint',
# MAGIC     'Same checkpoint restart preserves already committed file state.'
# MAGIC   );
# MAGIC
# MAGIC SELECT
# MAGIC   checkpoint_status,
# MAGIC   count(*) AS file_count,
# MAGIC   count(commit_time) AS committed_count
# MAGIC FROM checkpoint_file_state_day32
# MAGIC GROUP BY checkpoint_status
# MAGIC ORDER BY checkpoint_status;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 3 - Build a replay plan with PySpark
# MAGIC
# MAGIC **Purpose:** Compare landing files with checkpoint state and the existing bronze sink to decide whether each file should be skipped, quarantined, replayed, or processed.
# MAGIC
# MAGIC **Expected result:** Eight replay decisions are written: two checkpoint skips, one idempotent duplicate skip, one quarantine, and four process/replay actions.
# MAGIC
# MAGIC **Operational meaning:** Replay must be driven by source-file state plus sink idempotency, not by guesswork or by deleting the checkpoint.

# COMMAND ----------
from pyspark.sql import functions as F

landing_df = spark.table("de_learning.landing_files_checkpoint_day32")
state_df = spark.table("de_learning.checkpoint_file_state_day32")
bronze_df = spark.table("de_learning.orders_bronze_checkpoint_day32")

committed_paths_df = (
    state_df
    .where(F.col("commit_time").isNotNull())
    .select(F.col("source_file_path").alias("checkpoint_committed_path"))
    .distinct()
)

existing_sink_keys_df = (
    bronze_df
    .select(F.col("idempotency_key").alias("existing_idempotency_key"))
    .distinct()
)

candidate_df = (
    landing_df
    .withColumn(
        "candidate_idempotency_key",
        F.sha2(F.concat_ws("|", F.col("event_id"), F.col("order_id"), F.col("payload_hash")), 256),
    )
    .join(
        committed_paths_df,
        F.col("source_file_path") == F.col("checkpoint_committed_path"),
        "left",
    )
    .join(
        existing_sink_keys_df,
        F.col("candidate_idempotency_key") == F.col("existing_idempotency_key"),
        "left",
    )
)

decision_df = (
    candidate_df
    .withColumn("checkpoint_seen", F.col("checkpoint_committed_path").isNotNull())
    .withColumn("sink_key_seen", F.col("existing_idempotency_key").isNotNull())
    .withColumn(
        "decision_action",
        F.when(F.col("checkpoint_seen"), F.lit("SKIP_ALREADY_COMMITTED_CHECKPOINT"))
        .when(F.col("sink_key_seen"), F.lit("SKIP_DUPLICATE_IDEMPOTENT"))
        .when(F.col("payload_quality") == F.lit("corrupt"), F.lit("QUARANTINE_BAD_RECORD"))
        .when(F.col("payload_quality") == F.lit("corrected_bad_record"), F.lit("REPLAY_CORRECTED_RECORD"))
        .otherwise(F.lit("PROCESS_REPLAY")),
    )
    .withColumn(
        "recovery_reason",
        F.when(F.col("checkpoint_seen"), F.lit("Checkpoint already has a committed file record."))
        .when(F.col("sink_key_seen"), F.lit("Delta sink already has the idempotency key."))
        .when(F.col("payload_quality") == F.lit("corrupt"), F.lit("Malformed payload needs quarantine and correction."))
        .when(F.col("payload_quality") == F.lit("corrected_bad_record"), F.lit("Corrected replacement for the failed source event."))
        .otherwise(F.lit("Not seen in checkpoint or sink; safe to process from replay boundary.")),
    )
    .select(
        "source_file_path",
        "event_id",
        "order_id",
        "payload_quality",
        "candidate_idempotency_key",
        "checkpoint_seen",
        "sink_key_seen",
        "decision_action",
        "recovery_reason",
        F.current_timestamp().alias("decided_at"),
    )
)

(
    decision_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("de_learning.checkpoint_replay_decisions_day32")
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
# MAGIC **DataFrame meaning:** `landing_df` is the source-file inventory; `state_df` is checkpoint evidence; `bronze_df` is the sink that protects replay idempotency.
# MAGIC
# MAGIC **SQL equivalent:** The PySpark block is a `LEFT JOIN` from landing files to committed checkpoint paths and existing sink keys, followed by a `CASE WHEN` decision column.
# MAGIC
# MAGIC **Syntax notes:**
# MAGIC - `F.col("commit_time").isNotNull()` is the DataFrame version of `commit_time IS NOT NULL`.
# MAGIC - `withColumn` adds derived columns such as `candidate_idempotency_key` and `decision_action`.
# MAGIC - `F.when(...).otherwise(...)` mirrors SQL `CASE WHEN`.
# MAGIC - Spark evaluates lazily; the write and `display` actions trigger the plan.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 4 - Apply idempotent replay and quarantine the bad record
# MAGIC
# MAGIC **Purpose:** Write only replay-safe records into bronze and capture the corrupt payload in a bad-record replay table.
# MAGIC
# MAGIC **Expected result:** Bronze has six unique rows, bad-record replay has one quarantined file, and the duplicate replay file is skipped.
# MAGIC
# MAGIC **Operational meaning:** The sink should tolerate both a same-checkpoint restart and a defensive replay from a new boundary because idempotency keys block duplicate writes.

# COMMAND ----------
# MAGIC %sql
# MAGIC INSERT INTO orders_bad_records_replay_day32
# MAGIC SELECT
# MAGIC   l.source_file_path,
# MAGIC   l.event_id,
# MAGIC   l.order_id,
# MAGIC   l.payload_quality,
# MAGIC   l.payload_hash,
# MAGIC   d.recovery_reason AS quarantine_reason,
# MAGIC   'Fix malformed payload, land corrected replacement, and replay only from uncommitted boundary.' AS replay_policy,
# MAGIC   current_timestamp() AS captured_at
# MAGIC FROM landing_files_checkpoint_day32 l
# MAGIC INNER JOIN checkpoint_replay_decisions_day32 d
# MAGIC   ON l.source_file_path = d.source_file_path
# MAGIC WHERE d.decision_action = 'QUARANTINE_BAD_RECORD';
# MAGIC
# MAGIC INSERT INTO orders_bronze_checkpoint_day32
# MAGIC SELECT
# MAGIC   l.event_id,
# MAGIC   l.order_id,
# MAGIC   l.order_amount,
# MAGIC   l.source_file_path,
# MAGIC   l.source_batch_id,
# MAGIC   l.payload_hash,
# MAGIC   's3://checkpoints/de_learning/orders_autoloader_day32/' AS checkpoint_location,
# MAGIC   CASE
# MAGIC     WHEN d.decision_action = 'REPLAY_CORRECTED_RECORD' THEN 2
# MAGIC     ELSE 1
# MAGIC   END AS micro_batch_id,
# MAGIC   'run-032-004' AS ingestion_run_id,
# MAGIC   current_timestamp() AS ingested_at,
# MAGIC   d.candidate_idempotency_key AS idempotency_key,
# MAGIC   CASE
# MAGIC     WHEN d.decision_action = 'REPLAY_CORRECTED_RECORD' THEN 'REPLAY_COMMITTED'
# MAGIC     ELSE 'COMMITTED'
# MAGIC   END AS record_status
# MAGIC FROM landing_files_checkpoint_day32 l
# MAGIC INNER JOIN checkpoint_replay_decisions_day32 d
# MAGIC   ON l.source_file_path = d.source_file_path
# MAGIC LEFT ANTI JOIN orders_bronze_checkpoint_day32 b
# MAGIC   ON b.idempotency_key = d.candidate_idempotency_key
# MAGIC WHERE d.decision_action IN ('PROCESS_REPLAY', 'REPLAY_CORRECTED_RECORD');
# MAGIC
# MAGIC INSERT INTO orders_curated_day32
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   event_id,
# MAGIC   max(order_amount) AS order_amount,
# MAGIC   max(source_batch_id) AS source_batch_id,
# MAGIC   'one_curated_row_per_idempotent_order_event' AS curated_reason,
# MAGIC   current_timestamp() AS curated_at
# MAGIC FROM orders_bronze_checkpoint_day32
# MAGIC WHERE record_status IN ('COMMITTED', 'REPLAY_COMMITTED')
# MAGIC GROUP BY order_id, event_id;
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM orders_bronze_checkpoint_day32) AS bronze_rows,
# MAGIC   (SELECT count(DISTINCT idempotency_key) FROM orders_bronze_checkpoint_day32) AS distinct_bronze_keys,
# MAGIC   (SELECT count(*) FROM orders_bad_records_replay_day32) AS quarantined_bad_records,
# MAGIC   (SELECT count(*) FROM orders_curated_day32) AS curated_rows;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 5 - Choose rollback, forward fix, or investigation clone
# MAGIC
# MAGIC **Purpose:** Classify common checkpoint incidents into the safest recovery action.
# MAGIC
# MAGIC **Expected result:** Six incident rows map symptoms to an operator decision, including forward fix, checkpoint restore, read-only clone, and idempotent sink protection.
# MAGIC
# MAGIC **Operational meaning:** Recovery choices should be repeatable enough for on-call use; checkpoint deletion or location changes are high-risk reprocess events.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE checkpoint_incident_events_day32 (
# MAGIC   incident_id STRING,
# MAGIC   symptom STRING,
# MAGIC   evidence_query STRING,
# MAGIC   blast_radius STRING,
# MAGIC   rollback_viable BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO checkpoint_incident_events_day32 VALUES
# MAGIC   ('inc-032-001', 'Checkpoint directory was deleted before restart.', 'Inspect object-store audit logs and confirm checkpoint path is empty before any rerun.', 'Stream might reread all available source files.', false),
# MAGIC   ('inc-032-002', 'Bad record blocks the next micro-batch.', 'Compare failed source file with quarantine and parser error details.', 'New files wait behind the failed batch boundary.', true),
# MAGIC   ('inc-032-003', 'New checkpoint was started against the same sink.', 'Check sink duplicate keys and job run configuration history.', 'Duplicate writes are possible unless the sink is idempotent.', false),
# MAGIC   ('inc-032-004', 'On-call needs root cause analysis without mutating production state.', 'Clone checkpoint files to an investigation prefix and run read-only inspection.', 'Production stream should remain paused or isolated from investigation runs.', true),
# MAGIC   ('inc-032-005', 'Auto Loader schema location is corrupted or missing.', 'Compare schema location history with last successful stream version.', 'Schema inference might drift and break downstream expectations.', true),
# MAGIC   ('inc-032-006', 'Lifecycle policy removed older checkpoint objects.', 'Check cloud storage lifecycle rules on checkpoint and schema prefixes.', 'Exactly-once evidence might be incomplete.', false);
# MAGIC
# MAGIC CREATE TABLE checkpoint_incident_decisions_day32 (
# MAGIC   incident_id STRING,
# MAGIC   recovery_decision STRING,
# MAGIC   first_operator_action STRING,
# MAGIC   approval_needed STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO checkpoint_incident_decisions_day32 VALUES
# MAGIC   ('inc-032-001', 'RESTORE_CHECKPOINT_BACKUP_OR_APPROVED_FULL_REPROCESS', 'Stop the job and do not start a new checkpoint against the same non-idempotent sink.', 'Data platform owner plus downstream table owner'),
# MAGIC   ('inc-032-002', 'FORWARD_FIX_AND_REPLAY_CORRECTED_RECORD', 'Quarantine bad payload, land corrected replacement, restart with the same checkpoint.', 'On-call engineer'),
# MAGIC   ('inc-032-003', 'STOP_AND_RECONCILE_WITH_IDEMPOTENT_SINK_KEYS', 'Stop the rerun, count duplicate sink keys, then merge or delete duplicates by approved policy.', 'Data platform owner'),
# MAGIC   ('inc-032-004', 'CLONE_CHECKPOINT_FOR_READ_ONLY_INVESTIGATION', 'Copy checkpoint to an isolated path and keep production checkpoint unchanged.', 'On-call engineer'),
# MAGIC   ('inc-032-005', 'RESTORE_SCHEMA_LOCATION_OR_APPROVED_SCHEMA_REINFERENCE', 'Restore the schema prefix from backup before rerunning inference.', 'Data contract owner'),
# MAGIC   ('inc-032-006', 'REMOVE_LIFECYCLE_POLICY_AND_REBUILD_EVIDENCE', 'Disable checkpoint expiration rules and decide whether a full reprocess is required.', 'Platform and cloud storage owners');
# MAGIC
# MAGIC SELECT
# MAGIC   e.incident_id,
# MAGIC   e.symptom,
# MAGIC   d.recovery_decision,
# MAGIC   d.approval_needed
# MAGIC FROM checkpoint_incident_events_day32 e
# MAGIC INNER JOIN checkpoint_incident_decisions_day32 d
# MAGIC   ON e.incident_id = d.incident_id
# MAGIC ORDER BY e.incident_id;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 6 - Save operator command templates
# MAGIC
# MAGIC **Purpose:** Store concrete command/query templates for same-checkpoint restart, file-state inspection, investigation cloning, idempotent sink writes, and checkpoint retention guardrails.
# MAGIC
# MAGIC **Expected result:** Seven templates are available for an on-call runbook.
# MAGIC
# MAGIC **Operational meaning:** Recovery work is faster and safer when operators have reviewed commands ready before an incident.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE checkpoint_command_templates_day32 (
# MAGIC   template_name STRING,
# MAGIC   command_type STRING,
# MAGIC   command_text STRING,
# MAGIC   operator_note STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO checkpoint_command_templates_day32 VALUES
# MAGIC   ('autoloader_available_now_same_checkpoint', 'pyspark', 'spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", schema_location).load(source_path).writeStream.option("checkpointLocation", checkpoint_location).trigger(availableNow=True).toTable(sink_table)', 'Use the original checkpoint location to resume from committed source-file evidence.'),
# MAGIC   ('inspect_cloud_files_state', 'sql', 'SELECT * FROM cloud_files_state("s3://checkpoints/de_learning/orders_autoloader_day32/") ORDER BY commit_time DESC', 'Inspect discovered and committed files for an Auto Loader stream checkpoint.'),
# MAGIC   ('same_checkpoint_restart', 'job_config', 'Restart the failed task without changing source_path, schema_location, checkpoint_location, or sink_table.', 'Changing these paths turns a restart into a reprocess.'),
# MAGIC   ('clone_checkpoint_investigation', 'cloud_cli', 'Copy s3://checkpoints/de_learning/orders_autoloader_day32/ to s3://checkpoints-investigation/de_learning/orders_autoloader_day32/inc-032-004/ before read-only analysis.', 'Use an isolated clone for forensics; never mutate the production checkpoint during analysis.'),
# MAGIC   ('idempotent_merge_sink', 'sql', 'MERGE INTO target USING replay_source ON target.idempotency_key = replay_source.idempotency_key WHEN NOT MATCHED THEN INSERT *', 'A replay-safe sink needs a stable key independent of file arrival order.'),
# MAGIC   ('bad_record_quarantine_replay', 'sql', 'INSERT INTO bad_records SELECT * FROM source WHERE parser_status = "corrupt"; land corrected file; restart with same checkpoint.', 'Keep the failed payload as evidence and replay a corrected replacement.'),
# MAGIC   ('checkpoint_retention_guardrail', 'policy', 'Disable object-store lifecycle expiration on checkpoint and schema prefixes used by active production streams.', 'Expired checkpoint files can destroy exactly-once recovery evidence.');
# MAGIC
# MAGIC SELECT template_name, command_type, operator_note
# MAGIC FROM checkpoint_command_templates_day32
# MAGIC ORDER BY template_name;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 7 - Run final recovery checks
# MAGIC
# MAGIC **Purpose:** Validate that the recovery plan, replay application, incident decisions, command templates, and runbook evidence agree.
# MAGIC
# MAGIC **Expected result:** All check counts match the intended Day 32 recovery story.
# MAGIC
# MAGIC **Operational meaning:** A recovery is not complete until sink counts, duplicate protection, bad-record handling, and operator evidence are reviewable.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE checkpoint_recovery_runbook_day32 (
# MAGIC   step_number INT,
# MAGIC   runbook_step STRING,
# MAGIC   required_evidence STRING,
# MAGIC   done_criteria STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO checkpoint_recovery_runbook_day32 VALUES
# MAGIC   (1, 'Freeze the production write path.', 'Current job run id, cluster id, checkpoint path, schema path, and sink table.', 'No second writer is active against the same sink.'),
# MAGIC   (2, 'Inspect checkpoint and source-file state.', 'cloud_files_state output or equivalent checkpoint inventory.', 'Committed files and failed boundary are identified.'),
# MAGIC   (3, 'Classify the incident.', 'Incident decision table row with approval owner.', 'Operator knows whether to forward fix, restore, clone, or reprocess.'),
# MAGIC   (4, 'Replay only uncommitted records with sink idempotency.', 'Replay decision table and Delta sink idempotency key counts.', 'No duplicate idempotency keys exist in bronze.'),
# MAGIC   (5, 'Publish recovery evidence.', 'Run history, quarantine rows, final checks, and next prevention action.', 'Incident can be reviewed without rerunning the failed stream.');
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW checkpoint_final_checks_day32 AS
# MAGIC SELECT 'configs' AS check_name, count(*) AS actual_count, 1 AS expected_count FROM stream_checkpoint_config_day32
# MAGIC UNION ALL SELECT 'landing_files', count(*), 8 FROM landing_files_checkpoint_day32
# MAGIC UNION ALL SELECT 'checkpoint_state_rows', count(*), 3 FROM checkpoint_file_state_day32
# MAGIC UNION ALL SELECT 'replay_decisions', count(*), 8 FROM checkpoint_replay_decisions_day32
# MAGIC UNION ALL SELECT 'skip_committed', count(*), 2 FROM checkpoint_replay_decisions_day32 WHERE decision_action = 'SKIP_ALREADY_COMMITTED_CHECKPOINT'
# MAGIC UNION ALL SELECT 'skip_duplicate', count(*), 1 FROM checkpoint_replay_decisions_day32 WHERE decision_action = 'SKIP_DUPLICATE_IDEMPOTENT'
# MAGIC UNION ALL SELECT 'quarantined_bad_records', count(*), 1 FROM orders_bad_records_replay_day32
# MAGIC UNION ALL SELECT 'bronze_rows', count(*), 6 FROM orders_bronze_checkpoint_day32
# MAGIC UNION ALL SELECT 'distinct_bronze_keys', count(DISTINCT idempotency_key), 6 FROM orders_bronze_checkpoint_day32
# MAGIC UNION ALL SELECT 'curated_rows', count(*), 6 FROM orders_curated_day32
# MAGIC UNION ALL SELECT 'incident_decisions', count(*), 6 FROM checkpoint_incident_decisions_day32
# MAGIC UNION ALL SELECT 'command_templates', count(*), 7 FROM checkpoint_command_templates_day32
# MAGIC UNION ALL SELECT 'runbook_steps', count(*), 5 FROM checkpoint_recovery_runbook_day32;
# MAGIC
# MAGIC SELECT
# MAGIC   check_name,
# MAGIC   actual_count,
# MAGIC   expected_count,
# MAGIC   CASE WHEN actual_count = expected_count THEN 'PASS' ELSE 'FAIL' END AS check_status
# MAGIC FROM checkpoint_final_checks_day32
# MAGIC ORDER BY check_name;
