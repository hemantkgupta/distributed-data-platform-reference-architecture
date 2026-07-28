# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 29 - Auto Loader File Discovery And Cost Controls
# MAGIC
# MAGIC Goal: choose Auto Loader file detection mode, trigger strategy, rate limits, source cleanup policy, and monitoring signals for production ingestion sources.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: ingestion/loading, Auto Loader options, Lakeflow Jobs scheduling, monitoring, troubleshooting, and governance.
# MAGIC - Professional stretch: file-events migration, directory-listing cost control, `AvailableNow` batch sizing, checkpoint state, `cloudFiles.cleanSource` risk, and replay-safe source retention.
# MAGIC
# MAGIC This notebook simulates Auto Loader planning and file state with Delta tables. The command-template table contains production shapes for directory listing, managed file events, classic file notifications, `AvailableNow`, and `cleanSource`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Source Profiles, Landing Files, And File-State Tables
# MAGIC
# MAGIC Purpose: model six ingestion sources with different volume, latency, storage, retention, and consumer constraints.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS autoloader_final_checks_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_runbook_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_command_templates_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_alerts_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_monitoring_metrics_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_clean_source_decisions_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_clean_source_requests_day29;
# MAGIC DROP TABLE IF EXISTS raw_events_bronze_autoloader_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_file_state_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_microbatch_plan_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_discovery_decisions_day29;
# MAGIC DROP TABLE IF EXISTS landing_file_inventory_day29;
# MAGIC DROP TABLE IF EXISTS autoloader_source_profiles_day29;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_source_profiles_day29 (
# MAGIC   source_id STRING,
# MAGIC   source_owner STRING,
# MAGIC   files_per_day BIGINT,
# MAGIC   average_file_size_mb DOUBLE,
# MAGIC   latency_sla_minutes INT,
# MAGIC   source_directory_existing_files BIGINT,
# MAGIC   can_enable_file_events BOOLEAN,
# MAGIC   has_uc_external_location BOOLEAN,
# MAGIC   stream_runs_at_least_weekly BOOLEAN,
# MAGIC   multiple_consumers BOOLEAN,
# MAGIC   compliance_retain_raw_days INT,
# MAGIC   replay_window_days INT,
# MAGIC   low_latency_critical BOOLEAN,
# MAGIC   source_path STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_source_profiles_day29 VALUES
# MAGIC   ('orders_hourly', 'orders-platform', 300, 9.0, 60, 24000, true, true, true, false, 90, 90, false, '/Volumes/main/de_learning/raw_orders/hourly/'),
# MAGIC   ('clickstream_mobile', 'growth-analytics', 250000, 145.0, 5, 9000000, true, true, true, false, 30, 30, false, '/Volumes/main/de_learning/raw_clickstream/mobile/'),
# MAGIC   ('audit_logs_regulated', 'security-governance', 5000, 31.0, 15, 1200000, true, true, true, true, 2555, 365, false, '/Volumes/main/de_learning/raw_audit/logs/'),
# MAGIC   ('vendor_drop_daily', 'vendor-ingestion', 40, 2.0, 1440, 12000, false, true, true, false, 180, 180, false, '/Volumes/main/de_learning/raw_vendor/daily/'),
# MAGIC   ('ml_features_backfill', 'ml-platform', 80000, 568.0, 1440, 500000, false, true, true, false, 30, 30, false, '/Volumes/main/de_learning/raw_ml/features_backfill/'),
# MAGIC   ('iot_realtime', 'device-platform', 60000, 17.5, 1, 2500000, true, true, true, false, 14, 14, true, '/Volumes/main/de_learning/raw_iot/events/');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landing_file_inventory_day29 (
# MAGIC   source_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   file_size_mb DOUBLE,
# MAGIC   event_count BIGINT,
# MAGIC   arrival_batch STRING,
# MAGIC   payload_sample STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO landing_file_inventory_day29 VALUES
# MAGIC   ('orders_hourly', 'dbfs:/landing/day29/orders/hour=05/orders_001.json', TIMESTAMP '2026-07-28 05:00:00', 12.0, 12000, 'hour_05', '{"source":"orders","batch":"hour_05"}'),
# MAGIC   ('orders_hourly', 'dbfs:/landing/day29/orders/hour=05/orders_002.json', TIMESTAMP '2026-07-28 05:01:00', 8.0, 8000, 'hour_05', '{"source":"orders","batch":"hour_05"}'),
# MAGIC   ('orders_hourly', 'dbfs:/landing/day29/orders/hour=05/orders_003.json', TIMESTAMP '2026-07-28 05:02:00', 9.0, 9000, 'hour_05', '{"source":"orders","batch":"hour_05"}'),
# MAGIC   ('clickstream_mobile', 'dbfs:/landing/day29/clickstream/minute=00/click_001.json', TIMESTAMP '2026-07-28 05:00:10', 128.0, 620000, 'minute_00', '{"source":"clickstream","batch":"minute_00"}'),
# MAGIC   ('clickstream_mobile', 'dbfs:/landing/day29/clickstream/minute=00/click_002.json', TIMESTAMP '2026-07-28 05:00:20', 140.0, 680000, 'minute_00', '{"source":"clickstream","batch":"minute_00"}'),
# MAGIC   ('clickstream_mobile', 'dbfs:/landing/day29/clickstream/minute=00/click_003.json', TIMESTAMP '2026-07-28 05:00:30', 95.0, 440000, 'minute_00', '{"source":"clickstream","batch":"minute_00"}'),
# MAGIC   ('clickstream_mobile', 'dbfs:/landing/day29/clickstream/minute=00/click_004.json', TIMESTAMP '2026-07-28 05:00:40', 220.0, 980000, 'minute_00', '{"source":"clickstream","batch":"minute_00"}'),
# MAGIC   ('iot_realtime', 'dbfs:/landing/day29/iot/second=00/iot_001.json', TIMESTAMP '2026-07-28 05:00:01', 18.0, 90000, 'second_00', '{"source":"iot","batch":"second_00"}'),
# MAGIC   ('iot_realtime', 'dbfs:/landing/day29/iot/second=00/iot_002.json', TIMESTAMP '2026-07-28 05:00:02', 17.0, 85000, 'second_00', '{"source":"iot","batch":"second_00"}'),
# MAGIC   ('vendor_drop_daily', 'dbfs:/landing/day29/vendor/date=2026-07-28/vendor_001.json', TIMESTAMP '2026-07-28 04:50:00', 2.0, 1000, 'daily_2026_07_28', '{"source":"vendor","batch":"daily"}'),
# MAGIC   ('vendor_drop_daily', 'dbfs:/landing/day29/vendor/date=2026-07-28/vendor_002.json', TIMESTAMP '2026-07-28 04:51:00', 2.0, 1000, 'daily_2026_07_28', '{"source":"vendor","batch":"daily"}'),
# MAGIC   ('ml_features_backfill', 'dbfs:/landing/day29/ml/backfill/features_001.parquet', TIMESTAMP '2026-07-28 03:00:00', 512.0, 10000000, 'backfill_001', '{"source":"ml","batch":"backfill"}'),
# MAGIC   ('ml_features_backfill', 'dbfs:/landing/day29/ml/backfill/features_002.parquet', TIMESTAMP '2026-07-28 03:01:00', 488.0, 9500000, 'backfill_001', '{"source":"ml","batch":"backfill"}'),
# MAGIC   ('ml_features_backfill', 'dbfs:/landing/day29/ml/backfill/features_003.parquet', TIMESTAMP '2026-07-28 03:02:00', 700.0, 13000000, 'backfill_001', '{"source":"ml","batch":"backfill"}'),
# MAGIC   ('ml_features_backfill', 'dbfs:/landing/day29/ml/backfill/features_004.parquet', TIMESTAMP '2026-07-28 03:03:00', 530.0, 10000000, 'backfill_001', '{"source":"ml","batch":"backfill"}'),
# MAGIC   ('ml_features_backfill', 'dbfs:/landing/day29/ml/backfill/features_005.parquet', TIMESTAMP '2026-07-28 03:04:00', 610.0, 11500000, 'backfill_001', '{"source":"ml","batch":"backfill"}'),
# MAGIC   ('audit_logs_regulated', 'dbfs:/landing/day29/audit/hour=05/audit_001.json', TIMESTAMP '2026-07-28 05:05:00', 30.0, 45000, 'hour_05', '{"source":"audit","batch":"hour_05"}'),
# MAGIC   ('audit_logs_regulated', 'dbfs:/landing/day29/audit/hour=05/audit_002.json', TIMESTAMP '2026-07-28 05:06:00', 32.0, 47000, 'hour_05', '{"source":"audit","batch":"hour_05"}');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_file_state_day29 (
# MAGIC   source_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_size_mb DOUBLE,
# MAGIC   discovery_mode STRING,
# MAGIC   discovered_at TIMESTAMP,
# MAGIC   commit_time TIMESTAMP,
# MAGIC   micro_batch_id STRING,
# MAGIC   file_state_status STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   schema_location STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE raw_events_bronze_autoloader_day29 (
# MAGIC   source_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   event_count BIGINT,
# MAGIC   file_size_mb DOUBLE,
# MAGIC   _ingested_at TIMESTAMP,
# MAGIC   _ingest_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_id, COUNT(*) AS file_count, ROUND(SUM(file_size_mb), 2) AS backlog_mb
# MAGIC FROM landing_file_inventory_day29
# MAGIC GROUP BY source_id
# MAGIC ORDER BY source_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Six source profiles.
# MAGIC - Eighteen landed files.
# MAGIC - `clickstream_mobile` and `ml_features_backfill` have the largest simulated backlog.
# MAGIC
# MAGIC Operational meaning: discovery mode and trigger strategy depend on source workload shape, not on a single global Auto Loader default.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Choose File Detection Mode, Trigger, And Rate Limits
# MAGIC
# MAGIC Purpose: use PySpark to recommend directory listing, managed file events, classic file notifications, and trigger/rate-limit settings.

# COMMAND ----------

from pyspark.sql import functions as F

profiles_df = spark.table("de_learning.autoloader_source_profiles_day29")

decisions_df = (
    profiles_df
    .withColumn(
        "high_volume",
        (F.col("files_per_day") >= F.lit(10000))
        | (F.col("source_directory_existing_files") >= F.lit(100000)),
    )
    .withColumn("low_latency", F.col("latency_sla_minutes") <= F.lit(15))
    .withColumn(
        "file_events_ready",
        F.col("can_enable_file_events")
        & F.col("has_uc_external_location")
        & F.col("stream_runs_at_least_weekly"),
    )
    .withColumn(
        "recommended_discovery_mode",
        F.when(
            F.col("low_latency_critical") & F.col("can_enable_file_events"),
            F.lit("CLASSIC_FILE_NOTIFICATION"),
        )
        .when(F.col("file_events_ready"), F.lit("FILE_NOTIFICATION_WITH_FILE_EVENTS"))
        .when(F.col("high_volume"), F.lit("DIRECTORY_LISTING_WITH_BACKFILL_GUARDRAILS"))
        .otherwise(F.lit("DIRECTORY_LISTING")),
    )
    .withColumn(
        "recommended_trigger",
        F.when(F.col("low_latency_critical"), F.lit("processingTime_10_seconds"))
        .when(F.col("latency_sla_minutes") <= F.lit(15), F.lit("processingTime_1_minute"))
        .otherwise(F.lit("AvailableNow_scheduled")),
    )
    .withColumn(
        "max_files_per_trigger",
        F.when(F.col("source_id") == F.lit("clickstream_mobile"), F.lit(2))
        .when(F.col("source_id") == F.lit("iot_realtime"), F.lit(1))
        .when(F.col("source_id") == F.lit("ml_features_backfill"), F.lit(2))
        .otherwise(F.lit(1000)),
    )
    .withColumn(
        "max_bytes_per_trigger_mb",
        F.when(F.col("source_id") == F.lit("clickstream_mobile"), F.lit(300.0))
        .when(F.col("source_id") == F.lit("iot_realtime"), F.lit(64.0))
        .when(F.col("source_id") == F.lit("ml_features_backfill"), F.lit(1024.0))
        .otherwise(F.lit(2048.0)),
    )
    .withColumn(
        "operator_reason",
        F.when(
            F.col("recommended_discovery_mode") == F.lit("CLASSIC_FILE_NOTIFICATION"),
            F.lit("Very low latency source; queue-backed notification avoids directory listing and removes the file-events cache hop."),
        )
        .when(
            F.col("recommended_discovery_mode") == F.lit("FILE_NOTIFICATION_WITH_FILE_EVENTS"),
            F.lit("File events are enabled and the stream runs weekly, so discovery is incremental and scalable."),
        )
        .when(
            F.col("recommended_discovery_mode") == F.lit("DIRECTORY_LISTING_WITH_BACKFILL_GUARDRAILS"),
            F.lit("File events are not available; use AvailableNow, bounded triggers, and periodic backfill discipline."),
        )
        .otherwise(F.lit("Small scheduled source can start with directory listing.")),
    )
    .select(
        "source_id",
        "source_owner",
        "recommended_discovery_mode",
        "recommended_trigger",
        "max_files_per_trigger",
        "max_bytes_per_trigger_mb",
        "high_volume",
        "low_latency",
        "file_events_ready",
        "operator_reason",
    )
)

decisions_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "de_learning.autoloader_discovery_decisions_day29"
)

display(decisions_df.orderBy("source_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `clickstream_mobile`, `orders_hourly`, and `audit_logs_regulated` use file notification with file events.
# MAGIC - `iot_realtime` uses classic file notification because it is marked very latency-sensitive.
# MAGIC - `vendor_drop_daily` uses directory listing.
# MAGIC - `ml_features_backfill` uses directory listing with backfill/rate-limit guardrails.
# MAGIC
# MAGIC Operational meaning: Databricks recommends file notification mode with file events for most workloads, but very low latency and event-permission constraints can change the answer.
# MAGIC
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `profiles_df` represents source-level operational requirements: file volume, latency, file-events readiness, retention, and replay needs.
# MAGIC - SQL equivalent: `SELECT source_id, CASE WHEN ... THEN recommended_mode END FROM autoloader_source_profiles_day29`.
# MAGIC - `F.col(...)` references table columns inside expressions.
# MAGIC - `withColumn(...)` adds derived flags such as `high_volume`, `low_latency`, and `file_events_ready`.
# MAGIC - `F.when(...).otherwise(...)` is DataFrame syntax for SQL `CASE WHEN`.
# MAGIC - `select(...)` keeps the decision columns that become the saved table.
# MAGIC - PySpark is lazy until `write.saveAsTable(...)` and `display(...)` execute the plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Simulate AvailableNow Rate-Limited Micro-Batches
# MAGIC
# MAGIC Purpose: process only the files allowed by each source's `cloudFiles.maxFilesPerTrigger` and `cloudFiles.maxBytesPerTrigger` settings.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_microbatch_plan_day29 AS
# MAGIC WITH ranked AS (
# MAGIC   SELECT
# MAGIC     l.source_id,
# MAGIC     l.file_path,
# MAGIC     l.file_mod_time,
# MAGIC     l.file_size_mb,
# MAGIC     l.event_count,
# MAGIC     d.recommended_discovery_mode,
# MAGIC     d.recommended_trigger,
# MAGIC     d.max_files_per_trigger,
# MAGIC     d.max_bytes_per_trigger_mb,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY l.source_id ORDER BY l.file_mod_time, l.file_path) AS file_rank,
# MAGIC     SUM(l.file_size_mb) OVER (
# MAGIC       PARTITION BY l.source_id
# MAGIC       ORDER BY l.file_mod_time, l.file_path
# MAGIC       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC     ) AS running_mb
# MAGIC   FROM landing_file_inventory_day29 l
# MAGIC   JOIN autoloader_discovery_decisions_day29 d
# MAGIC     ON l.source_id = d.source_id
# MAGIC   LEFT ANTI JOIN autoloader_file_state_day29 s
# MAGIC     ON l.file_path = s.file_path
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN file_rank <= max_files_per_trigger AND running_mb <= max_bytes_per_trigger_mb
# MAGIC       THEN 'PROCESS_THIS_TRIGGER'
# MAGIC     ELSE 'DEFER_RATE_LIMIT'
# MAGIC   END AS microbatch_decision
# MAGIC FROM ranked;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_file_state_day29
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   file_path,
# MAGIC   file_size_mb,
# MAGIC   recommended_discovery_mode AS discovery_mode,
# MAGIC   current_timestamp() AS discovered_at,
# MAGIC   CASE WHEN microbatch_decision = 'PROCESS_THIS_TRIGGER' THEN current_timestamp() ELSE NULL END AS commit_time,
# MAGIC   'microbatch-2901' AS micro_batch_id,
# MAGIC   microbatch_decision AS file_state_status,
# MAGIC   concat('/Volumes/main/de_learning/ops/checkpoints/', source_id) AS checkpoint_location,
# MAGIC   concat('/Volumes/main/de_learning/ops/schemas/', source_id) AS schema_location
# MAGIC FROM autoloader_microbatch_plan_day29;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO raw_events_bronze_autoloader_day29
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   file_path AS source_file_path,
# MAGIC   event_count,
# MAGIC   file_size_mb,
# MAGIC   current_timestamp() AS _ingested_at,
# MAGIC   'auto-available-now-2901' AS _ingest_run_id
# MAGIC FROM autoloader_microbatch_plan_day29
# MAGIC WHERE microbatch_decision = 'PROCESS_THIS_TRIGGER';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_id, microbatch_decision, COUNT(*) AS file_count, ROUND(SUM(file_size_mb), 2) AS total_mb
# MAGIC FROM autoloader_microbatch_plan_day29
# MAGIC GROUP BY source_id, microbatch_decision
# MAGIC ORDER BY source_id, microbatch_decision;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `PROCESS_THIS_TRIGGER = 12` files.
# MAGIC - `DEFER_RATE_LIMIT = 6` files.
# MAGIC - `clickstream_mobile`, `iot_realtime`, and `ml_features_backfill` leave backlog because of rate limits.
# MAGIC
# MAGIC Operational meaning: `AvailableNow` processes the files available at query start, but it can still split work across multiple micro-batches using file and byte limits.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Gate `cloudFiles.cleanSource`
# MAGIC
# MAGIC Purpose: decide whether processed source files can be left alone, moved, or deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_clean_source_requests_day29 (
# MAGIC   request_id STRING,
# MAGIC   source_id STRING,
# MAGIC   clean_source_mode STRING,
# MAGIC   retention_days INT,
# MAGIC   source_bucket STRING,
# MAGIC   move_destination_bucket STRING,
# MAGIC   move_destination_path STRING,
# MAGIC   bucket_versioning_enabled BOOLEAN,
# MAGIC   foreach_batch_partial_consumption BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_clean_source_requests_day29 VALUES
# MAGIC   ('clean-2901', 'orders_hourly', 'MOVE', 30, 'lakehouse-raw-prod', 'lakehouse-raw-prod', 's3://lakehouse-raw-prod/archive/orders/', true, false),
# MAGIC   ('clean-2902', 'clickstream_mobile', 'DELETE', 14, 'lakehouse-clickstream-prod', null, null, true, false),
# MAGIC   ('clean-2903', 'audit_logs_regulated', 'DELETE', 30, 'lakehouse-audit-prod', null, null, true, false),
# MAGIC   ('clean-2904', 'vendor_drop_daily', 'MOVE', 30, 'vendor-landing-prod', 'lakehouse-archive-prod', 's3://lakehouse-archive-prod/vendor/', true, false),
# MAGIC   ('clean-2905', 'ml_features_backfill', 'OFF', 0, 'lakehouse-ml-prod', null, null, true, false),
# MAGIC   ('clean-2906', 'iot_realtime', 'DELETE', 5, 'lakehouse-iot-prod', null, null, false, false);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_clean_source_decisions_day29 AS
# MAGIC SELECT
# MAGIC   r.request_id,
# MAGIC   r.source_id,
# MAGIC   r.clean_source_mode,
# MAGIC   r.retention_days,
# MAGIC   p.multiple_consumers,
# MAGIC   p.compliance_retain_raw_days,
# MAGIC   r.bucket_versioning_enabled,
# MAGIC   r.foreach_batch_partial_consumption,
# MAGIC   CASE
# MAGIC     WHEN r.clean_source_mode = 'OFF' THEN 'APPROVE_OFF'
# MAGIC     WHEN p.multiple_consumers THEN 'BLOCK_MULTIPLE_CONSUMERS'
# MAGIC     WHEN r.foreach_batch_partial_consumption THEN 'BLOCK_FOREACH_BATCH_PARTIAL_CONSUMPTION'
# MAGIC     WHEN r.clean_source_mode = 'DELETE' AND r.retention_days <= 7 THEN 'BLOCK_DELETE_RETENTION_TOO_SHORT'
# MAGIC     WHEN r.clean_source_mode = 'DELETE' AND r.retention_days < p.compliance_retain_raw_days THEN 'BLOCK_RAW_RETENTION_REQUIREMENT'
# MAGIC     WHEN r.clean_source_mode = 'DELETE' AND r.bucket_versioning_enabled = false THEN 'BLOCK_DELETE_WITHOUT_BUCKET_VERSIONING'
# MAGIC     WHEN r.clean_source_mode = 'MOVE' AND r.source_bucket <> r.move_destination_bucket THEN 'BLOCK_MOVE_CROSS_BUCKET'
# MAGIC     WHEN r.clean_source_mode = 'MOVE' AND r.move_destination_path IS NULL THEN 'BLOCK_MISSING_MOVE_DESTINATION'
# MAGIC     WHEN r.clean_source_mode = 'MOVE' THEN 'APPROVE_MOVE'
# MAGIC     WHEN r.clean_source_mode = 'DELETE' THEN 'APPROVE_DELETE_WITH_VERSIONING'
# MAGIC     ELSE 'BLOCK_UNKNOWN_CLEAN_SOURCE_MODE'
# MAGIC   END AS cleanup_decision
# MAGIC FROM autoloader_clean_source_requests_day29 r
# MAGIC JOIN autoloader_source_profiles_day29 p
# MAGIC   ON r.source_id = p.source_id;
# MAGIC
# MAGIC SELECT * FROM autoloader_clean_source_decisions_day29 ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Two approvals: `APPROVE_MOVE`, `APPROVE_DELETE_WITH_VERSIONING`.
# MAGIC - One safe no-op: `APPROVE_OFF`.
# MAGIC - Three blocked requests: multiple consumers, cross-bucket move, and too-short delete retention.
# MAGIC
# MAGIC Operational meaning: `cloudFiles.cleanSource` can lower discovery and storage costs, but it deletes or moves source files. Gate it with consumer, retention, versioning, and destination checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Build `cloud_files_state`-Style Monitoring Metrics
# MAGIC
# MAGIC Purpose: expose backlog, queue, listing-cost, and cleanup-candidate metrics that an on-call data engineer can inspect.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_monitoring_metrics_day29 AS
# MAGIC SELECT
# MAGIC   s.source_id,
# MAGIC   d.recommended_discovery_mode,
# MAGIC   d.recommended_trigger,
# MAGIC   COUNT(*) AS discovered_files,
# MAGIC   SUM(CASE WHEN s.commit_time IS NOT NULL THEN 1 ELSE 0 END) AS committed_files,
# MAGIC   SUM(CASE WHEN s.commit_time IS NULL THEN 1 ELSE 0 END) AS num_files_outstanding,
# MAGIC   ROUND(SUM(CASE WHEN s.commit_time IS NULL THEN s.file_size_mb ELSE 0 END), 2) AS num_mb_outstanding,
# MAGIC   SUM(CASE WHEN s.discovery_mode IN ('FILE_NOTIFICATION_WITH_FILE_EVENTS', 'CLASSIC_FILE_NOTIFICATION') AND s.commit_time IS NULL THEN 1 ELSE 0 END) AS approximate_queue_size,
# MAGIC   CASE
# MAGIC     WHEN d.recommended_discovery_mode LIKE 'DIRECTORY_LISTING%' THEN CAST(CEIL(MAX(p.source_directory_existing_files) / 1000.0) AS BIGINT)
# MAGIC     ELSE 0
# MAGIC   END AS estimated_list_calls_per_trigger,
# MAGIC   SUM(CASE WHEN s.commit_time IS NOT NULL AND c.cleanup_decision IN ('APPROVE_MOVE', 'APPROVE_DELETE_WITH_VERSIONING') THEN 1 ELSE 0 END) AS cleanup_candidates_after_retention
# MAGIC FROM autoloader_file_state_day29 s
# MAGIC JOIN autoloader_discovery_decisions_day29 d
# MAGIC   ON s.source_id = d.source_id
# MAGIC JOIN autoloader_source_profiles_day29 p
# MAGIC   ON s.source_id = p.source_id
# MAGIC LEFT JOIN autoloader_clean_source_decisions_day29 c
# MAGIC   ON s.source_id = c.source_id
# MAGIC GROUP BY s.source_id, d.recommended_discovery_mode, d.recommended_trigger
# MAGIC ORDER BY s.source_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_alerts_day29 AS
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   CASE
# MAGIC     WHEN num_files_outstanding > 0 AND recommended_trigger = 'processingTime_10_seconds' THEN 'PAGE_REALTIME_BACKLOG'
# MAGIC     WHEN num_files_outstanding > 0 AND recommended_trigger = 'processingTime_1_minute' THEN 'WARN_LOW_LATENCY_BACKLOG'
# MAGIC     WHEN num_files_outstanding > 0 THEN 'TRACK_NEXT_AVAILABLE_NOW_RUN'
# MAGIC     ELSE 'OK'
# MAGIC   END AS backlog_alert,
# MAGIC   CASE
# MAGIC     WHEN estimated_list_calls_per_trigger > 100 THEN 'HIGH_DIRECTORY_LISTING_COST'
# MAGIC     WHEN approximate_queue_size > 0 THEN 'QUEUE_BACKLOG_VISIBLE'
# MAGIC     ELSE 'OK'
# MAGIC   END AS cost_or_queue_signal,
# MAGIC   num_files_outstanding,
# MAGIC   num_mb_outstanding,
# MAGIC   approximate_queue_size,
# MAGIC   estimated_list_calls_per_trigger
# MAGIC FROM autoloader_monitoring_metrics_day29;
# MAGIC
# MAGIC SELECT * FROM autoloader_alerts_day29 ORDER BY source_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `clickstream_mobile` reports queue backlog.
# MAGIC - `iot_realtime` pages because real-time backlog remains.
# MAGIC - `ml_features_backfill` shows high directory-listing cost.
# MAGIC - Completed sources are `OK`.
# MAGIC
# MAGIC Operational meaning: Auto Loader monitoring should separate queue backlog, directory listing cost, and delayed scheduled processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Store Production Command Templates
# MAGIC
# MAGIC Purpose: keep real Auto Loader command shapes next to the simulated decisions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_command_templates_day29 (
# MAGIC   template_name STRING,
# MAGIC   template_text STRING,
# MAGIC   when_to_use STRING,
# MAGIC   operational_meaning STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_command_templates_day29 VALUES
# MAGIC   (
# MAGIC     'directory_listing_available_now',
# MAGIC     '(spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", schema_path).option("cloudFiles.maxFilesPerTrigger", "1000").load(source_path).writeStream.option("checkpointLocation", checkpoint_path).trigger(availableNow=True).toTable("de_learning.raw_events_bronze_autoloader_day29"))',
# MAGIC     'Small or scheduled sources where directory listing cost is acceptable.',
# MAGIC     'Fast to start, but repeated listing can become expensive as directories grow.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'managed_file_events',
# MAGIC     '.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.useManagedFileEvents", "true").load("/Volumes/<catalog>/<schema>/<source_volume>/")',
# MAGIC     'Most scalable default when file events are enabled on the Unity Catalog external location or volume.',
# MAGIC     'Reduces repeated listing and improves file discovery scalability.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'classic_file_notification',
# MAGIC     '.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.useNotifications", "true").load("s3://bucket/path/")',
# MAGIC     'Very latency-sensitive streams that read directly from a cloud queue.',
# MAGIC     'Lower event path latency, but requires queue/resource setup and permission control.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'available_now_rate_limited',
# MAGIC     '.option("cloudFiles.maxFilesPerTrigger", "2000").option("cloudFiles.maxBytesPerTrigger", "10g").trigger(availableNow=True)',
# MAGIC     'Incremental batch processing with bounded micro-batches.',
# MAGIC     'Controls compute size and cloud-storage request cost for scheduled jobs.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'clean_source_move',
# MAGIC     '.option("cloudFiles.cleanSource", "MOVE").option("cloudFiles.cleanSource.moveDestination", "/Volumes/<catalog>/<schema>/<archive_volume>/orders/").option("cloudFiles.cleanSource.retentionDuration", "30 days")',
# MAGIC     'Archive processed files after a retention delay when there is one consumer and replay evidence exists.',
# MAGIC     'Can remove source files from slower consumers if used without ownership checks.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'cloud_files_state_monitoring',
# MAGIC     'SELECT * FROM cloud_files_state("/Volumes/<catalog>/<schema>/<ops_volume>/checkpoints/<stream_id>");',
# MAGIC     'Inspect file discovery, commit time, and cleanup eligibility from the checkpoint.',
# MAGIC     'Turns checkpoint state into incident evidence.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT template_name, when_to_use, operational_meaning
# MAGIC FROM autoloader_command_templates_day29
# MAGIC ORDER BY template_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Six command templates covering directory listing, managed file events, classic notifications, AvailableNow rate limits, cleanSource move, and `cloud_files_state`.
# MAGIC
# MAGIC Operational meaning: production teams should standardize discovery and cleanup options so risky settings are reviewed before deployment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Checks And Operator Runbook
# MAGIC
# MAGIC Purpose: validate the day-scoped artifacts and capture a concise runbook for future ingestion incidents.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_runbook_day29 AS
# MAGIC SELECT '1_choose_discovery' AS step_id,
# MAGIC        'Use file events for most scalable workloads; keep directory listing for small scheduled sources or blocked event setup.' AS operator_action,
# MAGIC        'autoloader_discovery_decisions_day29' AS evidence_table,
# MAGIC        'Every source has a discovery mode and trigger choice.' AS pass_condition
# MAGIC UNION ALL
# MAGIC SELECT '2_rate_limit_available_now',
# MAGIC        'Set max files and max bytes per trigger for large incremental batches.',
# MAGIC        'autoloader_microbatch_plan_day29',
# MAGIC        'Deferred files are intentional and visible.'
# MAGIC UNION ALL
# MAGIC SELECT '3_gate_cleanup',
# MAGIC        'Approve cleanSource only after checking consumers, retention, bucket versioning, and move destination.',
# MAGIC        'autoloader_clean_source_decisions_day29',
# MAGIC        'No destructive cleanup is approved without evidence.'
# MAGIC UNION ALL
# MAGIC SELECT '4_monitor_backlog',
# MAGIC        'Inspect outstanding files, queue size, list-call estimate, and cleanup candidates.',
# MAGIC        'autoloader_monitoring_metrics_day29, autoloader_alerts_day29',
# MAGIC        'Backlog and cost signals route to the right owner.'
# MAGIC UNION ALL
# MAGIC SELECT '5_debug_state',
# MAGIC        'Use cloud_files_state against the checkpoint to inspect discovered and committed files.',
# MAGIC        'autoloader_command_templates_day29',
# MAGIC        'Checkpoint state is queryable during incident triage.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW autoloader_final_checks_day29 AS
# MAGIC SELECT 'source_profiles' AS metric, COUNT(*) AS observed_count, 6 AS expected_count FROM autoloader_source_profiles_day29
# MAGIC UNION ALL
# MAGIC SELECT 'landing_files', COUNT(*), 18 FROM landing_file_inventory_day29
# MAGIC UNION ALL
# MAGIC SELECT 'discovery_decisions', COUNT(*), 6 FROM autoloader_discovery_decisions_day29
# MAGIC UNION ALL
# MAGIC SELECT 'file_state_rows', COUNT(*), 18 FROM autoloader_file_state_day29
# MAGIC UNION ALL
# MAGIC SELECT 'processed_files', COUNT(*), 12 FROM raw_events_bronze_autoloader_day29
# MAGIC UNION ALL
# MAGIC SELECT 'deferred_files', COUNT(*), 6 FROM autoloader_file_state_day29 WHERE commit_time IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'cleanup_requests', COUNT(*), 6 FROM autoloader_clean_source_decisions_day29
# MAGIC UNION ALL
# MAGIC SELECT 'cleanup_blocked', COUNT(*), 3 FROM autoloader_clean_source_decisions_day29 WHERE cleanup_decision LIKE 'BLOCK%'
# MAGIC UNION ALL
# MAGIC SELECT 'command_templates', COUNT(*), 6 FROM autoloader_command_templates_day29
# MAGIC UNION ALL
# MAGIC SELECT 'runbook_steps', COUNT(*), 5 FROM autoloader_runbook_day29;
# MAGIC
# MAGIC SELECT * FROM autoloader_final_checks_day29 ORDER BY metric;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM autoloader_runbook_day29 ORDER BY step_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All final-check metrics match expected counts.
# MAGIC - The runbook covers discovery choice, rate limits, cleanup gates, backlog monitoring, and checkpoint debugging.
# MAGIC
# MAGIC Operational meaning: a production Auto Loader design is incomplete until discovery cost, trigger cost, cleanup risk, and checkpoint observability are all explicit.
