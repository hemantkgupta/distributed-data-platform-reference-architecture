# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 26 - Ingestion Method Decision: COPY INTO vs Auto Loader
# MAGIC
# MAGIC Goal: choose the right Databricks ingestion pattern for each file source using arrival pattern, latency, schema drift, rescue-data needs, idempotency, checkpoint ownership, replay behavior, and cost pressure.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: data ingestion/loading, Delta tables, SQL loading, PySpark transformations, troubleshooting, and monitoring.
# MAGIC - Professional stretch: production ingestion design, checkpoint ownership, rescue-data strategy, replay safety, source immutability, and file-discovery cost/performance.
# MAGIC
# MAGIC This notebook simulates file arrival using Delta tables so it can run in a personal workspace. The final command templates show how the same decisions map to real `COPY INTO` and Auto Loader jobs against Unity Catalog volumes or cloud object storage.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Landing Files And Batch Load Targets
# MAGIC
# MAGIC Purpose: model a landing area, a Delta target table, file-level audit state, and quarantine evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS copy_candidates_raw_day26;
# MAGIC DROP VIEW IF EXISTS copy_candidates_parsed_day26;
# MAGIC DROP TABLE IF EXISTS ingestion_runbook_day26;
# MAGIC DROP TABLE IF EXISTS ingestion_execution_plan_day26;
# MAGIC DROP TABLE IF EXISTS ingestion_decisions_day26;
# MAGIC DROP TABLE IF EXISTS ingestion_source_requests_day26;
# MAGIC DROP TABLE IF EXISTS ingestion_command_templates_day26;
# MAGIC DROP TABLE IF EXISTS orders_quarantine_day26;
# MAGIC DROP TABLE IF EXISTS copy_into_file_audit_day26;
# MAGIC DROP TABLE IF EXISTS orders_delta_day26;
# MAGIC DROP TABLE IF EXISTS landing_files_day26;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landing_files_day26 (
# MAGIC   source_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   file_size_mb DOUBLE,
# MAGIC   arrival_batch STRING,
# MAGIC   schema_version STRING,
# MAGIC   source_files_immutable BOOLEAN,
# MAGIC   payload STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO landing_files_day26 VALUES
# MAGIC   (
# MAGIC     'partner_orders_daily',
# MAGIC     'dbfs:/landing/day26/partner_orders/batch_001/orders_001.json',
# MAGIC     TIMESTAMP'2026-07-24T05:30:00Z',
# MAGIC     1.2,
# MAGIC     'batch_001',
# MAGIC     'v1',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2601","order_id":2601,"customer_id":901,"order_date":"2026-07-23","amount":"210.00","status":"completed"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_daily',
# MAGIC     'dbfs:/landing/day26/partner_orders/batch_001/orders_002.json',
# MAGIC     TIMESTAMP'2026-07-24T05:31:00Z',
# MAGIC     1.1,
# MAGIC     'batch_001',
# MAGIC     'v1',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2602","order_id":2602,"customer_id":902,"order_date":"2026-07-23","amount":"95.50","status":"pending"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_daily',
# MAGIC     'dbfs:/landing/day26/partner_orders/batch_001/orders_003.json',
# MAGIC     TIMESTAMP'2026-07-24T05:32:00Z',
# MAGIC     1.0,
# MAGIC     'batch_001',
# MAGIC     'v1',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2603","order_id":2603,"customer_id":903,"order_date":"2026-07-23","amount":"bad_amount","status":"completed"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'clickstream_events',
# MAGIC     'dbfs:/landing/day26/clickstream/hour_0500/click_001.json',
# MAGIC     TIMESTAMP'2026-07-24T05:00:10Z',
# MAGIC     0.1,
# MAGIC     'hour_0500',
# MAGIC     'v2',
# MAGIC     true,
# MAGIC     '{"event_id":"clk-2601","session_id":"s-1","event_time":"2026-07-24T05:00:00Z","device_type":"mobile","campaign_id":"c-99"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'billing_export_hourly',
# MAGIC     'dbfs:/landing/day26/billing/hour_0500/billing_001.json',
# MAGIC     TIMESTAMP'2026-07-24T05:04:00Z',
# MAGIC     2.4,
# MAGIC     'hour_0500',
# MAGIC     'v3',
# MAGIC     true,
# MAGIC     '{"event_id":"bill-2601","account_id":"a-1","charge_amount":"19.95","currency":"USD","tax_region":"CA"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'pii_vendor_feed',
# MAGIC     'dbfs:/landing/day26/vendor/hour_0500/vendor_001.json',
# MAGIC     TIMESTAMP'2026-07-24T05:05:00Z',
# MAGIC     0.8,
# MAGIC     'hour_0500',
# MAGIC     'v1',
# MAGIC     false,
# MAGIC     '{"event_id":"vend-2601","customer_email":"person@example.com","consent_status":"granted"}'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day26 (
# MAGIC   event_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   normalized_status STRING,
# MAGIC   _ingested_at TIMESTAMP,
# MAGIC   _ingest_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_file_audit_day26 (
# MAGIC   file_path STRING,
# MAGIC   loaded_at TIMESTAMP,
# MAGIC   load_run_id STRING,
# MAGIC   target_table STRING,
# MAGIC   load_status STRING,
# MAGIC   parse_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_quarantine_day26 (
# MAGIC   source_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   event_id STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   raw_payload STRING,
# MAGIC   quarantined_at TIMESTAMP,
# MAGIC   load_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   arrival_batch,
# MAGIC   COUNT(*) AS file_count,
# MAGIC   ROUND(SUM(file_size_mb), 2) AS total_mb,
# MAGIC   SUM(CASE WHEN source_files_immutable THEN 0 ELSE 1 END) AS mutable_file_count
# MAGIC FROM landing_files_day26
# MAGIC GROUP BY source_id, arrival_batch
# MAGIC ORDER BY source_id, arrival_batch;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `partner_orders_daily` has 3 simulated files.
# MAGIC - One order file has a bad decimal value.
# MAGIC - `pii_vendor_feed` is marked as mutable, which is a production ingestion risk.
# MAGIC
# MAGIC Operational meaning: the landing inventory is part of ingestion evidence. You need source immutability, file counts, file size, arrival time, and raw payload evidence before deciding how to load.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Run A COPY INTO-Style Idempotent Batch Load
# MAGIC
# MAGIC Purpose: practice batch ingestion where rerunning the load must not duplicate already-processed files.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reference only: real COPY INTO against a Unity Catalog volume or cloud path.
# MAGIC -- COPY INTO de_learning.orders_delta_day26
# MAGIC -- FROM '/Volumes/<catalog>/<schema>/<volume>/partner_orders/'
# MAGIC -- FILEFORMAT = JSON
# MAGIC -- FORMAT_OPTIONS ('multiLine' = 'false')
# MAGIC -- COPY_OPTIONS ('mergeSchema' = 'false');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_candidates_raw_day26 AS
# MAGIC SELECT
# MAGIC   l.source_id,
# MAGIC   l.file_path AS source_file_path,
# MAGIC   l.file_mod_time,
# MAGIC   l.payload,
# MAGIC   get_json_object(l.payload, '$.event_id') AS event_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_id') AS INT) AS order_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.customer_id') AS INT) AS customer_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_date') AS DATE) AS order_date,
# MAGIC   try_cast(get_json_object(l.payload, '$.amount') AS DECIMAL(10,2)) AS amount,
# MAGIC   get_json_object(l.payload, '$.amount') AS raw_amount,
# MAGIC   upper(get_json_object(l.payload, '$.status')) AS normalized_status
# MAGIC FROM landing_files_day26 l
# MAGIC WHERE l.source_id = 'partner_orders_daily'
# MAGIC   AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM copy_into_file_audit_day26 a
# MAGIC     WHERE a.file_path = l.file_path
# MAGIC       AND a.target_table = 'orders_delta_day26'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_candidates_parsed_day26 AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN event_id IS NULL THEN 'QUARANTINE_MISSING_EVENT_ID'
# MAGIC     WHEN order_id IS NULL THEN 'QUARANTINE_BAD_ORDER_ID'
# MAGIC     WHEN amount IS NULL AND raw_amount IS NOT NULL THEN 'QUARANTINE_BAD_AMOUNT'
# MAGIC     WHEN order_date IS NULL THEN 'QUARANTINE_BAD_ORDER_DATE'
# MAGIC     ELSE 'VALID'
# MAGIC   END AS parse_status
# MAGIC FROM copy_candidates_raw_day26;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_delta_day26
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   normalized_status,
# MAGIC   current_timestamp() AS _ingested_at,
# MAGIC   'copy-run-2601' AS _ingest_run_id
# MAGIC FROM copy_candidates_parsed_day26
# MAGIC WHERE parse_status = 'VALID';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day26
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   source_file_path,
# MAGIC   event_id,
# MAGIC   parse_status AS quarantine_reason,
# MAGIC   payload AS raw_payload,
# MAGIC   current_timestamp() AS quarantined_at,
# MAGIC   'copy-run-2601' AS load_run_id
# MAGIC FROM copy_candidates_parsed_day26
# MAGIC WHERE parse_status <> 'VALID';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO copy_into_file_audit_day26
# MAGIC SELECT
# MAGIC   source_file_path AS file_path,
# MAGIC   current_timestamp() AS loaded_at,
# MAGIC   'copy-run-2601' AS load_run_id,
# MAGIC   'orders_delta_day26' AS target_table,
# MAGIC   CASE WHEN parse_status = 'VALID' THEN 'LOADED' ELSE 'QUARANTINED' END AS load_status,
# MAGIC   parse_status
# MAGIC FROM copy_candidates_parsed_day26;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'target_rows' AS metric, COUNT(*) AS actual_value FROM orders_delta_day26
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', COUNT(*) FROM orders_quarantine_day26
# MAGIC UNION ALL
# MAGIC SELECT 'audited_files', COUNT(*) FROM copy_into_file_audit_day26;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS remaining_unprocessed_partner_files
# MAGIC FROM landing_files_day26 l
# MAGIC WHERE l.source_id = 'partner_orders_daily'
# MAGIC   AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM copy_into_file_audit_day26 a
# MAGIC     WHERE a.file_path = l.file_path
# MAGIC       AND a.target_table = 'orders_delta_day26'
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_delta_day26` has 2 valid rows.
# MAGIC - `orders_quarantine_day26` has 1 bad-amount row.
# MAGIC - `remaining_unprocessed_partner_files` is 0 after the audit records are written.
# MAGIC
# MAGIC Operational meaning: `COPY INTO` is useful when a SQL-scheduled load can rely on file-level idempotency. The audit table makes the behavior explicit and replayable in this lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Register Ingestion Source Requirements
# MAGIC
# MAGIC Purpose: describe each source in terms of arrival pattern, schema drift, rescue needs, latency, file scale, cloud permissions, immutability, and cost pressure.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ingestion_source_requests_day26 (
# MAGIC   source_id STRING,
# MAGIC   arrival_pattern STRING,
# MAGIC   file_count_per_day BIGINT,
# MAGIC   average_file_size_mb DOUBLE,
# MAGIC   latency_sla_minutes INT,
# MAGIC   schema_drift_expected BOOLEAN,
# MAGIC   needs_rescue_data BOOLEAN,
# MAGIC   backfill_size_gb DOUBLE,
# MAGIC   requires_sql_only BOOLEAN,
# MAGIC   cloud_event_permissions_available BOOLEAN,
# MAGIC   source_files_immutable BOOLEAN,
# MAGIC   max_list_cost_index DOUBLE,
# MAGIC   owner_domain STRING,
# MAGIC   target_layer STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ingestion_source_requests_day26 VALUES
# MAGIC   ('partner_orders_daily', 'daily_batch', 3, 1.1, 1440, false, false, 2.5, true, false, true, 1.0, 'orders', 'bronze'),
# MAGIC   ('clickstream_events', 'continuous', 5000000, 0.1, 5, true, true, 0.0, false, true, true, 9.5, 'growth', 'bronze'),
# MAGIC   ('billing_export_hourly', 'hourly_batch', 1200, 2.4, 60, true, true, 25.0, false, false, true, 6.0, 'finance', 'bronze'),
# MAGIC   ('historical_orders_backfill', 'one_time_backfill', 80000, 8.0, 1440, false, false, 900.0, true, false, true, 3.0, 'orders', 'bronze'),
# MAGIC   ('pii_vendor_feed', 'hourly_batch', 200, 0.8, 60, true, true, 0.0, false, true, false, 2.0, 'privacy', 'bronze'),
# MAGIC   ('inventory_snapshot_daily', 'daily_batch', 20, 5.0, 1440, false, false, 20.0, true, false, true, 1.5, 'supply_chain', 'bronze');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ingestion_source_requests_day26
# MAGIC ORDER BY latency_sla_minutes, file_count_per_day DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - High-scale, low-latency, drift-prone sources are visible.
# MAGIC - Simple daily batch sources are visibly different from streaming-like sources.
# MAGIC - Mutable source files are explicitly flagged.
# MAGIC
# MAGIC Operational meaning: ingestion method choice is a workload decision. You should not choose Auto Loader or `COPY INTO` by habit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Score COPY INTO vs Auto Loader With PySpark
# MAGIC
# MAGIC Purpose: turn source requirements into a recommended ingestion method, checkpoint design, rescue strategy, and operational risk.

# COMMAND ----------

from pyspark.sql import functions as F

requests_df = spark.table("de_learning.ingestion_source_requests_day26")

decisions_df = (
    requests_df
    .withColumn("low_latency", F.col("latency_sla_minutes") <= F.lit(15))
    .withColumn("large_file_scale", F.col("file_count_per_day") >= F.lit(100000))
    .withColumn("schema_pressure", F.col("schema_drift_expected") | F.col("needs_rescue_data"))
    .withColumn("high_listing_cost", F.col("max_list_cost_index") >= F.lit(5.0))
    .withColumn(
        "recommended_method",
        F.when(~F.col("source_files_immutable"), F.lit("BLOCK_MUTABLE_SOURCE_REWRITE"))
        .when(
            F.col("low_latency") | F.col("large_file_scale"),
            F.when(
                F.col("cloud_event_permissions_available"),
                F.lit("AUTO_LOADER_FILE_EVENTS")
            ).otherwise(F.lit("AUTO_LOADER_DIRECTORY_LISTING_WITH_COST_GUARD"))
        )
        .when(
            F.col("schema_pressure"),
            F.when(
                F.col("cloud_event_permissions_available"),
                F.lit("AUTO_LOADER_FILE_EVENTS")
            ).otherwise(F.lit("AUTO_LOADER_DIRECTORY_LISTING_WITH_COST_GUARD"))
        )
        .when(F.col("arrival_pattern") == F.lit("one_time_backfill"), F.lit("COPY_INTO_BACKFILL"))
        .when(F.col("requires_sql_only"), F.lit("COPY_INTO_SCHEDULED"))
        .otherwise(F.lit("COPY_INTO_SCHEDULED"))
    )
    .withColumn(
        "decision_reason",
        F.when(~F.col("source_files_immutable"), F.lit("Source rewrites files; require immutable handoff or business-key dedupe before loading"))
        .when(F.col("low_latency"), F.lit("Low-latency file arrival needs incremental discovery and durable streaming checkpoint"))
        .when(F.col("large_file_scale"), F.lit("Large file count makes repeated directory listing and ad hoc SQL loads risky"))
        .when(F.col("schema_pressure"), F.lit("Schema drift or rescue-data needs favor Auto Loader schema tracking"))
        .when(F.col("arrival_pattern") == F.lit("one_time_backfill"), F.lit("Stable one-time backfill can use COPY INTO with explicit audit evidence"))
        .otherwise(F.lit("Stable scheduled batch can use COPY INTO with file-level idempotency"))
    )
    .withColumn(
        "checkpoint_design",
        F.when(F.col("recommended_method").like("COPY_INTO%"), F.lit("COPY INTO load history plus explicit file audit table"))
        .when(F.col("recommended_method").like("AUTO_LOADER%"), F.lit("Structured Streaming checkpoint plus cloudFiles.schemaLocation in governed storage"))
        .otherwise(F.lit("No production load until source immutability is fixed"))
    )
    .withColumn(
        "rescue_strategy",
        F.when(F.col("recommended_method").like("AUTO_LOADER%"), F.lit("Use rescued data column and quarantine unexpected fields or type mismatches"))
        .when(F.col("needs_rescue_data"), F.lit("Do not rely on COPY INTO alone; add parser/quarantine or switch to Auto Loader"))
        .otherwise(F.lit("Fail or quarantine malformed files with parse evidence"))
    )
    .withColumn(
        "operational_risk",
        F.when(F.col("recommended_method") == F.lit("BLOCK_MUTABLE_SOURCE_REWRITE"), F.lit("HIGH"))
        .when(F.col("recommended_method") == F.lit("AUTO_LOADER_DIRECTORY_LISTING_WITH_COST_GUARD"), F.lit("MEDIUM"))
        .when(F.col("high_listing_cost") & F.col("recommended_method").like("COPY_INTO%"), F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )
    .select(
        "source_id",
        "arrival_pattern",
        "file_count_per_day",
        "latency_sla_minutes",
        "schema_drift_expected",
        "needs_rescue_data",
        "backfill_size_gb",
        "requires_sql_only",
        "cloud_event_permissions_available",
        "source_files_immutable",
        "max_list_cost_index",
        "recommended_method",
        "decision_reason",
        "checkpoint_design",
        "rescue_strategy",
        "operational_risk"
    )
)

decisions_df.createOrReplaceTempView("ingestion_decisions_view_day26")
display(decisions_df.orderBy("operational_risk", "source_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `requests_df` represents source ingestion requirements, one row per upstream feed.
# MAGIC - `decisions_df` adds method recommendation, reason, checkpoint design, rescue strategy, and operational risk.
# MAGIC - SQL equivalent: `SELECT source_id, CASE WHEN schema_drift_expected THEN 'AUTO_LOADER' ELSE 'COPY_INTO' END FROM ingestion_source_requests_day26`.
# MAGIC - `F.col(...)` references columns inside decision expressions.
# MAGIC - `withColumn(...)` adds derived booleans and final recommendation fields.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - `like(...)` checks method families such as `COPY_INTO%` and `AUTO_LOADER%`.
# MAGIC - `createOrReplaceTempView(...)` lets SQL cells save PySpark results as Delta tables.
# MAGIC - PySpark remains lazy until `display(...)` triggers execution.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ingestion_decisions_day26
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM ingestion_decisions_view_day26;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   recommended_method,
# MAGIC   operational_risk,
# MAGIC   COUNT(*) AS source_count
# MAGIC FROM ingestion_decisions_day26
# MAGIC GROUP BY recommended_method, operational_risk
# MAGIC ORDER BY recommended_method, operational_risk;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   recommended_method,
# MAGIC   decision_reason,
# MAGIC   checkpoint_design,
# MAGIC   rescue_strategy
# MAGIC FROM ingestion_decisions_day26
# MAGIC ORDER BY source_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Stable daily batch and one-time backfill sources choose `COPY_INTO_*`.
# MAGIC - Low-latency, high-scale, or schema-drift sources choose Auto Loader.
# MAGIC - Mutable source files are blocked.
# MAGIC
# MAGIC Operational meaning: `COPY INTO` is an excellent SQL load primitive, but Auto Loader is the better default when discovery scale, schema evolution, rescue data, or low-latency incremental processing matter.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Build An Execution Plan And Runbook
# MAGIC
# MAGIC Purpose: translate method recommendations into execution controls that an operator can own.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ingestion_execution_plan_day26
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   source_id,
# MAGIC   recommended_method,
# MAGIC   owner_domain,
# MAGIC   target_layer,
# MAGIC   CASE
# MAGIC     WHEN recommended_method LIKE 'COPY_INTO%' THEN 'Lakeflow Job task running COPY INTO SQL'
# MAGIC     WHEN recommended_method = 'AUTO_LOADER_FILE_EVENTS' THEN 'Lakeflow Job or streaming table using Auto Loader with file events'
# MAGIC     WHEN recommended_method = 'AUTO_LOADER_DIRECTORY_LISTING_WITH_COST_GUARD' THEN 'Scheduled Auto Loader AvailableNow job with listing-cost monitor'
# MAGIC     ELSE 'Blocked source remediation ticket'
# MAGIC   END AS execution_pattern,
# MAGIC   CASE
# MAGIC     WHEN recommended_method LIKE 'COPY_INTO%' THEN 'Validate file audit count, target row count, quarantine count, and Delta history'
# MAGIC     WHEN recommended_method LIKE 'AUTO_LOADER%' THEN 'Validate checkpoint progress, rescued-data count, schema log, and stream query progress'
# MAGIC     ELSE 'Validate immutable source contract before enabling ingestion'
# MAGIC   END AS validation_gate,
# MAGIC   CASE
# MAGIC     WHEN recommended_method LIKE 'COPY_INTO%' THEN 'Rerun same COPY INTO; loaded files are skipped unless force is explicitly enabled'
# MAGIC     WHEN recommended_method LIKE 'AUTO_LOADER%' THEN 'Restart from checkpoint; do not delete checkpoint unless intentionally replaying'
# MAGIC     ELSE 'Do not run'
# MAGIC   END AS replay_rule,
# MAGIC   operational_risk
# MAGIC FROM ingestion_decisions_day26 d
# MAGIC JOIN ingestion_source_requests_day26 r USING (source_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ingestion_runbook_day26
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT * FROM VALUES
# MAGIC   ('RB-01', 'COPY_INTO_RERUN', 'Do not use force unless the goal is intentional reload; verify duplicate business keys after replay.'),
# MAGIC   ('RB-02', 'AUTO_LOADER_CHECKPOINT', 'Checkpoint and cloudFiles.schemaLocation must live in governed storage and must not be nested under the target table directory.'),
# MAGIC   ('RB-03', 'RESCUED_DATA_SPIKE', 'Route rescued records to quarantine, inspect schema drift, then decide whether to evolve schema or reject the source change.'),
# MAGIC   ('RB-04', 'MUTABLE_SOURCE_FILE', 'Block ingestion until upstream publishes immutable files or the pipeline adds deterministic business-key dedupe.'),
# MAGIC   ('RB-05', 'DIRECTORY_LISTING_COST', 'Prefer file events; if unavailable, schedule AvailableNow and monitor listing cost and source retention.')
# MAGIC AS t(runbook_id, incident_type, operator_action);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ingestion_execution_plan_day26
# MAGIC ORDER BY operational_risk DESC, source_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ingestion_runbook_day26
# MAGIC ORDER BY runbook_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Each source has an execution pattern, validation gate, and replay rule.
# MAGIC - Mutable files and directory-listing cost get explicit operator actions.
# MAGIC
# MAGIC Operational meaning: an ingestion design is incomplete until the operator knows how to rerun, replay, quarantine, and stop unsafe loads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Store Production Command Templates
# MAGIC
# MAGIC Purpose: preserve real Databricks command shapes for `COPY INTO`, Auto Loader with schema tracking, Auto Loader file events, and AvailableNow batch scheduling.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ingestion_command_templates_day26 (
# MAGIC   template_id STRING,
# MAGIC   recommended_method STRING,
# MAGIC   command_template STRING,
# MAGIC   when_to_use STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ingestion_command_templates_day26 VALUES
# MAGIC   (
# MAGIC     'TPL-01',
# MAGIC     'COPY_INTO_SCHEDULED',
# MAGIC     'COPY INTO catalog.schema.orders_delta FROM ''/Volumes/catalog/schema/volume/path/'' FILEFORMAT = JSON COPY_OPTIONS (''mergeSchema'' = ''false'')',
# MAGIC     'Stable files, SQL-first team, scheduled batch, modest file count, no rescue-data dependency'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TPL-02',
# MAGIC     'COPY_INTO_BACKFILL',
# MAGIC     'COPY INTO catalog.schema.orders_delta FROM ''/Volumes/catalog/schema/volume/backfill/'' FILEFORMAT = JSON COPY_OPTIONS (''mergeSchema'' = ''false'')',
# MAGIC     'Stable one-time or occasional bulk load where file-level idempotency is enough'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TPL-03',
# MAGIC     'AUTO_LOADER_FILE_EVENTS',
# MAGIC     'spark.readStream.format(''cloudFiles'').option(''cloudFiles.format'', ''json'').option(''cloudFiles.useManagedFileEvents'', ''true'').option(''cloudFiles.schemaLocation'', checkpoint_path).load(volume_path)',
# MAGIC     'Low latency, high scale, schema evolution, rescued data, and managed file events available'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TPL-04',
# MAGIC     'AUTO_LOADER_DIRECTORY_LISTING_WITH_COST_GUARD',
# MAGIC     'spark.readStream.format(''cloudFiles'').option(''cloudFiles.format'', ''json'').option(''cloudFiles.schemaLocation'', schema_path).load(volume_path).writeStream.option(''checkpointLocation'', checkpoint_path).trigger(availableNow=True)',
# MAGIC     'File events unavailable; use scheduled AvailableNow and monitor listing cost'
# MAGIC   ),
# MAGIC   (
# MAGIC     'TPL-05',
# MAGIC     'BLOCK_MUTABLE_SOURCE_REWRITE',
# MAGIC     'No load command; require immutable file contract or deterministic business-key dedupe first',
# MAGIC     'Source files can be overwritten or appended after initial publication'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT template_id, recommended_method, when_to_use
# MAGIC FROM ingestion_command_templates_day26
# MAGIC ORDER BY template_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Five templates connect the decision table to production command shapes.
# MAGIC
# MAGIC Operational meaning: templates prevent vague recommendations. A design review should name the command family, checkpoint location, schema location, and replay behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Validation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'landing_files' AS check_name, CAST(COUNT(*) AS STRING) AS actual_value, '6' AS expected_value
# MAGIC FROM landing_files_day26
# MAGIC UNION ALL
# MAGIC SELECT 'copy_target_rows', CAST(COUNT(*) AS STRING), '2'
# MAGIC FROM orders_delta_day26
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', CAST(COUNT(*) AS STRING), '1'
# MAGIC FROM orders_quarantine_day26
# MAGIC UNION ALL
# MAGIC SELECT 'source_requests', CAST(COUNT(*) AS STRING), '6'
# MAGIC FROM ingestion_source_requests_day26
# MAGIC UNION ALL
# MAGIC SELECT 'decisions', CAST(COUNT(*) AS STRING), '6'
# MAGIC FROM ingestion_decisions_day26
# MAGIC UNION ALL
# MAGIC SELECT 'command_templates', CAST(COUNT(*) AS STRING), '5'
# MAGIC FROM ingestion_command_templates_day26;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   recommended_method,
# MAGIC   COUNT(*) AS source_count
# MAGIC FROM ingestion_decisions_day26
# MAGIC GROUP BY recommended_method
# MAGIC ORDER BY source_count DESC, recommended_method;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Validation counts match expected values.
# MAGIC - Method distribution includes `COPY_INTO_*`, Auto Loader variants, and a blocked mutable-source case.
# MAGIC
# MAGIC Operational meaning: Day 26 starts the ingestion/loading segment by making method selection evidence-based before building more detailed COPY INTO and Auto Loader labs.
