# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 27 - COPY INTO Batch Ingestion Audit
# MAGIC
# MAGIC Goal: run a focused `COPY INTO`-style batch ingestion lab with validation evidence, file audit, rerun safety, malformed-row quarantine, duplicate business-key detection, and a controlled force-reload decision.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: ingestion/loading, Delta tables, SQL loading, transformation/modeling, monitoring/troubleshooting, and governance/security evidence.
# MAGIC - Professional stretch: replay safety, mutable-file risk, force reload governance, incident runbooks, and production-grade auditability.
# MAGIC
# MAGIC This notebook simulates object-storage files with Delta tables so the lab runs without external cloud credentials. The command-template table stores real `COPY INTO` shapes for Unity Catalog volumes or external cloud paths.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Landing Inventory, Target, Audit, And Quarantine Tables
# MAGIC
# MAGIC Purpose: prepare day-scoped objects for a repeatable batch ingestion run.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS copy_candidates_raw_day27;
# MAGIC DROP VIEW IF EXISTS copy_candidates_parsed_day27;
# MAGIC DROP VIEW IF EXISTS copy_rerun_candidates_day27;
# MAGIC DROP TABLE IF EXISTS copy_into_runbook_day27;
# MAGIC DROP TABLE IF EXISTS copy_into_command_templates_day27;
# MAGIC DROP TABLE IF EXISTS copy_into_force_decisions_day27;
# MAGIC DROP TABLE IF EXISTS force_reload_requests_day27;
# MAGIC DROP TABLE IF EXISTS business_key_duplicates_day27;
# MAGIC DROP TABLE IF EXISTS copy_into_rerun_report_day27;
# MAGIC DROP TABLE IF EXISTS copy_into_validation_summary_day27;
# MAGIC DROP TABLE IF EXISTS orders_quarantine_day27;
# MAGIC DROP TABLE IF EXISTS copy_into_file_audit_day27;
# MAGIC DROP TABLE IF EXISTS orders_delta_day27;
# MAGIC DROP TABLE IF EXISTS landing_order_files_day27;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landing_order_files_day27 (
# MAGIC   source_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   file_size_bytes BIGINT,
# MAGIC   arrival_batch STRING,
# MAGIC   file_hash STRING,
# MAGIC   source_files_immutable BOOLEAN,
# MAGIC   payload STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO landing_order_files_day27 VALUES
# MAGIC   (
# MAGIC     'partner_orders_copy_day27',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_001.json',
# MAGIC     TIMESTAMP '2026-07-25 05:30:00',
# MAGIC     428,
# MAGIC     'batch_001',
# MAGIC     'hash-day27-001',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2701","order_id":2701,"customer_id":971,"order_date":"2026-07-24","amount":"210.00","status":"completed"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_copy_day27',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_002.json',
# MAGIC     TIMESTAMP '2026-07-25 05:31:00',
# MAGIC     416,
# MAGIC     'batch_001',
# MAGIC     'hash-day27-002',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2702","order_id":2702,"customer_id":972,"order_date":"2026-07-24","amount":"95.50","status":"pending"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_copy_day27',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_003_bad_amount.json',
# MAGIC     TIMESTAMP '2026-07-25 05:32:00',
# MAGIC     430,
# MAGIC     'batch_001',
# MAGIC     'hash-day27-003',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2703","order_id":2703,"customer_id":973,"order_date":"2026-07-24","amount":"bad_amount","status":"completed"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_copy_day27',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_004_duplicate_key.json',
# MAGIC     TIMESTAMP '2026-07-25 05:33:00',
# MAGIC     425,
# MAGIC     'batch_001',
# MAGIC     'hash-day27-004',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2704","order_id":2702,"customer_id":972,"order_date":"2026-07-24","amount":"100.00","status":"completed"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_copy_day27',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_005_late.json',
# MAGIC     TIMESTAMP '2026-07-25 05:35:00',
# MAGIC     419,
# MAGIC     'batch_001',
# MAGIC     'hash-day27-005',
# MAGIC     true,
# MAGIC     '{"event_id":"evt-2705","order_id":2705,"customer_id":975,"order_date":"2026-07-23","amount":"51.25","status":"completed"}'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day27 (
# MAGIC   event_id STRING,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   normalized_status STRING,
# MAGIC   source_file_path STRING,
# MAGIC   file_hash STRING,
# MAGIC   _ingested_at TIMESTAMP,
# MAGIC   _ingest_run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_file_audit_day27 (
# MAGIC   file_path STRING,
# MAGIC   file_hash STRING,
# MAGIC   target_table STRING,
# MAGIC   load_run_id STRING,
# MAGIC   load_status STRING,
# MAGIC   validation_status STRING,
# MAGIC   loaded_at TIMESTAMP,
# MAGIC   force_reload BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_quarantine_day27 (
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
# MAGIC   COUNT(*) AS landing_file_count,
# MAGIC   SUM(file_size_bytes) AS total_bytes,
# MAGIC   SUM(CASE WHEN source_files_immutable THEN 0 ELSE 1 END) AS mutable_file_count
# MAGIC FROM landing_order_files_day27
# MAGIC GROUP BY source_id, arrival_batch;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - One landing source: `partner_orders_copy_day27`.
# MAGIC - Five files in `batch_001`.
# MAGIC - Zero mutable files in the normal batch.
# MAGIC
# MAGIC Operational meaning: before running `COPY INTO`, operators need the landing-file inventory, target table, audit table, and quarantine table ready so each rerun has evidence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Validate And Load New Files
# MAGIC
# MAGIC Purpose: mimic a `COPY INTO` run by validating candidate files, loading parse-valid rows, quarantining malformed rows, and recording all processed files in an audit table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reference only: dry-run validation shape for real files.
# MAGIC -- COPY INTO de_learning.orders_delta_day27
# MAGIC -- FROM '/Volumes/<catalog>/<schema>/<volume>/partner_orders/'
# MAGIC -- FILEFORMAT = JSON
# MAGIC -- VALIDATE ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_candidates_raw_day27 AS
# MAGIC SELECT
# MAGIC   l.source_id,
# MAGIC   l.file_path AS source_file_path,
# MAGIC   l.file_hash,
# MAGIC   l.file_mod_time,
# MAGIC   l.payload,
# MAGIC   get_json_object(l.payload, '$.event_id') AS event_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_id') AS INT) AS order_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.customer_id') AS INT) AS customer_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_date') AS DATE) AS order_date,
# MAGIC   try_cast(get_json_object(l.payload, '$.amount') AS DECIMAL(10,2)) AS amount,
# MAGIC   get_json_object(l.payload, '$.amount') AS raw_amount,
# MAGIC   upper(get_json_object(l.payload, '$.status')) AS normalized_status
# MAGIC FROM landing_order_files_day27 l
# MAGIC WHERE l.source_id = 'partner_orders_copy_day27'
# MAGIC   AND l.arrival_batch = 'batch_001'
# MAGIC   AND l.source_files_immutable = true
# MAGIC   AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM copy_into_file_audit_day27 a
# MAGIC     WHERE a.file_path = l.file_path
# MAGIC       AND a.target_table = 'orders_delta_day27'
# MAGIC       AND a.force_reload = false
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_candidates_parsed_day27 AS
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN event_id IS NULL THEN 'MISSING_EVENT_ID'
# MAGIC     WHEN order_id IS NULL THEN 'BAD_ORDER_ID'
# MAGIC     WHEN customer_id IS NULL THEN 'BAD_CUSTOMER_ID'
# MAGIC     WHEN order_date IS NULL THEN 'BAD_ORDER_DATE'
# MAGIC     WHEN amount IS NULL THEN 'BAD_AMOUNT'
# MAGIC     WHEN normalized_status NOT IN ('COMPLETED', 'PENDING', 'CANCELLED') THEN 'BAD_STATUS'
# MAGIC     ELSE 'VALID'
# MAGIC   END AS validation_status
# MAGIC FROM copy_candidates_raw_day27;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_validation_summary_day27 AS
# MAGIC SELECT
# MAGIC   validation_status,
# MAGIC   COUNT(*) AS file_count,
# MAGIC   collect_list(source_file_path) AS source_files
# MAGIC FROM copy_candidates_parsed_day27
# MAGIC GROUP BY validation_status;
# MAGIC
# MAGIC SELECT * FROM copy_into_validation_summary_day27 ORDER BY validation_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_delta_day27
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   normalized_status,
# MAGIC   source_file_path,
# MAGIC   file_hash,
# MAGIC   current_timestamp() AS _ingested_at,
# MAGIC   'copy-run-2701' AS _ingest_run_id
# MAGIC FROM copy_candidates_parsed_day27
# MAGIC WHERE validation_status = 'VALID';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day27
# MAGIC SELECT
# MAGIC   source_file_path,
# MAGIC   event_id,
# MAGIC   validation_status AS quarantine_reason,
# MAGIC   payload AS raw_payload,
# MAGIC   current_timestamp() AS quarantined_at,
# MAGIC   'copy-run-2701' AS load_run_id
# MAGIC FROM copy_candidates_parsed_day27
# MAGIC WHERE validation_status <> 'VALID';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO copy_into_file_audit_day27
# MAGIC SELECT
# MAGIC   source_file_path AS file_path,
# MAGIC   file_hash,
# MAGIC   'orders_delta_day27' AS target_table,
# MAGIC   'copy-run-2701' AS load_run_id,
# MAGIC   CASE WHEN validation_status = 'VALID' THEN 'LOADED' ELSE 'QUARANTINED' END AS load_status,
# MAGIC   validation_status,
# MAGIC   current_timestamp() AS loaded_at,
# MAGIC   false AS force_reload
# MAGIC FROM copy_candidates_parsed_day27;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM orders_delta_day27) AS loaded_rows,
# MAGIC   (SELECT COUNT(*) FROM orders_quarantine_day27) AS quarantined_rows,
# MAGIC   (SELECT COUNT(*) FROM copy_into_file_audit_day27) AS audited_files;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `loaded_rows = 4`.
# MAGIC - `quarantined_rows = 1`.
# MAGIC - `audited_files = 5`.
# MAGIC
# MAGIC Operational meaning: batch ingestion should create both data and evidence. The malformed file is not silently dropped, and every processed file is auditable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Prove Rerun Idempotency
# MAGIC
# MAGIC Purpose: rerun the same candidate discovery and confirm already-audited files are skipped.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_rerun_candidates_day27 AS
# MAGIC SELECT
# MAGIC   l.file_path,
# MAGIC   l.file_hash,
# MAGIC   l.arrival_batch
# MAGIC FROM landing_order_files_day27 l
# MAGIC WHERE l.source_id = 'partner_orders_copy_day27'
# MAGIC   AND l.arrival_batch = 'batch_001'
# MAGIC   AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM copy_into_file_audit_day27 a
# MAGIC     WHERE a.file_path = l.file_path
# MAGIC       AND a.target_table = 'orders_delta_day27'
# MAGIC       AND a.force_reload = false
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_rerun_report_day27 AS
# MAGIC SELECT
# MAGIC   'copy-run-2702' AS load_run_id,
# MAGIC   COUNT(*) AS candidate_file_count,
# MAGIC   CASE
# MAGIC     WHEN COUNT(*) = 0 THEN 'NOOP_ALREADY_AUDITED'
# MAGIC     ELSE 'LOAD_NEW_FILES'
# MAGIC   END AS rerun_decision,
# MAGIC   current_timestamp() AS checked_at
# MAGIC FROM copy_rerun_candidates_day27;
# MAGIC
# MAGIC SELECT * FROM copy_into_rerun_report_day27;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `candidate_file_count = 0`.
# MAGIC - `rerun_decision = NOOP_ALREADY_AUDITED`.
# MAGIC
# MAGIC Operational meaning: `COPY INTO` is useful for retriable scheduled batch loads because reruns should not duplicate files already loaded into the target.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Detect Duplicate Business Keys
# MAGIC
# MAGIC Purpose: catch duplicates that file-level idempotency does not solve.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE business_key_duplicates_day27 AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   COUNT(*) AS row_count,
# MAGIC   COUNT(DISTINCT event_id) AS event_count,
# MAGIC   collect_list(event_id) AS event_ids,
# MAGIC   collect_list(source_file_path) AS source_files,
# MAGIC   MIN(_ingested_at) AS first_seen_at,
# MAGIC   MAX(_ingested_at) AS last_seen_at
# MAGIC FROM orders_delta_day27
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1;
# MAGIC
# MAGIC SELECT * FROM business_key_duplicates_day27 ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `order_id = 2702` appears with `row_count = 2`.
# MAGIC
# MAGIC Operational meaning: `COPY INTO` tracks files, not business keys. Silver promotion still needs dedupe, merge, or rejection rules based on source grain.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Decide When Force Reload Is Allowed
# MAGIC
# MAGIC Purpose: use PySpark to approve or block force reload requests using audit evidence and operational controls.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE force_reload_requests_day27 (
# MAGIC   request_id STRING,
# MAGIC   file_path STRING,
# MAGIC   original_file_hash STRING,
# MAGIC   new_file_hash STRING,
# MAGIC   requested_by STRING,
# MAGIC   reason STRING,
# MAGIC   upstream_root_cause STRING,
# MAGIC   has_owner_approval BOOLEAN,
# MAGIC   has_reconciliation_query BOOLEAN,
# MAGIC   has_backout_plan BOOLEAN,
# MAGIC   expected_duplicate_business_keys INT,
# MAGIC   requested_force BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO force_reload_requests_day27 VALUES
# MAGIC   (
# MAGIC     'fr-2701',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_001.json',
# MAGIC     'hash-day27-001',
# MAGIC     'hash-day27-001-rewrite',
# MAGIC     'orders-oncall@databricks.example',
# MAGIC     'Upstream corrected amount after first publication',
# MAGIC     'Mutable-file contract breach',
# MAGIC     true,
# MAGIC     true,
# MAGIC     true,
# MAGIC     1,
# MAGIC     true
# MAGIC   ),
# MAGIC   (
# MAGIC     'fr-2702',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_001/orders_003_bad_amount.json',
# MAGIC     'hash-day27-003',
# MAGIC     'hash-day27-003-retry',
# MAGIC     'orders-oncall@databricks.example',
# MAGIC     'Retry quarantined malformed file without owner signoff',
# MAGIC     'Bad decimal emitted by source',
# MAGIC     false,
# MAGIC     false,
# MAGIC     true,
# MAGIC     0,
# MAGIC     true
# MAGIC   ),
# MAGIC   (
# MAGIC     'fr-2703',
# MAGIC     'dbfs:/landing/day27/partner_orders/batch_002/orders_999_new.json',
# MAGIC     null,
# MAGIC     'hash-day27-999',
# MAGIC     'orders-oncall@databricks.example',
# MAGIC     'New late-arriving file was never loaded',
# MAGIC     'Late delivery',
# MAGIC     true,
# MAGIC     true,
# MAGIC     true,
# MAGIC     0,
# MAGIC     true
# MAGIC   );

# COMMAND ----------

from pyspark.sql import functions as F

audit_df = spark.table("de_learning.copy_into_file_audit_day27")
requests_df = spark.table("de_learning.force_reload_requests_day27")

audited_files_df = (
    audit_df
    .where(F.col("target_table") == F.lit("orders_delta_day27"))
    .groupBy("file_path")
    .agg(
        F.max("loaded_at").alias("last_loaded_at"),
        F.max("file_hash").alias("last_audited_hash"),
        F.count("*").alias("audit_record_count"),
    )
)

force_decisions_df = (
    requests_df
    .join(audited_files_df, on="file_path", how="left")
    .withColumn("file_previously_audited", F.col("last_loaded_at").isNotNull())
    .withColumn(
        "hash_changed",
        F.coalesce(F.col("new_file_hash") != F.col("original_file_hash"), F.lit(false)),
    )
    .withColumn(
        "missing_evidence",
        F.concat_ws(
            ", ",
            F.when(~F.col("has_owner_approval"), F.lit("owner_approval")),
            F.when(~F.col("has_reconciliation_query"), F.lit("reconciliation_query")),
            F.when(~F.col("has_backout_plan"), F.lit("backout_plan")),
        ),
    )
    .withColumn(
        "force_decision",
        F.when(~F.col("requested_force"), F.lit("NO_FORCE_REQUESTED"))
        .when(~F.col("file_previously_audited"), F.lit("REJECT_FORCE_USE_NORMAL_COPY"))
        .when(F.length(F.col("missing_evidence")) > 0, F.lit("BLOCK_MISSING_FORCE_EVIDENCE"))
        .when(~F.col("hash_changed"), F.lit("BLOCK_NO_HASH_CHANGE"))
        .when(
            F.col("expected_duplicate_business_keys") > 0,
            F.lit("APPROVE_FORCE_WITH_DEDUPE_RECONCILIATION"),
        )
        .otherwise(F.lit("APPROVE_FORCE_RELOAD")),
    )
    .withColumn(
        "required_operator_action",
        F.when(
            F.col("force_decision") == "APPROVE_FORCE_WITH_DEDUPE_RECONCILIATION",
            F.lit("Run force reload only with downstream merge or duplicate cleanup evidence."),
        )
        .when(
            F.col("force_decision") == "BLOCK_MISSING_FORCE_EVIDENCE",
            F.concat(F.lit("Collect missing evidence: "), F.col("missing_evidence")),
        )
        .when(
            F.col("force_decision") == "REJECT_FORCE_USE_NORMAL_COPY",
            F.lit("Do not use force. Let the normal COPY INTO run discover the new file."),
        )
        .otherwise(F.lit("Follow standard reload runbook.")),
    )
    .select(
        "request_id",
        "file_path",
        "file_previously_audited",
        "hash_changed",
        "requested_force",
        "missing_evidence",
        "expected_duplicate_business_keys",
        "force_decision",
        "required_operator_action",
        "last_loaded_at",
    )
)

force_decisions_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "de_learning.copy_into_force_decisions_day27"
)

display(force_decisions_df.orderBy("request_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `fr-2701` is approved only with dedupe/reconciliation because the file was already audited and a hash changed.
# MAGIC - `fr-2702` is blocked because owner approval and reconciliation evidence are missing.
# MAGIC - `fr-2703` is rejected for force because it is a new file and should use normal `COPY INTO`.
# MAGIC
# MAGIC Operational meaning: `force = true` is an incident-control path, not a default retry knob. Use it only when audit, owner approval, reconciliation, and backout evidence exist.
# MAGIC
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `audit_df` represents processed file evidence; `requests_df` represents operator reload requests.
# MAGIC - SQL equivalent: join reload requests to file audit, then use `CASE WHEN` to produce `force_decision`.
# MAGIC - `F.col("file_path")` references columns safely inside DataFrame expressions.
# MAGIC - `groupBy(...).agg(...)` creates one audit summary row per file.
# MAGIC - `withColumn(...)` adds derived booleans and decision labels without mutating the original DataFrame.
# MAGIC - PySpark is lazy until `write.saveAsTable(...)` and `display(...)` trigger execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Store Production COPY INTO Command Templates
# MAGIC
# MAGIC Purpose: keep executable command shapes close to the evidence tables so an operator can adapt the simulation to real Unity Catalog volumes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_command_templates_day27 (
# MAGIC   template_name STRING,
# MAGIC   template_sql STRING,
# MAGIC   when_to_use STRING,
# MAGIC   operational_risk STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO copy_into_command_templates_day27 VALUES
# MAGIC   (
# MAGIC     'validate_all_before_load',
# MAGIC     'COPY INTO de_learning.orders_delta_day27 FROM ''/Volumes/<catalog>/<schema>/<volume>/partner_orders/'' FILEFORMAT = JSON VALIDATE ALL',
# MAGIC     'Preview and validate source files before committing data.',
# MAGIC     'Validation is not a substitute for quarantine and downstream quality gates.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'explicit_file_batch',
# MAGIC     'COPY INTO de_learning.orders_delta_day27 FROM ''/Volumes/<catalog>/<schema>/<volume>/partner_orders/'' FILEFORMAT = JSON FILES = (''orders_001.json'', ''orders_002.json'')',
# MAGIC     'Load a bounded batch when the exact object list is known.',
# MAGIC     'FILES lists are capped and must not become manual production bookkeeping.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'normal_batch_load',
# MAGIC     'COPY INTO de_learning.orders_delta_day27 FROM ''/Volumes/<catalog>/<schema>/<volume>/partner_orders/'' FILEFORMAT = JSON FORMAT_OPTIONS (''multiLine'' = ''false'') COPY_OPTIONS (''mergeSchema'' = ''false'')',
# MAGIC     'Run scheduled SQL-first batch ingestion for immutable files.',
# MAGIC     'Schema drift and malformed data still need explicit handling.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'pattern_batch_load',
# MAGIC     'COPY INTO de_learning.orders_delta_day27 FROM ''/Volumes/<catalog>/<schema>/<volume>/partner_orders/'' FILEFORMAT = JSON PATTERN = ''batch_001/.*[.]json''',
# MAGIC     'Restrict loading to a known batch or partition-like prefix.',
# MAGIC     'Bad patterns can skip files or include the wrong source slice.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'force_reload_incident_only',
# MAGIC     'COPY INTO de_learning.orders_delta_day27 FROM ''/Volumes/<catalog>/<schema>/<volume>/partner_orders/'' FILEFORMAT = JSON COPY_OPTIONS (''force'' = ''true'')',
# MAGIC     'Reload files only after approval, reconciliation query, and backout plan are recorded.',
# MAGIC     'Can duplicate data or overwrite assumptions if business-key reconciliation is missing.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT template_name, when_to_use, operational_risk
# MAGIC FROM copy_into_command_templates_day27
# MAGIC ORDER BY template_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Five templates: validation, explicit files, normal load, pattern load, and force reload.
# MAGIC
# MAGIC Operational meaning: production teams should standardize command shapes and require review for risky options such as force reload.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Build The Operator Runbook And Final Checks
# MAGIC
# MAGIC Purpose: summarize the evidence an on-call data engineer should inspect after the batch load.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_runbook_day27 AS
# MAGIC SELECT '1_validate_files' AS step_id,
# MAGIC        'Run COPY INTO ... VALIDATE ALL or equivalent parsing checks before committing rows.' AS operator_action,
# MAGIC        'copy_into_validation_summary_day27' AS evidence_table,
# MAGIC        'No malformed records are hidden.' AS pass_condition
# MAGIC UNION ALL
# MAGIC SELECT '2_load_and_quarantine',
# MAGIC        'Load valid rows, quarantine invalid rows, and audit every processed source file.',
# MAGIC        'orders_delta_day27, orders_quarantine_day27, copy_into_file_audit_day27',
# MAGIC        'Loaded + quarantined file count equals candidate file count.'
# MAGIC UNION ALL
# MAGIC SELECT '3_rerun_safely',
# MAGIC        'Rerun candidate discovery and confirm already-audited files are skipped.',
# MAGIC        'copy_into_rerun_report_day27',
# MAGIC        'Rerun candidate count is zero for the same batch.'
# MAGIC UNION ALL
# MAGIC SELECT '4_check_business_keys',
# MAGIC        'Check duplicate business keys before silver promotion.',
# MAGIC        'business_key_duplicates_day27',
# MAGIC        'Duplicates have merge, reject, or remediation decisions.'
# MAGIC UNION ALL
# MAGIC SELECT '5_control_force',
# MAGIC        'Allow force reload only with audit evidence, approval, reconciliation, and backout plan.',
# MAGIC        'copy_into_force_decisions_day27',
# MAGIC        'No force reload runs without required evidence.';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   'landing_files' AS metric,
# MAGIC   COUNT(*) AS observed_count,
# MAGIC   5 AS expected_count
# MAGIC FROM landing_order_files_day27
# MAGIC UNION ALL
# MAGIC SELECT 'loaded_rows', COUNT(*), 4 FROM orders_delta_day27
# MAGIC UNION ALL
# MAGIC SELECT 'quarantined_rows', COUNT(*), 1 FROM orders_quarantine_day27
# MAGIC UNION ALL
# MAGIC SELECT 'audited_files', COUNT(*), 5 FROM copy_into_file_audit_day27
# MAGIC UNION ALL
# MAGIC SELECT 'duplicate_business_keys', COUNT(*), 1 FROM business_key_duplicates_day27
# MAGIC UNION ALL
# MAGIC SELECT 'force_decisions', COUNT(*), 3 FROM copy_into_force_decisions_day27
# MAGIC UNION ALL
# MAGIC SELECT 'command_templates', COUNT(*), 5 FROM copy_into_command_templates_day27
# MAGIC ORDER BY metric;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM copy_into_runbook_day27 ORDER BY step_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Every final-check metric equals its expected count.
# MAGIC - The runbook lists validation, load/quarantine, rerun, duplicate-key, and force-control checks.
# MAGIC
# MAGIC Operational meaning: an ingestion batch is not finished when rows land. It is finished when rerun, quarantine, duplicate-key, and force-reload evidence are reviewable.
