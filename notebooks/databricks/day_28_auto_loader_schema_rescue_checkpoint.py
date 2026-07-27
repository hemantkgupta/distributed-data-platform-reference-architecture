# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 28 - Auto Loader Schema Evolution, Rescue Data, And Checkpoints
# MAGIC
# MAGIC Goal: practice Auto Loader-style incremental ingestion with schema evolution, `_rescued_data`, `_corrupt_record`, checkpoint ownership, Unity Catalog storage paths, monitoring checks, and a weekly readiness checkpoint.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: ingestion/loading, PySpark transformations, Delta targets, troubleshooting/monitoring, and governance/security.
# MAGIC - Professional stretch: schema evolution mode choice, checkpoint durability, rescue-data isolation, Lakeflow production expectations, cost/performance guardrails, and deployment runbooks.
# MAGIC
# MAGIC This notebook simulates Auto Loader file discovery with Delta tables so it can run without external cloud credentials. The command-template cells show real Auto Loader and Lakeflow shapes for Unity Catalog volumes or external locations.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Landing Files, Bronze Target, Checkpoint, And Schema Registry
# MAGIC
# MAGIC Purpose: model a file source with two arrivals: a clean first micro-batch and a second micro-batch with additive fields, type drift, nested drift, and malformed JSON.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS autoloader_microbatch_001_raw_day28;
# MAGIC DROP VIEW IF EXISTS autoloader_microbatch_001_parsed_day28;
# MAGIC DROP VIEW IF EXISTS autoloader_final_checks_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_command_templates_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_quality_expectations_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_resource_violations_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_resource_plan_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_schema_mode_decisions_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_schema_evolution_scenarios_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_monitoring_day28;
# MAGIC DROP TABLE IF EXISTS orders_quarantine_day28;
# MAGIC DROP TABLE IF EXISTS orders_bronze_autoloader_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_schema_registry_day28;
# MAGIC DROP TABLE IF EXISTS autoloader_checkpoint_day28;
# MAGIC DROP TABLE IF EXISTS landing_order_files_day28;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landing_order_files_day28 (
# MAGIC   source_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   file_size_bytes BIGINT,
# MAGIC   arrival_sequence INT,
# MAGIC   arrival_batch STRING,
# MAGIC   payload STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO landing_order_files_day28 VALUES
# MAGIC   (
# MAGIC     'partner_orders_autoloader_day28',
# MAGIC     'dbfs:/landing/day28/partner_orders/batch_001/orders_001.json',
# MAGIC     TIMESTAMP '2026-07-27 05:30:00',
# MAGIC     415,
# MAGIC     1,
# MAGIC     'batch_001',
# MAGIC     '{"event_id":"evt-2801","order_id":2801,"customer_id":981,"order_date":"2026-07-26","amount":"210.00","status":"completed"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_autoloader_day28',
# MAGIC     'dbfs:/landing/day28/partner_orders/batch_001/orders_002.json',
# MAGIC     TIMESTAMP '2026-07-27 05:31:00',
# MAGIC     412,
# MAGIC     1,
# MAGIC     'batch_001',
# MAGIC     '{"event_id":"evt-2802","order_id":2802,"customer_id":982,"order_date":"2026-07-26","amount":"95.50","status":"pending"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_autoloader_day28',
# MAGIC     'dbfs:/landing/day28/partner_orders/batch_002/orders_003_coupon.json',
# MAGIC     TIMESTAMP '2026-07-27 05:40:00',
# MAGIC     484,
# MAGIC     2,
# MAGIC     'batch_002',
# MAGIC     '{"event_id":"evt-2803","order_id":2803,"customer_id":983,"order_date":"2026-07-27","amount":"45.00","status":"completed","coupon_code":"JULY25","channel":"mobile"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_autoloader_day28',
# MAGIC     'dbfs:/landing/day28/partner_orders/batch_002/orders_004_bad_amount.json',
# MAGIC     TIMESTAMP '2026-07-27 05:41:00',
# MAGIC     472,
# MAGIC     2,
# MAGIC     'batch_002',
# MAGIC     '{"event_id":"evt-2804","order_id":2804,"customer_id":984,"order_date":"2026-07-27","amount":"bad_amount","status":"completed","loyalty_tier":"gold"}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_autoloader_day28',
# MAGIC     'dbfs:/landing/day28/partner_orders/batch_002/orders_005_shipping.json',
# MAGIC     TIMESTAMP '2026-07-27 05:42:00',
# MAGIC     535,
# MAGIC     2,
# MAGIC     'batch_002',
# MAGIC     '{"event_id":"evt-2805","order_id":2805,"customer_id":985,"order_date":"2026-07-27","amount":"72.00","status":"completed","shipping":{"city":"Pune","priority":"express"}}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'partner_orders_autoloader_day28',
# MAGIC     'dbfs:/landing/day28/partner_orders/batch_002/orders_006_corrupt.json',
# MAGIC     TIMESTAMP '2026-07-27 05:43:00',
# MAGIC     276,
# MAGIC     2,
# MAGIC     'batch_002',
# MAGIC     '{"event_id":"evt-2806","order_id":2806,"customer_id":986,"order_date":"2026-07-27","amount":'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_checkpoint_day28 (
# MAGIC   stream_id STRING,
# MAGIC   file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   discovered_at TIMESTAMP,
# MAGIC   processed_at TIMESTAMP,
# MAGIC   micro_batch_id STRING,
# MAGIC   processing_status STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   schema_location STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_schema_registry_day28 (
# MAGIC   stream_id STRING,
# MAGIC   schema_version INT,
# MAGIC   schema_evolution_mode STRING,
# MAGIC   schema_location STRING,
# MAGIC   discovered_micro_batch_id STRING,
# MAGIC   columns_json STRING,
# MAGIC   discovered_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_bronze_autoloader_day28 (
# MAGIC   event_id STRING,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   normalized_status STRING,
# MAGIC   source_file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   _rescued_data STRING,
# MAGIC   _corrupt_record STRING,
# MAGIC   _ingested_at TIMESTAMP,
# MAGIC   _ingest_run_id STRING,
# MAGIC   _ingest_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_quarantine_day28 (
# MAGIC   event_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   raw_payload STRING,
# MAGIC   rescued_data STRING,
# MAGIC   corrupt_record STRING,
# MAGIC   quarantined_at TIMESTAMP,
# MAGIC   stream_id STRING,
# MAGIC   micro_batch_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT arrival_batch, COUNT(*) AS file_count, SUM(file_size_bytes) AS total_bytes
# MAGIC FROM landing_order_files_day28
# MAGIC GROUP BY arrival_batch
# MAGIC ORDER BY arrival_batch;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `batch_001` has 2 base-schema files.
# MAGIC - `batch_002` has 4 files with schema drift or bad data.
# MAGIC
# MAGIC Operational meaning: Auto Loader pipelines are long-lived; you must expect later arrivals to differ from the first inferred schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Process The First AvailableNow Micro-Batch
# MAGIC
# MAGIC Purpose: run a clean first micro-batch, persist base rows, advance the checkpoint, and record schema version 1.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reference only: real Auto Loader AvailableNow shape for a Unity Catalog volume.
# MAGIC -- checkpoint_path = "/Volumes/<catalog>/<schema>/<ops_volume>/checkpoints/orders_auto_day28"
# MAGIC -- schema_path = "/Volumes/<catalog>/<schema>/<ops_volume>/schemas/orders_auto_day28"
# MAGIC -- (spark.readStream
# MAGIC --   .format("cloudFiles")
# MAGIC --   .option("cloudFiles.format", "json")
# MAGIC --   .option("cloudFiles.schemaLocation", schema_path)
# MAGIC --   .load("/Volumes/<catalog>/<schema>/<source_volume>/partner_orders/")
# MAGIC --   .writeStream
# MAGIC --   .option("checkpointLocation", checkpoint_path)
# MAGIC --   .trigger(availableNow=True)
# MAGIC --   .toTable("de_learning.orders_bronze_autoloader_day28"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW autoloader_microbatch_001_raw_day28 AS
# MAGIC SELECT
# MAGIC   l.file_path AS source_file_path,
# MAGIC   l.file_mod_time,
# MAGIC   l.payload,
# MAGIC   get_json_object(l.payload, '$.event_id') AS event_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_id') AS INT) AS order_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.customer_id') AS INT) AS customer_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_date') AS DATE) AS order_date,
# MAGIC   try_cast(get_json_object(l.payload, '$.amount') AS DECIMAL(10,2)) AS amount,
# MAGIC   upper(get_json_object(l.payload, '$.status')) AS normalized_status
# MAGIC FROM landing_order_files_day28 l
# MAGIC WHERE l.arrival_sequence = 1
# MAGIC   AND NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM autoloader_checkpoint_day28 c
# MAGIC     WHERE c.file_path = l.file_path
# MAGIC       AND c.stream_id = 'orders-auto-stream-day28'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW autoloader_microbatch_001_parsed_day28 AS
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   normalized_status,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   CAST(NULL AS STRING) AS _rescued_data,
# MAGIC   CAST(NULL AS STRING) AS _corrupt_record,
# MAGIC   current_timestamp() AS _ingested_at,
# MAGIC   'auto-available-now-2801' AS _ingest_run_id,
# MAGIC   'ACCEPTED' AS _ingest_status,
# MAGIC   payload
# MAGIC FROM autoloader_microbatch_001_raw_day28;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_bronze_autoloader_day28
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   normalized_status,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   _rescued_data,
# MAGIC   _corrupt_record,
# MAGIC   _ingested_at,
# MAGIC   _ingest_run_id,
# MAGIC   _ingest_status
# MAGIC FROM autoloader_microbatch_001_parsed_day28;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_checkpoint_day28
# MAGIC SELECT
# MAGIC   'orders-auto-stream-day28' AS stream_id,
# MAGIC   source_file_path AS file_path,
# MAGIC   file_mod_time,
# MAGIC   current_timestamp() AS discovered_at,
# MAGIC   current_timestamp() AS processed_at,
# MAGIC   'microbatch-001' AS micro_batch_id,
# MAGIC   'PROCESSED' AS processing_status,
# MAGIC   '/Volumes/main/de_learning/ops/checkpoints/orders-auto-stream-day28' AS checkpoint_location,
# MAGIC   '/Volumes/main/de_learning/ops/schemas/orders-auto-stream-day28' AS schema_location
# MAGIC FROM autoloader_microbatch_001_parsed_day28;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_schema_registry_day28 VALUES
# MAGIC   (
# MAGIC     'orders-auto-stream-day28',
# MAGIC     1,
# MAGIC     'addNewColumns',
# MAGIC     '/Volumes/main/de_learning/ops/schemas/orders-auto-stream-day28',
# MAGIC     'microbatch-001',
# MAGIC     '["event_id","order_id","customer_id","order_date","amount","status"]',
# MAGIC     current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM orders_bronze_autoloader_day28) AS bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM autoloader_checkpoint_day28) AS checkpoint_files,
# MAGIC   (SELECT MAX(schema_version) FROM autoloader_schema_registry_day28) AS current_schema_version;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `bronze_rows = 2`.
# MAGIC - `checkpoint_files = 2`.
# MAGIC - `current_schema_version = 1`.
# MAGIC
# MAGIC Operational meaning: the checkpoint and schema location are durable stream state. They are as important as the target Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Process Drifted Files With Rescue And Corrupt-Record Handling
# MAGIC
# MAGIC Purpose: use PySpark to discover files not in the checkpoint, parse known fields, preserve unexpected fields in `_rescued_data`, and isolate corrupt records.

# COMMAND ----------

from pyspark.sql import functions as F

landing_df = spark.table("de_learning.landing_order_files_day28")
checkpoint_df = spark.table("de_learning.autoloader_checkpoint_day28")

new_files_df = (
    landing_df
    .where(F.col("source_id") == F.lit("partner_orders_autoloader_day28"))
    .join(
        checkpoint_df
        .where(F.col("stream_id") == F.lit("orders-auto-stream-day28"))
        .select("file_path"),
        on="file_path",
        how="left_anti",
    )
)

raw_amount_col = F.get_json_object(F.col("payload"), "$.amount")
coupon_col = F.get_json_object(F.col("payload"), "$.coupon_code")
channel_col = F.get_json_object(F.col("payload"), "$.channel")
loyalty_col = F.get_json_object(F.col("payload"), "$.loyalty_tier")
shipping_city_col = F.get_json_object(F.col("payload"), "$.shipping.city")
shipping_priority_col = F.get_json_object(F.col("payload"), "$.shipping.priority")

drifted_files_df = (
    new_files_df
    .withColumn("event_id", F.get_json_object(F.col("payload"), "$.event_id"))
    .withColumn("order_id", F.get_json_object(F.col("payload"), "$.order_id").cast("int"))
    .withColumn("customer_id", F.get_json_object(F.col("payload"), "$.customer_id").cast("int"))
    .withColumn("order_date", F.to_date(F.get_json_object(F.col("payload"), "$.order_date")))
    .withColumn("amount", F.expr("try_cast(get_json_object(payload, '$.amount') as decimal(10,2))"))
    .withColumn("normalized_status", F.upper(F.get_json_object(F.col("payload"), "$.status")))
    .withColumn("has_corrupt_record", F.col("event_id").isNull() & ~F.col("payload").endswith("}"))
    .withColumn(
        "has_rescued_data",
        coupon_col.isNotNull()
        | channel_col.isNotNull()
        | loyalty_col.isNotNull()
        | shipping_city_col.isNotNull()
        | shipping_priority_col.isNotNull()
        | (raw_amount_col.isNotNull() & F.col("amount").isNull()),
    )
    .withColumn(
        "_rescued_data",
        F.when(
            F.col("has_rescued_data"),
            F.to_json(
                F.struct(
                    coupon_col.alias("coupon_code"),
                    channel_col.alias("channel"),
                    loyalty_col.alias("loyalty_tier"),
                    shipping_city_col.alias("shipping_city"),
                    shipping_priority_col.alias("shipping_priority"),
                    F.when(
                        raw_amount_col.isNotNull() & F.col("amount").isNull(),
                        raw_amount_col,
                    ).alias("amount_raw"),
                    F.col("file_path").alias("source_file_path"),
                )
            ),
        ).otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "_corrupt_record",
        F.when(F.col("has_corrupt_record"), F.col("payload")).otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "_ingest_status",
        F.when(F.col("has_corrupt_record"), F.lit("CORRUPT_RECORD"))
        .when(
            F.col("event_id").isNull()
            | F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("order_date").isNull()
            | F.col("amount").isNull(),
            F.lit("QUARANTINE_PARSE_FAILURE"),
        )
        .when(F.col("_rescued_data").isNotNull(), F.lit("ACCEPTED_WITH_RESCUE"))
        .otherwise(F.lit("ACCEPTED")),
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingest_run_id", F.lit("auto-available-now-2802"))
    .select(
        "event_id",
        "order_id",
        "customer_id",
        "order_date",
        "amount",
        "normalized_status",
        F.col("file_path").alias("source_file_path"),
        "file_mod_time",
        "_rescued_data",
        "_corrupt_record",
        "_ingested_at",
        "_ingest_run_id",
        "_ingest_status",
        F.col("payload").alias("raw_payload"),
    )
)

drifted_files_df.createOrReplaceTempView("autoloader_microbatch_002_candidates_day28")
display(drifted_files_df.orderBy("source_file_path"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `landing_df` represents all visible source files; `checkpoint_df` represents files already discovered by the Auto Loader stream.
# MAGIC - SQL equivalent: `SELECT ... FROM landing l WHERE NOT EXISTS (SELECT 1 FROM checkpoint c WHERE c.file_path = l.file_path)`.
# MAGIC - `join(..., how="left_anti")` keeps only rows in the landing table that do not match the checkpoint.
# MAGIC - `F.get_json_object(...)` extracts JSON fields from the raw payload string.
# MAGIC - `withColumn(...)` adds parsed columns, rescue flags, corrupt-record flags, and ingest status.
# MAGIC - `F.when(...).otherwise(...)` is DataFrame syntax for SQL `CASE WHEN`.
# MAGIC - `createOrReplaceTempView(...)` exposes the PySpark DataFrame to later SQL cells.
# MAGIC - Transformations are lazy until `display(...)`, `write`, or another action runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ingest_status, COUNT(*) AS row_count
# MAGIC FROM autoloader_microbatch_002_candidates_day28
# MAGIC GROUP BY _ingest_status
# MAGIC ORDER BY _ingest_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_bronze_autoloader_day28
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   normalized_status,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   _rescued_data,
# MAGIC   _corrupt_record,
# MAGIC   _ingested_at,
# MAGIC   _ingest_run_id,
# MAGIC   _ingest_status
# MAGIC FROM autoloader_microbatch_002_candidates_day28;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day28
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   CASE
# MAGIC     WHEN _ingest_status = 'CORRUPT_RECORD' THEN 'payload could not be parsed as JSON'
# MAGIC     WHEN amount IS NULL THEN 'amount failed decimal parsing'
# MAGIC     ELSE 'required field parsing failed'
# MAGIC   END AS quarantine_reason,
# MAGIC   raw_payload,
# MAGIC   _rescued_data AS rescued_data,
# MAGIC   _corrupt_record AS corrupt_record,
# MAGIC   current_timestamp() AS quarantined_at,
# MAGIC   'orders-auto-stream-day28' AS stream_id,
# MAGIC   'microbatch-002' AS micro_batch_id
# MAGIC FROM autoloader_microbatch_002_candidates_day28
# MAGIC WHERE _ingest_status IN ('QUARANTINE_PARSE_FAILURE', 'CORRUPT_RECORD');

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_checkpoint_day28
# MAGIC SELECT
# MAGIC   'orders-auto-stream-day28' AS stream_id,
# MAGIC   source_file_path AS file_path,
# MAGIC   file_mod_time,
# MAGIC   current_timestamp() AS discovered_at,
# MAGIC   current_timestamp() AS processed_at,
# MAGIC   'microbatch-002' AS micro_batch_id,
# MAGIC   _ingest_status AS processing_status,
# MAGIC   '/Volumes/main/de_learning/ops/checkpoints/orders-auto-stream-day28' AS checkpoint_location,
# MAGIC   '/Volumes/main/de_learning/ops/schemas/orders-auto-stream-day28' AS schema_location
# MAGIC FROM autoloader_microbatch_002_candidates_day28;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_schema_registry_day28 VALUES
# MAGIC   (
# MAGIC     'orders-auto-stream-day28',
# MAGIC     2,
# MAGIC     'rescue',
# MAGIC     '/Volumes/main/de_learning/ops/schemas/orders-auto-stream-day28',
# MAGIC     'microbatch-002',
# MAGIC     '["event_id","order_id","customer_id","order_date","amount","status","_rescued_data","_corrupt_record"]',
# MAGIC     current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM orders_bronze_autoloader_day28) AS bronze_rows,
# MAGIC   (SELECT COUNT(*) FROM orders_quarantine_day28) AS quarantine_rows,
# MAGIC   (SELECT COUNT(*) FROM autoloader_checkpoint_day28) AS checkpoint_files,
# MAGIC   (SELECT COUNT(*) FROM orders_bronze_autoloader_day28 WHERE _rescued_data IS NOT NULL) AS rescued_rows,
# MAGIC   (SELECT COUNT(*) FROM orders_bronze_autoloader_day28 WHERE _corrupt_record IS NOT NULL) AS corrupt_rows;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Micro-batch 2 status counts: 2 `ACCEPTED_WITH_RESCUE`, 1 `QUARANTINE_PARSE_FAILURE`, 1 `CORRUPT_RECORD`.
# MAGIC - Final totals: `bronze_rows = 6`, `quarantine_rows = 2`, `checkpoint_files = 6`, `rescued_rows = 3`, `corrupt_rows = 1`.
# MAGIC
# MAGIC Operational meaning: rescue data lets the pipeline preserve unexpected fields and type mismatches. Corrupt records still need isolation so broken JSON does not flow downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Choose The Schema Evolution Mode
# MAGIC
# MAGIC Purpose: map common source contracts to Auto Loader schema behavior.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_schema_evolution_scenarios_day28 (
# MAGIC   scenario_id STRING,
# MAGIC   source_contract_shape STRING,
# MAGIC   schema_known BOOLEAN,
# MAGIC   additive_columns_expected BOOLEAN,
# MAGIC   compatible_type_widening_expected BOOLEAN,
# MAGIC   strict_contract_required BOOLEAN,
# MAGIC   unpredictable_payload BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_schema_evolution_scenarios_day28 VALUES
# MAGIC   ('schema-fixed-orders', 'Known schema owned by source team', true, false, false, true, false),
# MAGIC   ('vendor-additive-fields', 'Vendor frequently adds nullable fields', false, true, false, false, false),
# MAGIC   ('parquet-type-widening', 'Parquet metrics widen int to long', false, true, true, false, false),
# MAGIC   ('regulated-contract', 'Strict contract must fail on new fields', true, false, false, true, false),
# MAGIC   ('raw-api-webhooks', 'Webhook shape changes unpredictably by event type', false, true, true, false, true),
# MAGIC   ('keep-running-rescue', 'Keep stream alive while preserving unexpected fields', true, true, false, false, false);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_schema_mode_decisions_day28 AS
# MAGIC SELECT
# MAGIC   scenario_id,
# MAGIC   source_contract_shape,
# MAGIC   CASE
# MAGIC     WHEN unpredictable_payload THEN 'singleVariantColumn'
# MAGIC     WHEN strict_contract_required THEN 'failOnNewColumns'
# MAGIC     WHEN compatible_type_widening_expected THEN 'addNewColumnsWithTypeWidening'
# MAGIC     WHEN additive_columns_expected AND schema_known THEN 'rescue'
# MAGIC     WHEN additive_columns_expected THEN 'addNewColumns'
# MAGIC     ELSE 'none_with_explicit_schema'
# MAGIC   END AS recommended_schema_strategy,
# MAGIC   CASE
# MAGIC     WHEN unpredictable_payload THEN 'Query-time schema-on-read flexibility is more important than typed bronze columns.'
# MAGIC     WHEN strict_contract_required THEN 'Stop the stream and force producer/schema-owner intervention.'
# MAGIC     WHEN compatible_type_widening_expected THEN 'Allow compatible widening while rescuing unsupported type changes.'
# MAGIC     WHEN additive_columns_expected AND schema_known THEN 'Keep the table schema stable and inspect unexpected fields in _rescued_data.'
# MAGIC     WHEN additive_columns_expected THEN 'Let Auto Loader merge new fields, then restart through Lakeflow Jobs.'
# MAGIC     ELSE 'Provide explicit schema and do not evolve automatically.'
# MAGIC   END AS operator_reason
# MAGIC FROM autoloader_schema_evolution_scenarios_day28;
# MAGIC
# MAGIC SELECT * FROM autoloader_schema_mode_decisions_day28 ORDER BY scenario_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Fixed schemas use explicit schema control.
# MAGIC - Additive vendor drift uses `addNewColumns` or `rescue`.
# MAGIC - Compatible type changes use `addNewColumnsWithTypeWidening`.
# MAGIC - Strict contracts use `failOnNewColumns`.
# MAGIC - Unpredictable payloads use `Variant`-style ingestion.
# MAGIC
# MAGIC Operational meaning: schema evolution mode is a source-contract decision, not a convenience toggle.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Validate Checkpoint And Unity Catalog Resource Placement
# MAGIC
# MAGIC Purpose: catch production misconfiguration before a stream loses state or violates Unity Catalog storage boundaries.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_resource_plan_day28 (
# MAGIC   stream_id STRING,
# MAGIC   source_path STRING,
# MAGIC   target_table STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   schema_location STRING,
# MAGIC   target_table_location STRING,
# MAGIC   uc_managed_storage BOOLEAN,
# MAGIC   lifecycle_policy_on_checkpoint BOOLEAN,
# MAGIC   one_checkpoint_per_source BOOLEAN,
# MAGIC   uses_file_events BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_resource_plan_day28 VALUES
# MAGIC   (
# MAGIC     'orders-auto-stream-day28',
# MAGIC     '/Volumes/main/de_learning/raw_orders/partner_orders/',
# MAGIC     'de_learning.orders_bronze_autoloader_day28',
# MAGIC     '/Volumes/main/de_learning/ops/checkpoints/orders-auto-stream-day28',
# MAGIC     '/Volumes/main/de_learning/ops/schemas/orders-auto-stream-day28',
# MAGIC     '/Volumes/main/de_learning/tables/orders_bronze_autoloader_day28',
# MAGIC     true,
# MAGIC     false,
# MAGIC     true,
# MAGIC     true
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders-bad-nested-checkpoint-day28',
# MAGIC     '/Volumes/main/de_learning/raw_orders/partner_orders/',
# MAGIC     'de_learning.orders_bronze_autoloader_day28',
# MAGIC     '/Volumes/main/de_learning/tables/orders_bronze_autoloader_day28/_checkpoint',
# MAGIC     '/Volumes/main/de_learning/tables/orders_bronze_autoloader_day28/_schemas',
# MAGIC     '/Volumes/main/de_learning/tables/orders_bronze_autoloader_day28',
# MAGIC     true,
# MAGIC     false,
# MAGIC     true,
# MAGIC     false
# MAGIC   ),
# MAGIC   (
# MAGIC     'orders-bad-lifecycle-day28',
# MAGIC     '/Volumes/main/de_learning/raw_orders/partner_orders/',
# MAGIC     'de_learning.orders_bronze_autoloader_day28',
# MAGIC     's3://raw-bucket/tmp/checkpoints/shared-orders',
# MAGIC     's3://raw-bucket/tmp/schemas/shared-orders',
# MAGIC     '/Volumes/main/de_learning/tables/orders_bronze_autoloader_day28',
# MAGIC     false,
# MAGIC     true,
# MAGIC     false,
# MAGIC     false
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_resource_violations_day28 AS
# MAGIC SELECT
# MAGIC   stream_id,
# MAGIC   CASE
# MAGIC     WHEN checkpoint_location LIKE concat(target_table_location, '%') THEN 'CHECKPOINT_NESTED_UNDER_TABLE'
# MAGIC     WHEN schema_location LIKE concat(target_table_location, '%') THEN 'SCHEMA_LOCATION_NESTED_UNDER_TABLE'
# MAGIC     WHEN uc_managed_storage = false THEN 'NOT_UC_MANAGED_STORAGE'
# MAGIC     WHEN lifecycle_policy_on_checkpoint THEN 'CHECKPOINT_HAS_LIFECYCLE_POLICY'
# MAGIC     WHEN one_checkpoint_per_source = false THEN 'CHECKPOINT_SHARED_ACROSS_SOURCES'
# MAGIC     WHEN uses_file_events = false THEN 'NO_FILE_EVENTS_COST_RISK'
# MAGIC     ELSE 'OK'
# MAGIC   END AS first_violation,
# MAGIC   checkpoint_location,
# MAGIC   schema_location
# MAGIC FROM autoloader_resource_plan_day28;
# MAGIC
# MAGIC SELECT * FROM autoloader_resource_violations_day28 ORDER BY stream_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders-auto-stream-day28` is `OK`.
# MAGIC - The nested-checkpoint plan is rejected.
# MAGIC - The lifecycle/shared-state plan is rejected.
# MAGIC
# MAGIC Operational meaning: checkpoint and schema state must live in governed, durable, separate locations. Losing checkpoint state forces a full restart and can reprocess files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Build Monitoring Metrics And Quality Expectations
# MAGIC
# MAGIC Purpose: expose the evidence that a Lakeflow Job, dashboard, or alert should inspect.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_monitoring_day28 AS
# MAGIC SELECT 'files_landed' AS metric_name, COUNT(*) AS metric_value, 'source inventory' AS metric_group
# MAGIC FROM landing_order_files_day28
# MAGIC UNION ALL
# MAGIC SELECT 'files_checkpointed', COUNT(*), 'checkpoint'
# MAGIC FROM autoloader_checkpoint_day28
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_rows', COUNT(*), 'target'
# MAGIC FROM orders_bronze_autoloader_day28
# MAGIC UNION ALL
# MAGIC SELECT 'rescued_rows', COUNT(*), 'quality'
# MAGIC FROM orders_bronze_autoloader_day28
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'corrupt_rows', COUNT(*), 'quality'
# MAGIC FROM orders_bronze_autoloader_day28
# MAGIC WHERE _corrupt_record IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', COUNT(*), 'quality'
# MAGIC FROM orders_quarantine_day28
# MAGIC UNION ALL
# MAGIC SELECT 'resource_violations', COUNT(*), 'operations'
# MAGIC FROM autoloader_resource_violations_day28
# MAGIC WHERE first_violation <> 'OK';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_quality_expectations_day28 AS
# MAGIC SELECT
# MAGIC   'checkpoint_covers_all_files' AS expectation_name,
# MAGIC   CASE
# MAGIC     WHEN (SELECT COUNT(*) FROM landing_order_files_day28) = (SELECT COUNT(*) FROM autoloader_checkpoint_day28)
# MAGIC       THEN 'PASS'
# MAGIC     ELSE 'FAIL'
# MAGIC   END AS expectation_status,
# MAGIC   'All discovered files should be represented in checkpoint state.' AS expectation_reason
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'no_corrupt_records',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM orders_bronze_autoloader_day28 WHERE _corrupt_record IS NOT NULL) = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   'Corrupt records should be isolated and reviewed.'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'rescued_data_review_required',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM orders_bronze_autoloader_day28 WHERE _rescued_data IS NOT NULL) = 0 THEN 'PASS' ELSE 'REVIEW' END,
# MAGIC   'Rescued data means schema drift or type mismatch needs an owner decision.'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'resource_plan_clean',
# MAGIC   CASE WHEN (SELECT COUNT(*) FROM autoloader_resource_violations_day28 WHERE first_violation <> 'OK') = 0 THEN 'PASS' ELSE 'FAIL' END,
# MAGIC   'Checkpoint, schema, and source locations must be production-safe.';
# MAGIC
# MAGIC SELECT * FROM autoloader_quality_expectations_day28 ORDER BY expectation_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `checkpoint_covers_all_files = PASS`.
# MAGIC - `no_corrupt_records = FAIL`.
# MAGIC - `rescued_data_review_required = REVIEW`.
# MAGIC - `resource_plan_clean = FAIL` because the lab includes bad example plans.
# MAGIC
# MAGIC Operational meaning: monitoring should separate data quality issues from resource-placement issues. Both can break production, but they route to different owners.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Store Production Command Templates And Final Checks
# MAGIC
# MAGIC Purpose: keep real Auto Loader, Lakeflow, and monitoring command shapes next to the simulated evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_command_templates_day28 (
# MAGIC   template_name STRING,
# MAGIC   template_text STRING,
# MAGIC   when_to_use STRING,
# MAGIC   operational_meaning STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_command_templates_day28 VALUES
# MAGIC   (
# MAGIC     'available_now_autoloader_python',
# MAGIC     '(spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", schema_path).option("rescuedDataColumn", "_rescued_data").option("columnNameOfCorruptRecord", "_corrupt_record").load(source_path).writeStream.option("checkpointLocation", checkpoint_path).trigger(availableNow=True).toTable("de_learning.orders_bronze_autoloader_day28"))',
# MAGIC     'Incremental ingestion as a triggered batch job when latency can be minutes or scheduled.',
# MAGIC     'Keeps checkpoint and schema state durable while minimizing idle compute.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'schema_rescue_mode',
# MAGIC     '.option("cloudFiles.schemaEvolutionMode", "rescue").option("rescuedDataColumn", "_rescued_data")',
# MAGIC     'Known bronze schema should stay stable while unexpected fields are preserved.',
# MAGIC     'Prevents silent data loss and avoids stream failure for additive drift.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'lakeflow_streaming_table_sql',
# MAGIC     'CREATE OR REFRESH STREAMING TABLE de_learning.orders_bronze_autoloader_day28 AS SELECT * FROM STREAM read_files("/Volumes/<catalog>/<schema>/<volume>/partner_orders/", format => "json");',
# MAGIC     'Lakeflow declarative ingestion where Databricks manages pipeline state.',
# MAGIC     'Moves orchestration, quality expectations, and event-log monitoring into Lakeflow.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'cloud_files_state_monitoring',
# MAGIC     'SELECT * FROM cloud_files_state("/Volumes/<catalog>/<schema>/<ops_volume>/checkpoints/orders-auto-stream-day28");',
# MAGIC     'Inspect discovered file state for an Auto Loader checkpoint.',
# MAGIC     'Supports incident triage, replay analysis, and stuck-file debugging.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'quality_expectations',
# MAGIC     '@dlt.expect("no rescued data", "_rescued_data IS NULL"); @dlt.expect("no corrupt records", "_corrupt_record IS NULL")',
# MAGIC     'Attach data quality checks to a Lakeflow pipeline.',
# MAGIC     'Turns drift and corrupt payloads into visible pipeline health signals.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW autoloader_final_checks_day28 AS
# MAGIC SELECT 'landing_files' AS metric, COUNT(*) AS observed_count, 6 AS expected_count FROM landing_order_files_day28
# MAGIC UNION ALL
# MAGIC SELECT 'checkpoint_files', COUNT(*), 6 FROM autoloader_checkpoint_day28
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_rows', COUNT(*), 6 FROM orders_bronze_autoloader_day28
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', COUNT(*), 2 FROM orders_quarantine_day28
# MAGIC UNION ALL
# MAGIC SELECT 'rescued_rows', COUNT(*), 3 FROM orders_bronze_autoloader_day28 WHERE _rescued_data IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'corrupt_rows', COUNT(*), 1 FROM orders_bronze_autoloader_day28 WHERE _corrupt_record IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'schema_versions', COUNT(*), 2 FROM autoloader_schema_registry_day28
# MAGIC UNION ALL
# MAGIC SELECT 'command_templates', COUNT(*), 5 FROM autoloader_command_templates_day28;
# MAGIC
# MAGIC SELECT * FROM autoloader_final_checks_day28 ORDER BY metric;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All final-check metrics match the expected counts.
# MAGIC - Command templates include AvailableNow, rescue mode, Lakeflow SQL, `cloud_files_state`, and quality expectations.
# MAGIC
# MAGIC Operational meaning: Auto Loader readiness is a combination of typed bronze data, checkpoint state, schema state, quality evidence, and monitoring hooks.
