# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 12 - Ingestion Choices: COPY INTO, Auto Loader, Checkpoints, And Rescue Data
# MAGIC
# MAGIC Goal: practice ingestion method selection, file-level idempotency, checkpoint/replay reasoning, schema drift handling, and rescued/quarantined data.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: ingestion/loading, Delta tables, SQL/Python transformations, troubleshooting, and monitoring.
# MAGIC - Professional stretch: checkpoint design, replay safety, rescued data, schema drift response, cost/performance tradeoffs.
# MAGIC
# MAGIC Note: this notebook uses Delta tables to simulate object-storage file arrival and ingestion checkpoints so it can run in a personal Databricks workspace. In production, `COPY INTO` and Auto Loader read from cloud storage or Unity Catalog volumes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Landing Files, Targets, And Checkpoints
# MAGIC
# MAGIC Purpose: model files arriving in cloud storage, plus the checkpoint tables that prevent duplicate file processing.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE landing_order_files_day12 (
# MAGIC   file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   payload STRING,
# MAGIC   arrival_group STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO landing_order_files_day12 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_001/orders_001.json',
# MAGIC     TIMESTAMP'2026-07-01T06:00:00Z',
# MAGIC     '{"event_id":"evt-1201","order_id":1201,"customer_id":501,"order_date":"2026-06-30","amount":"250.00","status":"completed"}',
# MAGIC     'arrival_001'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_001/orders_002.json',
# MAGIC     TIMESTAMP'2026-07-01T06:01:00Z',
# MAGIC     '{"event_id":"evt-1202","order_id":1202,"customer_id":502,"order_date":"2026-06-30","amount":"90.00","status":"pending"}',
# MAGIC     'arrival_001'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_001/orders_003.json',
# MAGIC     TIMESTAMP'2026-07-01T06:02:00Z',
# MAGIC     '{"event_id":"evt-1203","order_id":1203,"customer_id":503,"order_date":"2026-06-30","amount":"bad_amount","status":"completed"}',
# MAGIC     'arrival_001'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_001/orders_004.json',
# MAGIC     TIMESTAMP'2026-07-01T06:03:00Z',
# MAGIC     '{"event_id":"evt-1204","order_id":1204,"customer_id":504,"order_date":"2026-06-30","amount":"35.50","status":"completed","coupon_code":"WELCOME10"}',
# MAGIC     'arrival_001'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE copy_into_checkpoint_day12 (
# MAGIC   file_path STRING,
# MAGIC   loaded_at TIMESTAMP,
# MAGIC   load_run_id STRING,
# MAGIC   target_table STRING,
# MAGIC   load_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_checkpoint_day12 (
# MAGIC   file_path STRING,
# MAGIC   loaded_at TIMESTAMP,
# MAGIC   load_run_id STRING,
# MAGIC   target_table STRING,
# MAGIC   load_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_bronze_copy_day12 (
# MAGIC   event_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   _rescued_data STRING,
# MAGIC   _ingested_at TIMESTAMP,
# MAGIC   _ingest_run_id STRING,
# MAGIC   _ingest_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_bronze_autoloader_day12 (
# MAGIC   event_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   _rescued_data STRING,
# MAGIC   _ingested_at TIMESTAMP,
# MAGIC   _ingest_run_id STRING,
# MAGIC   _ingest_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_quarantine_day12 (
# MAGIC   ingestion_method STRING,
# MAGIC   event_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   raw_payload STRING,
# MAGIC   rescued_data STRING,
# MAGIC   quarantined_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT arrival_group, COUNT(*) AS file_count
# MAGIC FROM landing_order_files_day12
# MAGIC GROUP BY arrival_group
# MAGIC ORDER BY arrival_group;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `arrival_001` has 4 simulated files.
# MAGIC - One file has a bad decimal value.
# MAGIC - One file has an unexpected `coupon_code` field.
# MAGIC
# MAGIC Operational meaning: the landing area is the evidence layer. The checkpoint records which files have already been processed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Run A COPY INTO-Style Batch Load
# MAGIC
# MAGIC Purpose: practice scheduled file ingestion where reruns should not reload the same file.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reference only: when you have a Unity Catalog volume or cloud path, the real command looks like this.
# MAGIC -- COPY INTO de_learning.orders_bronze_copy_day12
# MAGIC -- FROM '/Volumes/<catalog>/<schema>/<volume>/orders/'
# MAGIC -- FILEFORMAT = JSON
# MAGIC -- FORMAT_OPTIONS ('multiLine' = 'false')
# MAGIC -- COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_candidates_raw_day12 AS
# MAGIC SELECT
# MAGIC   l.file_path AS source_file_path,
# MAGIC   l.file_mod_time,
# MAGIC   l.payload,
# MAGIC   get_json_object(l.payload, '$.event_id') AS event_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_id') AS INT) AS order_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.customer_id') AS INT) AS customer_id,
# MAGIC   try_cast(get_json_object(l.payload, '$.order_date') AS DATE) AS order_date,
# MAGIC   try_cast(get_json_object(l.payload, '$.amount') AS DECIMAL(10,2)) AS amount,
# MAGIC   get_json_object(l.payload, '$.status') AS status,
# MAGIC   get_json_object(l.payload, '$.coupon_code') AS coupon_code
# MAGIC FROM landing_order_files_day12 l
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM copy_into_checkpoint_day12 c
# MAGIC   WHERE c.file_path = l.file_path
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW copy_candidates_parsed_day12 AS
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status,
# MAGIC   CASE
# MAGIC     WHEN coupon_code IS NOT NULL THEN to_json(named_struct('coupon_code', coupon_code))
# MAGIC     ELSE NULL
# MAGIC   END AS _rescued_data,
# MAGIC   current_timestamp() AS _ingested_at,
# MAGIC   'copy-batch-001' AS _ingest_run_id,
# MAGIC   CASE
# MAGIC     WHEN event_id IS NULL OR order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR amount IS NULL
# MAGIC       THEN 'QUARANTINE'
# MAGIC     WHEN coupon_code IS NOT NULL THEN 'ACCEPTED_WITH_RESCUE'
# MAGIC     ELSE 'ACCEPTED'
# MAGIC   END AS _ingest_status,
# MAGIC   payload
# MAGIC FROM copy_candidates_raw_day12;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_bronze_copy_day12
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status,
# MAGIC   _rescued_data,
# MAGIC   _ingested_at,
# MAGIC   _ingest_run_id,
# MAGIC   _ingest_status
# MAGIC FROM copy_candidates_parsed_day12;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day12
# MAGIC SELECT
# MAGIC   'COPY_INTO_SIMULATED' AS ingestion_method,
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   'required parse failed' AS quarantine_reason,
# MAGIC   payload AS raw_payload,
# MAGIC   _rescued_data AS rescued_data,
# MAGIC   current_timestamp() AS quarantined_at
# MAGIC FROM copy_candidates_parsed_day12
# MAGIC WHERE _ingest_status = 'QUARANTINE';

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO copy_into_checkpoint_day12 t
# MAGIC USING (
# MAGIC   SELECT DISTINCT
# MAGIC     source_file_path AS file_path,
# MAGIC     current_timestamp() AS loaded_at,
# MAGIC     _ingest_run_id AS load_run_id,
# MAGIC     'orders_bronze_copy_day12' AS target_table,
# MAGIC     'LOADED' AS load_status
# MAGIC   FROM copy_candidates_parsed_day12
# MAGIC ) s
# MAGIC ON t.file_path = s.file_path
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (file_path, loaded_at, load_run_id, target_table, load_status)
# MAGIC   VALUES (s.file_path, s.loaded_at, s.load_run_id, s.target_table, s.load_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ingest_status, COUNT(*) AS row_count
# MAGIC FROM orders_bronze_copy_day12
# MAGIC GROUP BY _ingest_status
# MAGIC ORDER BY _ingest_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS files_not_yet_loaded_by_copy_style
# MAGIC FROM landing_order_files_day12 l
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM copy_into_checkpoint_day12 c
# MAGIC   WHERE c.file_path = l.file_path
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_bronze_copy_day12` has 4 rows.
# MAGIC - Status counts: 2 `ACCEPTED`, 1 `ACCEPTED_WITH_RESCUE`, 1 `QUARANTINE`.
# MAGIC - `files_not_yet_loaded_by_copy_style` is 0 immediately after checkpointing.
# MAGIC
# MAGIC Operational meaning: `COPY INTO`-style ingestion is file-idempotent. It protects reruns from reloading the same file, but it does not automatically decide whether a business event is valid.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Simulate A Second File Arrival
# MAGIC
# MAGIC Purpose: add new files, including a negative amount, a duplicate business event, and another schema-drift field.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO landing_order_files_day12 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_002/orders_005.json',
# MAGIC     TIMESTAMP'2026-07-01T06:10:00Z',
# MAGIC     '{"event_id":"evt-1205","order_id":1205,"customer_id":505,"order_date":"2026-07-01","amount":"500.00","status":"completed"}',
# MAGIC     'arrival_002'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_002/orders_006.json',
# MAGIC     TIMESTAMP'2026-07-01T06:11:00Z',
# MAGIC     '{"event_id":"evt-1206","order_id":1206,"customer_id":506,"order_date":"2026-07-01","amount":"-25.00","status":"pending"}',
# MAGIC     'arrival_002'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_002/orders_007.json',
# MAGIC     TIMESTAMP'2026-07-01T06:12:00Z',
# MAGIC     '{"event_id":"evt-1202","order_id":1202,"customer_id":502,"order_date":"2026-06-30","amount":"95.00","status":"pending"}',
# MAGIC     'arrival_002'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day12/batch_002/orders_008.json',
# MAGIC     TIMESTAMP'2026-07-01T06:13:00Z',
# MAGIC     '{"event_id":"evt-1208","order_id":1208,"customer_id":508,"order_date":"2026-07-01","amount":"60.00","status":"completed","coupon_code":"JULY10","source_system":"mobile"}',
# MAGIC     'arrival_002'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT arrival_group, COUNT(*) AS file_count
# MAGIC FROM landing_order_files_day12
# MAGIC GROUP BY arrival_group
# MAGIC ORDER BY arrival_group;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `arrival_001` has 4 files.
# MAGIC - `arrival_002` has 4 files.
# MAGIC
# MAGIC Operational meaning: ingestion pipelines should expect incremental arrivals, not one perfect batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Evaluate Auto Loader-Style New File Discovery With PySpark
# MAGIC
# MAGIC Purpose: use PySpark to find files not present in the Auto Loader checkpoint, parse payloads, and classify rows.

# COMMAND ----------

from pyspark.sql import functions as F

landing_df = spark.table("de_learning.landing_order_files_day12")
checkpoint_df = spark.table("de_learning.autoloader_checkpoint_day12")

new_files_df = (
    landing_df
    .join(checkpoint_df.select("file_path"), on="file_path", how="left_anti")
)

coupon_col = F.get_json_object(F.col("payload"), "$.coupon_code")
source_system_col = F.get_json_object(F.col("payload"), "$.source_system")

parsed_df = (
    new_files_df
    .withColumn("event_id", F.get_json_object(F.col("payload"), "$.event_id"))
    .withColumn("order_id", F.get_json_object(F.col("payload"), "$.order_id").cast("int"))
    .withColumn("customer_id", F.get_json_object(F.col("payload"), "$.customer_id").cast("int"))
    .withColumn("order_date", F.to_date(F.get_json_object(F.col("payload"), "$.order_date")))
    .withColumn("amount", F.expr("try_cast(get_json_object(payload, '$.amount') as decimal(10,2))"))
    .withColumn("status", F.get_json_object(F.col("payload"), "$.status"))
    .withColumn(
        "_rescued_data",
        F.when(
            coupon_col.isNotNull() | source_system_col.isNotNull(),
            F.to_json(F.struct(
                coupon_col.alias("coupon_code"),
                source_system_col.alias("source_system")
            ))
        ).otherwise(F.lit(None).cast("string"))
    )
    .withColumn(
        "_ingest_status",
        F.when(
            F.col("event_id").isNull()
            | F.col("order_id").isNull()
            | F.col("customer_id").isNull()
            | F.col("order_date").isNull()
            | F.col("amount").isNull()
            | (F.col("amount") < F.lit(0)),
            F.lit("QUARANTINE")
        )
        .when(F.col("_rescued_data").isNotNull(), F.lit("ACCEPTED_WITH_RESCUE"))
        .otherwise(F.lit("ACCEPTED"))
    )
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_ingest_run_id", F.lit("autoloader-trigger-001"))
    .select(
        "event_id",
        F.col("file_path").alias("source_file_path"),
        "file_mod_time",
        "order_id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "_rescued_data",
        "_ingested_at",
        "_ingest_run_id",
        "_ingest_status",
        F.col("payload").alias("raw_payload")
    )
)

parsed_df.createOrReplaceTempView("autoloader_candidates_day12")
display(parsed_df.orderBy("source_file_path"))

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - `landing_df` represents all files currently visible in the landing area.
# MAGIC - `checkpoint_df` represents files already discovered by the Auto Loader-style process.
# MAGIC - `join(..., how="left_anti")` means "keep landing rows that do not have a matching checkpoint row." SQL equivalent: `WHERE NOT EXISTS (...)`.
# MAGIC - `withColumn(...)` adds parsed or derived columns. SQL equivalent: expressions in a `SELECT` list.
# MAGIC - `F.get_json_object(...)` extracts a JSON field from the payload string.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - `createOrReplaceTempView(...)` exposes the PySpark result back to SQL as `autoloader_candidates_day12`.
# MAGIC - PySpark is lazily evaluated; the work actually runs when `display(...)` asks for results.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ingest_status, COUNT(*) AS row_count
# MAGIC FROM autoloader_candidates_day12
# MAGIC GROUP BY _ingest_status
# MAGIC ORDER BY _ingest_status;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 8 Auto Loader-style candidates, because its checkpoint started empty.
# MAGIC - Expected status counts: 4 `ACCEPTED`, 2 `ACCEPTED_WITH_RESCUE`, 2 `QUARANTINE`.
# MAGIC
# MAGIC Operational meaning: Auto Loader-style discovery tracks files independently from the SQL batch checkpoint. If you start a new ingestion mechanism with an empty checkpoint, it sees all available files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Persist Auto Loader-Style Results And Advance The Checkpoint
# MAGIC
# MAGIC Purpose: write candidate rows to bronze, record quarantine evidence, and mark files as processed.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_bronze_autoloader_day12
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   file_mod_time,
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status,
# MAGIC   _rescued_data,
# MAGIC   _ingested_at,
# MAGIC   _ingest_run_id,
# MAGIC   _ingest_status
# MAGIC FROM autoloader_candidates_day12;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day12
# MAGIC SELECT
# MAGIC   'AUTO_LOADER_SIMULATED' AS ingestion_method,
# MAGIC   event_id,
# MAGIC   source_file_path,
# MAGIC   CASE
# MAGIC     WHEN amount IS NULL THEN 'amount parse failed'
# MAGIC     WHEN amount < 0 THEN 'amount was negative'
# MAGIC     ELSE 'required parse failed'
# MAGIC   END AS quarantine_reason,
# MAGIC   raw_payload,
# MAGIC   _rescued_data AS rescued_data,
# MAGIC   current_timestamp() AS quarantined_at
# MAGIC FROM autoloader_candidates_day12
# MAGIC WHERE _ingest_status = 'QUARANTINE';

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO autoloader_checkpoint_day12 t
# MAGIC USING (
# MAGIC   SELECT DISTINCT
# MAGIC     source_file_path AS file_path,
# MAGIC     current_timestamp() AS loaded_at,
# MAGIC     _ingest_run_id AS load_run_id,
# MAGIC     'orders_bronze_autoloader_day12' AS target_table,
# MAGIC     'LOADED' AS load_status
# MAGIC   FROM autoloader_candidates_day12
# MAGIC ) s
# MAGIC ON t.file_path = s.file_path
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (file_path, loaded_at, load_run_id, target_table, load_status)
# MAGIC   VALUES (s.file_path, s.loaded_at, s.load_run_id, s.target_table, s.load_status);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _ingest_status, COUNT(*) AS row_count
# MAGIC FROM orders_bronze_autoloader_day12
# MAGIC GROUP BY _ingest_status
# MAGIC ORDER BY _ingest_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS files_not_yet_discovered_by_autoloader_style
# MAGIC FROM landing_order_files_day12 l
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1
# MAGIC   FROM autoloader_checkpoint_day12 c
# MAGIC   WHERE c.file_path = l.file_path
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_bronze_autoloader_day12` has 8 rows.
# MAGIC - `files_not_yet_discovered_by_autoloader_style` is 0 after checkpointing.
# MAGIC - `orders_quarantine_day12` now contains quarantine evidence from both ingestion methods.
# MAGIC
# MAGIC Operational meaning: a checkpoint is operational state. Deleting or changing it changes replay behavior.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Check File Idempotency Versus Business-Key Idempotency
# MAGIC
# MAGIC Purpose: prove that file-level dedupe does not automatically dedupe duplicate business events.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   event_id,
# MAGIC   COUNT(*) AS bronze_occurrences,
# MAGIC   collect_set(source_file_path) AS source_files
# MAGIC FROM orders_bronze_autoloader_day12
# MAGIC GROUP BY event_id
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY event_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_file_path,
# MAGIC   event_id,
# MAGIC   _ingest_status,
# MAGIC   _rescued_data
# MAGIC FROM orders_bronze_autoloader_day12
# MAGIC WHERE _rescued_data IS NOT NULL
# MAGIC ORDER BY source_file_path;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ingestion_method,
# MAGIC   quarantine_reason,
# MAGIC   COUNT(*) AS quarantine_count
# MAGIC FROM orders_quarantine_day12
# MAGIC GROUP BY ingestion_method, quarantine_reason
# MAGIC ORDER BY ingestion_method, quarantine_reason;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `evt-1202` appears twice in Auto Loader-style bronze because it arrived in two different files.
# MAGIC - Rows with `coupon_code` or `source_system` have `_rescued_data`.
# MAGIC - Quarantine evidence explains parse and negative-amount failures.
# MAGIC
# MAGIC Operational meaning: ingestion tools prevent duplicate file processing. You still need business-key dedupe and quality gates downstream.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Decide Which Ingestion Pattern Fits
# MAGIC
# MAGIC Purpose: build a small decision matrix for `COPY INTO`, Auto Loader, and Lakeflow Connect-style managed ingestion.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ingestion_method_decision_day12 (
# MAGIC   method_name STRING,
# MAGIC   best_for STRING,
# MAGIC   state_tracking STRING,
# MAGIC   schema_drift_handling STRING,
# MAGIC   operational_risk STRING,
# MAGIC   cost_performance_note STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ingestion_method_decision_day12 VALUES
# MAGIC   (
# MAGIC     'COPY INTO',
# MAGIC     'scheduled SQL ingestion from known file locations',
# MAGIC     'file load history on the target Delta table',
# MAGIC     'schema options during load; explicit validation still needed',
# MAGIC     'easy to confuse file idempotency with business-key idempotency',
# MAGIC     'simple operational model for periodic loads'
# MAGIC   ),
# MAGIC   (
# MAGIC     'Auto Loader',
# MAGIC     'continuous or high-scale file ingestion from cloud storage',
# MAGIC     'stream checkpoint plus schema location',
# MAGIC     'schema inference/evolution and rescued data patterns',
# MAGIC     'checkpoint loss or schema-mode changes can cause replay surprises',
# MAGIC     'scales file discovery better than repeatedly listing huge directories'
# MAGIC   ),
# MAGIC   (
# MAGIC     'Lakeflow Connect',
# MAGIC     'managed connector-based ingestion from supported sources',
# MAGIC     'managed pipeline/source state',
# MAGIC     'connector-dependent schema and CDC semantics',
# MAGIC     'less custom code, but you must understand connector guarantees',
# MAGIC     'managed path can reduce maintenance when the connector matches the source'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ingestion_method_decision_day12 ORDER BY method_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - A compact decision matrix for method selection.
# MAGIC
# MAGIC Operational meaning: ingestion design is a workload choice, not a preference. Frequency, scale, file count, schema drift, source type, and operational ownership drive the choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 8 - Final Operational Checks
# MAGIC
# MAGIC Purpose: create the checks you would want before calling ingestion healthy.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'landing_files' AS check_name, COUNT(*) AS observed_value FROM landing_order_files_day12
# MAGIC UNION ALL
# MAGIC SELECT 'copy_checkpoint_files', COUNT(*) FROM copy_into_checkpoint_day12
# MAGIC UNION ALL
# MAGIC SELECT 'autoloader_checkpoint_files', COUNT(*) FROM autoloader_checkpoint_day12
# MAGIC UNION ALL
# MAGIC SELECT 'copy_bronze_rows', COUNT(*) FROM orders_bronze_copy_day12
# MAGIC UNION ALL
# MAGIC SELECT 'autoloader_bronze_rows', COUNT(*) FROM orders_bronze_autoloader_day12
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', COUNT(*) FROM orders_quarantine_day12;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_bronze_autoloader_day12;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Landing files: 8.
# MAGIC - Copy checkpoint files: 4.
# MAGIC - Auto Loader-style checkpoint files: 8.
# MAGIC - Copy bronze rows: 4.
# MAGIC - Auto Loader-style bronze rows: 8.
# MAGIC - Quarantine rows: 3 total: 1 from copy-style load, 2 from Auto Loader-style load.
# MAGIC
# MAGIC Operational meaning: production ingestion needs file counts, checkpoint counts, rejected rows, rescued-data rows, duplicate business-key checks, and Delta history.
