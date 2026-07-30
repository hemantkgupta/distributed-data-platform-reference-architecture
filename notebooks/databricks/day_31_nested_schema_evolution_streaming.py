# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 31 - Nested Schema Evolution In Streaming Ingestion
# MAGIC
# MAGIC Goal: simulate Auto Loader schema evolution for nested JSON streams, including schema hints, `addNewColumns`, `addNewColumnsWithTypeWidening`, `rescue`, case-sensitive rescue behavior, and restart evidence.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: ingestion/loading, Auto Loader schema evolution, JSON parsing, Delta tables, troubleshooting, monitoring, and governance.
# MAGIC - Professional stretch: production restart behavior, schema-location evidence, type-widening rollout gates, rescued-data review, case-sensitive ingestion risk, and incident runbooks.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Streaming-Like Raw Batches And Schema Baseline
# MAGIC
# MAGIC Purpose: model a stream that starts with a stable nested JSON contract, then receives nested field additions, type widening, incompatible types, case mismatches, optional extras, and malformed JSON.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS nested_schema_final_checks_day31;
# MAGIC DROP TABLE IF EXISTS nested_schema_runbook_day31;
# MAGIC DROP TABLE IF EXISTS nested_schema_command_templates_day31;
# MAGIC DROP TABLE IF EXISTS nested_schema_restart_history_day31;
# MAGIC DROP TABLE IF EXISTS orders_schema_evolution_silver_day31;
# MAGIC DROP TABLE IF EXISTS orders_schema_evolution_quarantine_day31;
# MAGIC DROP TABLE IF EXISTS schema_evolution_decisions_day31;
# MAGIC DROP TABLE IF EXISTS schema_change_events_day31;
# MAGIC DROP TABLE IF EXISTS orders_schema_evolution_bronze_day31;
# MAGIC DROP TABLE IF EXISTS autoloader_schema_location_versions_day31;
# MAGIC DROP TABLE IF EXISTS autoloader_stream_config_day31;
# MAGIC DROP TABLE IF EXISTS orders_schema_evolution_raw_day31;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_stream_config_day31 (
# MAGIC   stream_id STRING,
# MAGIC   source_path STRING,
# MAGIC   checkpoint_location STRING,
# MAGIC   schema_location STRING,
# MAGIC   schema_evolution_mode STRING,
# MAGIC   rescued_data_column STRING,
# MAGIC   reader_case_sensitive BOOLEAN,
# MAGIC   schema_hints STRING,
# MAGIC   restart_policy STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_stream_config_day31 VALUES
# MAGIC   (
# MAGIC     'orders_nested_stream_day31',
# MAGIC     '/Volumes/main/de_learning/raw_orders/day31/',
# MAGIC     '/Volumes/main/de_learning/checkpoints/orders_nested_stream_day31/',
# MAGIC     '/Volumes/main/de_learning/schemas/orders_nested_stream_day31/',
# MAGIC     'addNewColumns',
# MAGIC     '_rescued_data',
# MAGIC     true,
# MAGIC     'order_id STRING, customer.customer_id STRING, customer.email STRING, customer.segment STRING, order_ts TIMESTAMP, pricing.subtotal DOUBLE, pricing.tax DOUBLE, items.element.quantity BIGINT',
# MAGIC     'Lakeflow Jobs task retry after UnknownFieldException'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE autoloader_schema_location_versions_day31 (
# MAGIC   schema_version INT,
# MAGIC   effective_batch_id STRING,
# MAGIC   schema_change_summary STRING,
# MAGIC   schema_fields STRING,
# MAGIC   operator_note STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO autoloader_schema_location_versions_day31 VALUES
# MAGIC   (
# MAGIC     1,
# MAGIC     'batch_001',
# MAGIC     'Baseline nested order contract.',
# MAGIC     'order_id, customer.customer_id, customer.email, customer.segment, order_ts, pricing.subtotal, pricing.tax, pricing.currency, shipping.address.city, shipping.address.state, items.element.sku, items.element.quantity, items.element.unit_price',
# MAGIC     'Initial schema stored under cloudFiles.schemaLocation.'
# MAGIC   ),
# MAGIC   (
# MAGIC     2,
# MAGIC     'batch_002',
# MAGIC     'Nested optional fields discovered and appended.',
# MAGIC     'v1 fields plus customer.marketing_opt_in, shipping.address.postal_code, items.element.fulfillment_center',
# MAGIC     'addNewColumns updates schema location, then stream restart resumes processing.'
# MAGIC   ),
# MAGIC   (
# MAGIC     3,
# MAGIC     'batch_003',
# MAGIC     'Quantity widened from INT expectation to BIGINT hint.',
# MAGIC     'v2 fields plus items.element.quantity as BIGINT',
# MAGIC     'Type widening requires runtime support or a predeclared schema hint.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_schema_evolution_raw_day31 (
# MAGIC   source_file_path STRING,
# MAGIC   ingest_batch_id STRING,
# MAGIC   discovered_at TIMESTAMP,
# MAGIC   payload STRING,
# MAGIC   scenario STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_schema_evolution_raw_day31 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_001/orders_3101.json',
# MAGIC     'batch_001',
# MAGIC     TIMESTAMP '2026-07-30 05:00:00',
# MAGIC     '{"order_id":"ord-3101","customer":{"customer_id":"cust-31","email":"cust31@example.com","segment":"retail"},"order_ts":"2026-07-30T04:59:00Z","pricing":{"subtotal":45.00,"tax":3.60,"currency":"USD"},"shipping":{"address":{"city":"Austin","state":"TX","country":"US"},"method":"GROUND"},"items":[{"sku":"sku-1","quantity":2,"unit_price":20.00},{"sku":"sku-2","quantity":1,"unit_price":5.00}]}',
# MAGIC     'baseline_v1'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_001/orders_3102.json',
# MAGIC     'batch_001',
# MAGIC     TIMESTAMP '2026-07-30 05:01:00',
# MAGIC     '{"order_id":"ord-3102","customer":{"customer_id":"cust-32","email":"cust32@example.com","segment":"business"},"order_ts":"2026-07-30T05:00:00Z","pricing":{"subtotal":80.00,"tax":6.40,"currency":"USD"},"shipping":{"address":{"city":"Denver","state":"CO","country":"US"},"method":"AIR"},"items":[{"sku":"sku-3","quantity":4,"unit_price":20.00}]}',
# MAGIC     'baseline_v1'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_002/orders_3103_nested_additions.json',
# MAGIC     'batch_002',
# MAGIC     TIMESTAMP '2026-07-30 05:05:00',
# MAGIC     '{"order_id":"ord-3103","customer":{"customer_id":"cust-33","email":"cust33@example.com","segment":"retail","marketing_opt_in":true},"order_ts":"2026-07-30T05:04:00Z","pricing":{"subtotal":27.00,"tax":2.16,"currency":"USD"},"shipping":{"address":{"city":"Seattle","state":"WA","country":"US","postal_code":"98101"},"method":"GROUND"},"items":[{"sku":"sku-4","quantity":1,"unit_price":27.00,"fulfillment_center":"fc-west"}]}',
# MAGIC     'new_nested_fields'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_003/orders_3104_quantity_widening.json',
# MAGIC     'batch_003',
# MAGIC     TIMESTAMP '2026-07-30 05:10:00',
# MAGIC     '{"order_id":"ord-3104","customer":{"customer_id":"cust-34","email":"cust34@example.com","segment":"business"},"order_ts":"2026-07-30T05:09:00Z","pricing":{"subtotal":3000000000.00,"tax":0.00,"currency":"USD"},"shipping":{"address":{"city":"Chicago","state":"IL","country":"US"},"method":"GROUND"},"items":[{"sku":"sku-bulk","quantity":3000000000,"unit_price":1.00}]}',
# MAGIC     'type_widening_quantity'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_004/orders_3105_bad_pricing.json',
# MAGIC     'batch_004',
# MAGIC     TIMESTAMP '2026-07-30 05:15:00',
# MAGIC     '{"order_id":"ord-3105","customer":{"customer_id":"cust-35","email":"cust35@example.com","segment":"retail"},"order_ts":"2026-07-30T05:14:00Z","pricing":{"subtotal":"manual-review","tax":1.20,"currency":"USD"},"shipping":{"address":{"city":"Boston","state":"MA","country":"US"},"method":"GROUND"},"items":[{"sku":"sku-5","quantity":1,"unit_price":15.00}]}',
# MAGIC     'incompatible_type_pricing'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_004/orders_3106_case_mismatch.json',
# MAGIC     'batch_004',
# MAGIC     TIMESTAMP '2026-07-30 05:16:00',
# MAGIC     '{"Order_Id":"ord-3106","customer":{"customer_id":"cust-36","email":"cust36@example.com","segment":"retail"},"order_ts":"2026-07-30T05:15:00Z","pricing":{"subtotal":11.00,"tax":0.88,"currency":"USD"},"shipping":{"address":{"city":"Portland","state":"OR","country":"US"},"method":"GROUND"},"items":[{"sku":"sku-6","quantity":1,"unit_price":11.00}]}',
# MAGIC     'case_mismatch_required_key'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_005/orders_3107_new_optional_shape.json',
# MAGIC     'batch_005',
# MAGIC     TIMESTAMP '2026-07-30 05:20:00',
# MAGIC     '{"order_id":"ord-3107","customer":{"customer_id":"cust-37","email":"cust37@example.com","segment":"retail"},"order_ts":"2026-07-30T05:19:00Z","pricing":{"subtotal":22.00,"tax":1.76,"currency":"USD"},"shipping":{"address":{"city":"Phoenix","state":"AZ","country":"US"},"method":"GROUND"},"items":[{"sku":"sku-7","quantity":2,"unit_price":11.00,"dimensions":{"height_cm":10,"width_cm":4}}],"gift_wrap":{"requested":true,"message":"happy birthday"}}',
# MAGIC     'new_top_level_and_nested_optional'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day31/orders/batch_005/orders_3108_corrupt.json',
# MAGIC     'batch_005',
# MAGIC     TIMESTAMP '2026-07-30 05:21:00',
# MAGIC     '{"order_id":"ord-3108","customer":{"customer_id":"cust-38","email":"bad@example.com"},"items":[{"sku":"sku-8","quantity":1}',
# MAGIC     'malformed_json'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT scenario, ingest_batch_id, source_file_path, length(payload) AS payload_length
# MAGIC FROM orders_schema_evolution_raw_day31
# MAGIC ORDER BY discovered_at;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Eight streaming-like raw records across five discovery batches.
# MAGIC - One stream configuration row and three schema-location snapshots.
# MAGIC
# MAGIC Operational meaning: schema evolution is only debuggable if the source file, batch, checkpoint, schema location, mode, and restart policy are explicit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Parse With Baseline Schema And Preserve Drift Evidence
# MAGIC
# MAGIC Purpose: parse the stream with a stable v1 schema while capturing nested additions, type mismatches, case mismatches, and malformed JSON as reviewable evidence.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_schema_evolution_bronze_day31 AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     source_file_path,
# MAGIC     ingest_batch_id,
# MAGIC     discovered_at,
# MAGIC     payload,
# MAGIC     scenario,
# MAGIC     from_json(
# MAGIC       payload,
# MAGIC       'order_id STRING,
# MAGIC        customer STRUCT<customer_id: STRING, email: STRING, segment: STRING>,
# MAGIC        order_ts TIMESTAMP,
# MAGIC        pricing STRUCT<subtotal: DOUBLE, tax: DOUBLE, currency: STRING>,
# MAGIC        shipping STRUCT<address: STRUCT<city: STRING, state: STRING, country: STRING>, method: STRING>,
# MAGIC        items ARRAY<STRUCT<sku: STRING, quantity: INT, unit_price: DOUBLE>>'
# MAGIC     ) AS order_doc,
# MAGIC     get_json_object(payload, '$.Order_Id') AS raw_case_order_id,
# MAGIC     get_json_object(payload, '$.customer.marketing_opt_in') AS raw_customer_marketing_opt_in,
# MAGIC     get_json_object(payload, '$.shipping.address.postal_code') AS raw_shipping_postal_code,
# MAGIC     get_json_object(payload, '$.items[0].fulfillment_center') AS raw_item_fulfillment_center,
# MAGIC     get_json_object(payload, '$.items[0].dimensions') AS raw_item_dimensions,
# MAGIC     get_json_object(payload, '$.gift_wrap') AS raw_gift_wrap,
# MAGIC     get_json_object(payload, '$.pricing.subtotal') AS raw_pricing_subtotal,
# MAGIC     try_cast(get_json_object(payload, '$.pricing.subtotal') AS DOUBLE) AS parsed_pricing_subtotal_from_raw,
# MAGIC     try_cast(get_json_object(payload, '$.items[0].quantity') AS BIGINT) AS raw_first_item_quantity_bigint
# MAGIC   FROM orders_schema_evolution_raw_day31
# MAGIC )
# MAGIC SELECT
# MAGIC   source_file_path,
# MAGIC   ingest_batch_id,
# MAGIC   discovered_at,
# MAGIC   payload,
# MAGIC   scenario,
# MAGIC   order_doc,
# MAGIC   CASE
# MAGIC     WHEN order_doc IS NULL THEN 'MALFORMED_JSON'
# MAGIC     WHEN order_doc.order_id IS NULL THEN 'MISSING_REQUIRED_KEY_AFTER_PARSE'
# MAGIC     ELSE 'PARSED_WITH_BASELINE_SCHEMA'
# MAGIC   END AS parse_status,
# MAGIC   CASE
# MAGIC     WHEN order_doc IS NULL
# MAGIC       OR raw_case_order_id IS NOT NULL
# MAGIC       OR raw_customer_marketing_opt_in IS NOT NULL
# MAGIC       OR raw_shipping_postal_code IS NOT NULL
# MAGIC       OR raw_item_fulfillment_center IS NOT NULL
# MAGIC       OR raw_item_dimensions IS NOT NULL
# MAGIC       OR raw_gift_wrap IS NOT NULL
# MAGIC       OR (raw_pricing_subtotal IS NOT NULL AND parsed_pricing_subtotal_from_raw IS NULL)
# MAGIC       OR raw_first_item_quantity_bigint > 2147483647
# MAGIC       THEN to_json(named_struct(
# MAGIC         'corrupt_record', CASE WHEN order_doc IS NULL THEN payload ELSE NULL END,
# MAGIC         'case_sensitive_order_id', raw_case_order_id,
# MAGIC         'customer_marketing_opt_in', raw_customer_marketing_opt_in,
# MAGIC         'shipping_postal_code', raw_shipping_postal_code,
# MAGIC         'item_fulfillment_center', raw_item_fulfillment_center,
# MAGIC         'item_dimensions', raw_item_dimensions,
# MAGIC         'gift_wrap', raw_gift_wrap,
# MAGIC         'raw_pricing_subtotal', CASE WHEN raw_pricing_subtotal IS NOT NULL AND parsed_pricing_subtotal_from_raw IS NULL THEN raw_pricing_subtotal ELSE NULL END,
# MAGIC         'raw_first_item_quantity_bigint', CASE WHEN raw_first_item_quantity_bigint > 2147483647 THEN raw_first_item_quantity_bigint ELSE NULL END
# MAGIC       ))
# MAGIC     ELSE NULL
# MAGIC   END AS rescued_data_simulated
# MAGIC FROM parsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   scenario,
# MAGIC   parse_status,
# MAGIC   order_doc.order_id AS order_id,
# MAGIC   order_doc.pricing.subtotal AS parsed_subtotal,
# MAGIC   order_doc.items[0].quantity AS parsed_first_quantity,
# MAGIC   rescued_data_simulated
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC ORDER BY discovered_at;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Seven records parse into a struct; the malformed record has `order_doc IS NULL`.
# MAGIC - Six records carry simulated rescued evidence for new fields, type mismatch, type widening, case mismatch, or corrupt JSON.
# MAGIC
# MAGIC Operational meaning: the parser can keep the stream moving only when drift is made visible and routed to the correct mode or review path.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Classify Schema Change Events
# MAGIC
# MAGIC Purpose: turn parser evidence into operational events: new nested fields, type-widening candidates, incompatible types, case mismatches, and corrupt records.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE schema_change_events_day31 AS
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'NEW_NESTED_FIELD' AS change_type,
# MAGIC        'customer.marketing_opt_in' AS field_path,
# MAGIC        'addNewColumns can append the nested field; stream restarts after UnknownFieldException.' AS operational_interpretation
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.customer.marketing_opt_in') IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'NEW_NESTED_FIELD',
# MAGIC        'shipping.address.postal_code',
# MAGIC        'addNewColumns can append the nested field; stream restarts after UnknownFieldException.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.shipping.address.postal_code') IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'NEW_NESTED_FIELD',
# MAGIC        'items.element.fulfillment_center',
# MAGIC        'Array element fields need explicit child-field tracking and test coverage.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.items[0].fulfillment_center') IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'TYPE_WIDENING_CANDIDATE',
# MAGIC        'items.element.quantity',
# MAGIC        'Quantity exceeds INT range; use addNewColumnsWithTypeWidening on supported runtimes or predeclare BIGINT schema hint.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE try_cast(get_json_object(payload, '$.items[0].quantity') AS BIGINT) > 2147483647
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'INCOMPATIBLE_TYPE',
# MAGIC        'pricing.subtotal',
# MAGIC        'Existing DOUBLE field arrived as a nonnumeric string; rescue and quarantine rather than widening.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.pricing.subtotal') IS NOT NULL
# MAGIC   AND try_cast(get_json_object(payload, '$.pricing.subtotal') AS DOUBLE) IS NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'CASE_MISMATCH',
# MAGIC        'Order_Id',
# MAGIC        'With case-sensitive rescue enabled, the casing variant is rescued and the required order_id remains missing.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.Order_Id') IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'NEW_TOP_LEVEL_FIELD',
# MAGIC        'gift_wrap',
# MAGIC        'Optional new top-level object should be added only after contract owner review.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.gift_wrap') IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'NEW_NESTED_FIELD',
# MAGIC        'items.element.dimensions',
# MAGIC        'Nested object inside array element should be modeled only if downstream consumers need it.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE get_json_object(payload, '$.items[0].dimensions') IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT source_file_path, ingest_batch_id, scenario,
# MAGIC        'CORRUPT_RECORD',
# MAGIC        '_corrupt_record',
# MAGIC        'Incomplete JSON belongs in bad-record or quarantine handling, not schema evolution.'
# MAGIC FROM orders_schema_evolution_bronze_day31
# MAGIC WHERE order_doc IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT change_type, field_path, scenario, operational_interpretation
# MAGIC FROM schema_change_events_day31
# MAGIC ORDER BY ingest_batch_id, source_file_path, field_path;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Nine schema-change events across six non-baseline records.
# MAGIC - New nested fields are distinct from incompatible types, type widening, case mismatches, and corrupt records.
# MAGIC
# MAGIC Operational meaning: treating all drift as one generic parser failure leads to the wrong fix. Each event needs a different mode, owner, and restart decision.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Decide Evolution Mode With PySpark
# MAGIC
# MAGIC Purpose: choose whether each file should process with current schema, restart after evolution, use type widening or hints, rescue and quarantine, or route to corrupt-record handling.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("de_learning.orders_schema_evolution_bronze_day31")
events_df = spark.table("de_learning.schema_change_events_day31")

event_rollup_df = (
    events_df
    .groupBy("source_file_path")
    .agg(
        F.collect_set("change_type").alias("change_types"),
        F.max(F.when(F.col("change_type").isin("NEW_NESTED_FIELD", "NEW_TOP_LEVEL_FIELD"), F.lit(1)).otherwise(F.lit(0))).alias("has_new_column_int"),
        F.max(F.when(F.col("change_type") == F.lit("TYPE_WIDENING_CANDIDATE"), F.lit(1)).otherwise(F.lit(0))).alias("has_type_widening_int"),
        F.max(F.when(F.col("change_type") == F.lit("INCOMPATIBLE_TYPE"), F.lit(1)).otherwise(F.lit(0))).alias("has_incompatible_type_int"),
        F.max(F.when(F.col("change_type") == F.lit("CASE_MISMATCH"), F.lit(1)).otherwise(F.lit(0))).alias("has_case_mismatch_int"),
        F.max(F.when(F.col("change_type") == F.lit("CORRUPT_RECORD"), F.lit(1)).otherwise(F.lit(0))).alias("has_corrupt_record_int"),
    )
)

decisions_df = (
    bronze_df
    .join(event_rollup_df, on="source_file_path", how="left")
    .withColumn(
        "change_types",
        F.when(F.col("change_types").isNull(), F.array(F.lit("NO_CHANGE"))).otherwise(F.col("change_types")),
    )
    .withColumn("has_new_column", F.coalesce(F.col("has_new_column_int"), F.lit(0)) == F.lit(1))
    .withColumn("has_type_widening", F.coalesce(F.col("has_type_widening_int"), F.lit(0)) == F.lit(1))
    .withColumn("has_incompatible_type", F.coalesce(F.col("has_incompatible_type_int"), F.lit(0)) == F.lit(1))
    .withColumn("has_case_mismatch", F.coalesce(F.col("has_case_mismatch_int"), F.lit(0)) == F.lit(1))
    .withColumn("has_corrupt_record", F.coalesce(F.col("has_corrupt_record_int"), F.lit(0)) == F.lit(1))
    .withColumn(
        "recommended_evolution_mode",
        F.when(F.col("has_corrupt_record"), F.lit("badRecordsPath_or_quarantine"))
        .when(F.col("has_incompatible_type") | F.col("has_case_mismatch"), F.lit("rescue"))
        .when(F.col("has_type_widening"), F.lit("addNewColumnsWithTypeWidening_or_schemaHints"))
        .when(F.col("has_new_column"), F.lit("addNewColumns"))
        .otherwise(F.lit("current_schema")),
    )
    .withColumn(
        "decision_action",
        F.when(F.col("has_corrupt_record"), F.lit("QUARANTINE_CORRUPT_RECORD"))
        .when(F.col("has_case_mismatch"), F.lit("QUARANTINE_CASE_MISMATCH_REQUIRED_KEY"))
        .when(F.col("has_incompatible_type"), F.lit("QUARANTINE_AND_RESCUE_TYPE_MISMATCH"))
        .when(F.col("has_type_widening"), F.lit("RESTART_WITH_TYPE_WIDENING_OR_SCHEMA_HINT"))
        .when(F.col("has_new_column"), F.lit("RESTART_AFTER_SCHEMA_EVOLUTION"))
        .otherwise(F.lit("PROCESS_WITH_CURRENT_SCHEMA")),
    )
    .withColumn(
        "restart_required",
        F.col("decision_action").isin("RESTART_AFTER_SCHEMA_EVOLUTION", "RESTART_WITH_TYPE_WIDENING_OR_SCHEMA_HINT"),
    )
    .withColumn(
        "publish_ready_after_restart",
        ~F.col("decision_action").isin(
            "QUARANTINE_CORRUPT_RECORD",
            "QUARANTINE_CASE_MISMATCH_REQUIRED_KEY",
            "QUARANTINE_AND_RESCUE_TYPE_MISMATCH",
        ),
    )
    .select(
        "source_file_path",
        "ingest_batch_id",
        "scenario",
        "parse_status",
        "change_types",
        "recommended_evolution_mode",
        "decision_action",
        "restart_required",
        "publish_ready_after_restart",
        "rescued_data_simulated",
    )
)

decisions_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "de_learning.schema_evolution_decisions_day31"
)

(
    decisions_df
    .where(~F.col("publish_ready_after_restart"))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("de_learning.orders_schema_evolution_quarantine_day31")
)

display(decisions_df.orderBy("ingest_batch_id", "source_file_path"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Eight decision rows, one per source file.
# MAGIC - Two baseline files process with the current schema.
# MAGIC - Three files require restart after schema evolution or type widening.
# MAGIC - Three files are quarantined for incompatible type, case mismatch, or corrupt JSON.
# MAGIC
# MAGIC Operational meaning: schema evolution should be automated only for compatible drift. Bad records and contract violations still need explicit quarantine.
# MAGIC
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - DataFrame: `bronze_df` is parsed file evidence; `events_df` is one row per detected schema-change event.
# MAGIC - SQL equivalent: aggregate events by file, then `CASE WHEN` on `has_new_column`, `has_type_widening`, `has_incompatible_type`, `has_case_mismatch`, and `has_corrupt_record`.
# MAGIC - `groupBy(...).agg(...)` creates one event summary per file.
# MAGIC - `collect_set(...)` keeps the distinct change types for operator review.
# MAGIC - `F.when(...).otherwise(...)` is PySpark's `CASE WHEN`.
# MAGIC - `write.saveAsTable(...)` is the action that persists the lazy DataFrame plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Simulate Restart History And Publishable Silver
# MAGIC
# MAGIC Purpose: model the evidence an operator expects after Auto Loader detects new columns, updates schema location, restarts, and publishes only approved records.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nested_schema_restart_history_day31 AS
# MAGIC SELECT 'run_001' AS run_id,
# MAGIC        'batch_001' AS batch_id,
# MAGIC        'addNewColumns' AS schema_evolution_mode,
# MAGIC        'SUCCESS' AS run_status,
# MAGIC        1 AS schema_version_before,
# MAGIC        1 AS schema_version_after,
# MAGIC        false AS restart_required,
# MAGIC        'Baseline schema processed without drift.' AS operator_evidence
# MAGIC UNION ALL
# MAGIC SELECT 'run_002',
# MAGIC        'batch_002',
# MAGIC        'addNewColumns',
# MAGIC        'FAILED_UNKNOWN_FIELD_EXCEPTION',
# MAGIC        1,
# MAGIC        2,
# MAGIC        true,
# MAGIC        'Auto Loader discovered nested additions and updated schema location before failing the stream.'
# MAGIC UNION ALL
# MAGIC SELECT 'run_003',
# MAGIC        'batch_002',
# MAGIC        'addNewColumns',
# MAGIC        'SUCCESS_AFTER_RESTART',
# MAGIC        2,
# MAGIC        2,
# MAGIC        false,
# MAGIC        'Lakeflow Jobs restart resumes processing using schema version 2.'
# MAGIC UNION ALL
# MAGIC SELECT 'run_004',
# MAGIC        'batch_003',
# MAGIC        'addNewColumnsWithTypeWidening',
# MAGIC        'SUCCESS_AFTER_TYPE_WIDENING_OR_HINT',
# MAGIC        2,
# MAGIC        3,
# MAGIC        true,
# MAGIC        'Quantity requires BIGINT support; use type widening on supported runtimes or a BIGINT schema hint.'
# MAGIC UNION ALL
# MAGIC SELECT 'run_005',
# MAGIC        'batch_004',
# MAGIC        'rescue',
# MAGIC        'SUCCESS_WITH_QUARANTINE',
# MAGIC        3,
# MAGIC        3,
# MAGIC        false,
# MAGIC        'Incompatible pricing type and case-mismatched required key are rescued and quarantined.'
# MAGIC UNION ALL
# MAGIC SELECT 'run_006',
# MAGIC        'batch_005',
# MAGIC        'addNewColumns_plus_badRecordsPath',
# MAGIC        'SUCCESS_WITH_ONE_QUARANTINE',
# MAGIC        3,
# MAGIC        3,
# MAGIC        false,
# MAGIC        'Optional new fields are reviewable, but corrupt JSON routes to bad-record handling.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_schema_evolution_silver_day31 AS
# MAGIC SELECT
# MAGIC   b.order_doc.order_id AS order_id,
# MAGIC   b.order_doc.customer.customer_id AS customer_id,
# MAGIC   b.order_doc.customer.email AS customer_email,
# MAGIC   b.order_doc.customer.segment AS customer_segment,
# MAGIC   b.order_doc.order_ts AS order_ts,
# MAGIC   coalesce(
# MAGIC     try_cast(get_json_object(b.payload, '$.pricing.subtotal') AS DOUBLE),
# MAGIC     b.order_doc.pricing.subtotal
# MAGIC   ) AS subtotal,
# MAGIC   b.order_doc.pricing.currency AS currency,
# MAGIC   try_cast(get_json_object(b.payload, '$.items[0].quantity') AS BIGINT) AS first_item_quantity,
# MAGIC   get_json_object(b.payload, '$.customer.marketing_opt_in') AS marketing_opt_in,
# MAGIC   get_json_object(b.payload, '$.shipping.address.postal_code') AS shipping_postal_code,
# MAGIC   get_json_object(b.payload, '$.gift_wrap') AS gift_wrap_json,
# MAGIC   d.recommended_evolution_mode,
# MAGIC   d.decision_action,
# MAGIC   b.source_file_path,
# MAGIC   b.ingest_batch_id
# MAGIC FROM orders_schema_evolution_bronze_day31 b
# MAGIC INNER JOIN schema_evolution_decisions_day31 d
# MAGIC   ON b.source_file_path = d.source_file_path
# MAGIC WHERE d.publish_ready_after_restart = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, subtotal, first_item_quantity, recommended_evolution_mode, decision_action
# MAGIC FROM orders_schema_evolution_silver_day31
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT run_id, batch_id, run_status, schema_version_before, schema_version_after, restart_required, operator_evidence
# MAGIC FROM nested_schema_restart_history_day31
# MAGIC ORDER BY run_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Five records are publishable after current-schema processing, schema restart, or type-widening/hint handling.
# MAGIC - Three records remain quarantined.
# MAGIC - Restart history shows the `UnknownFieldException` style stop, schema-location update, and successful retry.
# MAGIC
# MAGIC Operational meaning: a green silver table is not enough. Operators also need restart history to explain why the stream paused and why replay is safe.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Capture Command Templates
# MAGIC
# MAGIC Purpose: store deployable option shapes for Auto Loader schema evolution, rescue, hints, case-sensitive behavior, and Lakeflow `from_json` inference.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nested_schema_command_templates_day31 (
# MAGIC   template_name STRING,
# MAGIC   command_shape STRING,
# MAGIC   when_to_use STRING,
# MAGIC   operational_meaning STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO nested_schema_command_templates_day31 VALUES
# MAGIC   (
# MAGIC     'auto_loader_add_new_columns',
# MAGIC     '.option("cloudFiles.schemaLocation", schema_path).option("cloudFiles.schemaEvolutionMode", "addNewColumns").option("rescuedDataColumn", "_rescued_data")',
# MAGIC     'Default inferred-schema mode for compatible new columns.',
# MAGIC     'Stream fails after schema update, then restart resumes with the appended columns.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'auto_loader_rescue_mode',
# MAGIC     '.option("cloudFiles.schemaEvolutionMode", "rescue").option("rescuedDataColumn", "_rescued_data")',
# MAGIC     'Use when schema drift should not automatically change the published schema.',
# MAGIC     'New or mismatched fields stay reviewable without forcing schema evolution.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'auto_loader_type_widening',
# MAGIC     '.option("cloudFiles.schemaEvolutionMode", "addNewColumnsWithTypeWidening")',
# MAGIC     'Use on supported runtimes when widening such as INT to BIGINT is acceptable.',
# MAGIC     'Requires rollout gates because automatic type widening changes downstream expectations.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'schema_hints_nested_field',
# MAGIC     '.option("cloudFiles.schemaHints", "items.element.quantity BIGINT, customer.marketing_opt_in BOOLEAN")',
# MAGIC     'Predeclare expected nested field types while still letting Auto Loader infer the rest.',
# MAGIC     'Avoids unstable first-sample inference for known contract fields.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'case_sensitive_rescue_control',
# MAGIC     '.option("readerCaseSensitive", "false")',
# MAGIC     'Use only when the source contract explicitly allows case-insensitive field names.',
# MAGIC     'Can reduce case-mismatch rescue, but may hide producer contract bugs.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'lakeflow_from_json_schema_key',
# MAGIC     'from_json(value, NULL, map("schemaLocationKey", "orders_payload_v1"))',
# MAGIC     'Use in Lakeflow pipelines when automatic JSON blob schema inference is approved.',
# MAGIC     'Each from_json expression needs a unique schemaLocationKey.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'bad_records_path',
# MAGIC     '.option("badRecordsPath", bad_records_path)',
# MAGIC     'Route incomplete or malformed JSON records outside normal schema evolution.',
# MAGIC     'Separates corrupt input from compatible schema drift.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT template_name, when_to_use, operational_meaning
# MAGIC FROM nested_schema_command_templates_day31
# MAGIC ORDER BY template_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Seven command templates cover schema location, evolution modes, rescue, type widening, hints, case behavior, Lakeflow inference, and bad records.
# MAGIC
# MAGIC Operational meaning: schema-evolution behavior belongs in deployment review, not as ad hoc notebook options.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Checks And Operator Runbook
# MAGIC
# MAGIC Purpose: validate day-scoped artifacts and capture a practical runbook for schema-evolution incidents.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nested_schema_runbook_day31 AS
# MAGIC SELECT '1_pin_schema_location' AS step_id,
# MAGIC        'Give every Auto Loader stream its own checkpoint and schema location.' AS operator_action,
# MAGIC        'autoloader_stream_config_day31' AS evidence_table,
# MAGIC        'Stream config names checkpointLocation and cloudFiles.schemaLocation.' AS pass_condition
# MAGIC UNION ALL
# MAGIC SELECT '2_classify_drift',
# MAGIC        'Separate new fields, type widening, incompatible types, case mismatch, and corrupt records.',
# MAGIC        'schema_change_events_day31',
# MAGIC        'Every drift event has a type and field path.'
# MAGIC UNION ALL
# MAGIC SELECT '3_restart_safely',
# MAGIC        'For addNewColumns, restart only after schema location shows the new schema version.',
# MAGIC        'nested_schema_restart_history_day31',
# MAGIC        'UnknownFieldException and retry evidence are visible.'
# MAGIC UNION ALL
# MAGIC SELECT '4_quarantine_contract_breaks',
# MAGIC        'Rescue and quarantine incompatible types, required-key case mismatch, and corrupt JSON.',
# MAGIC        'orders_schema_evolution_quarantine_day31',
# MAGIC        'Blocked records include rescued evidence.'
# MAGIC UNION ALL
# MAGIC SELECT '5_promote_after_evidence',
# MAGIC        'Publish only rows that are current-schema valid or safe after restart or type-widening review.',
# MAGIC        'orders_schema_evolution_silver_day31',
# MAGIC        'Silver rows include decision action and source lineage.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW nested_schema_final_checks_day31 AS
# MAGIC SELECT 'raw_rows' AS metric, COUNT(*) AS observed_count, 8 AS expected_count FROM orders_schema_evolution_raw_day31
# MAGIC UNION ALL
# MAGIC SELECT 'stream_configs', COUNT(*), 1 FROM autoloader_stream_config_day31
# MAGIC UNION ALL
# MAGIC SELECT 'schema_versions', COUNT(*), 3 FROM autoloader_schema_location_versions_day31
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_rows', COUNT(*), 8 FROM orders_schema_evolution_bronze_day31
# MAGIC UNION ALL
# MAGIC SELECT 'parsed_struct_rows', COUNT(*), 7 FROM orders_schema_evolution_bronze_day31 WHERE order_doc IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'rescued_evidence_rows', COUNT(*), 6 FROM orders_schema_evolution_bronze_day31 WHERE rescued_data_simulated IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'schema_change_events', COUNT(*), 9 FROM schema_change_events_day31
# MAGIC UNION ALL
# MAGIC SELECT 'decision_rows', COUNT(*), 8 FROM schema_evolution_decisions_day31
# MAGIC UNION ALL
# MAGIC SELECT 'restart_required_rows', COUNT(*), 3 FROM schema_evolution_decisions_day31 WHERE restart_required = true
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', COUNT(*), 3 FROM orders_schema_evolution_quarantine_day31
# MAGIC UNION ALL
# MAGIC SELECT 'silver_rows', COUNT(*), 5 FROM orders_schema_evolution_silver_day31
# MAGIC UNION ALL
# MAGIC SELECT 'restart_history_rows', COUNT(*), 6 FROM nested_schema_restart_history_day31
# MAGIC UNION ALL
# MAGIC SELECT 'command_templates', COUNT(*), 7 FROM nested_schema_command_templates_day31
# MAGIC UNION ALL
# MAGIC SELECT 'runbook_steps', COUNT(*), 5 FROM nested_schema_runbook_day31;
# MAGIC
# MAGIC SELECT * FROM nested_schema_final_checks_day31 ORDER BY metric;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nested_schema_runbook_day31 ORDER BY step_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All final-check metrics match expected counts.
# MAGIC - The runbook covers schema location, drift classification, restart evidence, quarantine, and promotion.
# MAGIC
# MAGIC Operational meaning: production schema evolution is not just auto-merge. It is a controlled loop: detect, classify, update schema state, restart when safe, rescue when unsafe, and publish with evidence.
