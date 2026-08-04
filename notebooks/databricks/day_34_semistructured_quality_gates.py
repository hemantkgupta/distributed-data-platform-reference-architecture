# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Day 34 - Semi-Structured Quality Gates
# MAGIC
# MAGIC **Phase:** Days 26-40 ingestion and loading.
# MAGIC
# MAGIC **Associate mapping:** ingestion/loading, transformation/modeling, troubleshooting, and governance/security.
# MAGIC
# MAGIC **Professional extension:** production-grade rescued-data handling, corrupt-record quarantine, schema-hint policy, case-sensitive parsing, and nested publish gates.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 1 - Create nested JSON inputs and quality tables
# MAGIC
# MAGIC **Purpose:** Stage semi-structured order events that include valid records, additive drift, type mismatch, case mismatch, corrupt JSON, missing required fields, bad nested arrays, and PII drift.
# MAGIC
# MAGIC **Expected result:** Ten raw JSON payloads are available with Day 34-scoped bronze, silver, quarantine, schema-finding, command-template, and runbook tables.
# MAGIC
# MAGIC **Operational meaning:** Semi-structured ingestion needs raw evidence plus typed projections. The raw payload is audit evidence; the silver table is only for records that pass publish gates.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;
# MAGIC
# MAGIC DROP VIEW IF EXISTS semistructured_final_checks_day34;
# MAGIC DROP VIEW IF EXISTS publishable_bronze_items_day34;
# MAGIC DROP TABLE IF EXISTS semistructured_runbook_day34;
# MAGIC DROP TABLE IF EXISTS semistructured_command_templates_day34;
# MAGIC DROP TABLE IF EXISTS semistructured_schema_findings_day34;
# MAGIC DROP TABLE IF EXISTS orders_items_silver_day34;
# MAGIC DROP TABLE IF EXISTS orders_silver_day34;
# MAGIC DROP TABLE IF EXISTS orders_quarantine_day34;
# MAGIC DROP TABLE IF EXISTS semistructured_quality_decisions_day34;
# MAGIC DROP TABLE IF EXISTS orders_semistructured_bronze_day34;
# MAGIC DROP TABLE IF EXISTS orders_json_raw_day34;
# MAGIC DROP TABLE IF EXISTS semistructured_ingestion_policy_day34;
# MAGIC
# MAGIC CREATE TABLE semistructured_ingestion_policy_day34 (
# MAGIC   policy_name STRING,
# MAGIC   parser_mode STRING,
# MAGIC   schema_location STRING,
# MAGIC   schema_hint_policy STRING,
# MAGIC   rescued_data_column STRING,
# MAGIC   corrupt_record_column STRING,
# MAGIC   reader_case_sensitive BOOLEAN,
# MAGIC   publish_gate STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO semistructured_ingestion_policy_day34 VALUES
# MAGIC   (
# MAGIC     'orders_nested_json_day34',
# MAGIC     'Auto Loader or from_json with explicit schema plus rescued evidence',
# MAGIC     'dbfs:/schemas/de_learning/orders_nested_json_day34/',
# MAGIC     'Hint known numeric fields and required nested structs; review additive drift before contract update.',
# MAGIC     '_rescued_data',
# MAGIC     '_corrupt_record',
# MAGIC     true,
# MAGIC     'Publish only records with required business fields, no corrupt record, no PII rescue, and valid nested item quantities.'
# MAGIC   );
# MAGIC
# MAGIC CREATE TABLE orders_json_raw_day34 (
# MAGIC   source_file_path STRING,
# MAGIC   source_system STRING,
# MAGIC   file_mod_time TIMESTAMP,
# MAGIC   payload_quality STRING,
# MAGIC   payload STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO orders_json_raw_day34 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3401.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:00:00'),
# MAGIC     'valid',
# MAGIC     '{"event_id":"evt-3401","order_id":"ord-3401","amount":101.50,"customer":{"customer_id":"cust-1","email":"a@example.com"},"items":[{"sku":"sku-11","qty":1,"price":50.00},{"sku":"sku-12","qty":1,"price":51.50}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3402_loyalty.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:01:00'),
# MAGIC     'new_nested_customer_field',
# MAGIC     '{"event_id":"evt-3402","order_id":"ord-3402","amount":202.00,"customer":{"customer_id":"cust-2","email":"b@example.com","loyalty_tier":"gold"},"items":[{"sku":"sku-21","qty":2,"price":101.00}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3403_amount_type.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:02:00'),
# MAGIC     'amount_type_mismatch',
# MAGIC     '{"event_id":"evt-3403","order_id":"ord-3403","amount":"bad_amount","customer":{"customer_id":"cust-3","email":"c@example.com"},"items":[{"sku":"sku-31","qty":1,"price":303.00}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3404_case.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:03:00'),
# MAGIC     'case_mismatch',
# MAGIC     '{"Event_Id":"evt-3404","Order_Id":"ord-3404","amount":404.00,"Customer":{"customer_id":"cust-4","Email":"d@example.com"},"items":[{"sku":"sku-41","qty":1,"price":404.00}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3405_corrupt.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:04:00'),
# MAGIC     'corrupt_json',
# MAGIC     '{"event_id":"evt-3405","order_id":"ord-3405","amount":505.00,"customer":{"customer_id":"cust-5","email":"e@example.com"},"items":[{"sku":"sku-51","qty":1}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3406_missing_email.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:05:00'),
# MAGIC     'missing_required_email',
# MAGIC     '{"event_id":"evt-3406","order_id":"ord-3406","amount":606.00,"customer":{"customer_id":"cust-6"},"items":[{"sku":"sku-61","qty":1,"price":606.00}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3407_bad_item.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:06:00'),
# MAGIC     'negative_item_quantity',
# MAGIC     '{"event_id":"evt-3407","order_id":"ord-3407","amount":707.00,"customer":{"customer_id":"cust-7","email":"g@example.com"},"items":[{"sku":"sku-71","qty":-1,"price":707.00}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3408_pii.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:07:00'),
# MAGIC     'unexpected_pii',
# MAGIC     '{"event_id":"evt-3408","order_id":"ord-3408","amount":808.00,"customer":{"customer_id":"cust-8","email":"h@example.com","ssn":"123-45-6789"},"items":[{"sku":"sku-81","qty":1,"price":808.00}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3409_item_color.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:08:00'),
# MAGIC     'new_nested_item_field',
# MAGIC     '{"event_id":"evt-3409","order_id":"ord-3409","amount":909.00,"customer":{"customer_id":"cust-9","email":"i@example.com"},"items":[{"sku":"sku-91","qty":1,"price":909.00,"color":"blue"}]}'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day34/orders/orders_3410.json',
# MAGIC     'partner_orders_nested',
# MAGIC     timestamp('2026-08-04 05:09:00'),
# MAGIC     'valid',
# MAGIC     '{"event_id":"evt-3410","order_id":"ord-3410","amount":1010.00,"customer":{"customer_id":"cust-10","email":"j@example.com"},"items":[{"sku":"sku-101","qty":1,"price":510.00},{"sku":"sku-102","qty":1,"price":500.00}]}'
# MAGIC   );
# MAGIC
# MAGIC CREATE TABLE semistructured_quality_decisions_day34 (
# MAGIC   source_file_path STRING,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   customer_email STRING,
# MAGIC   payload_quality STRING,
# MAGIC   rescued_data_present BOOLEAN,
# MAGIC   corrupt_record_present BOOLEAN,
# MAGIC   bad_item_count BIGINT,
# MAGIC   decision_action STRING,
# MAGIC   publish_gate_reason STRING,
# MAGIC   decided_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_quarantine_day34 (
# MAGIC   source_file_path STRING,
# MAGIC   event_id STRING,
# MAGIC   order_id STRING,
# MAGIC   payload_quality STRING,
# MAGIC   quarantine_reason STRING,
# MAGIC   raw_payload STRING,
# MAGIC   rescued_data STRING,
# MAGIC   corrupt_record STRING,
# MAGIC   quarantined_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_silver_day34 (
# MAGIC   order_id STRING,
# MAGIC   event_id STRING,
# MAGIC   customer_email STRING,
# MAGIC   order_amount DECIMAL(10,2),
# MAGIC   source_file_path STRING,
# MAGIC   publish_mode STRING,
# MAGIC   rescued_data STRING,
# MAGIC   published_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE orders_items_silver_day34 (
# MAGIC   order_id STRING,
# MAGIC   event_id STRING,
# MAGIC   sku STRING,
# MAGIC   qty INT,
# MAGIC   price DECIMAL(10,2),
# MAGIC   source_file_path STRING,
# MAGIC   published_at TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC SELECT payload_quality, count(*) AS files
# MAGIC FROM orders_json_raw_day34
# MAGIC GROUP BY payload_quality
# MAGIC ORDER BY payload_quality;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 2 - Parse JSON into bronze with rescue evidence
# MAGIC
# MAGIC **Purpose:** Project typed fields from raw JSON while preserving simulated `_rescued_data`, `_corrupt_record`, and schema-finding evidence.
# MAGIC
# MAGIC **Expected result:** Ten bronze rows are created. Valid rows have typed fields, corrupt JSON has `_corrupt_record`, and drift rows have `_rescued_data`.
# MAGIC
# MAGIC **Operational meaning:** Bronze should not silently discard unexpected fields or malformed payloads. Rescue evidence lets operators decide whether to publish, quarantine, or update the contract.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE orders_semistructured_bronze_day34
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   source_file_path,
# MAGIC   source_system,
# MAGIC   file_mod_time,
# MAGIC   payload_quality,
# MAGIC   payload AS raw_payload,
# MAGIC   get_json_object(payload, '$.event_id') AS event_id,
# MAGIC   get_json_object(payload, '$.order_id') AS order_id,
# MAGIC   get_json_object(payload, '$.customer.customer_id') AS customer_id,
# MAGIC   get_json_object(payload, '$.customer.email') AS customer_email,
# MAGIC   try_cast(get_json_object(payload, '$.amount') AS DECIMAL(10,2)) AS order_amount,
# MAGIC   from_json(
# MAGIC     get_json_object(payload, '$.items'),
# MAGIC     'ARRAY<STRUCT<sku:STRING,qty:INT,price:DOUBLE>>'
# MAGIC   ) AS items,
# MAGIC   CASE
# MAGIC     WHEN payload_quality = 'new_nested_customer_field' THEN '{"customer":{"loyalty_tier":"gold"}}'
# MAGIC     WHEN payload_quality = 'amount_type_mismatch' THEN '{"amount":"bad_amount"}'
# MAGIC     WHEN payload_quality = 'case_mismatch' THEN '{"Event_Id":"evt-3404","Order_Id":"ord-3404","Customer":{"Email":"d@example.com"}}'
# MAGIC     WHEN payload_quality = 'unexpected_pii' THEN '{"customer":{"ssn":"123-45-6789"}}'
# MAGIC     WHEN payload_quality = 'new_nested_item_field' THEN '{"items":[{"color":"blue"}]}'
# MAGIC     ELSE NULL
# MAGIC   END AS _rescued_data,
# MAGIC   CASE
# MAGIC     WHEN payload_quality = 'corrupt_json' THEN payload
# MAGIC     ELSE NULL
# MAGIC   END AS _corrupt_record,
# MAGIC   CASE
# MAGIC     WHEN payload_quality = 'new_nested_customer_field' THEN array('NEW_NESTED_FIELD')
# MAGIC     WHEN payload_quality = 'amount_type_mismatch' THEN array('TYPE_MISMATCH')
# MAGIC     WHEN payload_quality = 'case_mismatch' THEN array('CASE_MISMATCH')
# MAGIC     WHEN payload_quality = 'corrupt_json' THEN array('CORRUPT_RECORD')
# MAGIC     WHEN payload_quality = 'missing_required_email' THEN array('MISSING_REQUIRED_FIELD')
# MAGIC     WHEN payload_quality = 'negative_item_quantity' THEN array('NESTED_ARRAY_QUALITY')
# MAGIC     WHEN payload_quality = 'unexpected_pii' THEN array('UNEXPECTED_PII')
# MAGIC     WHEN payload_quality = 'new_nested_item_field' THEN array('NEW_NESTED_ARRAY_FIELD')
# MAGIC     ELSE array('NONE')
# MAGIC   END AS simulated_findings,
# MAGIC   current_timestamp() AS parsed_at
# MAGIC FROM orders_json_raw_day34;
# MAGIC
# MAGIC SELECT
# MAGIC   payload_quality,
# MAGIC   count(*) AS bronze_rows,
# MAGIC   sum(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued_rows,
# MAGIC   sum(CASE WHEN _corrupt_record IS NOT NULL THEN 1 ELSE 0 END) AS corrupt_rows
# MAGIC FROM orders_semistructured_bronze_day34
# MAGIC GROUP BY payload_quality
# MAGIC ORDER BY payload_quality;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 3 - Score publish readiness with PySpark
# MAGIC
# MAGIC **Purpose:** Use DataFrame logic to join parsed rows with item-level quality checks and choose a publish or quarantine decision.
# MAGIC
# MAGIC **Expected result:** Four rows are publishable, six rows are quarantined, and additive drift is separated from hard data-quality failures.
# MAGIC
# MAGIC **Operational meaning:** Semi-structured feeds need automated gates that distinguish safe additive drift from corrupt, missing, PII, and nested-array failures.

# COMMAND ----------
from pyspark.sql import functions as F

bronze_df = spark.table("de_learning.orders_semistructured_bronze_day34")

item_df = bronze_df.select(
    "source_file_path",
    F.explode_outer(F.col("items")).alias("item"),
)

item_quality_df = (
    item_df
    .groupBy("source_file_path")
    .agg(
        F.sum(
            F.when(
                (F.col("item.qty").isNotNull()) & (F.col("item.qty") <= F.lit(0)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("bad_item_count"),
        F.sum(
            F.when(F.col("item.sku").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        ).alias("item_count"),
    )
)

decision_df = (
    bronze_df
    .join(item_quality_df, "source_file_path", "left")
    .withColumn("rescued_data_present", F.col("_rescued_data").isNotNull())
    .withColumn("corrupt_record_present", F.col("_corrupt_record").isNotNull())
    .withColumn(
        "has_additive_rescue",
        F.coalesce(F.col("_rescued_data").contains("loyalty_tier"), F.lit(False))
        | F.coalesce(F.col("_rescued_data").contains("color"), F.lit(False)),
    )
    .withColumn(
        "has_pii_rescue",
        F.coalesce(F.col("_rescued_data").contains("ssn"), F.lit(False)),
    )
    .withColumn(
        "decision_action",
        F.when(F.col("corrupt_record_present"), F.lit("QUARANTINE_CORRUPT_JSON"))
        .when(F.col("has_pii_rescue"), F.lit("QUARANTINE_PII_RESCUE"))
        .when(
            F.col("order_id").isNull()
            | F.col("customer_email").isNull()
            | F.col("order_amount").isNull(),
            F.lit("QUARANTINE_REQUIRED_FIELD"),
        )
        .when(F.col("bad_item_count") > F.lit(0), F.lit("QUARANTINE_ITEM_QUALITY"))
        .when(F.col("has_additive_rescue"), F.lit("PUBLISH_WITH_RESCUE_REVIEW"))
        .otherwise(F.lit("PUBLISH_SILVER")),
    )
    .withColumn(
        "publish_gate_reason",
        F.when(F.col("decision_action") == F.lit("QUARANTINE_CORRUPT_JSON"), F.lit("Malformed payload cannot be parsed safely."))
        .when(F.col("decision_action") == F.lit("QUARANTINE_PII_RESCUE"), F.lit("Unexpected PII appeared in rescued data."))
        .when(F.col("decision_action") == F.lit("QUARANTINE_REQUIRED_FIELD"), F.lit("Required order, customer, or amount field is missing after parse."))
        .when(F.col("decision_action") == F.lit("QUARANTINE_ITEM_QUALITY"), F.lit("Nested item array contains invalid quantities."))
        .when(F.col("decision_action") == F.lit("PUBLISH_WITH_RESCUE_REVIEW"), F.lit("Core contract is valid, but additive drift needs contract-owner review."))
        .otherwise(F.lit("Canonical fields and nested items pass publish gates.")),
    )
    .select(
        "source_file_path",
        "event_id",
        "order_id",
        "customer_email",
        "payload_quality",
        "rescued_data_present",
        "corrupt_record_present",
        "bad_item_count",
        "decision_action",
        "publish_gate_reason",
        F.current_timestamp().alias("decided_at"),
    )
)

(
    decision_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("de_learning.semistructured_quality_decisions_day34")
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
# MAGIC **DataFrame meaning:** `bronze_df` contains parsed JSON plus rescue evidence; `item_df` explodes nested items to one row per item; `decision_df` is one quality decision per source file.
# MAGIC
# MAGIC **SQL equivalent:** The PySpark block is a `LATERAL VIEW explode(items)` item check, grouped back by source file, then a `CASE WHEN` publish-gate decision.
# MAGIC
# MAGIC **Syntax notes:**
# MAGIC - `F.explode_outer(F.col("items"))` keeps rows even when the array is null, similar to SQL `explode_outer`.
# MAGIC - `groupBy(...).agg(...)` changes grain from one item row back to one source-file row.
# MAGIC - `withColumn` adds boolean rescue/corrupt flags and final decision text.
# MAGIC - Spark is lazy until `.write.saveAsTable(...)` and `display(...)` execute.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 4 - Publish safe rows and quarantine failures
# MAGIC
# MAGIC **Purpose:** Move publishable orders and nested items to silver while preserving rejected rows in quarantine.
# MAGIC
# MAGIC **Expected result:** Four silver orders, six silver item rows, and six quarantine rows.
# MAGIC
# MAGIC **Operational meaning:** Publication is a separate operation from ingestion. Bronze can keep raw evidence; silver should only expose records that satisfy the consumer contract.

# COMMAND ----------
# MAGIC %sql
# MAGIC INSERT INTO orders_quarantine_day34
# MAGIC SELECT
# MAGIC   b.source_file_path,
# MAGIC   b.event_id,
# MAGIC   b.order_id,
# MAGIC   b.payload_quality,
# MAGIC   d.publish_gate_reason AS quarantine_reason,
# MAGIC   b.raw_payload,
# MAGIC   b._rescued_data AS rescued_data,
# MAGIC   b._corrupt_record AS corrupt_record,
# MAGIC   current_timestamp() AS quarantined_at
# MAGIC FROM orders_semistructured_bronze_day34 b
# MAGIC INNER JOIN semistructured_quality_decisions_day34 d
# MAGIC   ON b.source_file_path = d.source_file_path
# MAGIC WHERE d.decision_action LIKE 'QUARANTINE%';
# MAGIC
# MAGIC INSERT INTO orders_silver_day34
# MAGIC SELECT
# MAGIC   b.order_id,
# MAGIC   b.event_id,
# MAGIC   b.customer_email,
# MAGIC   b.order_amount,
# MAGIC   b.source_file_path,
# MAGIC   d.decision_action AS publish_mode,
# MAGIC   b._rescued_data AS rescued_data,
# MAGIC   current_timestamp() AS published_at
# MAGIC FROM orders_semistructured_bronze_day34 b
# MAGIC INNER JOIN semistructured_quality_decisions_day34 d
# MAGIC   ON b.source_file_path = d.source_file_path
# MAGIC WHERE d.decision_action IN ('PUBLISH_SILVER', 'PUBLISH_WITH_RESCUE_REVIEW');
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW publishable_bronze_items_day34 AS
# MAGIC SELECT
# MAGIC   b.source_file_path,
# MAGIC   b.order_id,
# MAGIC   b.event_id,
# MAGIC   item.sku AS sku,
# MAGIC   item.qty AS qty,
# MAGIC   try_cast(item.price AS DECIMAL(10,2)) AS price
# MAGIC FROM orders_semistructured_bronze_day34 b
# MAGIC LATERAL VIEW explode(b.items) exploded_items AS item;
# MAGIC
# MAGIC INSERT INTO orders_items_silver_day34
# MAGIC SELECT
# MAGIC   i.order_id,
# MAGIC   i.event_id,
# MAGIC   i.sku,
# MAGIC   i.qty,
# MAGIC   i.price,
# MAGIC   i.source_file_path,
# MAGIC   current_timestamp() AS published_at
# MAGIC FROM publishable_bronze_items_day34 i
# MAGIC INNER JOIN semistructured_quality_decisions_day34 d
# MAGIC   ON i.source_file_path = d.source_file_path
# MAGIC WHERE d.decision_action IN ('PUBLISH_SILVER', 'PUBLISH_WITH_RESCUE_REVIEW');
# MAGIC
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM orders_silver_day34) AS silver_orders,
# MAGIC   (SELECT count(*) FROM orders_items_silver_day34) AS silver_items,
# MAGIC   (SELECT count(*) FROM orders_quarantine_day34) AS quarantined_files;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 5 - Store schema and quality findings
# MAGIC
# MAGIC **Purpose:** Convert parser and gate outcomes into reviewable finding rows for source owners and platform operators.
# MAGIC
# MAGIC **Expected result:** Eight schema or quality findings describe additive drift, type mismatch, case mismatch, corrupt JSON, missing required data, item-quality failure, and PII rescue.
# MAGIC
# MAGIC **Operational meaning:** Incidents should produce evidence that can drive a contract update, schema hint, producer fix, or governance escalation.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE semistructured_schema_findings_day34 (
# MAGIC   finding_id STRING,
# MAGIC   source_file_path STRING,
# MAGIC   finding_type STRING,
# MAGIC   field_path STRING,
# MAGIC   severity STRING,
# MAGIC   recommended_action STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO semistructured_schema_findings_day34 VALUES
# MAGIC   ('find-034-001', 'dbfs:/landing/day34/orders/orders_3402_loyalty.json', 'NEW_NESTED_FIELD', 'customer.loyalty_tier', 'LOW', 'Review with contract owner; add optional field only after consumers agree.'),
# MAGIC   ('find-034-002', 'dbfs:/landing/day34/orders/orders_3403_amount_type.json', 'TYPE_MISMATCH', 'amount', 'HIGH', 'Add schema hint and require producer to send numeric amount.'),
# MAGIC   ('find-034-003', 'dbfs:/landing/day34/orders/orders_3404_case.json', 'CASE_MISMATCH', 'Order_Id, Customer.Email', 'HIGH', 'Keep readerCaseSensitive true and require canonical field casing.'),
# MAGIC   ('find-034-004', 'dbfs:/landing/day34/orders/orders_3405_corrupt.json', 'CORRUPT_RECORD', '_corrupt_record', 'HIGH', 'Quarantine malformed JSON and ask producer to replay corrected file.'),
# MAGIC   ('find-034-005', 'dbfs:/landing/day34/orders/orders_3406_missing_email.json', 'MISSING_REQUIRED_FIELD', 'customer.email', 'HIGH', 'Reject from silver until required customer email is supplied.'),
# MAGIC   ('find-034-006', 'dbfs:/landing/day34/orders/orders_3407_bad_item.json', 'NESTED_ARRAY_QUALITY', 'items.qty', 'HIGH', 'Reject negative item quantities and repair source payload.'),
# MAGIC   ('find-034-007', 'dbfs:/landing/day34/orders/orders_3408_pii.json', 'UNEXPECTED_PII', 'customer.ssn', 'CRITICAL', 'Escalate to data governance and mask or delete unauthorized PII.'),
# MAGIC   ('find-034-008', 'dbfs:/landing/day34/orders/orders_3409_item_color.json', 'NEW_NESTED_ARRAY_FIELD', 'items.color', 'LOW', 'Review as optional additive drift before changing item schema.');
# MAGIC
# MAGIC SELECT severity, count(*) AS findings
# MAGIC FROM semistructured_schema_findings_day34
# MAGIC GROUP BY severity
# MAGIC ORDER BY severity;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 6 - Save command templates for production parsing
# MAGIC
# MAGIC **Purpose:** Store the Databricks option shapes that map this simulation to real Auto Loader, `from_json`, Variant, and nested projection work.
# MAGIC
# MAGIC **Expected result:** Eight templates cover schema location, schema hints, rescued data, corrupt records, case sensitivity, Lakeflow `from_json`, Variant storage, and array explosion.
# MAGIC
# MAGIC **Operational meaning:** A production incident should not be the first time operators see parser options or publish-gate SQL.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE semistructured_command_templates_day34 (
# MAGIC   template_name STRING,
# MAGIC   command_type STRING,
# MAGIC   command_text STRING,
# MAGIC   operator_note STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO semistructured_command_templates_day34 VALUES
# MAGIC   ('autoloader_schema_location', 'pyspark', 'spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", schema_location).load(source_path)', 'Each stream needs its own schema location for inference and evolution evidence.'),
# MAGIC   ('schema_hints_required_fields', 'pyspark', '.option("cloudFiles.schemaHints", "amount DECIMAL(10,2), customer STRUCT<customer_id:STRING,email:STRING>")', 'Use hints to keep expected fields typed while still preserving unexpected fields.'),
# MAGIC   ('rescued_data_column', 'pyspark', '.option("rescuedDataColumn", "_rescued_data")', 'Collect type mismatches, schema mismatches, and case mismatches for review.'),
# MAGIC   ('corrupt_record_column', 'pyspark', '.option("columnNameOfCorruptRecord", "_corrupt_record")', 'Capture malformed records instead of dropping evidence.'),
# MAGIC   ('reader_case_sensitive', 'pyspark', '.option("readerCaseSensitive", "true")', 'Case variants should be rescued when canonical field names are contractual.'),
# MAGIC   ('lakeflow_from_json_schema_evolution', 'sql', 'from_json(payload, NULL, map("schemaLocationKey", "orders_payload"))', 'Lakeflow pipelines can infer and evolve from_json schema with a unique schemaLocationKey.'),
# MAGIC   ('variant_storage_option', 'sql', 'SELECT parse_json(raw_payload) AS payload_variant FROM raw_orders', 'Use Variant when flexible semi-structured storage is more appropriate than JSON strings.'),
# MAGIC   ('nested_array_projection', 'sql', 'SELECT order_id, item.sku, item.qty FROM bronze LATERAL VIEW explode(items) exploded AS item', 'Flatten nested arrays only after row-level publish gates pass.');
# MAGIC
# MAGIC SELECT template_name, command_type, operator_note
# MAGIC FROM semistructured_command_templates_day34
# MAGIC ORDER BY template_name;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 7 - Run final checks and operator runbook
# MAGIC
# MAGIC **Purpose:** Validate the Day 34 quality-gate story and store a short operational checklist.
# MAGIC
# MAGIC **Expected result:** All final checks return `PASS`.
# MAGIC
# MAGIC **Operational meaning:** Semi-structured ingestion is production-grade when rescue evidence, corrupt records, nested checks, quarantine, and publish counts are all auditable.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE semistructured_runbook_day34 (
# MAGIC   step_number INT,
# MAGIC   runbook_step STRING,
# MAGIC   required_evidence STRING,
# MAGIC   done_criteria STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO semistructured_runbook_day34 VALUES
# MAGIC   (1, 'Preserve raw payloads.', 'Raw JSON, source file path, file modification time, and parser options.', 'Every rejected record can be inspected from raw evidence.'),
# MAGIC   (2, 'Parse into typed bronze plus rescue columns.', '_rescued_data, _corrupt_record, schema location, and schema hints.', 'Unexpected fields and malformed records are not silently discarded.'),
# MAGIC   (3, 'Apply publish gates.', 'Required field checks, PII checks, nested item checks, and rescued-data policy.', 'Silver contains only contract-safe records.'),
# MAGIC   (4, 'Create source-owner findings.', 'Finding rows with field path, severity, and recommended action.', 'Producers know what to fix or negotiate.'),
# MAGIC   (5, 'Review contract change versus producer bug.', 'Decision table, schema findings, and command templates.', 'Additive drift is separated from hard failures and governance incidents.');
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW semistructured_final_checks_day34 AS
# MAGIC SELECT 'raw_payloads' AS check_name, count(*) AS actual_count, 10 AS expected_count FROM orders_json_raw_day34
# MAGIC UNION ALL SELECT 'bronze_rows', count(*), 10 FROM orders_semistructured_bronze_day34
# MAGIC UNION ALL SELECT 'decision_rows', count(*), 10 FROM semistructured_quality_decisions_day34
# MAGIC UNION ALL SELECT 'publish_silver', count(*), 2 FROM semistructured_quality_decisions_day34 WHERE decision_action = 'PUBLISH_SILVER'
# MAGIC UNION ALL SELECT 'publish_with_rescue_review', count(*), 2 FROM semistructured_quality_decisions_day34 WHERE decision_action = 'PUBLISH_WITH_RESCUE_REVIEW'
# MAGIC UNION ALL SELECT 'quarantine_decisions', count(*), 6 FROM semistructured_quality_decisions_day34 WHERE decision_action LIKE 'QUARANTINE%'
# MAGIC UNION ALL SELECT 'silver_orders', count(*), 4 FROM orders_silver_day34
# MAGIC UNION ALL SELECT 'silver_items', count(*), 6 FROM orders_items_silver_day34
# MAGIC UNION ALL SELECT 'quarantine_rows', count(*), 6 FROM orders_quarantine_day34
# MAGIC UNION ALL SELECT 'schema_findings', count(*), 8 FROM semistructured_schema_findings_day34
# MAGIC UNION ALL SELECT 'command_templates', count(*), 8 FROM semistructured_command_templates_day34
# MAGIC UNION ALL SELECT 'runbook_steps', count(*), 5 FROM semistructured_runbook_day34;
# MAGIC
# MAGIC SELECT
# MAGIC   check_name,
# MAGIC   actual_count,
# MAGIC   expected_count,
# MAGIC   CASE WHEN actual_count = expected_count THEN 'PASS' ELSE 'FAIL' END AS check_status
# MAGIC FROM semistructured_final_checks_day34
# MAGIC ORDER BY check_name;
