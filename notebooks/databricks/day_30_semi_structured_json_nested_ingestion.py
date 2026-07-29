# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 30 - Semi-Structured JSON And Nested Ingestion
# MAGIC
# MAGIC Goal: parse nested JSON payloads into typed structs and arrays, preserve schema-drift evidence, flatten arrays safely, and publish only quality-approved silver/gold tables.
# MAGIC
# MAGIC Certification mapping:
# MAGIC
# MAGIC - Associate: ingestion/loading, JSON parsing, Delta table creation, transformation/modeling, troubleshooting malformed records, and governance-ready publication.
# MAGIC - Professional stretch: explicit schemas versus schema inference, nested quality gates, array explosion grain, rescued-data evidence, VARIANT tradeoffs, and production runbooks.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Raw Nested JSON Payloads
# MAGIC
# MAGIC Purpose: load a realistic raw landing table with valid nested JSON, schema drift, type mismatches, missing required keys, empty arrays, case mismatch, and malformed JSON.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS nested_json_final_checks_day30;
# MAGIC DROP TABLE IF EXISTS nested_json_runbook_day30;
# MAGIC DROP TABLE IF EXISTS nested_json_command_templates_day30;
# MAGIC DROP TABLE IF EXISTS nested_json_parser_decisions_day30;
# MAGIC DROP TABLE IF EXISTS sku_revenue_gold_day30;
# MAGIC DROP TABLE IF EXISTS orders_silver_day30;
# MAGIC DROP TABLE IF EXISTS orders_nested_quarantine_day30;
# MAGIC DROP TABLE IF EXISTS orders_nested_quality_day30;
# MAGIC DROP TABLE IF EXISTS order_events_silver_day30;
# MAGIC DROP TABLE IF EXISTS order_items_silver_day30;
# MAGIC DROP TABLE IF EXISTS orders_nested_bronze_day30;
# MAGIC DROP TABLE IF EXISTS orders_nested_raw_day30;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_nested_raw_day30 (
# MAGIC   source_file_path STRING,
# MAGIC   ingest_batch_id STRING,
# MAGIC   ingestion_ts TIMESTAMP,
# MAGIC   payload STRING,
# MAGIC   source_record_hint STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_nested_raw_day30 VALUES
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_001.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:00:00',
# MAGIC     '{"order_id":"ord-3001","customer":{"customer_id":"cust-1","email":"cust1@example.com","segment":"retail"},"order_ts":"2026-07-29T04:58:00Z","status":"PLACED","pricing":{"subtotal":56.00,"tax":4.48,"currency":"USD"},"shipping":{"address":{"city":"Austin","state":"TX","country":"US"},"method":"GROUND","priority":false},"items":[{"sku":"sku-1","quantity":2,"unit_price":20.00,"discount":0.00},{"sku":"sku-2","quantity":1,"unit_price":18.00,"discount":2.00}],"events":[{"event_type":"created","event_ts":"2026-07-29T04:58:00Z"},{"event_type":"paid","event_ts":"2026-07-29T04:59:00Z"}]}',
# MAGIC     'valid_two_items'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_002_bad_item.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:01:00',
# MAGIC     '{"order_id":"ord-3002","customer":{"customer_id":"cust-2","email":"cust2@example.com","segment":"retail"},"order_ts":"2026-07-29T05:00:00Z","status":"PLACED","pricing":{"subtotal":0.00,"tax":0.00,"currency":"USD"},"shipping":{"address":{"city":"Denver","state":"CO","country":"US"},"method":"GROUND","priority":false},"items":[{"sku":"sku-1","quantity":0,"unit_price":25.00,"discount":0.00}],"events":[{"event_type":"created","event_ts":"2026-07-29T05:00:00Z"}]}',
# MAGIC     'bad_item_quantity'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_003_bad_pricing.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:02:00',
# MAGIC     '{"order_id":"ord-3003","customer":{"customer_id":"cust-3","email":"cust3@example.com","segment":"business"},"order_ts":"2026-07-29T05:01:00Z","status":"PLACED","pricing":{"subtotal":"not-a-number","tax":3.20,"currency":"USD"},"shipping":{"address":{"city":"Seattle","state":"WA","country":"US"},"method":"AIR","priority":true},"items":[{"sku":"sku-3","quantity":1,"unit_price":40.00,"discount":0.00}],"events":[{"event_type":"created","event_ts":"2026-07-29T05:01:00Z"}]}',
# MAGIC     'type_mismatch_pricing'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_004_missing_customer.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:03:00',
# MAGIC     '{"order_id":"ord-3004","customer":{"email":"unknown@example.com","segment":"retail"},"order_ts":"2026-07-29T05:02:00Z","status":"PLACED","pricing":{"subtotal":31.00,"tax":2.48,"currency":"USD"},"shipping":{"address":{"city":"Chicago","state":"IL","country":"US"},"method":"GROUND","priority":false},"items":[{"sku":"sku-4","quantity":1,"unit_price":31.00,"discount":0.00}],"events":[{"event_type":"created","event_ts":"2026-07-29T05:02:00Z"}]}',
# MAGIC     'missing_customer_id'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_005_schema_drift.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:04:00',
# MAGIC     '{"order_id":"ord-3005","customer":{"customer_id":"cust-5","email":"cust5@example.com","segment":"retail"},"order_ts":"2026-07-29T05:03:00Z","status":"PLACED","pricing":{"subtotal":74.00,"tax":5.92,"currency":"USD"},"shipping":{"address":{"city":"Boston","state":"MA","country":"US"},"method":"GROUND","priority":false},"items":[{"sku":"sku-3","quantity":2,"unit_price":22.00,"discount":0.00},{"sku":"sku-4","quantity":1,"unit_price":30.00,"discount":0.00}],"events":[{"event_type":"created","event_ts":"2026-07-29T05:03:00Z"}],"loyalty_tier":"gold","coupon_code":"SAVE10"}',
# MAGIC     'new_optional_fields'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_006_malformed.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:05:00',
# MAGIC     '{"order_id":"ord-3006","customer":{"customer_id":"cust-6","email":"bad@example.com"},"items":[{"sku":"sku-9","quantity":1}',
# MAGIC     'malformed_json'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_007_empty_items.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:06:00',
# MAGIC     '{"order_id":"ord-3007","customer":{"customer_id":"cust-7","email":"cust7@example.com","segment":"retail"},"order_ts":"2026-07-29T05:06:00Z","status":"PLACED","pricing":{"subtotal":0.00,"tax":0.00,"currency":"USD"},"shipping":{"address":{"city":"Phoenix","state":"AZ","country":"US"},"method":"GROUND","priority":false},"items":[],"events":[{"event_type":"created","event_ts":"2026-07-29T05:06:00Z"}]}',
# MAGIC     'empty_items_array'
# MAGIC   ),
# MAGIC   (
# MAGIC     'dbfs:/landing/day30/orders/date=2026-07-29/orders_008_case_mismatch.json',
# MAGIC     'day30_batch_001',
# MAGIC     TIMESTAMP '2026-07-29 05:07:00',
# MAGIC     '{"Order_Id":"ord-3008","customer":{"customer_id":"cust-8","email":"cust8@example.com","segment":"retail"},"order_ts":"2026-07-29T05:07:00Z","status":"PLACED","pricing":{"subtotal":10.00,"tax":0.80,"currency":"USD"},"shipping":{"address":{"city":"Portland","state":"OR","country":"US"},"method":"GROUND","priority":false},"items":[{"sku":"sku-5","quantity":1,"unit_price":10.00,"discount":0.00}],"events":[{"event_type":"created","event_ts":"2026-07-29T05:07:00Z"}]}',
# MAGIC     'case_mismatch_order_id'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_record_hint, source_file_path, length(payload) AS payload_length
# MAGIC FROM orders_nested_raw_day30
# MAGIC ORDER BY ingestion_ts;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Eight raw payload rows.
# MAGIC - The raw table keeps the original string payload and file-level evidence.
# MAGIC
# MAGIC Operational meaning: bronze ingestion should preserve enough raw context to replay, quarantine, and explain parser decisions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Parse JSON With An Explicit Nested Schema
# MAGIC
# MAGIC Purpose: convert JSON strings into a typed nested struct while preserving selected schema-drift evidence outside the contract.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_nested_bronze_day30 AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT
# MAGIC     source_file_path,
# MAGIC     ingest_batch_id,
# MAGIC     ingestion_ts,
# MAGIC     payload,
# MAGIC     source_record_hint,
# MAGIC     from_json(
# MAGIC       payload,
# MAGIC       'order_id STRING,
# MAGIC        customer STRUCT<customer_id: STRING, email: STRING, segment: STRING>,
# MAGIC        order_ts TIMESTAMP,
# MAGIC        status STRING,
# MAGIC        pricing STRUCT<subtotal: DOUBLE, tax: DOUBLE, currency: STRING>,
# MAGIC        shipping STRUCT<address: STRUCT<city: STRING, state: STRING, country: STRING>, method: STRING, priority: BOOLEAN>,
# MAGIC        items ARRAY<STRUCT<sku: STRING, quantity: INT, unit_price: DOUBLE, discount: DOUBLE>>,
# MAGIC        events ARRAY<STRUCT<event_type: STRING, event_ts: TIMESTAMP>>'
# MAGIC     ) AS order_doc,
# MAGIC     get_json_object(payload, '$.loyalty_tier') AS raw_loyalty_tier,
# MAGIC     get_json_object(payload, '$.coupon_code') AS raw_coupon_code,
# MAGIC     get_json_object(payload, '$.Order_Id') AS raw_case_order_id
# MAGIC   FROM orders_nested_raw_day30
# MAGIC )
# MAGIC SELECT
# MAGIC   source_file_path,
# MAGIC   ingest_batch_id,
# MAGIC   ingestion_ts,
# MAGIC   payload,
# MAGIC   source_record_hint,
# MAGIC   order_doc,
# MAGIC   CASE
# MAGIC     WHEN order_doc IS NULL THEN 'MALFORMED_JSON'
# MAGIC     WHEN order_doc.order_id IS NULL THEN 'SCHEMA_MISMATCH_OR_MISSING_KEY'
# MAGIC     ELSE 'PARSED'
# MAGIC   END AS parse_status,
# MAGIC   CASE
# MAGIC     WHEN raw_loyalty_tier IS NOT NULL OR raw_coupon_code IS NOT NULL OR raw_case_order_id IS NOT NULL
# MAGIC       THEN to_json(named_struct(
# MAGIC         'loyalty_tier', raw_loyalty_tier,
# MAGIC         'coupon_code', raw_coupon_code,
# MAGIC         'case_sensitive_order_id', raw_case_order_id
# MAGIC       ))
# MAGIC     ELSE NULL
# MAGIC   END AS schema_drift_json
# MAGIC FROM parsed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   source_record_hint,
# MAGIC   parse_status,
# MAGIC   order_doc.order_id AS order_id,
# MAGIC   order_doc.customer.customer_id AS customer_id,
# MAGIC   size(order_doc.items) AS item_count,
# MAGIC   order_doc.pricing.subtotal AS pricing_subtotal,
# MAGIC   schema_drift_json
# MAGIC FROM orders_nested_bronze_day30
# MAGIC ORDER BY ingestion_ts;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Seven rows produce a struct; one malformed JSON row has `order_doc IS NULL`.
# MAGIC - Two rows carry schema drift evidence: new optional fields and case-mismatched `Order_Id`.
# MAGIC
# MAGIC Operational meaning: explicit schemas make downstream columns typed and predictable, but you need a deliberate place to store unexpected fields.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Flatten Arrays At The Correct Grain
# MAGIC
# MAGIC Purpose: explode nested arrays into item and event tables without losing the raw file lineage.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order_items_silver_day30 AS
# MAGIC SELECT
# MAGIC   b.order_doc.order_id AS order_id,
# MAGIC   b.order_doc.customer.customer_id AS customer_id,
# MAGIC   b.order_doc.order_ts AS order_ts,
# MAGIC   b.order_doc.status AS order_status,
# MAGIC   b.order_doc.pricing.currency AS currency,
# MAGIC   item.sku AS sku,
# MAGIC   item.quantity AS quantity,
# MAGIC   item.unit_price AS unit_price,
# MAGIC   item.discount AS discount,
# MAGIC   round((item.quantity * item.unit_price) - coalesce(item.discount, 0.0), 2) AS item_net_amount,
# MAGIC   b.source_file_path,
# MAGIC   b.ingest_batch_id,
# MAGIC   b.ingestion_ts
# MAGIC FROM orders_nested_bronze_day30 b
# MAGIC LATERAL VIEW explode(b.order_doc.items) item_rows AS item
# MAGIC WHERE b.order_doc IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order_events_silver_day30 AS
# MAGIC SELECT
# MAGIC   b.order_doc.order_id AS order_id,
# MAGIC   event.event_type AS event_type,
# MAGIC   event.event_ts AS event_ts,
# MAGIC   b.source_file_path,
# MAGIC   b.ingest_batch_id
# MAGIC FROM orders_nested_bronze_day30 b
# MAGIC LATERAL VIEW explode_outer(b.order_doc.events) event_rows AS event
# MAGIC WHERE b.order_doc IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, sku, quantity, unit_price, discount, item_net_amount, source_file_path
# MAGIC FROM order_items_silver_day30
# MAGIC ORDER BY order_id, sku;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Eight item-grain rows from the parsed payloads.
# MAGIC - Empty item arrays produce no item rows; malformed JSON produces no item rows.
# MAGIC
# MAGIC Operational meaning: the array explosion defines the silver table grain. If the grain is wrong, every downstream metric is wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Score Nested Quality With PySpark
# MAGIC
# MAGIC Purpose: create row-level quality evidence for parse failures, missing keys, empty arrays, bad item values, type mismatches, and subtotal reconciliation.

# COMMAND ----------

from pyspark.sql import functions as F

bronze_df = spark.table("de_learning.orders_nested_bronze_day30")

quality_df = (
    bronze_df
    .withColumn("order_id", F.col("order_doc.order_id"))
    .withColumn("customer_id", F.col("order_doc.customer.customer_id"))
    .withColumn("item_count", F.size(F.col("order_doc.items")))
    .withColumn("pricing_subtotal", F.col("order_doc.pricing.subtotal"))
    .withColumn(
        "computed_items_subtotal",
        F.expr(
            """
            aggregate(
              order_doc.items,
              CAST(0.0 AS DOUBLE),
              (acc, x) -> acc + (
                coalesce(x.quantity, 0) * coalesce(x.unit_price, 0.0) - coalesce(x.discount, 0.0)
              )
            )
            """
        ),
    )
    .withColumn(
        "has_bad_item",
        F.expr("exists(order_doc.items, x -> x.sku IS NULL OR x.quantity <= 0 OR x.unit_price <= 0)"),
    )
    .withColumn(
        "quality_status",
        F.when(F.col("parse_status") != F.lit("PARSED"), F.lit("QUARANTINE_PARSE"))
        .when(F.col("order_id").isNull() | F.col("customer_id").isNull(), F.lit("QUARANTINE_REQUIRED_KEYS"))
        .when(F.col("item_count") <= F.lit(0), F.lit("QUARANTINE_EMPTY_ITEMS"))
        .when(F.col("has_bad_item"), F.lit("QUARANTINE_BAD_ITEM"))
        .when(F.col("pricing_subtotal").isNull(), F.lit("QUARANTINE_BAD_PRICING"))
        .when(F.abs(F.col("pricing_subtotal") - F.col("computed_items_subtotal")) > F.lit(0.01), F.lit("REVIEW_TOTAL_MISMATCH"))
        .otherwise(F.lit("PASS")),
    )
    .withColumn(
        "quality_reason",
        F.when(F.col("quality_status") == F.lit("PASS"), F.lit("Typed nested payload passed parser and business checks."))
        .when(F.col("quality_status") == F.lit("QUARANTINE_PARSE"), F.lit("JSON could not be parsed or did not satisfy the explicit schema."))
        .when(F.col("quality_status") == F.lit("QUARANTINE_REQUIRED_KEYS"), F.lit("Required order or customer key is missing after parsing."))
        .when(F.col("quality_status") == F.lit("QUARANTINE_EMPTY_ITEMS"), F.lit("Order has no item rows to publish."))
        .when(F.col("quality_status") == F.lit("QUARANTINE_BAD_ITEM"), F.lit("At least one exploded item has null SKU, non-positive quantity, or non-positive price."))
        .when(F.col("quality_status") == F.lit("QUARANTINE_BAD_PRICING"), F.lit("Pricing subtotal could not be parsed as a numeric value."))
        .otherwise(F.lit("Parsed subtotal does not reconcile with item subtotal.")),
    )
    .select(
        "source_file_path",
        "ingest_batch_id",
        "ingestion_ts",
        "source_record_hint",
        "order_id",
        "customer_id",
        "parse_status",
        "item_count",
        "pricing_subtotal",
        "computed_items_subtotal",
        "schema_drift_json",
        "quality_status",
        "quality_reason",
    )
)

quality_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "de_learning.orders_nested_quality_day30"
)

(
    quality_df
    .where(F.col("quality_status") != F.lit("PASS"))
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("de_learning.orders_nested_quarantine_day30")
)

display(quality_df.orderBy("ingestion_ts"))

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Two rows pass quality checks.
# MAGIC - Six rows land in the quarantine table with a specific status and reason.
# MAGIC
# MAGIC Operational meaning: a production nested JSON parser must prove why each raw record is publishable, reviewable, or quarantined.
# MAGIC
# MAGIC PySpark Notes:
# MAGIC
# MAGIC - DataFrame: `bronze_df` represents the parsed bronze table; `quality_df` adds quality columns and persists evidence tables.
# MAGIC - SQL equivalent: this is a `SELECT` with `CASE WHEN`, nested-field access like `order_doc.customer.customer_id`, `aggregate(...)`, and `exists(...)`.
# MAGIC - `F.col("order_doc.order_id")` references a nested struct field.
# MAGIC - `withColumn(...)` adds derived quality fields without mutating the original DataFrame.
# MAGIC - `F.expr(...)` lets PySpark call Spark SQL higher-order functions such as `aggregate` and `exists`.
# MAGIC - Transformations are lazy until `write.saveAsTable(...)` or `display(...)` executes an action.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Publish Clean Silver And Gold Tables
# MAGIC
# MAGIC Purpose: publish only quality-approved rows and aggregate item-grain data without including quarantined records.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_day30 AS
# MAGIC SELECT
# MAGIC   q.order_id,
# MAGIC   q.customer_id,
# MAGIC   b.order_doc.customer.email AS customer_email,
# MAGIC   b.order_doc.customer.segment AS customer_segment,
# MAGIC   b.order_doc.order_ts AS order_ts,
# MAGIC   b.order_doc.status AS status,
# MAGIC   b.order_doc.pricing.currency AS currency,
# MAGIC   q.pricing_subtotal,
# MAGIC   q.computed_items_subtotal,
# MAGIC   b.order_doc.shipping.address.city AS shipping_city,
# MAGIC   b.order_doc.shipping.address.state AS shipping_state,
# MAGIC   b.order_doc.shipping.method AS shipping_method,
# MAGIC   q.schema_drift_json,
# MAGIC   q.source_file_path,
# MAGIC   q.ingest_batch_id,
# MAGIC   q.ingestion_ts
# MAGIC FROM orders_nested_quality_day30 q
# MAGIC INNER JOIN orders_nested_bronze_day30 b
# MAGIC   ON q.source_file_path = b.source_file_path
# MAGIC WHERE q.quality_status = 'PASS';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE sku_revenue_gold_day30 AS
# MAGIC SELECT
# MAGIC   i.sku,
# MAGIC   count(DISTINCT i.order_id) AS order_count,
# MAGIC   sum(i.quantity) AS units,
# MAGIC   round(sum(i.item_net_amount), 2) AS net_revenue,
# MAGIC   min(i.order_ts) AS first_order_ts,
# MAGIC   max(i.order_ts) AS last_order_ts
# MAGIC FROM order_items_silver_day30 i
# MAGIC INNER JOIN orders_silver_day30 o
# MAGIC   ON i.order_id = o.order_id
# MAGIC GROUP BY i.sku;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_day30 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sku_revenue_gold_day30 ORDER BY sku;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_silver_day30` has two publishable orders.
# MAGIC - `sku_revenue_gold_day30` has four SKU aggregates from the publishable orders only.
# MAGIC
# MAGIC Operational meaning: silver/gold tables should be downstream of quality evidence, not directly downstream of raw parsing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Capture Parser Strategy And Command Templates
# MAGIC
# MAGIC Purpose: make the production parser choice explicit for stable schemas, exploratory semi-structured payloads, and Lakeflow pipelines.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nested_json_parser_decisions_day30 AS
# MAGIC SELECT 'stable_order_contract' AS source_shape,
# MAGIC        'Use explicit from_json schema into structs and arrays.' AS parser_decision,
# MAGIC        'Best when the contract is known and downstream tables need typed columns and statistics.' AS when_to_use,
# MAGIC        'Store unexpected fields in schema_drift_json or Auto Loader _rescued_data.' AS drift_strategy,
# MAGIC        'Use for silver publication and BI-facing tables.' AS production_note
# MAGIC UNION ALL
# MAGIC SELECT 'frequent_unknown_payloads',
# MAGIC        'Use VARIANT for raw exploratory storage on supported runtimes, then project stable fields later.',
# MAGIC        'Best when nested shapes vary too often for early strict modeling.',
# MAGIC        'Promote only reviewed fields to typed silver columns.',
# MAGIC        'Do not cluster or partition directly by VARIANT fields; extract typed columns first.'
# MAGIC UNION ALL
# MAGIC SELECT 'pipeline_managed_json_blob',
# MAGIC        'Use Lakeflow from_json schema inference with schemaLocationKey when preview feature fit is acceptable.',
# MAGIC        'Best when Lakeflow owns schema tracking and restart behavior.',
# MAGIC        'Give each from_json expression a unique schemaLocationKey.',
# MAGIC        'Treat preview features as rollout-gated in production.'
# MAGIC UNION ALL
# MAGIC SELECT 'regulated_source',
# MAGIC        'Use explicit schema plus quarantine and raw retention.',
# MAGIC        'Best when auditability and reproducibility matter more than flexible ingestion.',
# MAGIC        'Keep raw payload, file path, parser status, and rescued evidence.',
# MAGIC        'Govern access to raw payloads because nested fields often contain PII.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nested_json_command_templates_day30 (
# MAGIC   template_name STRING,
# MAGIC   command_shape STRING,
# MAGIC   when_to_use STRING,
# MAGIC   operational_meaning STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO nested_json_command_templates_day30 VALUES
# MAGIC   (
# MAGIC     'auto_loader_json_with_rescue',
# MAGIC     '.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", schema_path).option("rescuedDataColumn", "_rescued_data").load(source_path)',
# MAGIC     'Ingest JSON files while preserving fields that do not match the inferred or hinted schema.',
# MAGIC     'Prevents silent data loss and gives operators reviewable rescued evidence.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'from_json_explicit_schema',
# MAGIC     'from_json(payload, "order_id STRING, customer STRUCT<customer_id: STRING>, items ARRAY<STRUCT<sku: STRING, quantity: INT>>")',
# MAGIC     'Parse known JSON contracts from raw string columns.',
# MAGIC     'Creates typed nested columns that downstream jobs can validate and optimize.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'lakeflow_from_json_inference',
# MAGIC     'from_json(value, NULL, map("schemaLocationKey", "orders_payload_v1"))',
# MAGIC     'Let Lakeflow pipelines infer and evolve a JSON blob schema when the feature is approved for the workload.',
# MAGIC     'Moves schema tracking into pipeline metadata and reduces manual parser maintenance.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'variant_exploration',
# MAGIC     'SELECT parse_json(payload) AS raw_variant FROM raw_json_table',
# MAGIC     'Explore highly variable JSON before the stable silver contract is known.',
# MAGIC     'Separates exploratory flexibility from governed typed publication.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'explode_items',
# MAGIC     'LATERAL VIEW explode(order_doc.items) item_rows AS item',
# MAGIC     'Flatten array-of-struct fields to item grain.',
# MAGIC     'Makes the row grain explicit before aggregation.'
# MAGIC   ),
# MAGIC   (
# MAGIC     'nested_quality_gate',
# MAGIC     'exists(order_doc.items, x -> x.quantity <= 0 OR x.unit_price <= 0)',
# MAGIC     'Detect invalid nested array elements before publication.',
# MAGIC     'Finds bad child records even when the parent JSON parses successfully.'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nested_json_parser_decisions_day30 ORDER BY source_shape;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT template_name, when_to_use, operational_meaning
# MAGIC FROM nested_json_command_templates_day30
# MAGIC ORDER BY template_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Four parser decisions and six command templates.
# MAGIC - The lab distinguishes explicit typed parsing, VARIANT exploration, Auto Loader rescue, and Lakeflow schema inference.
# MAGIC
# MAGIC Operational meaning: parser strategy is an architecture decision. It controls performance, governance, schema drift handling, and downstream modeling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Checks And Operator Runbook
# MAGIC
# MAGIC Purpose: validate all day-scoped artifacts and capture a runbook for nested JSON ingestion incidents.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nested_json_runbook_day30 AS
# MAGIC SELECT '1_preserve_raw' AS step_id,
# MAGIC        'Keep payload, source file, batch id, and ingestion timestamp in bronze.' AS operator_action,
# MAGIC        'orders_nested_raw_day30' AS evidence_table,
# MAGIC        'Every parsed row can be traced to source evidence.' AS pass_condition
# MAGIC UNION ALL
# MAGIC SELECT '2_parse_typed',
# MAGIC        'Use explicit from_json schemas for stable contracts and record schema drift separately.',
# MAGIC        'orders_nested_bronze_day30',
# MAGIC        'Parser status and schema_drift_json are populated.'
# MAGIC UNION ALL
# MAGIC SELECT '3_explode_at_grain',
# MAGIC        'Flatten arrays only into tables whose grain is clear, such as one row per order item.',
# MAGIC        'order_items_silver_day30',
# MAGIC        'Item rows include order id and file lineage.'
# MAGIC UNION ALL
# MAGIC SELECT '4_quarantine_before_publish',
# MAGIC        'Run nested quality gates before publishing silver and gold tables.',
# MAGIC        'orders_nested_quality_day30, orders_nested_quarantine_day30',
# MAGIC        'Only PASS rows reach orders_silver_day30.'
# MAGIC UNION ALL
# MAGIC SELECT '5_choose_parser_strategy',
# MAGIC        'Use explicit schema, VARIANT, or Lakeflow inference based on contract stability and operational maturity.',
# MAGIC        'nested_json_parser_decisions_day30',
# MAGIC        'The parser choice has an owner-readable rationale.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW nested_json_final_checks_day30 AS
# MAGIC SELECT 'raw_rows' AS metric, COUNT(*) AS observed_count, 8 AS expected_count FROM orders_nested_raw_day30
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_rows', COUNT(*), 8 FROM orders_nested_bronze_day30
# MAGIC UNION ALL
# MAGIC SELECT 'parsed_struct_rows', COUNT(*), 7 FROM orders_nested_bronze_day30 WHERE order_doc IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'malformed_rows', COUNT(*), 1 FROM orders_nested_bronze_day30 WHERE order_doc IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'schema_drift_rows', COUNT(*), 2 FROM orders_nested_bronze_day30 WHERE schema_drift_json IS NOT NULL
# MAGIC UNION ALL
# MAGIC SELECT 'item_rows', COUNT(*), 8 FROM order_items_silver_day30
# MAGIC UNION ALL
# MAGIC SELECT 'event_rows', COUNT(*), 8 FROM order_events_silver_day30
# MAGIC UNION ALL
# MAGIC SELECT 'quality_rows', COUNT(*), 8 FROM orders_nested_quality_day30
# MAGIC UNION ALL
# MAGIC SELECT 'quarantine_rows', COUNT(*), 6 FROM orders_nested_quarantine_day30
# MAGIC UNION ALL
# MAGIC SELECT 'silver_orders', COUNT(*), 2 FROM orders_silver_day30
# MAGIC UNION ALL
# MAGIC SELECT 'gold_skus', COUNT(*), 4 FROM sku_revenue_gold_day30
# MAGIC UNION ALL
# MAGIC SELECT 'parser_decisions', COUNT(*), 4 FROM nested_json_parser_decisions_day30
# MAGIC UNION ALL
# MAGIC SELECT 'command_templates', COUNT(*), 6 FROM nested_json_command_templates_day30
# MAGIC UNION ALL
# MAGIC SELECT 'runbook_steps', COUNT(*), 5 FROM nested_json_runbook_day30;
# MAGIC
# MAGIC SELECT * FROM nested_json_final_checks_day30 ORDER BY metric;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nested_json_runbook_day30 ORDER BY step_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - All final-check metrics match expected counts.
# MAGIC - The runbook covers raw preservation, typed parsing, array-grain modeling, quarantine gates, and parser strategy.
# MAGIC
# MAGIC Operational meaning: nested JSON ingestion is production-grade only when parsing, quality, grain, drift evidence, and publication gates are all explicit.
