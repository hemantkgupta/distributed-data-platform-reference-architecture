# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # Day 37 - PySpark Ingestion Validation Helpers
# MAGIC
# MAGIC **Phase:** Days 26-40 ingestion and loading.
# MAGIC
# MAGIC **Associate mapping:** ingestion/loading, transformation/modeling with PySpark and SQL, troubleshooting/monitoring, and governance/security.
# MAGIC
# MAGIC **Professional extension:** reusable DataFrame validation helpers, deterministic SQL reconciliation, quarantine evidence, duplicate detection, and production-quality publish gates.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 1 - Create raw ingestion records and rule tables
# MAGIC
# MAGIC **Purpose:** Stage a small ingestion batch with valid rows, missing keys, schema mismatch, bad numeric data, unsupported currency, stale event time, PII drift, and duplicate business keys.
# MAGIC
# MAGIC **Expected result:** Ten raw rows and eight helper-catalog rows are created.
# MAGIC
# MAGIC **Operational meaning:** A validation helper is useful only when every rule has a visible input, SQL equivalent, and operational action.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;
# MAGIC
# MAGIC DROP VIEW IF EXISTS ingestion_validation_final_checks_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_remediation_plan_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_validation_reconciliation_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_sql_expected_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_validation_failures_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_validation_summary_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_validation_decisions_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_helper_catalog_day37;
# MAGIC DROP TABLE IF EXISTS ingestion_raw_events_day37;
# MAGIC
# MAGIC CREATE TABLE ingestion_raw_events_day37 (
# MAGIC   source_file_path STRING,
# MAGIC   source_system STRING,
# MAGIC   ingestion_method STRING,
# MAGIC   event_id STRING,
# MAGIC   business_key STRING,
# MAGIC   event_ts TIMESTAMP,
# MAGIC   expected_schema_version INT,
# MAGIC   schema_version INT,
# MAGIC   amount_text STRING,
# MAGIC   currency STRING,
# MAGIC   customer_email STRING,
# MAGIC   item_count INT,
# MAGIC   raw_record_hash STRING,
# MAGIC   payload_json STRING,
# MAGIC   source_mod_time TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_raw_events_day37 VALUES
# MAGIC   ('dbfs:/landing/day37/orders/order_3701.json', 'orders_api', 'AUTO_LOADER', 'evt-3701', 'ord-3701', timestamp('2026-08-07 05:00:00'), 3, 3, '100.25', 'USD', 'a@example.com', 2, 'hash-3701', '{"order_id":"ord-3701","amount":100.25}', timestamp('2026-08-07 05:01:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3702.json', 'orders_api', 'AUTO_LOADER', 'evt-3702', 'ord-3702', timestamp('2026-08-07 05:01:00'), 3, 3, '205.00', 'INR', 'b@example.com', 1, 'hash-3702', '{"order_id":"ord-3702","amount":205.00}', timestamp('2026-08-07 05:02:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3703_missing_key.json', 'orders_api', 'AUTO_LOADER', 'evt-3703', NULL, timestamp('2026-08-07 05:02:00'), 3, 3, '301.00', 'USD', 'c@example.com', 1, 'hash-3703', '{"amount":301.00}', timestamp('2026-08-07 05:03:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3704_schema.json', 'orders_api', 'AUTO_LOADER', 'evt-3704', 'ord-3704', timestamp('2026-08-07 05:03:00'), 3, 2, '404.00', 'USD', 'd@example.com', 1, 'hash-3704', '{"order_id":"ord-3704","schema_version":2}', timestamp('2026-08-07 05:04:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3705_amount.json', 'orders_api', 'AUTO_LOADER', 'evt-3705', 'ord-3705', timestamp('2026-08-07 05:04:00'), 3, 3, 'bad_amount', 'USD', 'e@example.com', 1, 'hash-3705', '{"order_id":"ord-3705","amount":"bad_amount"}', timestamp('2026-08-07 05:05:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3706_currency.json', 'orders_api', 'COPY_INTO', 'evt-3706', 'ord-3706', timestamp('2026-08-07 05:05:00'), 3, 3, '606.00', 'BTC', 'f@example.com', 1, 'hash-3706', '{"order_id":"ord-3706","currency":"BTC"}', timestamp('2026-08-07 05:06:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3707_stale.json', 'orders_archive', 'COPY_INTO', 'evt-3707', 'ord-3707', timestamp('2026-07-01 05:00:00'), 3, 3, '707.00', 'USD', 'g@example.com', 1, 'hash-3707', '{"order_id":"ord-3707","event_ts":"2026-07-01"}', timestamp('2026-08-07 05:07:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3708_pii.json', 'orders_api', 'AUTO_LOADER', 'evt-3708', 'ord-3708', timestamp('2026-08-07 05:08:00'), 3, 3, '808.00', 'USD', 'h@example.com', 1, 'hash-3708', '{"order_id":"ord-3708","ssn":"123-45-6789"}', timestamp('2026-08-07 05:09:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3709_dup_a.json', 'orders_api', 'AUTO_LOADER', 'evt-3709a', 'ord-3709', timestamp('2026-08-07 05:10:00'), 3, 3, '909.00', 'EUR', 'i@example.com', 1, 'hash-3709a', '{"order_id":"ord-3709","amount":909.00}', timestamp('2026-08-07 05:11:00')),
# MAGIC   ('dbfs:/landing/day37/orders/order_3709_dup_b.json', 'orders_api', 'AUTO_LOADER', 'evt-3709b', 'ord-3709', timestamp('2026-08-07 05:12:00'), 3, 3, '910.00', 'EUR', 'j@example.com', 1, 'hash-3709b', '{"order_id":"ord-3709","amount":910.00}', timestamp('2026-08-07 05:13:00'));
# MAGIC
# MAGIC CREATE TABLE ingestion_helper_catalog_day37 (
# MAGIC   helper_name STRING,
# MAGIC   dataframe_api STRING,
# MAGIC   sql_equivalent STRING,
# MAGIC   production_use STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_helper_catalog_day37 VALUES
# MAGIC   ('required_non_null', 'F.col plus isNotNull plus trim/length', 'column IS NOT NULL AND length(trim(column)) > 0', 'Block rows missing keys needed for idempotent sinks.'),
# MAGIC   ('schema_version_match', 'F.col(left) == F.col(right)', 'schema_version = expected_schema_version', 'Block incompatible producer payloads.'),
# MAGIC   ('numeric_cast_range', 'cast plus between', 'try_cast(amount AS DECIMAL) BETWEEN min AND max', 'Block bad numeric strings and out-of-policy measures.'),
# MAGIC   ('allowed_values', 'isin', 'currency IN (...)', 'Block unsupported domain values.'),
# MAGIC   ('freshness_window', 'timestamp comparison', 'event_ts >= timestamp(...)', 'Block stale events unless a bounded backfill is approved.'),
# MAGIC   ('pii_blocklist_scan', 'lower plus rlike', 'lower(payload_json) RLIKE pattern', 'Block sensitive fields before publication.'),
# MAGIC   ('duplicate_business_key', 'groupBy plus count plus join', 'GROUP BY business_key HAVING count(*) > 1', 'Block ambiguous upserts and replay duplication.'),
# MAGIC   ('temp_view_bridge', 'createOrReplaceTempView', 'CREATE OR REPLACE TEMP VIEW', 'Reconcile DataFrame output with SQL checks.');
# MAGIC
# MAGIC SELECT count(*) AS raw_rows FROM ingestion_raw_events_day37
# MAGIC UNION ALL
# MAGIC SELECT count(*) AS helper_rows FROM ingestion_helper_catalog_day37;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 2 - Apply reusable PySpark validation helpers
# MAGIC
# MAGIC **Purpose:** Convert raw ingestion records into pass/fail decisions with reusable DataFrame expressions for keys, schema version, amount, currency, freshness, PII, and duplicates.
# MAGIC
# MAGIC **Expected result:** `ingestion_validation_decisions_day37` contains two publishable rows and eight quarantined rows.
# MAGIC
# MAGIC **Operational meaning:** PySpark validation helpers make ingestion gates repeatable and testable across COPY INTO, Auto Loader, and Lakeflow Connect landing tables.

# COMMAND ----------
from pyspark.sql import functions as F

spark.sql("USE SCHEMA de_learning")

raw_df = spark.table("ingestion_raw_events_day37")

def required_non_null(column_name):
    value = F.col(column_name)
    return value.isNotNull() & (F.length(F.trim(value.cast("string"))) > 0)

def matches_expected_version(actual_column, expected_column):
    return F.col(actual_column) == F.col(expected_column)

def decimal_between(column_name, lower_bound, upper_bound):
    value = F.col(column_name).cast("decimal(10,2)")
    return value.between(F.lit(lower_bound).cast("decimal(10,2)"), F.lit(upper_bound).cast("decimal(10,2)"))

def allowed_values(column_name, values):
    return F.col(column_name).isin(values)

business_key_counts_df = (
    raw_df
    .where(F.col("business_key").isNotNull())
    .groupBy("business_key")
    .agg(F.count("*").alias("business_key_count"))
)

source_file_counts_df = (
    raw_df
    .groupBy("source_file_path")
    .agg(F.count("*").alias("source_file_count"))
)

validated_df = (
    raw_df
    .join(business_key_counts_df, on="business_key", how="left")
    .join(source_file_counts_df, on="source_file_path", how="left")
    .withColumn("amount_decimal", F.col("amount_text").cast("decimal(10,2)"))
    .withColumn("business_key_present", required_non_null("business_key"))
    .withColumn(
        "schema_version_valid",
        F.coalesce(matches_expected_version("schema_version", "expected_schema_version"), F.lit(False)),
    )
    .withColumn("amount_valid", F.coalesce(decimal_between("amount_text", "0.01", "10000.00"), F.lit(False)))
    .withColumn("currency_allowed", F.coalesce(allowed_values("currency", ["USD", "EUR", "INR"]), F.lit(False)))
    .withColumn(
        "event_fresh_enough",
        F.coalesce(F.col("event_ts") >= F.to_timestamp(F.lit("2026-08-01 00:00:00")), F.lit(False)),
    )
    .withColumn("pii_clear", F.coalesce(~F.lower(F.col("payload_json")).rlike("ssn|card_number|passport"), F.lit(False)))
    .withColumn("business_key_duplicate", F.coalesce(F.col("business_key_count") > 1, F.lit(False)))
    .withColumn("source_file_duplicate", F.coalesce(F.col("source_file_count") > 1, F.lit(False)))
    .withColumn(
        "record_gate_action",
        F.when(
            F.col("business_key_present")
            & F.col("schema_version_valid")
            & F.col("amount_valid")
            & F.col("currency_allowed")
            & F.col("event_fresh_enough")
            & F.col("pii_clear")
            & ~F.col("business_key_duplicate")
            & ~F.col("source_file_duplicate"),
            F.lit("PUBLISH"),
        ).otherwise(F.lit("QUARANTINE")),
    )
    .withColumn(
        "primary_failure_reason",
        F.when(~F.col("business_key_present"), F.lit("MISSING_BUSINESS_KEY"))
        .when(~F.col("schema_version_valid"), F.lit("SCHEMA_VERSION_MISMATCH"))
        .when(~F.col("amount_valid"), F.lit("INVALID_AMOUNT"))
        .when(~F.col("currency_allowed"), F.lit("UNSUPPORTED_CURRENCY"))
        .when(~F.col("event_fresh_enough"), F.lit("STALE_EVENT"))
        .when(~F.col("pii_clear"), F.lit("PII_DETECTED"))
        .when(F.col("business_key_duplicate"), F.lit("DUPLICATE_BUSINESS_KEY"))
        .when(F.col("source_file_duplicate"), F.lit("DUPLICATE_SOURCE_FILE"))
        .otherwise(F.lit("PUBLISHABLE")),
    )
    .withColumn(
        "validation_score",
        F.lit(100)
        - F.when(~F.col("business_key_present"), F.lit(20)).otherwise(F.lit(0))
        - F.when(~F.col("schema_version_valid"), F.lit(15)).otherwise(F.lit(0))
        - F.when(~F.col("amount_valid"), F.lit(15)).otherwise(F.lit(0))
        - F.when(~F.col("currency_allowed"), F.lit(10)).otherwise(F.lit(0))
        - F.when(~F.col("event_fresh_enough"), F.lit(10)).otherwise(F.lit(0))
        - F.when(~F.col("pii_clear"), F.lit(25)).otherwise(F.lit(0))
        - F.when(F.col("business_key_duplicate"), F.lit(20)).otherwise(F.lit(0))
        - F.when(F.col("source_file_duplicate"), F.lit(20)).otherwise(F.lit(0)),
    )
    .select(
        "source_file_path",
        "source_system",
        "ingestion_method",
        "event_id",
        "business_key",
        "event_ts",
        "expected_schema_version",
        "schema_version",
        "amount_text",
        "amount_decimal",
        "currency",
        "customer_email",
        "business_key_count",
        "source_file_count",
        "business_key_present",
        "schema_version_valid",
        "amount_valid",
        "currency_allowed",
        "event_fresh_enough",
        "pii_clear",
        "business_key_duplicate",
        "source_file_duplicate",
        "record_gate_action",
        "primary_failure_reason",
        "validation_score",
    )
)

validated_df.createOrReplaceTempView("ingestion_validation_decisions_view_day37")

spark.sql("""
CREATE OR REPLACE TABLE ingestion_validation_decisions_day37
USING DELTA
AS
SELECT *
FROM ingestion_validation_decisions_view_day37
""")

display(validated_df.orderBy("source_file_path"))

# COMMAND ----------
# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC - `raw_df` represents the raw Day 37 ingestion batch before validation.
# MAGIC - `validated_df` represents one publish or quarantine decision per source record.
# MAGIC - SQL equivalent: `CASE WHEN business_key IS NOT NULL AND schema_version = expected_schema_version AND ... THEN 'PUBLISH' ELSE 'QUARANTINE' END`.
# MAGIC - `F.col("business_key")` creates a column expression evaluated across the cluster.
# MAGIC - `withColumn` adds validation flags and does not mutate `raw_df`.
# MAGIC - `groupBy(...).agg(F.count("*"))` finds duplicate keys before joining counts back to the row grain.
# MAGIC - `createOrReplaceTempView` bridges DataFrame output back into SQL for reconciliation.
# MAGIC - The transformations are lazy until `spark.sql(...)` and `display(...)` trigger execution.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 3 - Reconcile PySpark output against a SQL equivalent
# MAGIC
# MAGIC **Purpose:** Build the same decision table in SQL and prove the PySpark helper output is deterministic.
# MAGIC
# MAGIC **Expected result:** Ten reconciliation rows match between SQL and PySpark.
# MAGIC
# MAGIC **Operational meaning:** Production helper libraries should have SQL-equivalent tests so teams can debug behavior without guessing what the DataFrame chain did.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE ingestion_sql_expected_day37
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH business_key_counts AS (
# MAGIC   SELECT business_key, count(*) AS business_key_count
# MAGIC   FROM ingestion_raw_events_day37
# MAGIC   WHERE business_key IS NOT NULL
# MAGIC   GROUP BY business_key
# MAGIC ),
# MAGIC source_file_counts AS (
# MAGIC   SELECT source_file_path, count(*) AS source_file_count
# MAGIC   FROM ingestion_raw_events_day37
# MAGIC   GROUP BY source_file_path
# MAGIC ),
# MAGIC flags AS (
# MAGIC   SELECT
# MAGIC     r.source_file_path,
# MAGIC     r.event_id,
# MAGIC     r.business_key,
# MAGIC     coalesce(r.schema_version = r.expected_schema_version, false) AS schema_version_valid,
# MAGIC     coalesce(try_cast(r.amount_text AS DECIMAL(10,2)) BETWEEN CAST('0.01' AS DECIMAL(10,2)) AND CAST('10000.00' AS DECIMAL(10,2)), false) AS amount_valid,
# MAGIC     coalesce(r.currency IN ('USD', 'EUR', 'INR'), false) AS currency_allowed,
# MAGIC     coalesce(r.event_ts >= timestamp('2026-08-01 00:00:00'), false) AS event_fresh_enough,
# MAGIC     coalesce(NOT (lower(r.payload_json) RLIKE 'ssn|card_number|passport'), false) AS pii_clear,
# MAGIC     coalesce(b.business_key_count > 1, false) AS business_key_duplicate,
# MAGIC     coalesce(s.source_file_count > 1, false) AS source_file_duplicate,
# MAGIC     r.business_key IS NOT NULL AND length(trim(r.business_key)) > 0 AS business_key_present
# MAGIC   FROM ingestion_raw_events_day37 r
# MAGIC   LEFT JOIN business_key_counts b
# MAGIC     ON r.business_key = b.business_key
# MAGIC   LEFT JOIN source_file_counts s
# MAGIC     ON r.source_file_path = s.source_file_path
# MAGIC )
# MAGIC SELECT
# MAGIC   source_file_path,
# MAGIC   event_id,
# MAGIC   CASE
# MAGIC     WHEN business_key_present
# MAGIC       AND schema_version_valid
# MAGIC       AND amount_valid
# MAGIC       AND currency_allowed
# MAGIC       AND event_fresh_enough
# MAGIC       AND pii_clear
# MAGIC       AND NOT business_key_duplicate
# MAGIC       AND NOT source_file_duplicate
# MAGIC     THEN 'PUBLISH'
# MAGIC     ELSE 'QUARANTINE'
# MAGIC   END AS expected_gate_action,
# MAGIC   CASE
# MAGIC     WHEN NOT business_key_present THEN 'MISSING_BUSINESS_KEY'
# MAGIC     WHEN NOT schema_version_valid THEN 'SCHEMA_VERSION_MISMATCH'
# MAGIC     WHEN NOT amount_valid THEN 'INVALID_AMOUNT'
# MAGIC     WHEN NOT currency_allowed THEN 'UNSUPPORTED_CURRENCY'
# MAGIC     WHEN NOT event_fresh_enough THEN 'STALE_EVENT'
# MAGIC     WHEN NOT pii_clear THEN 'PII_DETECTED'
# MAGIC     WHEN business_key_duplicate THEN 'DUPLICATE_BUSINESS_KEY'
# MAGIC     WHEN source_file_duplicate THEN 'DUPLICATE_SOURCE_FILE'
# MAGIC     ELSE 'PUBLISHABLE'
# MAGIC   END AS expected_failure_reason
# MAGIC FROM flags;
# MAGIC
# MAGIC CREATE TABLE ingestion_validation_reconciliation_day37
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   p.source_file_path,
# MAGIC   p.event_id,
# MAGIC   p.record_gate_action AS pyspark_gate_action,
# MAGIC   s.expected_gate_action AS sql_gate_action,
# MAGIC   p.primary_failure_reason AS pyspark_failure_reason,
# MAGIC   s.expected_failure_reason AS sql_failure_reason,
# MAGIC   CASE
# MAGIC     WHEN p.record_gate_action = s.expected_gate_action
# MAGIC       AND p.primary_failure_reason = s.expected_failure_reason
# MAGIC     THEN 'MATCH'
# MAGIC     ELSE 'REVISIT'
# MAGIC   END AS reconciliation_status
# MAGIC FROM ingestion_validation_decisions_day37 p
# MAGIC JOIN ingestion_sql_expected_day37 s
# MAGIC   ON p.source_file_path = s.source_file_path;
# MAGIC
# MAGIC SELECT reconciliation_status, count(*) AS rows
# MAGIC FROM ingestion_validation_reconciliation_day37
# MAGIC GROUP BY reconciliation_status;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 4 - Summarize failures with PySpark
# MAGIC
# MAGIC **Purpose:** Use `where`, `select`, `groupBy`, and `agg` to create summary and failure-review tables from the validation decisions.
# MAGIC
# MAGIC **Expected result:** A summary table by gate action and failure reason, plus eight failure rows for quarantine review.
# MAGIC
# MAGIC **Operational meaning:** Validation helpers should leave compact operational evidence for dashboards, Lakeflow Jobs alerts, and incident review.

# COMMAND ----------
from pyspark.sql import functions as F

decisions_day37_df = spark.table("ingestion_validation_decisions_day37")

summary_df = (
    decisions_day37_df
    .groupBy("record_gate_action", "primary_failure_reason")
    .agg(
        F.count("*").alias("record_count"),
        F.min("validation_score").alias("min_validation_score"),
        F.max("validation_score").alias("max_validation_score"),
    )
    .orderBy("record_gate_action", "primary_failure_reason")
)

failures_df = (
    decisions_day37_df
    .where(F.col("record_gate_action") == "QUARANTINE")
    .select(
        "source_file_path",
        "source_system",
        "ingestion_method",
        "event_id",
        "business_key",
        "primary_failure_reason",
        "validation_score",
    )
    .orderBy("primary_failure_reason", "source_file_path")
)

summary_df.createOrReplaceTempView("ingestion_validation_summary_view_day37")
failures_df.createOrReplaceTempView("ingestion_validation_failures_view_day37")

spark.sql("""
CREATE OR REPLACE TABLE ingestion_validation_summary_day37
USING DELTA
AS
SELECT *
FROM ingestion_validation_summary_view_day37
""")

spark.sql("""
CREATE OR REPLACE TABLE ingestion_validation_failures_day37
USING DELTA
AS
SELECT *
FROM ingestion_validation_failures_view_day37
""")

display(summary_df)

# COMMAND ----------
# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC - `decisions_day37_df` represents the persisted validation decision table from Lab Part 2.
# MAGIC - `summary_df` changes the grain from one row per record to one row per gate action and failure reason.
# MAGIC - `failures_df` keeps only quarantined records for operational review.
# MAGIC - SQL equivalent: `SELECT record_gate_action, primary_failure_reason, count(*) FROM decisions GROUP BY 1,2`.
# MAGIC - `where` filters rows; `select` chooses columns; `groupBy` plus `agg` performs grouped aggregation.
# MAGIC - `createOrReplaceTempView` makes the DataFrame result queryable from SQL before saving it as a Delta table.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 5 - Create remediation actions
# MAGIC
# MAGIC **Purpose:** Map each failure reason to the right producer, governance, replay, or pipeline action.
# MAGIC
# MAGIC **Expected result:** Eight remediation rows exist, one for each validation failure type.
# MAGIC
# MAGIC **Operational meaning:** Quarantine without a next action becomes a backlog. Each failure class needs an owner, evidence, and release condition.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE TABLE ingestion_remediation_plan_day37 (
# MAGIC   failure_reason STRING,
# MAGIC   owner STRING,
# MAGIC   immediate_action STRING,
# MAGIC   evidence_required STRING,
# MAGIC   release_condition STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC INSERT INTO ingestion_remediation_plan_day37 VALUES
# MAGIC   ('MISSING_BUSINESS_KEY', 'source owner', 'Reject or replay with a valid business key.', 'Raw payload, source file path, and producer ticket.', 'Every publishable row has an idempotency key.'),
# MAGIC   ('SCHEMA_VERSION_MISMATCH', 'contract owner', 'Hold publication and review schema compatibility.', 'Expected and actual schema version plus Delta history.', 'Contract compatibility is approved or producer replays compatible payloads.'),
# MAGIC   ('INVALID_AMOUNT', 'source owner', 'Quarantine malformed numeric values.', 'Raw amount text and parser failure sample.', 'Amount parses to decimal and passes range policy.'),
# MAGIC   ('UNSUPPORTED_CURRENCY', 'data product owner', 'Reject unsupported domain value or update approved value set.', 'Currency value, producer system, and approval record.', 'Currency is in the allowed set or contract is updated.'),
# MAGIC   ('STALE_EVENT', 'pipeline owner', 'Treat as bounded backfill, not regular incremental data.', 'Input manifest, event time window, and replay approval.', 'Backfill window is approved and target reconciliation passes.'),
# MAGIC   ('PII_DETECTED', 'governance owner', 'Pause publication and classify sensitive field.', 'Quarantine sample, PII tag, mask or row-filter decision.', 'Masking, row filtering, or deny policy is approved before publish.'),
# MAGIC   ('DUPLICATE_BUSINESS_KEY', 'pipeline owner', 'Stop ambiguous upsert and request dedupe policy.', 'Duplicate keys, source files, and event ordering evidence.', 'One winning record is selected by deterministic ordering or producer replays.'),
# MAGIC   ('DUPLICATE_SOURCE_FILE', 'platform owner', 'Hold repeated source file and inspect discovery state.', 'Source file path count, checkpoint, and loaded-file audit.', 'File identity and checkpoint state prove exactly-once behavior.');
# MAGIC
# MAGIC SELECT owner, count(*) AS remediation_types
# MAGIC FROM ingestion_remediation_plan_day37
# MAGIC GROUP BY owner
# MAGIC ORDER BY owner;

# COMMAND ----------
# MAGIC %md
# MAGIC ## Lab Part 6 - Run final checks
# MAGIC
# MAGIC **Purpose:** Verify the validation helper catalog, raw records, decisions, SQL reconciliation, summaries, failures, and remediation mappings.
# MAGIC
# MAGIC **Expected result:** The final check view returns only `PASS` rows.
# MAGIC
# MAGIC **Operational meaning:** The lab leaves a reusable validation-helper pattern: build flags in PySpark, reconcile with SQL, summarize failures, and attach remediation actions.

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW ingestion_validation_final_checks_day37 AS
# MAGIC SELECT
# MAGIC   'raw_rows' AS check_name,
# MAGIC   count(*) AS observed_value,
# MAGIC   10 AS expected_value,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END AS status
# MAGIC FROM ingestion_raw_events_day37
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'helper_catalog_rows',
# MAGIC   count(*),
# MAGIC   8,
# MAGIC   CASE WHEN count(*) = 8 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_helper_catalog_day37
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'decision_rows',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_validation_decisions_day37
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'publish_rows',
# MAGIC   count(*),
# MAGIC   2,
# MAGIC   CASE WHEN count(*) = 2 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_validation_decisions_day37
# MAGIC WHERE record_gate_action = 'PUBLISH'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'quarantine_rows',
# MAGIC   count(*),
# MAGIC   8,
# MAGIC   CASE WHEN count(*) = 8 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_validation_decisions_day37
# MAGIC WHERE record_gate_action = 'QUARANTINE'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'sql_reconciliation_matches',
# MAGIC   count(*),
# MAGIC   10,
# MAGIC   CASE WHEN count(*) = 10 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_validation_reconciliation_day37
# MAGIC WHERE reconciliation_status = 'MATCH'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'failure_rows',
# MAGIC   count(*),
# MAGIC   8,
# MAGIC   CASE WHEN count(*) = 8 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_validation_failures_day37
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'remediation_rows',
# MAGIC   count(*),
# MAGIC   8,
# MAGIC   CASE WHEN count(*) = 8 THEN 'PASS' ELSE 'FAIL' END
# MAGIC FROM ingestion_remediation_plan_day37;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ingestion_validation_final_checks_day37
# MAGIC ORDER BY check_name;
