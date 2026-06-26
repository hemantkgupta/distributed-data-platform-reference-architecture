# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 9 - Schema Evolution And Compatibility
# MAGIC
# MAGIC Goal: evolve a Delta table schema safely, then block an incompatible candidate before publication.
# MAGIC
# MAGIC Lab flow:
# MAGIC
# MAGIC - Create a baseline order table and a contract column registry.
# MAGIC - Add a nullable column safely.
# MAGIC - Create one compatible candidate and one incompatible candidate.
# MAGIC - Use PySpark to evaluate schema compatibility.
# MAGIC - Publish only the compatible candidate.
# MAGIC - Use Delta history to explain what changed.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Baseline Table And Contract
# MAGIC
# MAGIC Purpose: create version 1 of an orders table plus the expected contract columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day9 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contract_name' = 'orders.current',
# MAGIC   'contract_version' = '1',
# MAGIC   'grain' = 'one row per current order'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_delta_day9 VALUES
# MAGIC   (1, 101, DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (2, 102, DATE '2026-06-01', CAST(155.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (3, 103, DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_contract_columns_day9 (
# MAGIC   column_name STRING,
# MAGIC   expected_type STRING,
# MAGIC   required BOOLEAN,
# MAGIC   compatibility_rule STRING,
# MAGIC   contract_version INT
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_contract_columns_day9 VALUES
# MAGIC   ('order_id', 'int', true, 'required_identity_column', 2),
# MAGIC   ('customer_id', 'int', true, 'required_dimension_column', 2),
# MAGIC   ('order_date', 'date', true, 'required_partition_dimension', 2),
# MAGIC   ('amount', 'decimal(10,2)', true, 'required_measure_column', 2),
# MAGIC   ('status', 'string', true, 'required_lifecycle_column', 2),
# MAGIC   ('sales_channel', 'string', false, 'nullable_additive_column', 2);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day9 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_contract_columns_day9 ORDER BY column_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_delta_day9;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_delta_day9` has 3 rows and 5 columns.
# MAGIC - Contract registry already describes version 2, where `sales_channel` is optional.
# MAGIC
# MAGIC Operational meaning: the contract is the compatibility target. The table is the current physical state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Add A Nullable Column Safely
# MAGIC
# MAGIC Purpose: evolve the table with a backward-compatible additive column.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_delta_day9
# MAGIC ADD COLUMNS (
# MAGIC   sales_channel STRING COMMENT 'Nullable sales channel added in contract v2'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_delta_day9 (
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status,
# MAGIC   sales_channel
# MAGIC )
# MAGIC VALUES
# MAGIC   (4, 104, DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'pending', 'web'),
# MAGIC   (5, 105, DATE '2026-06-03', CAST(300.00 AS DECIMAL(10,2)), 'completed', 'store');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day9 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_delta_day9;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Existing rows 1, 2, and 3 have `sales_channel = NULL`.
# MAGIC - New rows 4 and 5 can populate `sales_channel`.
# MAGIC
# MAGIC Operational meaning: adding a nullable column is usually backward-compatible because old readers can ignore it and old rows remain valid.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Create Compatible And Incompatible Candidates
# MAGIC
# MAGIC Purpose: stage one safe candidate and one unsafe candidate before publication.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_candidate_compatible_day9
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contract_name' = 'orders.current',
# MAGIC   'contract_version' = '2',
# MAGIC   'candidate_type' = 'compatible'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   amount,
# MAGIC   status,
# MAGIC   sales_channel
# MAGIC FROM orders_delta_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_candidate_incompatible_day9
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contract_name' = 'orders.current',
# MAGIC   'contract_version' = '2',
# MAGIC   'candidate_type' = 'incompatible'
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   order_date,
# MAGIC   CAST(amount AS STRING) AS amount,
# MAGIC   sales_channel
# MAGIC FROM orders_delta_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_candidate_compatible_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_candidate_incompatible_day9;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Compatible candidate has all contract columns with matching types.
# MAGIC - Incompatible candidate changes `amount` from `decimal(10,2)` to `string` and omits required `status`.
# MAGIC
# MAGIC Operational meaning: candidate tables are allowed to exist, but publication should be gated by compatibility checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Evaluate Schema Compatibility With PySpark
# MAGIC
# MAGIC Purpose: compare actual table schemas with the contract and produce pass/fail evidence.

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F


def schema_rows_for_table(table_name, candidate_name):
    fields = spark.table(f"de_learning.{table_name}").schema.fields
    return [
        Row(
            candidate_table=table_name,
            candidate_name=candidate_name,
            column_name=field.name,
            actual_type=field.dataType.simpleString(),
            actual_nullable=field.nullable,
        )
        for field in fields
    ]


def evaluate_schema(table_name, candidate_name):
    contract_df = spark.table("de_learning.orders_contract_columns_day9")
    actual_schema_df = spark.createDataFrame(schema_rows_for_table(table_name, candidate_name))

    return (
        contract_df
        .join(actual_schema_df, on="column_name", how="full_outer")
        .withColumn("candidate_table", F.coalesce(F.col("candidate_table"), F.lit(table_name)))
        .withColumn("candidate_name", F.coalesce(F.col("candidate_name"), F.lit(candidate_name)))
        .withColumn(
            "severity",
            F.when(F.col("required") == True, F.lit("BLOCKER"))
             .when(F.col("expected_type").isNull(), F.lit("WARN"))
             .otherwise(F.lit("INFO")),
        )
        .withColumn(
            "outcome",
            F.when(F.col("expected_type").isNull(), F.lit("WARN_EXTRA_COLUMN"))
             .when(F.col("actual_type").isNull() & (F.col("required") == True), F.lit("FAIL_MISSING_REQUIRED_COLUMN"))
             .when(F.col("actual_type").isNull(), F.lit("PASS_OPTIONAL_COLUMN_ABSENT"))
             .when(F.col("actual_type") != F.col("expected_type"), F.lit("FAIL_TYPE_MISMATCH"))
             .otherwise(F.lit("PASS")),
        )
        .withColumn(
            "compatible",
            ~F.col("outcome").isin("FAIL_MISSING_REQUIRED_COLUMN", "FAIL_TYPE_MISMATCH"),
        )
        .select(
            "candidate_name",
            "candidate_table",
            "column_name",
            "expected_type",
            "actual_type",
            "required",
            "severity",
            "outcome",
            "compatible",
        )
    )


schema_compatibility_df = (
    evaluate_schema("orders_candidate_compatible_day9", "compatible")
    .unionByName(evaluate_schema("orders_candidate_incompatible_day9", "incompatible"))
)

schema_compatibility_df.createOrReplaceTempView("schema_compatibility_results_day9")

display(schema_compatibility_df.orderBy("candidate_name", "column_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC This DataFrame represents schema compatibility evidence: one row per candidate column check.
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT contract.column_name,
# MAGIC        contract.expected_type,
# MAGIC        actual.actual_type,
# MAGIC        CASE
# MAGIC          WHEN actual.actual_type IS NULL AND contract.required THEN 'FAIL_MISSING_REQUIRED_COLUMN'
# MAGIC          WHEN actual.actual_type <> contract.expected_type THEN 'FAIL_TYPE_MISMATCH'
# MAGIC          ELSE 'PASS'
# MAGIC        END AS outcome
# MAGIC FROM contract
# MAGIC FULL OUTER JOIN actual_schema actual
# MAGIC   ON contract.column_name = actual.column_name;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.table(...).schema.fields` reads schema metadata, not table rows.
# MAGIC - `spark.createDataFrame(...)` turns Python `Row` objects into a DataFrame.
# MAGIC - `join(..., how="full_outer")` catches missing and extra columns.
# MAGIC - `F.when(...).otherwise(...)` is SQL `CASE WHEN`.
# MAGIC - No compatibility result is computed until `display(...)` or a later SQL write runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE schema_compatibility_evidence_day9
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM schema_compatibility_results_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM schema_compatibility_evidence_day9
# MAGIC ORDER BY candidate_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `compatible` candidate has all `PASS` outcomes.
# MAGIC - `incompatible` candidate has at least:
# MAGIC   - `amount` -> `FAIL_TYPE_MISMATCH`
# MAGIC   - `status` -> `FAIL_MISSING_REQUIRED_COLUMN`
# MAGIC
# MAGIC Operational meaning: schema compatibility should be evidence, not a human eyeballing `DESCRIBE` output.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Create Publication Decisions
# MAGIC
# MAGIC Purpose: summarize compatibility evidence into publish/block decisions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW schema_publication_decisions_day9 AS
# MAGIC SELECT
# MAGIC   candidate_name,
# MAGIC   candidate_table,
# MAGIC   SUM(CASE WHEN compatible = false AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS blocker_failures,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN compatible = false AND severity = 'BLOCKER' THEN 1 ELSE 0 END) = 0
# MAGIC     THEN 'READY_TO_PUBLISH'
# MAGIC     ELSE 'BLOCKED'
# MAGIC   END AS publication_decision
# MAGIC FROM schema_compatibility_evidence_day9
# MAGIC GROUP BY candidate_name, candidate_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE schema_publication_decisions_delta_day9
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM schema_publication_decisions_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM schema_publication_decisions_delta_day9
# MAGIC ORDER BY candidate_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `compatible` -> `READY_TO_PUBLISH`.
# MAGIC - `incompatible` -> `BLOCKED`.
# MAGIC
# MAGIC Operational meaning: publication should depend on gate output, not on whether a candidate table exists.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Publish Only The Compatible Candidate
# MAGIC
# MAGIC Purpose: expose the evolved table only when schema gates pass.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_published_day9
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'contract_name' = 'orders.current',
# MAGIC   'contract_version' = '2',
# MAGIC   'publication_status' = 'PUBLISHED',
# MAGIC   'schema_change' = 'added nullable sales_channel'
# MAGIC )
# MAGIC AS
# MAGIC SELECT c.*
# MAGIC FROM orders_candidate_compatible_day9 c
# MAGIC CROSS JOIN schema_publication_decisions_delta_day9 d
# MAGIC WHERE d.candidate_name = 'compatible'
# MAGIC   AND d.publication_decision = 'READY_TO_PUBLISH';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_published_day9 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS incompatible_published_rows
# MAGIC FROM orders_candidate_incompatible_day9 c
# MAGIC CROSS JOIN schema_publication_decisions_delta_day9 d
# MAGIC WHERE d.candidate_name = 'incompatible'
# MAGIC   AND d.publication_decision = 'READY_TO_PUBLISH';

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_published_day9` has 5 rows and includes `sales_channel`.
# MAGIC - `incompatible_published_rows = 0`.
# MAGIC
# MAGIC Operational meaning: compatible additive evolution is allowed; required-column removal and type changes are blocked.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Inspect Schema History
# MAGIC
# MAGIC Purpose: use Delta metadata to explain when and how the schema changed.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_published_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_published_day9;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   candidate_name,
# MAGIC   column_name,
# MAGIC   expected_type,
# MAGIC   actual_type,
# MAGIC   outcome
# MAGIC FROM schema_compatibility_evidence_day9
# MAGIC WHERE outcome <> 'PASS'
# MAGIC ORDER BY candidate_name, column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `DESCRIBE orders_published_day9` shows `sales_channel`.
# MAGIC - History shows table creation/alter/write operations.
# MAGIC - Compatibility evidence explains why the incompatible candidate was blocked.
# MAGIC
# MAGIC Operational meaning: Delta history tells you what changed; compatibility evidence tells you whether the change was safe.
