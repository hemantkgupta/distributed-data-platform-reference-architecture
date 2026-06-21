# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 5 - Publication State and Ownership
# MAGIC
# MAGIC Theme: consumers should bind to published versions, not whatever table a pipeline happened to write.
# MAGIC
# MAGIC Objectives:
# MAGIC
# MAGIC - Model a small data-product registry with owner, current version, and published table.
# MAGIC - Create publication requests for candidate tables.
# MAGIC - Validate owner, expected version, proposed version, and quality evidence.
# MAGIC - Block unsafe requests before consumers can see them.
# MAGIC - Publish only the request that passes all gates.
# MAGIC - Learn why publication is a state transition, not just a table write.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concept: Publication Is The Visibility Boundary
# MAGIC
# MAGIC A pipeline can write a table, but that does not mean consumers should use it.
# MAGIC
# MAGIC A production platform needs a durable decision record:
# MAGIC
# MAGIC - Who requested the change?
# MAGIC - Is the requester the owner?
# MAGIC - Which current version did the requester expect?
# MAGIC - Which candidate table is being proposed?
# MAGIC - Which quality checks passed or failed?
# MAGIC - Which version is currently published to consumers?
# MAGIC
# MAGIC The rule: consumers bind only to `PUBLISHED` state.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Current Published Version

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_published_v4_day5 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   current_amount DECIMAL(10,2),
# MAGIC   current_status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'publication_status' = 'PUBLISHED',
# MAGIC   'version' = '4'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_published_v4_day5 VALUES
# MAGIC   (1, 101, DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (2, 102, DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (3, 103, DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (4, 104, DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'completed');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE data_products_delta_day5 (
# MAGIC   product_id STRING,
# MAGIC   owner_email STRING,
# MAGIC   current_version INT,
# MAGIC   published_table STRING,
# MAGIC   publication_status STRING,
# MAGIC   last_publication_request_id STRING,
# MAGIC   published_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO data_products_delta_day5 VALUES
# MAGIC   (
# MAGIC     'orders_current',
# MAGIC     'orders-owner@example.com',
# MAGIC     4,
# MAGIC     'orders_published_v4_day5',
# MAGIC     'PUBLISHED',
# MAGIC     'bootstrap-v4',
# MAGIC     TIMESTAMP '2026-06-20 06:00:00'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_products_delta_day5;

# COMMAND ----------

# MAGIC %md
# MAGIC Operational reading:
# MAGIC
# MAGIC - `orders_published_v4_day5` is visible to consumers.
# MAGIC - Candidate writes are not visible just because they exist.
# MAGIC - The registry is the control-plane source of truth for what consumers should read.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Create Candidate Tables
# MAGIC
# MAGIC `orders_candidate_v5_day5` is a good candidate. `orders_candidate_bad_day5` has duplicate `order_id` and a null amount.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_candidate_v5_day5 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   current_amount DECIMAL(10,2),
# MAGIC   current_status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'candidate_version' = '5',
# MAGIC   'publication_status' = 'CANDIDATE'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_candidate_v5_day5 VALUES
# MAGIC   (1, 101, DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (2, 102, DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (3, 103, DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (4, 104, DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (5, 105, DATE '2026-06-03', CAST(300.00 AS DECIMAL(10,2)), 'pending');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_candidate_bad_day5 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   current_amount DECIMAL(10,2),
# MAGIC   current_status STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'candidate_version' = '5',
# MAGIC   'publication_status' = 'CANDIDATE'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_candidate_bad_day5 VALUES
# MAGIC   (1, 101, DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (2, 102, DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (4, 104, DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (4, 104, DATE '2026-06-02', CAST(95.00 AS DECIMAL(10,2)), 'completed'),
# MAGIC   (5, 105, DATE '2026-06-03', CAST(NULL AS DECIMAL(10,2)), 'pending');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Create Publication Requests
# MAGIC
# MAGIC Four requests propose a version 5 publication. Only one should be allowed through.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE publication_requests_delta_day5 (
# MAGIC   request_id STRING,
# MAGIC   product_id STRING,
# MAGIC   requested_by STRING,
# MAGIC   expected_current_version INT,
# MAGIC   proposed_version INT,
# MAGIC   target_table STRING,
# MAGIC   requested_at TIMESTAMP,
# MAGIC   request_status STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO publication_requests_delta_day5 VALUES
# MAGIC   ('pub-001', 'orders_current', 'orders-owner@example.com', 4, 5, 'orders_candidate_v5_day5', TIMESTAMP '2026-06-21 06:00:00', 'PROPOSED'),
# MAGIC   ('pub-002', 'orders_current', 'analyst@example.com', 4, 5, 'orders_candidate_v5_day5', TIMESTAMP '2026-06-21 06:01:00', 'PROPOSED'),
# MAGIC   ('pub-003', 'orders_current', 'orders-owner@example.com', 3, 5, 'orders_candidate_v5_day5', TIMESTAMP '2026-06-21 06:02:00', 'PROPOSED'),
# MAGIC   ('pub-004', 'orders_current', 'orders-owner@example.com', 4, 5, 'orders_candidate_bad_day5', TIMESTAMP '2026-06-21 06:03:00', 'PROPOSED');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM publication_requests_delta_day5 ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected:
# MAGIC
# MAGIC - `pub-001` should pass.
# MAGIC - `pub-002` should fail ownership.
# MAGIC - `pub-003` should fail expected-version check.
# MAGIC - `pub-004` should fail quality checks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Evaluate Publication Gates With PySpark
# MAGIC
# MAGIC We use PySpark because this combines table-driven metadata with per-candidate quality checks.

# COMMAND ----------

from pyspark.sql import functions as F

requests_df = spark.table("de_learning.publication_requests_delta_day5")
products_df = spark.table("de_learning.data_products_delta_day5")

request_context_df = (
    requests_df
    .join(products_df, on="product_id", how="left")
    .withColumn("owner_ok", F.col("requested_by") == F.col("owner_email"))
    .withColumn("expected_version_ok", F.col("expected_current_version") == F.col("current_version"))
    .withColumn("version_increment_ok", F.col("proposed_version") == F.col("current_version") + F.lit(1))
)

quality_rows = []

for request in request_context_df.select("request_id", "target_table").collect():
    request_id = request["request_id"]
    target_table = request["target_table"]
    candidate_df = spark.table(f"de_learning.{target_table}")

    row_count = candidate_df.count()
    duplicate_order_ids = (
        candidate_df
        .groupBy("order_id")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    null_amounts = candidate_df.where(F.col("current_amount").isNull()).count()

    quality_rows.append((request_id, "row_count_positive", "BLOCKER", "PASS" if row_count > 0 else "FAIL", str(row_count)))
    quality_rows.append((request_id, "unique_order_id", "BLOCKER", "PASS" if duplicate_order_ids == 0 else "FAIL", str(duplicate_order_ids)))
    quality_rows.append((request_id, "current_amount_not_null", "BLOCKER", "PASS" if null_amounts == 0 else "FAIL", str(null_amounts)))

quality_checks_df = spark.createDataFrame(
    quality_rows,
    ["request_id", "check_name", "severity", "outcome", "observed_value"],
)

quality_summary_df = (
    quality_checks_df
    .groupBy("request_id")
    .agg(
        F.sum(
            F.when(
                (F.col("severity") == "BLOCKER") & (F.col("outcome") == "FAIL"),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("blocker_failures")
    )
)

decision_df = (
    request_context_df
    .join(quality_summary_df, on="request_id", how="left")
    .fillna({"blocker_failures": 0})
    .withColumn(
        "decision_reason_raw",
        F.concat_ws(
            "; ",
            F.when(~F.col("owner_ok"), F.lit("requester is not product owner")),
            F.when(~F.col("expected_version_ok"), F.lit("expected_current_version is stale")),
            F.when(~F.col("version_increment_ok"), F.lit("proposed_version is not next version")),
            F.when(
                F.col("blocker_failures") > 0,
                F.concat(F.col("blocker_failures").cast("string"), F.lit(" blocking quality check(s) failed")),
            ),
        ),
    )
    .withColumn(
        "publication_decision",
        F.when(
            F.col("owner_ok")
            & F.col("expected_version_ok")
            & F.col("version_increment_ok")
            & (F.col("blocker_failures") == 0),
            F.lit("READY_TO_PUBLISH"),
        ).otherwise(F.lit("BLOCKED")),
    )
    .withColumn(
        "decision_reason",
        F.when(F.col("publication_decision") == "READY_TO_PUBLISH", F.lit("all publication checks passed"))
        .otherwise(F.col("decision_reason_raw")),
    )
    .select(
        "request_id",
        "product_id",
        "requested_by",
        "owner_email",
        "expected_current_version",
        "current_version",
        "proposed_version",
        "target_table",
        "owner_ok",
        "expected_version_ok",
        "version_increment_ok",
        "blocker_failures",
        "publication_decision",
        "decision_reason",
    )
)

quality_checks_df.createOrReplaceTempView("publication_quality_checks_day5")
decision_df.createOrReplaceTempView("publication_decisions_day5")

display(decision_df.orderBy("request_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Notes - Publication Gate Evaluation
# MAGIC
# MAGIC SQL equivalent for the structural part:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   r.*,
# MAGIC   p.owner_email,
# MAGIC   p.current_version,
# MAGIC   r.requested_by = p.owner_email AS owner_ok,
# MAGIC   r.expected_current_version = p.current_version AS expected_version_ok,
# MAGIC   r.proposed_version = p.current_version + 1 AS version_increment_ok
# MAGIC FROM publication_requests_delta_day5 r
# MAGIC JOIN data_products_delta_day5 p
# MAGIC   ON r.product_id = p.product_id;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.table(...)` reads a SQL table into a DataFrame.
# MAGIC - `.join(..., on="product_id", how="left")` is a SQL left join.
# MAGIC - `.withColumn(...)` adds boolean check columns.
# MAGIC - `F.col("requested_by") == F.col("owner_email")` compares two columns.
# MAGIC - The small `collect()` loop is acceptable here because there are only four requests; production systems would evaluate checks through a scalable validation service or query plan.
# MAGIC - `createOrReplaceTempView(...)` makes PySpark results queryable from later `%sql` cells.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Persist Evidence And Decisions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE publication_evidence_delta_day5
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM publication_quality_checks_day5;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE publication_decisions_delta_day5
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM publication_decisions_day5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM publication_evidence_delta_day5
# MAGIC ORDER BY request_id, check_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   request_id,
# MAGIC   requested_by,
# MAGIC   target_table,
# MAGIC   owner_ok,
# MAGIC   expected_version_ok,
# MAGIC   version_increment_ok,
# MAGIC   blocker_failures,
# MAGIC   publication_decision,
# MAGIC   decision_reason
# MAGIC FROM publication_decisions_delta_day5
# MAGIC ORDER BY request_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected decisions:
# MAGIC
# MAGIC ```text
# MAGIC pub-001 READY_TO_PUBLISH  all publication checks passed
# MAGIC pub-002 BLOCKED           requester is not product owner
# MAGIC pub-003 BLOCKED           expected_current_version is stale
# MAGIC pub-004 BLOCKED           2 blocking quality check(s) failed
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Publish Only The Approved Candidate
# MAGIC
# MAGIC The candidate table already exists, but consumers still should not bind to it directly. Publication creates or replaces the stable published table and updates the registry.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_published_day5
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'publication_status' = 'PUBLISHED',
# MAGIC   'published_from_request_id' = 'pub-001',
# MAGIC   'version' = '5'
# MAGIC )
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM orders_candidate_v5_day5;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO data_products_delta_day5 AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     'orders_current' AS product_id,
# MAGIC     5 AS current_version,
# MAGIC     'orders_published_day5' AS published_table,
# MAGIC     'PUBLISHED' AS publication_status,
# MAGIC     'pub-001' AS last_publication_request_id,
# MAGIC     TIMESTAMP '2026-06-21 06:10:00' AS published_at
# MAGIC ) AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.current_version = source.current_version,
# MAGIC   target.published_table = source.published_table,
# MAGIC   target.publication_status = source.publication_status,
# MAGIC   target.last_publication_request_id = source.last_publication_request_id,
# MAGIC   target.published_at = source.published_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM data_products_delta_day5;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_published_day5 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY data_products_delta_day5;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_published_day5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Observations
# MAGIC
# MAGIC - Four proposed requests exist, but only one is ready to publish.
# MAGIC - The non-owner request is blocked even though its target table is good.
# MAGIC - The stale expected-version request is blocked to prevent lost updates.
# MAGIC - The bad candidate request is blocked because duplicate order ids and null amounts violate quality gates.
# MAGIC - Consumers should use the product registry's `published_table` and `current_version`, not the existence of candidate tables.
# MAGIC
# MAGIC Principal takeaway: publication is an atomic visibility decision. It turns validation evidence plus owner approval into a stable consumer-facing version.
