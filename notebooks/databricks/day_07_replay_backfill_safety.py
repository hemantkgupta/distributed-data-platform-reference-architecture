# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 7 - Replay And Backfill Safety
# MAGIC
# MAGIC Goal: prove why daily reruns and backfills need deterministic inputs, run records, and idempotent publication.
# MAGIC
# MAGIC Lab flow:
# MAGIC
# MAGIC - Show how naive append duplicates gold metrics on rerun.
# MAGIC - Build a deterministic candidate with an input fingerprint.
# MAGIC - Publish with `MERGE` so replay does not duplicate rows.
# MAGIC - Detect when the same batch id points at changed input.
# MAGIC - Reprocess a corrected snapshot with a new batch id.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create A Stable Source Snapshot And Empty Targets
# MAGIC
# MAGIC Purpose: create a silver-like source table and separate naive/idempotent gold targets.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_silver_snapshot_day7 (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   current_amount DECIMAL(10,2),
# MAGIC   current_status STRING,
# MAGIC   last_source_lsn BIGINT,
# MAGIC   source_snapshot_id STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'silver',
# MAGIC   'grain' = 'one row per current order',
# MAGIC   'lesson' = 'day7 replay and backfill safety'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_silver_snapshot_day7 VALUES
# MAGIC   (1, 101, DATE '2026-06-01', CAST(250.00 AS DECIMAL(10,2)), 'completed', 1001, 'src-snap-001'),
# MAGIC   (2, 102, DATE '2026-06-01', CAST(140.00 AS DECIMAL(10,2)), 'completed', 1003, 'src-snap-001'),
# MAGIC   (3, 103, DATE '2026-06-02', CAST(400.00 AS DECIMAL(10,2)), 'completed', 1004, 'src-snap-001'),
# MAGIC   (4, 104, DATE '2026-06-02', CAST(90.00 AS DECIMAL(10,2)), 'completed', 1006, 'src-snap-001'),
# MAGIC   (5, 105, DATE '2026-06-03', CAST(300.00 AS DECIMAL(10,2)), 'pending', 1007, 'src-snap-001');

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_gold_naive_append_day7 (
# MAGIC   run_id STRING,
# MAGIC   metric_date DATE,
# MAGIC   order_count BIGINT,
# MAGIC   completed_revenue DECIMAL(10,2),
# MAGIC   loaded_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'publication_style' = 'naive append'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_gold_idempotent_day7 (
# MAGIC   metric_date DATE,
# MAGIC   order_count BIGINT,
# MAGIC   completed_revenue DECIMAL(10,2),
# MAGIC   batch_id STRING,
# MAGIC   input_fingerprint STRING,
# MAGIC   source_row_count BIGINT,
# MAGIC   published_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'medallion_layer' = 'gold',
# MAGIC   'grain' = 'one row per metric_date',
# MAGIC   'publication_style' = 'idempotent merge'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE backfill_runs_day7 (
# MAGIC   batch_id STRING,
# MAGIC   run_id STRING,
# MAGIC   input_start_date DATE,
# MAGIC   input_end_date DATE,
# MAGIC   input_fingerprint STRING,
# MAGIC   source_row_count BIGINT,
# MAGIC   decision STRING,
# MAGIC   created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'purpose' = 'records replay and backfill attempts'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver_snapshot_day7 ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - 5 source rows.
# MAGIC - Empty naive gold, idempotent gold, and run registry tables.
# MAGIC
# MAGIC Operational meaning: a backfill should name its input window and target, not just run an anonymous query.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Demonstrate The Bad Daily Rerun
# MAGIC
# MAGIC Purpose: show how append-only publication duplicates metrics when the same day is rerun.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_gold_naive_append_day7
# MAGIC SELECT
# MAGIC   'run-001' AS run_id,
# MAGIC   order_date AS metric_date,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END) AS completed_revenue,
# MAGIC   TIMESTAMP '2026-06-23 06:00:00' AS loaded_at
# MAGIC FROM orders_silver_snapshot_day7
# MAGIC WHERE order_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-02'
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_gold_naive_append_day7
# MAGIC SELECT
# MAGIC   'run-002' AS run_id,
# MAGIC   order_date AS metric_date,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE CAST(0.00 AS DECIMAL(10,2)) END) AS completed_revenue,
# MAGIC   TIMESTAMP '2026-06-23 06:05:00' AS loaded_at
# MAGIC FROM orders_silver_snapshot_day7
# MAGIC WHERE order_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-02'
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_naive_append_day7 ORDER BY metric_date, run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   metric_date,
# MAGIC   COUNT(*) AS rows_per_metric_date,
# MAGIC   SUM(order_count) AS apparent_order_count,
# MAGIC   SUM(completed_revenue) AS apparent_completed_revenue
# MAGIC FROM orders_gold_naive_append_day7
# MAGIC GROUP BY metric_date
# MAGIC ORDER BY metric_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `orders_gold_naive_append_day7` has 4 rows: 2 dates x 2 runs.
# MAGIC - `2026-06-01` appears to have order count `4` and revenue `780.00`.
# MAGIC - `2026-06-02` appears to have order count `4` and revenue `980.00`.
# MAGIC
# MAGIC Operational meaning: appending a daily aggregate is not replay-safe. A retry can look like real business growth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Build A Deterministic Candidate With PySpark
# MAGIC
# MAGIC Purpose: produce the gold candidate plus an input fingerprint for the exact source rows used.

# COMMAND ----------

from pyspark.sql import functions as F


def build_gold_candidate(batch_id, run_id, start_date, end_date):
    source_df = spark.table("de_learning.orders_silver_snapshot_day7")

    batch_input_df = source_df.where(
        (F.col("order_date") >= F.to_date(F.lit(start_date)))
        & (F.col("order_date") <= F.to_date(F.lit(end_date)))
    )

    row_token_df = batch_input_df.select(
        F.concat_ws(
            "|",
            F.col("order_id").cast("string"),
            F.col("customer_id").cast("string"),
            F.col("order_date").cast("string"),
            F.col("current_amount").cast("string"),
            F.col("current_status"),
            F.col("last_source_lsn").cast("string"),
            F.col("source_snapshot_id"),
        ).alias("row_token")
    )

    fingerprint_df = row_token_df.agg(
        F.count("*").cast("bigint").alias("source_row_count"),
        F.sha2(F.concat_ws("||", F.array_sort(F.collect_list("row_token"))), 256).alias("input_fingerprint"),
    )

    candidate_df = (
        batch_input_df
        .groupBy("order_date")
        .agg(
            F.count("*").cast("bigint").alias("order_count"),
            F.sum(
                F.when(
                    F.col("current_status") == F.lit("completed"),
                    F.col("current_amount"),
                ).otherwise(F.lit(0).cast("decimal(10,2)"))
            ).alias("completed_revenue"),
        )
        .withColumnRenamed("order_date", "metric_date")
        .crossJoin(fingerprint_df)
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("published_at", F.current_timestamp())
        .select(
            "metric_date",
            "order_count",
            "completed_revenue",
            "batch_id",
            "input_fingerprint",
            "source_row_count",
            "published_at",
        )
    )

    run_context_df = (
        fingerprint_df
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("input_start_date", F.to_date(F.lit(start_date)))
        .withColumn("input_end_date", F.to_date(F.lit(end_date)))
        .withColumn("decision", F.lit("READY_TO_PUBLISH"))
        .withColumn("created_at", F.current_timestamp())
        .select(
            "batch_id",
            "run_id",
            "input_start_date",
            "input_end_date",
            "input_fingerprint",
            "source_row_count",
            "decision",
            "created_at",
        )
    )

    candidate_df.createOrReplaceTempView("orders_gold_candidate_day7")
    run_context_df.createOrReplaceTempView("backfill_run_context_day7")
    return candidate_df, run_context_df


candidate_df, run_context_df = build_gold_candidate(
    "bf-2026-06-01-to-02-r1",
    "run-003",
    "2026-06-01",
    "2026-06-02",
)

display(candidate_df.orderBy("metric_date"))
display(run_context_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### PySpark Notes
# MAGIC
# MAGIC SQL equivalent shape:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_date AS metric_date,
# MAGIC        COUNT(*) AS order_count,
# MAGIC        SUM(CASE WHEN current_status = 'completed' THEN current_amount ELSE 0 END) AS completed_revenue
# MAGIC FROM orders_silver_snapshot_day7
# MAGIC WHERE order_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-02'
# MAGIC GROUP BY order_date;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.table(...)` reads the Delta table as a DataFrame.
# MAGIC - `where(...)` is SQL `WHERE`.
# MAGIC - `groupBy(...).agg(...)` is SQL `GROUP BY`.
# MAGIC - `F.concat_ws`, `F.array_sort`, `F.collect_list`, and `F.sha2` build a deterministic fingerprint from the input rows.
# MAGIC - DataFrames are lazy until `display(...)`, a write, or another action runs.

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Candidate has 2 rows: `2026-06-01` revenue `390.00`, `2026-06-02` revenue `490.00`.
# MAGIC - Run context has 1 row with a stable `input_fingerprint`.
# MAGIC
# MAGIC Operational meaning: an input fingerprint lets you distinguish "same replay" from "same batch id but changed input."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Register And Publish With Idempotent MERGE
# MAGIC
# MAGIC Purpose: publish the candidate so reruns update the same date keys instead of appending duplicates.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO backfill_runs_day7 AS target
# MAGIC USING backfill_run_context_day7 AS source
# MAGIC ON target.run_id = source.run_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_start_date = source.input_start_date,
# MAGIC   target.input_end_date = source.input_end_date,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.decision = source.decision,
# MAGIC   target.created_at = source.created_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   batch_id, run_id, input_start_date, input_end_date,
# MAGIC   input_fingerprint, source_row_count, decision, created_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.batch_id, source.run_id, source.input_start_date, source.input_end_date,
# MAGIC   source.input_fingerprint, source.source_row_count, source.decision, source.created_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_gold_idempotent_day7 AS target
# MAGIC USING orders_gold_candidate_day7 AS source
# MAGIC ON target.metric_date = source.metric_date
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.order_count = source.order_count,
# MAGIC   target.completed_revenue = source.completed_revenue,
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.published_at = source.published_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   metric_date, order_count, completed_revenue, batch_id,
# MAGIC   input_fingerprint, source_row_count, published_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.metric_date, source.order_count, source.completed_revenue, source.batch_id,
# MAGIC   source.input_fingerprint, source.source_row_count, source.published_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_idempotent_day7 ORDER BY metric_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Now rerun the same publish MERGE once more. The table should still have only 2 rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_gold_idempotent_day7 AS target
# MAGIC USING orders_gold_candidate_day7 AS source
# MAGIC ON target.metric_date = source.metric_date
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.order_count = source.order_count,
# MAGIC   target.completed_revenue = source.completed_revenue,
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.published_at = source.published_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   metric_date, order_count, completed_revenue, batch_id,
# MAGIC   input_fingerprint, source_row_count, published_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.metric_date, source.order_count, source.completed_revenue, source.batch_id,
# MAGIC   source.input_fingerprint, source.source_row_count, source.published_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS row_count,
# MAGIC   SUM(order_count) AS total_orders,
# MAGIC   SUM(completed_revenue) AS total_completed_revenue
# MAGIC FROM orders_gold_idempotent_day7;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - First publish creates 2 rows.
# MAGIC - Replaying the same MERGE still leaves 2 rows.
# MAGIC - Totals are `4` orders and `880.00` completed revenue.
# MAGIC
# MAGIC Operational meaning: idempotent publication uses the output grain key, here `metric_date`, to converge after retry.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Detect Same Batch Id With Changed Input
# MAGIC
# MAGIC Purpose: simulate a corrected source snapshot and block reuse of the old batch id.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_silver_snapshot_day7
# MAGIC SET
# MAGIC   current_amount = CAST(150.00 AS DECIMAL(10,2)),
# MAGIC   last_source_lsn = 1010,
# MAGIC   source_snapshot_id = 'src-snap-002'
# MAGIC WHERE order_id = 2;

# COMMAND ----------

candidate_df, run_context_df = build_gold_candidate(
    "bf-2026-06-01-to-02-r1",
    "run-004",
    "2026-06-01",
    "2026-06-02",
)

display(candidate_df.orderBy("metric_date"))
display(run_context_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rerun_guard_day7 AS
# MAGIC WITH prior_approved AS (
# MAGIC   SELECT batch_id, input_fingerprint
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       batch_id,
# MAGIC       input_fingerprint,
# MAGIC       created_at,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY batch_id ORDER BY created_at DESC) AS rn
# MAGIC     FROM backfill_runs_day7
# MAGIC     WHERE decision IN ('READY_TO_PUBLISH', 'REPLAY_SAME_INPUT')
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   current.batch_id,
# MAGIC   current.run_id,
# MAGIC   current.input_fingerprint,
# MAGIC   prior.input_fingerprint AS prior_input_fingerprint,
# MAGIC   CASE
# MAGIC     WHEN prior.batch_id IS NULL THEN 'READY_TO_PUBLISH'
# MAGIC     WHEN prior.input_fingerprint = current.input_fingerprint THEN 'REPLAY_SAME_INPUT'
# MAGIC     ELSE 'BLOCKED_SAME_BATCH_CHANGED_INPUT'
# MAGIC   END AS decision
# MAGIC FROM backfill_run_context_day7 current
# MAGIC LEFT JOIN prior_approved prior
# MAGIC   ON current.batch_id = prior.batch_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rerun_guard_day7;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO backfill_runs_day7 AS target
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     c.batch_id,
# MAGIC     c.run_id,
# MAGIC     c.input_start_date,
# MAGIC     c.input_end_date,
# MAGIC     c.input_fingerprint,
# MAGIC     c.source_row_count,
# MAGIC     g.decision,
# MAGIC     c.created_at
# MAGIC   FROM backfill_run_context_day7 c
# MAGIC   CROSS JOIN rerun_guard_day7 g
# MAGIC ) AS source
# MAGIC ON target.run_id = source.run_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_start_date = source.input_start_date,
# MAGIC   target.input_end_date = source.input_end_date,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.decision = source.decision,
# MAGIC   target.created_at = source.created_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   batch_id, run_id, input_start_date, input_end_date,
# MAGIC   input_fingerprint, source_row_count, decision, created_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.batch_id, source.run_id, source.input_start_date, source.input_end_date,
# MAGIC   source.input_fingerprint, source.source_row_count, source.decision, source.created_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_gold_idempotent_day7 AS target
# MAGIC USING (
# MAGIC   SELECT c.*
# MAGIC   FROM orders_gold_candidate_day7 c
# MAGIC   CROSS JOIN rerun_guard_day7 g
# MAGIC   WHERE g.decision IN ('READY_TO_PUBLISH', 'REPLAY_SAME_INPUT')
# MAGIC ) AS source
# MAGIC ON target.metric_date = source.metric_date
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.order_count = source.order_count,
# MAGIC   target.completed_revenue = source.completed_revenue,
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.published_at = source.published_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   metric_date, order_count, completed_revenue, batch_id,
# MAGIC   input_fingerprint, source_row_count, published_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.metric_date, source.order_count, source.completed_revenue, source.batch_id,
# MAGIC   source.input_fingerprint, source.source_row_count, source.published_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_idempotent_day7 ORDER BY metric_date;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `rerun_guard_day7` says `BLOCKED_SAME_BATCH_CHANGED_INPUT`.
# MAGIC - `orders_gold_idempotent_day7` still shows `2026-06-01` revenue `390.00`.
# MAGIC
# MAGIC Operational meaning: the same batch id must not silently mean two different input snapshots.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Publish The Correction With A New Batch Id
# MAGIC
# MAGIC Purpose: allow corrected input to publish only when it is named as a new backfill attempt.

# COMMAND ----------

candidate_df, run_context_df = build_gold_candidate(
    "bf-2026-06-01-to-02-r2",
    "run-005",
    "2026-06-01",
    "2026-06-02",
)

display(candidate_df.orderBy("metric_date"))
display(run_context_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO backfill_runs_day7 AS target
# MAGIC USING backfill_run_context_day7 AS source
# MAGIC ON target.run_id = source.run_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_start_date = source.input_start_date,
# MAGIC   target.input_end_date = source.input_end_date,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.decision = source.decision,
# MAGIC   target.created_at = source.created_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   batch_id, run_id, input_start_date, input_end_date,
# MAGIC   input_fingerprint, source_row_count, decision, created_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.batch_id, source.run_id, source.input_start_date, source.input_end_date,
# MAGIC   source.input_fingerprint, source.source_row_count, source.decision, source.created_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_gold_idempotent_day7 AS target
# MAGIC USING orders_gold_candidate_day7 AS source
# MAGIC ON target.metric_date = source.metric_date
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.order_count = source.order_count,
# MAGIC   target.completed_revenue = source.completed_revenue,
# MAGIC   target.batch_id = source.batch_id,
# MAGIC   target.input_fingerprint = source.input_fingerprint,
# MAGIC   target.source_row_count = source.source_row_count,
# MAGIC   target.published_at = source.published_at
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   metric_date, order_count, completed_revenue, batch_id,
# MAGIC   input_fingerprint, source_row_count, published_at
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.metric_date, source.order_count, source.completed_revenue, source.batch_id,
# MAGIC   source.input_fingerprint, source.source_row_count, source.published_at
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_gold_idempotent_day7 ORDER BY metric_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM backfill_runs_day7 ORDER BY created_at, run_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - `2026-06-01` revenue updates from `390.00` to `400.00`.
# MAGIC - `2026-06-02` remains `490.00`.
# MAGIC - Run registry records the blocked same-batch attempt and the approved new batch.
# MAGIC
# MAGIC Operational meaning: corrected backfills are allowed, but they need a new batch identity and an audit trail.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Final Operational Checks
# MAGIC
# MAGIC Purpose: verify grain, totals, run history, and Delta history.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT metric_date, COUNT(*) AS rows_per_metric_date
# MAGIC FROM orders_gold_idempotent_day7
# MAGIC GROUP BY metric_date
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   SUM(order_count) AS total_orders,
# MAGIC   SUM(completed_revenue) AS total_completed_revenue
# MAGIC FROM orders_gold_idempotent_day7;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   batch_id,
# MAGIC   run_id,
# MAGIC   input_start_date,
# MAGIC   input_end_date,
# MAGIC   source_row_count,
# MAGIC   decision
# MAGIC FROM backfill_runs_day7
# MAGIC ORDER BY created_at, run_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_gold_idempotent_day7;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected result:
# MAGIC
# MAGIC - Duplicate-grain query returns no rows.
# MAGIC - Final total revenue is `890.00`.
# MAGIC - Run registry explains `run-003`, blocked `run-004`, and approved `run-005`.
# MAGIC - Delta history shows table writes and merges.
# MAGIC
# MAGIC Operational meaning: production replay safety needs both data-state convergence and control-plane evidence.
