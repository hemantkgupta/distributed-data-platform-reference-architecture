# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 2 - Delta Operability, History, Time Travel, Schema Safety
# MAGIC
# MAGIC Theme: Delta tables as operationally reliable datasets.
# MAGIC
# MAGIC Objectives:
# MAGIC
# MAGIC - Understand Delta table versions.
# MAGIC - Use `DESCRIBE HISTORY` as an audit/debugging tool.
# MAGIC - Practice time travel.
# MAGIC - Inspect the schema exposed by a SQL table.
# MAGIC - Compare append, update, delete, and merge behavior.
# MAGIC - Answer operational questions: who changed this table, did row count change, and can I query before a bad write?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Create Baseline Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day2 AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 101, DATE '2026-06-01', CAST(250.00 AS DECIMAL(5,2)), 'completed'),
# MAGIC   (2, 102, DATE '2026-06-01', CAST(125.50 AS DECIMAL(5,2)), 'pending'),
# MAGIC   (3, 103, DATE '2026-06-02', CAST(400.00 AS DECIMAL(5,2)), 'completed')
# MAGIC AS t(order_id, customer_id, order_date, amount, status);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day2 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta_day2;

# COMMAND ----------

# MAGIC %md
# MAGIC Operational reading:
# MAGIC
# MAGIC - This baseline write creates a Delta commit.
# MAGIC - `DESCRIBE HISTORY` is the first answer to "who changed this table?"
# MAGIC - In a personal workspace, the user will usually be you; in production, this often points to a job, service principal, pipeline, or notebook user.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Generate Multiple Versions
# MAGIC
# MAGIC Every data-changing command creates a new Delta table version.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_delta_day2
# MAGIC VALUES (4, 104, DATE '2026-06-03', CAST(80.00 AS DECIMAL(5,2)), 'pending');

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_delta_day2
# MAGIC SET status = 'completed'
# MAGIC WHERE order_id = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_delta_day2
# MAGIC WHERE order_id = 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW orders_updates_day2 AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (4, 104, DATE '2026-06-03', CAST(85.00 AS DECIMAL(5,2)), 'completed'),
# MAGIC   (5, 105, DATE '2026-06-04', CAST(300.00 AS DECIMAL(5,2)), 'pending')
# MAGIC AS t(order_id, customer_id, order_date, amount, status);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO orders_delta_day2 AS target
# MAGIC USING orders_updates_day2 AS source
# MAGIC ON target.order_id = source.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   target.customer_id = source.customer_id,
# MAGIC   target.order_date = source.order_date,
# MAGIC   target.amount = source.amount,
# MAGIC   target.status = source.status
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   order_id, customer_id, order_date, amount, status
# MAGIC ) VALUES (
# MAGIC   source.order_id, source.customer_id, source.order_date, source.amount, source.status
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day2 ORDER BY order_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta_day2;

# COMMAND ----------

# MAGIC %md
# MAGIC Operational reading:
# MAGIC
# MAGIC - `operation` tells you whether the table changed through `CREATE OR REPLACE TABLE`, `WRITE`, `UPDATE`, `DELETE`, or `MERGE`.
# MAGIC - `operationMetrics` tells you useful impact details such as rows inserted, updated, deleted, copied, or output.
# MAGIC - This is why Delta is useful beyond Parquet: it gives you a transaction log, not just files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Ask The Operational Questions

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_delta_day2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS current_row_count
# MAGIC FROM orders_delta_day2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, COUNT(*) AS rows
# MAGIC FROM orders_delta_day2
# MAGIC GROUP BY status
# MAGIC ORDER BY status;

# COMMAND ----------

# MAGIC %md
# MAGIC Expected schema shape:
# MAGIC
# MAGIC ```text
# MAGIC col_name     data_type      comment
# MAGIC order_id     int            null
# MAGIC customer_id  int            null
# MAGIC order_date   date           null
# MAGIC amount       decimal(5,2)   null
# MAGIC status       string         null
# MAGIC ```
# MAGIC
# MAGIC Row-count question:
# MAGIC
# MAGIC - Baseline had 3 rows.
# MAGIC - After insert, update, delete, and merge, the table should have 4 rows.
# MAGIC - That change is expected because one row was deleted and two new order ids appeared across the write sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 4 - Save A Known Good Version Before A Bad Write
# MAGIC
# MAGIC This small PySpark cell captures the current Delta version into a Python variable. SQL alone can show the history, but Python makes the next time-travel query dynamic.

# COMMAND ----------

from pyspark.sql import functions as F

history_before_bad_write = spark.sql("DESCRIBE HISTORY orders_delta_day2")
version_before_bad_write = history_before_bad_write.agg(F.max("version").alias("version")).collect()[0]["version"]

print(f"Version before bad write: {version_before_bad_write}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Notes
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT MAX(version) AS version
# MAGIC FROM (DESCRIBE HISTORY orders_delta_day2);
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.sql("...")` runs SQL and returns a DataFrame.
# MAGIC - `.agg(F.max("version").alias("version"))` performs an aggregation like `SELECT MAX(version) AS version`.
# MAGIC - `.collect()` brings the small result back to the driver as Python data.
# MAGIC - Use `collect()` carefully in production; it is fine here because this returns one row.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 5 - Simulate A Bad Write

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO orders_delta_day2
# MAGIC VALUES (99, 999, DATE '2026-06-05', CAST(999.99 AS DECIMAL(5,2)), 'bad_write');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count_after_bad_write
# MAGIC FROM orders_delta_day2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day2 ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC Operational reading:
# MAGIC
# MAGIC - The row count changed after the bad write.
# MAGIC - The `status = 'bad_write'` row is visible in the current version.
# MAGIC - In production, you would decide between rollback/restore and forward-fix based on blast radius, downstream consumption, and governance rules.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 6 - Query The Table Before The Bad Write

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT *
        FROM orders_delta_day2 VERSION AS OF {version_before_bad_write}
        ORDER BY order_id
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Notes
# MAGIC
# MAGIC SQL equivalent if the saved version were `4`:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM orders_delta_day2 VERSION AS OF 4
# MAGIC ORDER BY order_id;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - The `f"""...{version_before_bad_write}..."""` string inserts the Python variable into the SQL text.
# MAGIC - `VERSION AS OF` is Delta time travel.
# MAGIC - This proves the table can still be queried as it existed before the bad write, as long as the old files/history have not been vacuumed away.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 7 - Schema Safety Check
# MAGIC
# MAGIC This cell intentionally tries to insert an extra column. We catch the failure so the notebook can continue.

# COMMAND ----------

try:
    spark.sql(
        """
        INSERT INTO orders_delta_day2
        SELECT
          100 AS order_id,
          1000 AS customer_id,
          DATE '2026-06-06' AS order_date,
          CAST(10.00 AS DECIMAL(5,2)) AS amount,
          'pending' AS status,
          'unexpected' AS extra_column
        """
    )
except Exception as error:
    print("Expected schema enforcement failure:")
    print(str(error)[:1000])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Observations
# MAGIC
# MAGIC - `DESCRIBE HISTORY` tells you who/what changed the table, when, and with which operation.
# MAGIC - `DESCRIBE orders_delta_day2` tells you the schema exposed to readers.
# MAGIC - Row count should change only when the business operation explains it.
# MAGIC - `VERSION AS OF` lets you query before the bad write.
# MAGIC - Delta gives you transaction history, time travel, schema enforcement, and safer mutation semantics beyond plain Parquet files.
