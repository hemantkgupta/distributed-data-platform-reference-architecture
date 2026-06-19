# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 1 - DataFrame, Delta Table, SQL Table
# MAGIC
# MAGIC Theme: understand the difference between a Spark DataFrame, a Delta table, and a SQL table name in the catalog.
# MAGIC
# MAGIC Objectives:
# MAGIC
# MAGIC - Create a small Spark DataFrame from Python data.
# MAGIC - Register the DataFrame as a temporary SQL view.
# MAGIC - Persist the rows as a Delta table.
# MAGIC - Query the same data through SQL.
# MAGIC - Inspect table metadata and Delta history.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS de_learning;
# MAGIC USE SCHEMA de_learning;

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark Basics For This Notebook
# MAGIC
# MAGIC SQL mental model:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_id, customer_id, amount
# MAGIC FROM some_rows;
# MAGIC ```
# MAGIC
# MAGIC PySpark mental model:
# MAGIC
# MAGIC ```python
# MAGIC df.select("order_id", "customer_id", "amount")
# MAGIC ```
# MAGIC
# MAGIC A DataFrame is a distributed table-shaped object controlled from Python. Most methods build a query plan; actions such as `display`, `count`, and table writes execute it.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

orders_schema = T.StructType(
    [
        T.StructField("order_id", T.IntegerType(), False),
        T.StructField("customer_id", T.IntegerType(), False),
        T.StructField("order_date", T.StringType(), False),
        T.StructField("amount", T.DoubleType(), False),
        T.StructField("status", T.StringType(), False),
    ]
)

orders_df = spark.createDataFrame(
    [
        (1, 101, "2026-06-01", 250.00, "completed"),
        (2, 102, "2026-06-01", 125.50, "pending"),
        (3, 103, "2026-06-02", 400.00, "completed"),
    ],
    schema=orders_schema,
)

orders_df = (
    orders_df
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("amount", F.col("amount").cast("decimal(10,2)"))
)

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What The PySpark Above Means
# MAGIC
# MAGIC SQL equivalent:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   order_id,
# MAGIC   customer_id,
# MAGIC   TO_DATE(order_date) AS order_date,
# MAGIC   CAST(amount AS DECIMAL(10,2)) AS amount,
# MAGIC   status
# MAGIC FROM inline_rows;
# MAGIC ```
# MAGIC
# MAGIC Notes:
# MAGIC
# MAGIC - `spark.createDataFrame(...)` creates a DataFrame from local Python rows.
# MAGIC - `T.StructType(...)` defines the schema explicitly, similar to a `CREATE TABLE` column list.
# MAGIC - `.withColumn("order_date", ...)` replaces `order_date` with a parsed date column.
# MAGIC - `F.col("amount")` means "the column named amount."
# MAGIC - `.cast("decimal(10,2)")` is the PySpark version of SQL `CAST`.

# COMMAND ----------

orders_df.createOrReplaceTempView("orders_dataframe_day1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 1 - Query A DataFrame Through A Temporary SQL View
# MAGIC
# MAGIC The DataFrame is not a durable table. `createOrReplaceTempView` gives SQL a temporary session name for the DataFrame.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, order_date, amount, status
# MAGIC FROM orders_dataframe_day1
# MAGIC ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 2 - Persist The Rows As A Delta Table
# MAGIC
# MAGIC Now we create a named SQL table backed by Delta. The table name is in the catalog; the storage format is Delta.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE orders_delta_day1
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT order_id, customer_id, order_date, amount, status
# MAGIC FROM orders_dataframe_day1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_delta_day1 ORDER BY order_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Part 3 - Inspect The SQL Table And Delta Metadata
# MAGIC
# MAGIC `DESCRIBE` answers what schema the SQL table exposes. `DESCRIBE HISTORY` answers what operations changed the Delta table.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE orders_delta_day1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_delta_day1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL orders_delta_day1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected Observations
# MAGIC
# MAGIC - `orders_df` is a Python variable pointing at a Spark DataFrame plan.
# MAGIC - `orders_dataframe_day1` is only a temporary SQL view over that DataFrame.
# MAGIC - `orders_delta_day1` is a durable SQL table backed by Delta.
# MAGIC - `DESCRIBE orders_delta_day1` shows the exposed columns and types.
# MAGIC - `DESCRIBE HISTORY orders_delta_day1` shows the table operation log.
# MAGIC
# MAGIC Principal takeaway: in Databricks, SQL tables are catalog objects, DataFrames are programmatic plans, and Delta is the storage/transaction layer that makes table changes operationally traceable.
