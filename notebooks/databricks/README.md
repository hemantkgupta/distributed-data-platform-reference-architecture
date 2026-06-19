# Databricks Notebooks

Save exported Databricks notebooks here as source `.py` files.

Naming convention:

```text
day_XX_checkpoint_name.py
```

Use Databricks source format so the file can round-trip back into a workspace:

```python
# Databricks notebook source

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM de_learning.orders_delta_day2;
```

Pure SQL that is meant to be reused across notebooks belongs in `../../sql/databricks/`.
