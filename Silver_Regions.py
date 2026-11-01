# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("parquet")\
    .load("abfss://source@databricksete1122.dfs.core.windows.net/regions")

# COMMAND ----------

df.display()

# COMMAND ----------

# DBTITLE 1,Cell 4
df.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://silver@databricksete1122.dfs.core.windows.net/regions")\
    .save()

# COMMAND ----------

df = spark.read.format("delta")\
    .load("abfss://silver@databricksete1122.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@databricksete1122.dfs.core.windows.net/regions'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM databricks_cata.silver.regions_silver