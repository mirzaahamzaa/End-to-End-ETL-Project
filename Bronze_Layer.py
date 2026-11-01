# Databricks notebook source
# MAGIC %md
# MAGIC ### **DATA Capabilities**

# COMMAND ----------

dbutils.widgets.text("file_name","")

# COMMAND ----------

p_filename=dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading** 
# MAGIC

# COMMAND ----------

# DBTITLE 1,2
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", f"abfss://bronze@databricksete1122.dfs.core.windows.net/checkpoint_{p_filename}") \
    .load(f"abfss://source@databricksete1122.dfs.core.windows.net/{p_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DATA WRITING**

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation",f"abfss://bronze@databricksete1122.dfs.core.windows.net/checkpoint_{p_filename}")\
    .option("path",f"abfss://bronze@databricksete1122.dfs.core.windows.net/{p_filename}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

# DBTITLE 1,8
df = spark.read.format("parquet").load("abfss://bronze@databricksete1122.dfs.core.windows.net/products")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC