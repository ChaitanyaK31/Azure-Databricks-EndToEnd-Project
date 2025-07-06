# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dynamic Way**

# COMMAND ----------

dbutils.widgets.text("file_name", "")
p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", f"abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://source@projectdbstorageaccount.dfs.core.windows.net/{p_file_name}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path",f"abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

df = spark.read.format("parquet")\
    .load(f"abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/{p_file_name}")

display(df)

# COMMAND ----------



# COMMAND ----------

