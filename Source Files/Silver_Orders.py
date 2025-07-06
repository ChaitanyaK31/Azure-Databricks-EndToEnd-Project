# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/orders")
df = df.drop("_rescued_data")
display(df)

# COMMAND ----------

df = df.withColumn("order_date", to_timestamp(col("order_date")))
df.display()

# COMMAND ----------

df = df.withColumn("year",year(col("order_date")))
df.display()

# COMMAND ----------

df1 = df.withColumn("dense_rank_flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1 = df1.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1 = df1.withColumn("row_number_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS projectdb_catalog.silver.orders
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@projectdbstorageaccount.dfs.core.windows.net/orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projectdb_catalog.silver.orders

# COMMAND ----------

