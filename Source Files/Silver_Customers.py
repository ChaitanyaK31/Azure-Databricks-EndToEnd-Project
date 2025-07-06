# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformation**

# COMMAND ----------

df = df.drop("_rescued_data")
df= df.withColumn("domain",split(col("email"),"@")[1])
df.display()

# COMMAND ----------

df.groupBy("domain").agg(count("customer_id").alias("total_customer")).orderBy(desc("total_customer"))

df_gmail = df.filter(col("domain")=="gmail.com")
df_gmail.display()
time.sleep(5)

df_yahoo = df.filter(col("domain")=="yahoo.com")
df_yahoo.display()
time.sleep(5)

df_hotmail = df.filter(col("domain")=="hotmail.com")
df_hotmail.display()

# COMMAND ----------

df = df.withColumn("full_name",concat(col("first_name").cast("string"),lit(" "),col("last_name").cast("string")))
df = df.drop("first_name","last_name")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Create Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS projectdb_catalog.silver.customers
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@projectdbstorageaccount.dfs.core.windows.net/customers'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projectdb_catalog.silver.customers