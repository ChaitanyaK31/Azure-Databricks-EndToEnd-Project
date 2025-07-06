# Databricks notebook source
df = spark.read.table("projectdb_catalog.bronze.regions")
df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/regions")

# COMMAND ----------

### Data Checkig in Silver
spark.read.format("delta").load("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/regions").display()
spark.read.format("delta").load("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/orders").display()
spark.read.format("delta").load("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/customers").display()
spark.read.format("delta").load("abfss://silver@projectdbstorageaccount.dfs.core.windows.net/products").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS projectdb_catalog.silver.regions
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@projectdbstorageaccount.dfs.core.windows.net/regions'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projectdb_catalog.silver.regions