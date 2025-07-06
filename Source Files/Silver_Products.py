# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format("parquet").load("abfss://bronze@projectdbstorageaccount.dfs.core.windows.net/products")
df = df.drop("_rescued_data")
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION projectdb_catalog.bronze.discount_function(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price * 0.90
# MAGIC

# COMMAND ----------

### SQL Query
# %sql
# SELECT product_id, price, projectdb_catalog.bronze.discount_function(price) 
# FROM products

### Python Query
df.withColumn('discounted_price', expr('projectdb_catalog.bronze.discount_function(price)')).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION projectdb_catalog.bronze.discount_function(p_brand STRING)
# MAGIC RETURNS STRING
# MAGIC LANGUAGE PYTHON
# MAGIC AS 
# MAGIC $$ 
# MAGIC     return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, projectdb_catalog.bronze.discount_function(brand) as brand_upper
# MAGIC FROM products

# COMMAND ----------

df.write.format("delta").mode("overwrite")\
    .option("path", "abfss://silver@projectdbstorageaccount.dfs.core.windows.net/products")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Writing**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS projectdb_catalog.silver.Products
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@projectdbstorageaccount.dfs.core.windows.net/Products'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projectdb_catalog.silver.products