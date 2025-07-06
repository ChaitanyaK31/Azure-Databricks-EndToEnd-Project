# Databricks notebook source
# MAGIC %md
# MAGIC # **DLT Pipeline**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Streaming Table**

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# Exceptions
my_rules = {
    "rule1":"product_id IS NOT NULL",
    "rule2":"product_name IS NOT NULL"
}

# COMMAND ----------

# Streaming Table
@dlt.table()
@dlt.expect_all_or_drop(my_rules)

def DimProducts_stage():
    df = spark.readStream.table("projectdb_catalog.silver.products")
    return df

# COMMAND ----------

# Streaming View
@dlt.view()

def DimProducts_view():
    df = spark.readstream.table(projectdb_catalog.live.Dimproducts_stage)
    return df

# COMMAND ----------

#DimProducts
dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
  target = "DimProducts",
  source = "DimProducts_view",
  keys = ["product_id"],
  stored_as_scd_type = 2
)

# COMMAND ----------

