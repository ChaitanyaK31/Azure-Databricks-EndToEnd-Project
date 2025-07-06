# Databricks notebook source
# MAGIC %md
# MAGIC ### **FACT ORDERS**

# COMMAND ----------

df = spark.sql("select * from projectdb_catalog.silver.orders")
df.display()

# COMMAND ----------

# Dimension DataFrame
df_dimcus = spark.sql("select DimCustomersKey, customer_id as DimCustomerId from projectdb_catalog.gold.dimcustomers")
df_dimpro = spark.sql("select product_id as DimProductId, product_id as DimProductKey from projectdb_catalog.gold.dimproducts")

# COMMAND ----------

# Fact DataFrame
df_fact = df.join("df_dimcus", df.customer_id == df_dimcus.DimCustomerId, how="left").join("df_dimpro", df.product_id == df_dimpro.DimProductId, how="left")

df_factorders = df_fact.drop("DimCustomerId", "DimProductKey","DimCustomersKey","DimProductId")
df_factorders.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformation : UPSERT**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("projectdb_catalog.gold.FactOrders"):
  delta_obj = DeltaTable.forPath(spark, "abfss://gold@projectdbstorageaccount.dfs.core.windows.net/FactOrders")
  delta_obj.alias("trg").merge(df_factorders.alias("src"), "trg.order_id = src.order_id")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
      
else:
  df_factorders.write.mode("overwrite").format("delta")\
    .option("path","abfss://gold@projectdbstorageaccount.dfs.core.windows.net/FactOrders")\
    .saveAsTable("projectdb_catalog.gold.FactOrders")
