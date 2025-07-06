# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading from Source**

# COMMAND ----------

df = spark.sql("select * from projectdb_catalog.silver.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformation : Part 1**

# COMMAND ----------

## Removing Duplicates
df = df.dropDuplicates(['customer_id'])

# COMMAND ----------

## Dividing New vs Old Records

# when records are updated (old records)
if init_load_flag == 0:
    df_old = spark.sql("select DimCustomersKey, customer_id, create_date, update_date from projectdb_catalog.gold.DimCustomers")

# when records are new (new records)
else: 
    df_old = spark.sql("select 0 DimCustomersKey, 0 customer_id, 0 create_date, 0 update_date where 1=0")


# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomersKey", "DimCustomersKey_old")\
        .withColumnRenamed("customer_id", "customer_id_old")\
        .withColumnRenamed("create_date", "create_date_old")\
        .withColumnRenamed("update_date", "update_date_old")

# COMMAND ----------

df_join = df.join(df_old, df.customer_id == df_old.customer_id_old,'left')
df_join.limit(10).display()

# COMMAND ----------

## seperating new vs old records

# old records in df_join
df_old = df_join.filter(df_join.DimCustomersKey_old.isNotNull())
# df_old.display()

# new records in df_join
df_new = df_join.filter(df_join.DimCustomersKey_old.isNull())
# df_new.display()
# -------------------------------------------------------------------------------------------------

## Preparing df_old
# dropping all columns which are not required
df_old = df_old.drop("customer_id_old", "update_date_old")

# renaming "DimCustomersKey_old", "old_create_date" column to "create_date" and assigning dataype as timestamp
df_old = df_old.withColumnRenamed("DimCustomersKey_old", "DimCustomersKey")
df_old = df_old.withColumnRenamed("create_date_old", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))

# creating "update_date" column with current_timestamp
df_old = df_old.withColumn("update_date", current_timestamp())

df_old.display()
# -------------------------------------------------------------------------------------------------

## Preparing df_new
# dropping all columns which are not required
df_new = df_new.drop("DimCustomersKey_old","customer_id_old", "create_date_old", "update_date_old")

# adding max surrogate key and DimCustomersKey
if init_load_flag == 1:
    max_surrogate_key = 0

else:
    df_max_surkey = spark.sql("select max(DimCustomersKey) as max_surrogate_key from projectdb_catalog.gold.DimCustomers")
    max_surrogate_key = df_max_surkey.collect()[0]['max_surrogate_key']

df_new = df_new.withColumn("DimCustomersKey", monotonically_increasing_id()+lit(1))
df_new = df_new.withColumn("DimCustomersKey", lit(max_surrogate_key)+col("DimCustomersKey"))

# creating update_date, create_date columns with current_timestamp
df_new = df_new.withColumn("create_date", current_timestamp())
df_new = df_new.withColumn("update_date", current_timestamp())

df_new.display()


# COMMAND ----------

#Combining both the dataframes
df_final = df_old.unionByName(df_new)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Transformation : Part 2 (UPSERT)**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("projectdb_catalog.gold.DimCustomers"):
  delta_obj = DeltaTable.forPath(spark, "abfss://gold@projectdbstorageaccount.dfs.core.windows.net/DimCustomers")
  delta_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomersKey = src.DimCustomersKey")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
  df_final.write.mode("overwrite").format("delta")\
  .option("path","abfss://gold@projectdbstorageaccount.dfs.core.windows.net/DimCustomers")\
  .saveAsTable("projectdb_catalog.gold.DimCustomers")
  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from projectdb_catalog.gold.DimCustomers