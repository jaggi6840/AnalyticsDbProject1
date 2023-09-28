# Databricks notebook source
dbutils.widgets.text("DB_NAME" , "")
DB_NAME1 = dbutils.widgets.get("DB_NAME")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Using this notebook to load delta table via COPY INTO COMMAND via Python

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### STEP 1 Create DATABASE DELTA TABLE 

# COMMAND ----------

# %sql
# create  TABLE IF NOT EXISTS  DELTA.Calendar_Delta (
# date_key int,
# date date, 
# year  int,
# month int,
# day int,
# day_name VARCHAR(50),
# day_of_year int,
# week_of_month int, 
# week_of_year VARCHAR(50),
# month_name VARCHAR(50),
# year_week int,
# year_month int
# )
# USING delta
# LOCATION '/mnt/covidreportingdatalake6/delta/Calendar_Delta'

# COMMAND ----------

# MAGIC %md STEP 1a Creating Delta Table with pyspark 

# COMMAND ----------

from delta.tables import * 

DeltaTable.createIfNotExists(spark)  \
.tableName("DELTA.Payment_Type_Delta") \
.addColumn("sub_type", dataType="Int") \
.addColumn("payment_type", dataType="String")\
.addColumn("value", dataType="String")\
.location("/mnt/covidreportingdatalake6/delta/Payment_Type_Delta") \
.execute()

# COMMAND ----------

Payment_Type_Source_Df = spark.sql("Select * from newyork_taxi.payment_type_ext")

# COMMAND ----------

Payment_Type_Instance = DeltaTable.forName(spark,"DELTA.Payment_Type_Delta")

# COMMAND ----------

from delta.tables import *

Payment_Type_Instance.alias("target").merge(Payment_Type_Source_Df.alias("source"),"target.payment_type = source.payment_type")  \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll()\
  .execute()

# COMMAND ----------

# MAGIC %sql SELECT * FROM DELTA.Payment_Type_Delta
