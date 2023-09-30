# Databricks notebook source
dbutils.widgets.text("DB_NAME" , "")
DB_NAME1 = dbutils.widgets.get("DB_NAME")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Using this notebook to load delta table via COPY INTO COMMAND via Python

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 1 Create DATABASE DELTA TABLE 

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

# MAGIC %md Step 1a Creating Delta Table with pyspark 

# COMMAND ----------

from delta.tables import * 

DeltaTable.createOrReplace(spark)  \
.tableName("DELTA.Rate_Code_Delta") \
.addColumn("rate_code_id", dataType="Int") \
.addColumn("rate_code", dataType="String")\
.location("/mnt/covidreportingdatalake6/delta/Rate_Code_Delta") \
.execute()

# COMMAND ----------

#%sql describe extended newyork_taxi.rate_code_ext

# COMMAND ----------

Rate_Code_Source_Df = spark.sql("Select * from newyork_taxi.rate_code_ext")

# COMMAND ----------

Rate_Code_Instance = DeltaTable.forName(spark,"DELTA.Rate_Code_Delta")

# COMMAND ----------

from delta.tables import *

Rate_Code_Instance.alias("target").merge(Rate_Code_Source_Df.alias("source"),"target.rate_code_id = source.rate_code_id")  \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll()\
  .execute()
