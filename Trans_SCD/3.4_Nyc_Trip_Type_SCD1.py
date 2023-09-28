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

DeltaTable.createOrReplace(spark)  \
.tableName("DELTA.Trip_Type_Delta") \
.addColumn("trip_type_desc", dataType="String") \
.addColumn("trip_type", dataType="Int")\
.location("/mnt/covidreportingdatalake6/delta/Trip_Type_Delta") \
.execute()

# COMMAND ----------

# MAGIC %sql describe extended newyork_taxi.trip_type_ext

# COMMAND ----------

# MAGIC %sql 
# MAGIC COPY INTO DELTA.Trip_Type_Delta
# MAGIC FROM '/mnt/covidreportingdatalake6/processed/trip_type_ext'
# MAGIC FILEFORMAT = parquet
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM DELTA.Trip_Type_Delta
