# Databricks notebook source
dbutils.widgets.text("DB_NAME" , "")
DB_NAME1 = dbutils.widgets.get("DB_NAME")

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
.tableName("DELTA.Calendar_Delta") \
.addColumn("date_key", dataType="INT") \
.addColumn("date", dataType="DATE")\
.addColumn("year", dataType="INT")\
.addColumn("month", dataType="INT")\
.addColumn("day", dataType="INT")\
.addColumn("day_name", dataType="String")\
.addColumn("day_of_year", dataType="INT")\
.addColumn("week_of_month", dataType="INT") \
.addColumn("week_of_year", dataType="String") \
.addColumn("month_name", dataType="String")\
.addColumn("year_week", dataType="INT")\
.addColumn("year_month", dataType="INT") \
.location("/mnt/covidreportingdatalake6/delta/Calendar_Delta") \
.execute()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 2 Create DF from Table Created in 107 notebook

# COMMAND ----------

Calendar_Source_Df = spark.sql("SELECT * FROM newyork_taxi.calendar_ext")

# COMMAND ----------

# MAGIC %md Step 3 Delta Instance is used to update merge table, to view data convert to toDF()

# COMMAND ----------

from delta.tables import *
Calendar_Target_Df= DeltaTable.forPath(spark ,"/mnt/covidreportingdatalake6/delta/Calendar_Delta")
#Calendar_Target_Df = deltaInstance1.toDF()

# COMMAND ----------

# MAGIC %md Step 4 Merge Source data to Delta using Delkta Instance 

# COMMAND ----------

from delta.tables import *

Calendar_Target_Df.alias("target").merge(Calendar_Source_Df.alias("source"),"target.date_key = source.date_key")  \
  .whenMatchedUpdateAll() \
  .whenNotMatchedInsertAll()\
  .execute()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Use below code if you want to update couple of fields 

# COMMAND ----------

# from delta.tables import *

# deltaInstance1.alias("target").merge(Calendar_Source_Df.alias("source"),"target.date_key = source.date_key") \
# .whenMatchedUpdate(set = {
# "date"		        :"date",
# "year"		        :"year",
# "month"		        :"month",

# }) \
# .whenNotMatchedInsert(values={
# "date_key"          :"date_key",
# "date"		        :"date",

# "year_month"		:"year_month"
# }).execute()
