# Databricks notebook source
dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %run "../set-up/parameters"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 1 - Reading Calendar  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema   

# COMMAND ----------

Calendar_Df = spark.read.format("csv") \
                     .schema(Calendar_Schema)\
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/calendar.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 - Writing  Taxi Zone Parquet File 
# MAGIC 1. Partition by Service Zone  

# COMMAND ----------

Calendar_Df.write.format("parquet") \
                  .partitionBy("year_month") \
                  .mode("Overwrite") \
                  .parquet(f"{DB_PROCESSED}/calendar")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3 - Create Database
# MAGIC #####Please look into 01_Nyc_Trip_Taxi_Zone_Ingestion  Notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Creating Temp and Global views

# COMMAND ----------

Calendar_Df.createOrReplaceTempView("newyork_taxi.calendar")
Calendar_Df.createOrReplaceGlobalTempView("newyork_taxi.calendar_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Calendar_Df.write.format("parquet") \
                  .partitionBy("year_month") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.calendar_int")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED newyork_taxi.calendar_int

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Calendar_Df.write.format("parquet") \
                  .partitionBy("year_month") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/calendar_ext" ) \
                  .saveAsTable("newyork_taxi.calendar_ext")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED newyork_taxi.calendar_ext
