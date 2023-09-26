# Databricks notebook source
dbutils.widgets.text("p_file_date" , "")
file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %run "../set-up/parameters"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Reading Trip Type  File from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema  
# MAGIC 1. Header is true as the file has header
# MAGIC 1. This file is a tab seprated file

# COMMAND ----------

Trip_Type_Df = spark.read.format("csv") \
                     .schema(Trip_Type_Schema)\
                     .option("sep" , "\t") \
                     .option("Header" , True) \
                     .load(f"{DB_RAW}/raw/trip_type.tsv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 2 - Writing  Taxi Zone Parquet File 
# MAGIC 1. Partition by Service Zone  

# COMMAND ----------

Trip_Type_Df.write.format("parquet") \
                  .partitionBy("trip_type") \
                  .mode("Overwrite") \
                  .parquet(f"{DB_PROCESSED}/trip_type")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 3 - Create Database
# MAGIC #####Please look into 01_Nyc_Trip_Taxi_Zone_Ingestion  Notebook 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 4 - Creating Temp and Global views

# COMMAND ----------

Trip_Type_Df.createOrReplaceTempView("newyork_taxi.trip_type")
Trip_Type_Df.createOrReplaceGlobalTempView("newyork_taxi.trip_type_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Trip_Type_Df.write.format("parquet") \
                  .partitionBy("trip_type") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.trip_type_int")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED newyork_taxi.trip_type_int

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Trip_Type_Df.write.format("parquet") \
                  .partitionBy("trip_type") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/trip_type_ext" ) \
                  .saveAsTable("newyork_taxi.trip_type_ext")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE EXTENDED newyork_taxi.trip_type_ext

# COMMAND ----------

# MAGIC %sql select * from newyork_taxi.trip_type_ext
