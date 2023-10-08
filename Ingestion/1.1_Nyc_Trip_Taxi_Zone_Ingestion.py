# Databricks notebook source
# MAGIC %md 
# MAGIC #*INGEST TAXI ZONE FILE*

# COMMAND ----------

dbutils.widgets.text("table_name" , "")
tbl_name = dbutils.widgets.get("table_name")

# COMMAND ----------

# MAGIC %run "../set-up/schema"

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 1 - Reading File from Taxi Zone File ADLS

# COMMAND ----------

# Schema is stored in set-up/Schema"
from pyspark.sql.functions import col
Taxi_Zone_Df = spark.read.format("csv") \
                     .option("InferSchema" , True)\
                     .option("Header" , True)\
                     .load(f"{DB_RAW}/raw/taxi_zone.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 1 a - Reading Taxi Zone File Without Header from ADLS 
# MAGIC 1. Read File  
# MAGIC 1. Declare Schema   

# COMMAND ----------

# Schema is stored in set-up/Schema"
Taxi_Zone_Df = spark.read.format("csv") \
                     .schema(Taxi_Zone_Schema)\
                     .load(f"{DB_RAW}/raw/taxi_zone_without_header.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 2 - Writing  Taxi Zone Parquet File 
# MAGIC 1. Partition by Service Zone  

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .parquet(f"{DB_PROCESSED}/Taxi_Zone")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3 - Create Database
# MAGIC 1. USE %sql to convert python to sql   

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3a - Create Database
# MAGIC 1. Create Database  

# COMMAND ----------

# %sql Create  Database NEWYORK_TAXI

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3b - Create Database
# MAGIC 1. Show Databases

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3c - Change Database
# MAGIC 1. Change Database 

# COMMAND ----------

# %sql 
# USE newyork_taxi;

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 3d - Show Current Database
# MAGIC 1. Change Database 

# COMMAND ----------

# %sql 
# SELECT CURRENT_DATABASE()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 4 - Creating Temp and Global views

# COMMAND ----------

Taxi_Zone_Df.createOrReplaceTempView("newyork_taxi.taxi_zone")
Taxi_Zone_Df.createOrReplaceGlobalTempView("hive_metastore.newyork_taxi.autoloader1.taxi_zone_g")

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 5 - Storing data as Hive Managed Table

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .option("comments","This is a internal Table ") \
                  .saveAsTable("newyork_taxi.taxi_zone_int")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.taxi_zone_int

# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 6 - Storing data as Hive EXTERNAL Table

# COMMAND ----------

Taxi_Zone_Df.write.format("parquet") \
                  .partitionBy("Service_Zone") \
                  .mode("Overwrite") \
                  .option("comments","This is a EXTERNAL Table ") \
                  .option("path", f"{DB_PROCESSED}/taxi_zone_ext" ) \
                  .saveAsTable("newyork_taxi.taxi_zone_ext")

# COMMAND ----------

# %sql 
# DESCRIBE EXTENDED newyork_taxi.taxi_zone_ext
